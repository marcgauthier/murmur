package originsignatures_test

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/origin"
	"github.com/marcgauthier/murmur/replication"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
	"github.com/marcgauthier/murmur/transport"
	"github.com/quic-go/quic-go"
)

func schemaConfig() *db.SchemaConfig {
	return &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{Name: "signed_rows", Columns: []schema.ColumnSchema{{Name: "id", Type: schema.ColBlob}, {Name: "name", Type: schema.ColText, Nullable: true}}}}}
}

func TestForwardedOriginSurvivesOfflineOriginAndHostileRelay(t *testing.T) {
	c := harness.NewCluster(t, harness.ClusterOptions{Name: "origin-signatures", NumNodes: 3, AwaitUnlock: true, ManualPeers: true, Schema: schemaConfig(), TrustedSnapshotSourcesByNode: map[int][]int{2: {}}})
	if err := c.AddPeer(0, 1); err != nil {
		t.Fatal(err)
	}
	if err := c.AddPeer(1, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.ExecSQL(0, "INSERT INTO signed_rows (id,name) VALUES (?,?)", fmt.Sprintf("%032x", 1), "from-A"); err != nil {
		t.Fatal(err)
	}
	waitRows(t, c, 1, 1)
	c.StopNode(0)
	// Read an actual locally committed signature from the stopped origin.
	registry, _ := origin.NewKeyRegistry(nil)
	for _, n := range c.Nodes {
		if err := registry.Add(n.NodeID, n.OriginKey.Public().(ed25519.PublicKey)); err != nil {
			t.Fatal(err)
		}
	}
	storageKey, err := hex.DecodeString(c.Nodes[0].KeyHex)
	if err != nil {
		t.Fatal(err)
	}
	local, err := db.Open(context.Background(), db.Config{Path: c.Nodes[0].PebbleDir, NodeID: c.Nodes[0].NodeID, DBID: c.DBID, OriginSigning: db.OriginSigningConfig{PrivateKey: c.Nodes[0].OriginKey, TrustedKeys: registry}, Schema: *schemaConfig(), Encryption: db.EncryptionConfig{Key: storageKey, KeyID: "remote-unlock-key"}})
	if err != nil {
		t.Fatal(err)
	}
	var original *codec.MutationBatch
	_, err = local.ScanReplicationLog(context.Background(), c.Nodes[0].NodeID, 1, 1, 1<<20, func(b *codec.MutationBatch) error {
		if err := codec.VerifyOrigin(b, c.DBID, c.Nodes[0].OriginKey.Public().(ed25519.PublicKey)); err != nil {
			t.Logf("signed digest %x actual %x mutations %+v", b.MutationDigest, codec.MutationDigest(b), b.Mutations)
			return err
		}
		encoded := codec.EncodeBatch(nil, b)
		copied, _, err := codec.DecodeBatch(encoded, codec.DefaultLimits())
		original = copied
		return err
	})
	_ = local.Close()
	if err != nil || original == nil {
		t.Fatalf("read committed transaction: %v", err)
	}
	if err := codec.VerifyOrigin(original, c.DBID, c.Nodes[0].OriginKey.Public().(ed25519.PublicKey)); err != nil {
		t.Fatal(err)
	}
	// C learns A's transaction exclusively from B while A remains stopped.
	if err := c.AddPeer(1, 2); err != nil {
		t.Fatal(err)
	}
	if err := c.AddPeer(2, 1); err != nil {
		t.Fatal(err)
	}
	waitRows(t, c, 2, 1)
	aDigest, _ := c.ComputeTableDigest(1, "signed_rows", "id")
	cDigest, _ := c.ComputeTableDigest(2, "signed_rows", "id")
	if aDigest != cDigest {
		t.Fatal("forwarded state differs")
	}
	c.StopNode(1)
	raw := dialRelay(t, c, original)
	defer raw.sess.Close()
	before := status(t, c.Nodes[2])
	baseline := metric(t, c.Nodes[2], "spedsql_repl_batches_invalid_total")
	attacks := []*codec.MutationBatch{}
	modified := copyBatch(original)
	modified.Sequence++
	modified.TxID = ids.NewTxID()
	modified.Mutations[0].Value = codec.Text("tampered")
	attacks = append(attacks, modified)
	fabricated := copyBatch(original)
	fabricated.Sequence++
	fabricated.TxID = ids.NewTxID()
	fabricated.HLC += 1000
	if err := codec.SignOrigin(fabricated, c.DBID, c.Nodes[1].OriginKey); err != nil {
		t.Fatal(err)
	}
	attacks = append(attacks, fabricated)
	unsigned := copyBatch(original)
	unsigned.Sequence++
	unsigned.TxID = ids.NewTxID()
	unsigned.SignatureVersion = 0
	attacks = append(attacks, unsigned)
	crossDB := copyBatch(original)
	crossDB.Sequence++
	crossDB.TxID = ids.NewTxID()
	if err := codec.SignOrigin(crossDB, ids.NewDBID(), c.Nodes[0].OriginKey); err != nil {
		t.Fatal(err)
	}
	attacks = append(attacks, crossDB)
	for _, b := range attacks {
		raw.send(t, replication.MsgBatches, replication.EncodeBatches(nil, []*codec.MutationBatch{b}))
	}
	waitMetric(t, c.Nodes[2], "spedsql_repl_batches_invalid_total", baseline+float64(len(attacks)))
	// Tampering a chunk identity must fail before durable staging or cleanup.
	frames, err := codec.EncodeTransactionChunks(original, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := codec.DecodeTransactionChunk(frames[0], 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	chunk.HLC++
	forgedChunk, err := codec.EncodeTransactionChunk(nil, chunk, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	signatureBefore := metric(t, c.Nodes[2], "spedsql_repl_origin_signature_invalid_total")
	raw.send(t, replication.MsgTransactionChunk, forgedChunk)
	waitMetric(t, c.Nodes[2], "spedsql_repl_origin_signature_invalid_total", signatureBefore+1)
	after := status(t, c.Nodes[2])
	if before.HLC != after.HLC || before.Generation != after.Generation {
		t.Fatalf("forgery changed progress: %+v -> %+v", before, after)
	}
	waitRows(t, c, 2, 1)
	for _, b := range attacks {
		if receipt(t, c.Nodes[2], b.TxID) {
			t.Fatalf("forgery received receipt %s", b.TxID)
		}
	}
	if !receipt(t, c.Nodes[2], original.TxID) {
		t.Fatal("honest transaction has no receipt")
	}
	// Restart C to prove the honest receipt and signed history are durable.
	raw.sess.Close()
	c.StopNode(2)
	c.StartNode(2)
	c.UnlockNode(2, c.Nodes[2].KeyHex)
	c.WaitNodeReady(2)
	waitRows(t, c, 2, 1)
	if !receipt(t, c.Nodes[2], original.TxID) {
		t.Fatal("honest receipt lost on restart")
	}
	// Replaying the signed original stays idempotent across restart.
	raw2 := dialRelay(t, c, original)
	defer raw2.sess.Close()
	generation := status(t, c.Nodes[2]).Generation
	raw2.send(t, replication.MsgBatches, replication.EncodeBatches(nil, []*codec.MutationBatch{original}))
	waitMetric(t, c.Nodes[2], "spedsql_repl_batches_received_total", 1)
	if got := status(t, c.Nodes[2]).Generation; got != generation {
		t.Fatal("duplicate changed generation")
	}
	// An authenticated relay is not automatically a trusted snapshot source.
	deniedBefore := metric(t, c.Nodes[2], "spedsql_repl_snapshot_manifests_rejected_total")
	raw2.send(t, replication.MsgSnapshotManifest, codec.EncodeManifest(nil, &codec.SnapshotManifest{FormatVersion: 2, SnapshotID: ids.NewTxID(), DBID: c.DBID, SchemaEpoch: original.SchemaEpoch, SchemaHash: original.SchemaHash, ChunkCount: 1, EncodedBytes: 1}))
	waitMetric(t, c.Nodes[2], "spedsql_repl_snapshot_manifests_rejected_total", deniedBefore+1)
	if got := status(t, c.Nodes[2]).Generation; got != generation {
		t.Fatal("untrusted snapshot changed generation")
	}
	waitRows(t, c, 2, 1)
}

type relay struct {
	sess   *transport.Session
	stream *quic.Stream
}

func dialRelay(t *testing.T, c *harness.Cluster, b *codec.MutationBatch) *relay {
	t.Helper()
	n := c.Nodes[1]
	read := func(name string) []byte {
		raw, err := os.ReadFile(filepath.Join(n.TLSDir, name))
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	creds, err := transport.CredentialsFromPEM(read("node.crt"), read("node.key"), read("ca.crt"), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sess, err := transport.Dial(ctx, c.Nodes[2].ReplAddr, creds, c.Nodes[2].NodeID)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := sess.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := &relay{sess: sess, stream: stream}
	r.send(t, replication.MsgHello, replication.EncodeHello(nil, &replication.Hello{ProtocolVersion: replication.ProtocolVersion, MinProtocolVersion: replication.MinProtocolVersion, NodeID: n.NodeID, DBID: c.DBID, SchemaEpoch: b.SchemaEpoch, SchemaHash: b.SchemaHash, Capabilities: replication.CapMergePolicies | replication.CapOriginSignatures | replication.CapTransactionChunks | replication.CapProgressPages, MaxTransactionBytes: 64 << 20}))
	_ = stream.SetReadDeadline(time.Now().Add(10 * time.Second))
	f, err := replication.ReadFrame(stream)
	if err != nil || f.Type != replication.MsgWelcome {
		t.Fatalf("relay welcome: %v %v", f, err)
	}
	// Drain acknowledgements/control traffic so the receiver stays responsive.
	go func() {
		for {
			_ = stream.SetReadDeadline(time.Now().Add(30 * time.Second))
			if _, err := replication.ReadFrame(stream); err != nil {
				return
			}
		}
	}()
	return r
}
func (r *relay) send(t *testing.T, typ uint16, payload []byte) {
	t.Helper()
	_ = r.stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err := replication.WriteFrame(r.stream, typ, 0, payload); err != nil {
		t.Fatal(err)
	}
}
func copyBatch(b *codec.MutationBatch) *codec.MutationBatch {
	out := *b
	out.Mutations = append([]codec.Mutation(nil), b.Mutations...)
	return &out
}

type nodeStatus struct {
	HLC        uint64 `json:"hlc"`
	Generation uint64 `json:"state_generation"`
}

func get(t *testing.T, n *harness.Node, path string) []byte {
	t.Helper()
	resp, err := http.Get("https://" + n.APIAddr + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("GET %s: %d %s %v", path, resp.StatusCode, raw, err)
	}
	return raw
}
func status(t *testing.T, n *harness.Node) nodeStatus {
	var s nodeStatus
	if err := json.Unmarshal(get(t, n, "/v1/status"), &s); err != nil {
		t.Fatal(err)
	}
	return s
}
func receipt(t *testing.T, n *harness.Node, id ids.TxID) bool {
	var s struct {
		Exists bool `json:"exists"`
	}
	if err := json.Unmarshal(get(t, n, "/v1/debug/receipt?txid="+id.String()), &s); err != nil {
		t.Fatal(err)
	}
	return s.Exists
}
func metric(t *testing.T, n *harness.Node, name string) float64 {
	t.Helper()
	for _, line := range strings.Split(string(get(t, n, "/metrics")), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			var value float64
			_, _ = fmt.Sscan(fields[1], &value)
			return value
		}
	}
	t.Fatalf("missing metric %s", name)
	return 0
}
func waitMetric(t *testing.T, n *harness.Node, name string, want float64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if metric(t, n, name) >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("metric %s did not reach %v", name, want)
}
func waitRows(t *testing.T, c *harness.Cluster, node, want int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if n, err := c.QueryRowCount(node, "signed_rows"); err == nil && n == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node %d did not reach %d rows", node, want)
}
