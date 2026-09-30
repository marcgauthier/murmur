// Hostile-schema acceptance: a malicious authenticated peer advertises
// forged schema manifests against a 3-node mesh — first a revision with a
// corrupted content digest, then an incompatible wild-version revision.
// The local node must quarantine the peer: refuse both forgeries (refusal
// recorded in metrics, attacker data-gated), never adopt or merge them,
// and keep replicating honestly with the rest of the mesh.
package hostileschema_test

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/replication"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestHostileSchemaManifestsQuarantined(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "hostile-schema",
		NumNodes:    3,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "schema_rows",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
	})
	target := 0 // attacker aims at node1

	// Positive control (pre-attack): the honest 3-mesh converges.
	if err := cluster.ExecSQL(0, "INSERT INTO schema_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 1), "baseline"); err != nil {
		t.Fatalf("baseline insert: %v", err)
	}
	waitConverged(t, cluster, "schema_rows", 1, 30*time.Second)

	api := cluster.Nodes[target].APIAddr
	baseReplConflicts := metricValue(t, api, "spedsql_repl_schema_conflicts_total")
	baseDBConflicts := metricValue(t, api, "spedsql_schema_conflicts_total")
	baseManifests := metricValue(t, api, "spedsql_repl_schema_manifests_received_total")
	baseAdoptions := metricValue(t, api, "spedsql_schema_adoptions_total")
	baseMerges := metricValue(t, api, "spedsql_schema_merges_total")
	schemaEpoch := metricValue(t, api, "spedsql_schema_epoch")

	atk := newAttacker(t, cluster)
	nodeAddr := cluster.Nodes[target].ReplAddr
	nodeID := cluster.Nodes[target].NodeID

	// --- S1: forged revision with a corrupted content digest.
	// Flip a byte inside the embedded Hash field (encoding offsets:
	// magic[0:4] version[4:12] node[12:28] time[28:36] hash[36:68]) so
	// the frame stays structurally valid but digest authentication fails.
	badDigest := atk.dialMismatch(t, nodeAddr, nodeID)
	served := badDigest.fetchManifest(t)
	tipBytes := schema.EncodeManifest(served.Revisions[0])
	if len(tipBytes) < 68 {
		t.Fatalf("served manifest too short to forge: %d bytes", len(tipBytes))
	}
	forged := append([]byte(nil), tipBytes...)
	forged[40] ^= 0xFF
	forged[51] ^= 0x01
	// Sanity: the corrupted bytes must fail local authentication (the
	// forgery targets the digest check, not the framing).
	if _, err := schema.DecodeManifest(forged); err == nil {
		t.Fatalf("corrupted manifest still decodes locally; forgery is vacuous")
	}
	payload := binary.BigEndian.AppendUint32(nil, 1)
	payload = binary.BigEndian.AppendUint32(payload, uint32(len(forged)))
	payload = append(payload, forged...)
	badDigest.send(t, replication.MsgSchemaManifest, 0, payload)
	// The stream must die: a ping sent after the forgery draws no pong.
	_ = badDigest.sendSoft(replication.MsgPing, 0, []byte{0, 0, 0, 0, 0, 0, 0, 71})
	if frames := badDigest.collect(5 * time.Second); anyPong(frames) {
		t.Fatalf("node answered ping after digest-forged manifest (stream should be dead)")
	}
	badDigest.close()
	// The forgery never reached the adoption engine: the received-manifest
	// counter (incremented only after successful decode) must not move.
	time.Sleep(500 * time.Millisecond)
	if got := metricValue(t, api, "spedsql_repl_schema_manifests_received_total"); got != baseManifests {
		t.Fatalf("manifests_received moved %v -> %v on digest forgery (forgery reached adoption)", baseManifests, got)
	}
	// The node itself survives the forgery.
	probe := atk.dialMismatch(t, nodeAddr, nodeID)
	probe.send(t, replication.MsgPing, 0, []byte{0, 0, 0, 0, 0, 0, 0, 72})
	if got := probe.collectUntil(10*time.Second, isPong([]byte{0, 0, 0, 0, 0, 0, 0, 72})); !anyPong(got) {
		probe.close()
		t.Fatalf("node dead after digest-forged manifest")
	}
	probe.close()

	// --- S2: incompatible wild-version revision (valid encoding, valid
	// digest, unbuildable content). Must be refused with ErrSchemaMismatch
	// and recorded as a schema conflict; never adopted or merged.
	evil := atk.dialMismatch(t, nodeAddr, nodeID)
	local := evil.fetchManifest(t).Revisions[0]
	// TEXT primary key: parses and authenticates (types are opaque
	// bytes on the wire) but can never build a registry (PK must be
	// BLOB), so the adoption engine must refuse it as incompatible.
	evilRev := &schema.Manifest{
		Version:       local.Version + 100,
		CreatedOnNode: atk.id,
		TimeCreated:   uint64(time.Now().UnixMilli()) << 16,
		Tables: []schema.TableSchema{{
			ID:   7,
			Name: "evil_rows",
			PK:   11,
			Columns: []schema.ColumnSchema{
				{ID: 11, Name: "id", Type: schema.ColText},
				{ID: 12, Name: "name", Type: schema.ColText, Nullable: true},
			},
		}},
	}
	evilRev.Hash = schema.ContentHash(evilRev.Version, evilRev.Tables)
	// Non-vacuous guard: the evil revision is well-formed (decodes and
	// authenticates locally), so the live refusal is the adoption
	// engine's incompatibility decision, not a parse failure.
	if _, err := schema.DecodeManifest(schema.EncodeManifest(evilRev)); err != nil {
		t.Fatalf("evil revision does not decode locally: %v", err)
	}
	evil.send(t, replication.MsgSchemaManifest, 0,
		replication.EncodeSchemaManifest(nil, &replication.SchemaManifestMsg{Revisions: []*schema.Manifest{evilRev}}))
	reply := evil.collectUntil(15*time.Second, isSchemaMismatchError)
	if !anyFrame(reply, isSchemaMismatchError) {
		t.Fatalf("incompatible revision drew no ErrSchemaMismatch; saw %s", frameSummary(reply))
	}
	// Attacker stays data-gated: drain and assert no data frames arrived.
	for _, fr := range evil.collect(2 * time.Second) {
		reply = append(reply, fr)
	}
	assertNoDataFrames(t, reply)
	evil.close()

	waitMetricDelta(t, api, "spedsql_repl_schema_manifests_received_total", baseManifests, 1, 15*time.Second)
	waitMetricDelta(t, api, "spedsql_repl_schema_conflicts_total", baseReplConflicts, 1, 15*time.Second)
	waitMetricDelta(t, api, "spedsql_schema_conflicts_total", baseDBConflicts, 1, 15*time.Second)
	// Quarantine: no adoption, no merge, schema identity untouched.
	if got := metricValue(t, api, "spedsql_schema_adoptions_total"); got != baseAdoptions {
		t.Fatalf("schema adoptions moved %v -> %v (forged schema adopted)", baseAdoptions, got)
	}
	if got := metricValue(t, api, "spedsql_schema_merges_total"); got != baseMerges {
		t.Fatalf("schema merges moved %v -> %v (forged schema merged)", baseMerges, got)
	}
	if got := metricValue(t, api, "spedsql_schema_epoch"); got != schemaEpoch {
		t.Fatalf("schema epoch moved %v -> %v during schema attacks", schemaEpoch, got)
	}

	// --- Positive control (post-attack): no crash, no stall. The attacked
	// node and the honest pair all keep replicating and converge.
	if err := cluster.ExecSQL(1, "INSERT INTO schema_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 2), "post-attack-1"); err != nil {
		t.Fatalf("post-attack insert node2: %v", err)
	}
	waitConverged(t, cluster, "schema_rows", 2, 30*time.Second)
	if err := cluster.ExecSQL(2, "INSERT INTO schema_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 3), "post-attack-2"); err != nil {
		t.Fatalf("post-attack insert node3: %v", err)
	}
	waitConverged(t, cluster, "schema_rows", 3, 30*time.Second)
	// The attacked node itself writes and the write replicates everywhere.
	if err := cluster.ExecSQL(0, "INSERT INTO schema_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 4), "attacked-node-write"); err != nil {
		t.Fatalf("attacked-node insert: %v", err)
	}
	waitConverged(t, cluster, "schema_rows", 4, 30*time.Second)
	// Explicit honest-pair check: nodes 2 and 3 agree bit-for-bit.
	d1, err := cluster.ComputeTableDigest(1, "schema_rows", "id")
	if err != nil {
		t.Fatalf("honest-pair digest node2: %v", err)
	}
	d2, err := cluster.ComputeTableDigest(2, "schema_rows", "id")
	if err != nil {
		t.Fatalf("honest-pair digest node3: %v", err)
	}
	if d1 != d2 {
		t.Fatalf("honest pair diverged: node2 %s != node3 %s", d1, d2)
	}
}

func anyFrame(frames []*replication.Frame, want func(*replication.Frame) bool) bool {
	for _, fr := range frames {
		if want(fr) {
			return true
		}
	}
	return false
}

func anyPong(frames []*replication.Frame) bool {
	return anyFrame(frames, func(fr *replication.Frame) bool { return fr.Type == replication.MsgPong })
}

func assertNoDataFrames(t *testing.T, frames []*replication.Frame) {
	t.Helper()
	data := map[uint16]bool{
		replication.MsgBatches:          true,
		replication.MsgSnapshotManifest: true,
		replication.MsgSnapshotChunk:    true,
		replication.MsgPlumtreeData:     true,
		replication.MsgTransactionChunk: true,
		replication.MsgProgressPage:     true,
	}
	for _, fr := range frames {
		if data[fr.Type] {
			t.Fatalf("attacker received data frame %s on quarantined session (saw %s)", frameName(fr.Type), frameSummary(frames))
		}
	}
	t.Logf("quarantined session carried only control frames: %s", frameSummary(frames))
}

func frameSummary(frames []*replication.Frame) string {
	if len(frames) == 0 {
		return "<none>"
	}
	var parts []string
	for _, fr := range frames {
		parts = append(parts, frameName(fr.Type))
	}
	return strings.Join(parts, ",")
}

func waitConverged(t *testing.T, c *harness.Cluster, table string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, table)
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, table, "id")
			if err != nil {
				ok = false
				break
			}
			if i == 0 {
				first = d
			} else if d != first {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	for i := range c.Nodes {
		n, nerr := c.QueryRowCount(i, table)
		d, derr := c.ComputeTableDigest(i, table, "id")
		t.Logf("node %d at timeout: count=%d countErr=%v digest=%s digestErr=%v", i, n, nerr, d, derr)
	}
	t.Fatalf("nodes did not converge on %d %s rows with equal digests within %v", want, table, timeout)
}

func metricValue(t *testing.T, apiAddr, name string) float64 {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("https://%s/metrics", apiAddr))
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		if !strings.HasPrefix(line, name) {
			continue
		}
		rest := strings.TrimSpace(strings.TrimPrefix(line, name))
		if i := strings.LastIndex(rest, " "); i >= 0 {
			rest = rest[i+1:]
		}
		v, err := strconv.ParseFloat(rest, 64)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		return v
	}
	t.Fatalf("counter %s not present in /metrics", name)
	return 0
}

func waitMetricDelta(t *testing.T, apiAddr, name string, base, wantDelta float64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := metricValue(t, apiAddr, name); got >= base+wantDelta {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("metric %s did not rise by %v from %v within %v", name, wantDelta, base, timeout)
}
