// Hostile-peer acceptance: a malicious authenticated peer fires forged
// batches, a replayed handshake, an absurd allocation-claim frame, and a
// compressed snapshot bomb at an honest 2-node mesh. Every attack must be
// rejected (rejection counters move, nothing foreign applies), the attacker
// must receive no data frames on its data-gated session, and the honest
// pair must stay converged and responsive throughout.
//
// Unsigned input from an unprovisioned origin is rejected before schema or
// HLC processing. Authorized origins still use the existing HLC/LWW rules.
package hostilepeer_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/replication"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestHostilePeerAttacksRejected(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "hostile-peer",
		NumNodes:    2,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "hostile_rows",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
	})
	target := 0 // attacker aims at node1; node2 is the honest witness

	// Positive control (pre-attack): honest writes replicate both ways.
	if err := cluster.ExecSQL(0, "INSERT INTO hostile_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 1), "honest-1"); err != nil {
		t.Fatalf("baseline insert node1: %v", err)
	}
	waitConverged(t, cluster, "hostile_rows", 1, 30*time.Second)
	if err := cluster.ExecSQL(1, "INSERT INTO hostile_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 2), "honest-2"); err != nil {
		t.Fatalf("baseline insert node2: %v", err)
	}
	waitConverged(t, cluster, "hostile_rows", 2, 30*time.Second)
	preAttack := nodeDigest(t, cluster, target, "hostile_rows")

	baseOriginUnknown := metricValue(t, cluster.Nodes[target].APIAddr, "spedsql_repl_origin_unknown_total")
	baseInvalid := metricValue(t, cluster.Nodes[target].APIAddr, "spedsql_repl_batches_invalid_total")
	baseReceived := metricValue(t, cluster.Nodes[target].APIAddr, "spedsql_repl_batches_received_total")
	baseSessions := metricValue(t, cluster.Nodes[target].APIAddr, "spedsql_repl_sessions_opened_total")
	schemaEpoch := metricValue(t, cluster.Nodes[target].APIAddr, "spedsql_schema_epoch")

	atk := newAttacker(t, cluster)
	nodeAddr := cluster.Nodes[target].ReplAddr
	nodeID := cluster.Nodes[target].NodeID
	handshakes := 0

	// --- P1 forged batches + P2 replayed handshake on one watched session.
	watched := atk.dialMismatch(t, nodeAddr, nodeID)
	handshakes++
	var seen []*replication.Frame

	// Forged batch: well-framed and structurally valid, but carrying a
	// writer schema the node never published. Origin trust rejects it before
	// schema synchronization, so it must never apply.
	forged := &codec.MutationBatch{
		ProtocolVersion: replication.ProtocolVersion,
		TxID:            ids.NewTxID(),
		OriginNode:      atk.id,
		Sequence:        1,
		HLC:             100,
		SchemaEpoch:     4242,
		SchemaHash:      randomHash(),
		Mutations: []codec.Mutation{
			{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("forged")},
		},
	}
	forgedPayload := replication.EncodeBatches(nil, []*codec.MutationBatch{forged})
	// Non-vacuous guard: the forged payload decodes cleanly, so the live
	// rejection below is origin authorization, not a parse failure.
	if _, err := replication.DecodeBatches(forgedPayload, codec.DefaultLimits()); err != nil {
		t.Fatalf("forged batch does not decode locally: %v", err)
	}
	watched.send(t, replication.MsgBatches, 0, forgedPayload)

	// Structurally valid frames with invalid batch identities: each must
	// be dropped (batches_invalid) with the session surviving.
	invalid := []*codec.MutationBatch{
		mkBatch(atk.id, 0, 100, replication.ProtocolVersion, 4242),       // zero sequence
		mkBatch(atk.id, 7, 0, replication.ProtocolVersion, 4242),         // zero HLC
		mkBatch(atk.id, 8, 100, replication.ProtocolVersion+99, 4242),    // bad protocol
		mkBatch(ids.NodeID{}, 9, 100, replication.ProtocolVersion, 4242), // zero origin
	}
	watched.send(t, replication.MsgBatches, 0, replication.EncodeBatches(nil, invalid))

	// Session must be alive: ping round-trips.
	nonce1 := []byte{0, 0, 0, 0, 0, 0, 0, 11}
	watched.send(t, replication.MsgPing, 0, nonce1)
	seen = append(seen, watched.collectUntil(10*time.Second, isPong(nonce1))...)
	assertSaw(t, seen, isPong(nonce1), "pong to ping-1 after forged batches")

	// P2: replay the handshake on the live control stream. Hello is only
	// valid as the first frame of a stream; mid-session it must draw an
	// error without killing the session.
	replayHello := replication.EncodeHello(nil, &replication.Hello{
		ProtocolVersion: replication.ProtocolVersion, MinProtocolVersion: replication.MinProtocolVersion,
		NodeID: atk.id, DBID: atk.dbid, SchemaEpoch: 9999, SchemaHash: randomHash(),
		Capabilities: replication.CapMergePolicies | replication.CapZstd | replication.CapOriginSignatures, MaxTransactionBytes: 64 << 20,
	})
	watched.send(t, replication.MsgHello, 0, replayHello)
	errFrames := watched.collectUntil(10*time.Second, func(fr *replication.Frame) bool {
		if fr.Type != replication.MsgError {
			return false
		}
		code, _, err := replication.DecodeError(fr.Payload)
		return err == nil && code == replication.ErrBadMessage
	})
	seen = append(seen, errFrames...)
	if len(errFrames) == 0 || errFrames[len(errFrames)-1].Type != replication.MsgError {
		t.Fatalf("replayed handshake drew no ErrBadMessage; saw %s", frameSummary(errFrames))
	}

	// Session still alive after the replay.
	nonce2 := []byte{0, 0, 0, 0, 0, 0, 0, 22}
	watched.send(t, replication.MsgPing, 0, nonce2)
	more := watched.collectUntil(10*time.Second, isPong(nonce2))
	seen = append(seen, more...)
	assertSaw(t, more, isPong(nonce2), "pong to ping-2 after replayed handshake")
	// Final drain: give the node a window to (wrongly) push data.
	seen = append(seen, watched.collect(2*time.Second)...)
	watched.close()

	// The data-gated attacker session must have carried no data frames.
	assertNoDataFrames(t, seen)

	// Rejection counters must have moved; no attack batch may have applied.
	waitMetricDelta(t, cluster.Nodes[target].APIAddr, "spedsql_repl_origin_unknown_total", baseOriginUnknown, 1, 15*time.Second)
	waitMetricDelta(t, cluster.Nodes[target].APIAddr, "spedsql_repl_batches_invalid_total", baseInvalid, 4, 15*time.Second)
	if got := metricValue(t, cluster.Nodes[target].APIAddr, "spedsql_repl_batches_received_total"); got != baseReceived {
		t.Fatalf("batches_received moved %v -> %v during attacks (an attack batch applied)", baseReceived, got)
	}
	assertDigestUnchanged(t, cluster, target, "hostile_rows", preAttack, "after forged-batch attacks")
	if n, err := cluster.QueryRowCount(target, "hostile_rows"); err != nil || n != 2 {
		t.Fatalf("node1 row count = %d, err = %v after attacks, want 2", n, err)
	}

	// --- P3: 256MiB allocation-claim frame. Must fail fast without a huge
	// alloc; the stream dies (or goes permanently silent) and the node
	// stays up.
	evil := atk.dialMismatch(t, nodeAddr, nodeID)
	handshakes++
	// Drain the session's opening control chatter (the attach-time schema
	// request) so post-attack reads are not polluted by it.
	evil.collectUntil(5*time.Second, func(fr *replication.Frame) bool {
		return fr.Type == replication.MsgSchemaRequest
	})
	var hdr [12]byte
	binary.BigEndian.PutUint16(hdr[0:2], 0x5244) // frame magic "RD"
	binary.BigEndian.PutUint16(hdr[2:4], replication.ProtocolVersion)
	binary.BigEndian.PutUint16(hdr[4:6], replication.MsgBatches)
	binary.BigEndian.PutUint16(hdr[6:8], 0)
	binary.BigEndian.PutUint32(hdr[8:12], 256<<20) // 256MiB claim
	_ = evil.stream.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := evil.stream.Write(hdr[:]); err != nil {
		t.Fatalf("write evil header: %v", err)
	}
	assertStreamDead(t, evil, "256MiB allocation claim")
	evil.close()
	// The node itself survives: a fresh session handshakes and pings fine.
	probe := atk.dialMismatch(t, nodeAddr, nodeID)
	handshakes++
	nonce3 := []byte{0, 0, 0, 0, 0, 0, 0, 33}
	probe.send(t, replication.MsgPing, 0, nonce3)
	if got := probe.collectUntil(10*time.Second, isPong(nonce3)); !anyFrame(got, isPong(nonce3)) {
		probe.close()
		t.Fatalf("node dead after 256MiB claim (no pong on fresh session)")
	}
	probe.close()

	// --- P4: compressed snapshot bomb. A tiny zstd frame expanding to
	// megabytes of undecodable chunk bytes; the stream must die, not the node.
	bomb := atk.dialMismatch(t, nodeAddr, nodeID)
	handshakes++
	bombRaw := bytes.Repeat([]byte{0xFF}, 4<<20)
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	compressed := enc.EncodeAll(bombRaw, nil)
	_ = enc.Close()
	if len(compressed) >= len(bombRaw)/4 {
		t.Fatalf("bomb does not compress: %d -> %d", len(bombRaw), len(compressed))
	}
	// Non-vacuous guard: the bomb decompresses locally, so rejection is by
	// product bounds/validation, not a corrupt test payload.
	dec, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("bomb does not decompress locally: %v", err)
	}
	if _, err := io.ReadAll(io.LimitReader(dec, 8<<20)); err != nil {
		t.Fatalf("bomb unreadable locally: %v", err)
	}
	dec.Close()
	bomb.collectUntil(5*time.Second, func(fr *replication.Frame) bool {
		return fr.Type == replication.MsgSchemaRequest
	})
	bomb.send(t, replication.MsgSnapshotChunk, replication.FlagZstd, compressed)
	assertStreamDead(t, bomb, "snapshot bomb")
	bomb.close()
	probe2 := atk.dialMismatch(t, nodeAddr, nodeID)
	handshakes++
	nonce4 := []byte{0, 0, 0, 0, 0, 0, 0, 44}
	probe2.send(t, replication.MsgPing, 0, nonce4)
	if got := probe2.collectUntil(10*time.Second, isPong(nonce4)); !anyFrame(got, isPong(nonce4)) {
		probe2.close()
		t.Fatalf("node dead after snapshot bomb (no pong on fresh session)")
	}
	probe2.close()

	// Every attack handshake attached (and the node is still counting).
	waitMetricDelta(t, cluster.Nodes[target].APIAddr, "spedsql_repl_sessions_opened_total", baseSessions, float64(handshakes), 15*time.Second)

	// --- P-HLC: unsigned far-future input from an untrusted origin must be
	// rejected before HLC observation; the mesh stays responsive.
	hlcSess := atk.dialMismatch(t, nodeAddr, nodeID)
	handshakes++
	hlcSess.send(t, replication.MsgSchemaRequest, 0,
		replication.EncodeSchemaRequest(nil, &replication.SchemaRequest{WantCurrent: true}))
	var manifest *replication.SchemaManifestMsg
	for _, fr := range hlcSess.collectUntil(15*time.Second, func(fr *replication.Frame) bool {
		return fr.Type == replication.MsgSchemaManifest
	}) {
		if fr.Type == replication.MsgSchemaManifest {
			m, err := replication.DecodeSchemaManifest(fr.Payload)
			if err != nil {
				t.Fatalf("decode served schema manifest: %v", err)
			}
			manifest = m
		}
	}
	if manifest == nil || len(manifest.Revisions) == 0 {
		hlcSess.close()
		t.Fatalf("node served no schema manifest to attacker")
	}
	tip := manifest.Revisions[0]
	var tableID, idCol, nameCol uint32
	for _, tb := range tip.Tables {
		if strings.EqualFold(tb.Name, "hostile_rows") {
			tableID = tb.ID
			for _, c := range tb.Columns {
				switch strings.ToLower(c.Name) {
				case "id":
					idCol = c.ID
				case "name":
					nameCol = c.ID
				}
			}
		}
	}
	if tableID == 0 || idCol == 0 || nameCol == 0 {
		hlcSess.close()
		t.Fatalf("hostile_rows ids not resolved (table=%d id=%d name=%d)", tableID, idCol, nameCol)
	}
	farFuture := (uint64(time.Now().UnixMilli()) + 10*365*24*3600*1000) << 16
	rowID := ids.NewRowID()
	idVal := append([]byte(nil), rowID[:]...)
	hlcBatch := &codec.MutationBatch{
		ProtocolVersion: replication.ProtocolVersion,
		TxID:            ids.NewTxID(),
		OriginNode:      atk.id,
		Sequence:        1,
		HLC:             farFuture,
		SchemaEpoch:     hlcSess.welcome.SchemaEpoch,
		SchemaHash:      hlcSess.welcome.SchemaHash,
		Mutations: []codec.Mutation{
			{TableID: tableID, RowID: rowID, ColumnID: idCol, Value: codec.Blob(idVal)},
			{TableID: tableID, RowID: rowID, ColumnID: nameCol, Value: codec.Text("far-future-hlc")},
		},
	}
	hlcSess.send(t, replication.MsgBatches, 0, replication.EncodeBatches(nil, []*codec.MutationBatch{hlcBatch}))
	// Round-trip before closing: the ordered stream guarantees the node
	// processed the batch before answering the ping (closing immediately
	// could drop the batch in flight).
	nonce5 := []byte{0, 0, 0, 0, 0, 0, 0, 55}
	hlcSess.send(t, replication.MsgPing, 0, nonce5)
	if got := hlcSess.collectUntil(10*time.Second, isPong(nonce5)); !anyFrame(got, isPong(nonce5)) {
		hlcSess.close()
		t.Fatalf("no pong after far-future batch send")
	}
	hlcSess.close()
	// The unprovisioned origin is rejected before HLC observation; the honest pair stays unchanged.
	waitConverged(t, cluster, "hostile_rows", 2, 30*time.Second)

	// --- Positive control (post-attack): the pair stays responsive and
	// converged; schema identity untouched by the attacks.
	if got := metricValue(t, cluster.Nodes[target].APIAddr, "spedsql_schema_epoch"); got != schemaEpoch {
		t.Fatalf("schema epoch moved %v -> %v during attacks", schemaEpoch, got)
	}
	if err := cluster.ExecSQL(1, "INSERT INTO hostile_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 4), "post-attack-1"); err != nil {
		t.Fatalf("post-attack insert node2: %v", err)
	}
	waitConverged(t, cluster, "hostile_rows", 3, 30*time.Second)
	if err := cluster.ExecSQL(0, "INSERT INTO hostile_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 5), "post-attack-2"); err != nil {
		t.Fatalf("post-attack insert node1: %v", err)
	}
	waitConverged(t, cluster, "hostile_rows", 4, 30*time.Second)
}

func mkBatch(origin ids.NodeID, seq, hlc uint64, proto uint16, epoch uint64) *codec.MutationBatch {
	return &codec.MutationBatch{
		ProtocolVersion: proto,
		TxID:            ids.NewTxID(),
		OriginNode:      origin,
		Sequence:        seq,
		HLC:             hlc,
		SchemaEpoch:     epoch,
		SchemaHash:      randomHash(),
		Mutations: []codec.Mutation{
			{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("x")},
		},
	}
}

func assertSaw(t *testing.T, frames []*replication.Frame, want func(*replication.Frame) bool, what string) {
	t.Helper()
	if !anyFrame(frames, want) {
		t.Fatalf("missing %s; saw %s", what, frameSummary(frames))
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

// assertStreamDead pings a stream that a fatal probe should have killed and
// asserts the node never answers: no pong (the ping was never processed)
// and no data frames. Stray repeats of attach-time control chatter are
// tolerated but logged.
func assertStreamDead(t *testing.T, s *evilSession, what string) {
	t.Helper()
	nonce := []byte{0xDE, 0xAD, 0, 0, 0, 0, 0, 0}
	_ = s.stream.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_ = replication.WriteFrame(s.stream, replication.MsgPing, 0, nonce)
	frames := s.collect(3 * time.Second)
	for _, fr := range frames {
		if fr.Type == replication.MsgPong {
			t.Fatalf("node answered ping after %s (stream should be dead)", what)
		}
		if _, ok := dataFrameTypes()[fr.Type]; ok {
			t.Fatalf("node sent data frame %s after %s", frameName(fr.Type), what)
		}
	}
	t.Logf("after %s the node sent only: %s", what, frameSummary(frames))
}

func assertNoDataFrames(t *testing.T, frames []*replication.Frame) {
	t.Helper()
	data := dataFrameTypes()
	for _, fr := range frames {
		if name, ok := data[fr.Type]; ok {
			t.Fatalf("attacker received data frame %s on data-gated session (saw %s)", name, frameSummary(frames))
		}
	}
	t.Logf("attacker session carried only control frames: %s", frameSummary(frames))
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

func nodeDigest(t *testing.T, cluster *harness.Cluster, idx int, table string) string {
	t.Helper()
	d, err := cluster.ComputeTableDigest(idx, table, "id")
	if err != nil {
		t.Fatalf("node %d digest: %v", idx, err)
	}
	return d
}

func assertDigestUnchanged(t *testing.T, cluster *harness.Cluster, idx int, table, want, ctx string) {
	t.Helper()
	if got := nodeDigest(t, cluster, idx, table); got != want {
		t.Fatalf("node %d %s digest changed %s: %s -> %s", idx, table, ctx, want, got)
	}
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
