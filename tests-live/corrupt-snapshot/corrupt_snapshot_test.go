// Corrupt-snapshot acceptance: a stale node rejoins while its only
// available snapshot source is hostile and serves bit-flipped chunks. The
// receiver must detect the content-digest mismatch, discard the transfer
// with nothing partially published (row count, digest, and state
// generation untouched), then retry from an honest peer and converge with
// bit-identical digests.
package corruptsnapshot_test

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/replication"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

const stale = 2 // node3 goes stale, then rejoins

func TestStaleNodeDiscardsCorruptSnapshotThenConverges(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "corrupt-snapshot",
		NumNodes:    3,
		AwaitUnlock: true,
		// Manual peers so the stale node restarts truly peerless: the
		// attacker becomes its only snapshot source by construction.
		ManualPeers: true,
		Replication: &harness.ReplicationOptions{
			MinLogRetentionMs:        1000,
			MaxOfflineLogRetentionMs: 20000,
			MinRetainedBatches:       10,
		},
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "snap_rows",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
	})

	// Phase 1: build the honest mesh explicitly and converge a baseline.
	for _, pair := range [][2]int{{0, 1}, {1, 0}, {0, 2}, {2, 0}, {1, 2}, {2, 1}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			t.Fatalf("AddPeer(%d,%d): %v", pair[0], pair[1], err)
		}
	}
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%032x", 1000+i)
		if err := cluster.ExecSQL(2, "INSERT INTO snap_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("stale-%d", i)); err != nil {
			t.Fatalf("node3 pre-stop write: %v", err)
		}
	}
	waitConverged(t, cluster, "snap_rows", 5, 60*time.Second)
	preStopDigest, err := cluster.ComputeTableDigest(stale, "snap_rows", "id")
	if err != nil {
		t.Fatalf("pre-stop digest: %v", err)
	}

	// Phase 2: isolate node3, stop it, and let the survivors write past
	// retention so the rejoin genuinely needs the snapshot path.
	for _, pair := range [][2]int{{2, 0}, {2, 1}, {0, 2}, {1, 2}} {
		if err := cluster.RemovePeer(pair[0], pair[1]); err != nil {
			t.Fatalf("RemovePeer(%d,%d): %v", pair[0], pair[1], err)
		}
	}
	cluster.StopNode(stale)
	for i := 0; i < 60; i++ {
		id := fmt.Sprintf("%032x", 2000+i)
		if err := cluster.ExecSQL(0, "INSERT INTO snap_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("fresh-%d", i)); err != nil {
			t.Fatalf("survivor write: %v", err)
		}
	}
	waitRowCount(t, cluster, 0, "snap_rows", 65, 30*time.Second)
	waitRowCount(t, cluster, 1, "snap_rows", 65, 30*time.Second)
	// Same expiry + GC-pass wait as snapshot-resync: node3's member
	// deadline (20s) must lapse plus one 30s GC pass before its needed
	// ranges are genuinely unrecoverable from logs.
	t.Logf("waiting for member-deadline expiry and a GC pass")
	time.Sleep(gcWait())

	// Phase 3: node3 restarts peerless; the attacker's inbound session is
	// its only source. The attacker induces a snapshot pull, then serves
	// bit-flipped chunks.
	cluster.StartNode(stale)
	cluster.UnlockNode(stale, cluster.Nodes[stale].KeyHex)
	cluster.WaitNodeReady(stale)
	victimAPI := cluster.Nodes[stale].APIAddr
	genBeforeAttack := metricValue(t, victimAPI, "spedsql_state_generation")

	atk := newAttacker(t, cluster)
	mal := atk.dialMismatch(t, cluster.Nodes[stale].ReplAddr, cluster.Nodes[stale].NodeID)
	defer mal.close()
	// Prove the "only available source" premise: exactly one connected peer.
	if got := metricValue(t, victimAPI, "spedsql_connected_peers"); got != 1 {
		t.Fatalf("stale node connected_peers = %v during attack, want exactly 1 (the attacker)", got)
	}
	// Induce the pull: an unsolicited snapshot-required error makes the
	// node request a snapshot from this peer over the live session.
	mal.send(t, replication.MsgError, 0,
		replication.EncodeError(nil, replication.ErrSnapshotRequired, "ranges unavailable"))
	reqs := mal.collectUntil(30*time.Second, func(fr *replication.Frame) bool {
		return fr.Type == replication.MsgSnapshotRequest
	})
	if !anyFrame(reqs, func(fr *replication.Frame) bool { return fr.Type == replication.MsgSnapshotRequest }) {
		t.Fatalf("stale node never requested a snapshot from the attacker")
	}
	forged := buildCorruptSnapshot(t, atk, mal.welcome)
	serveCorrupt(t, mal, forged)

	// The receiver must detect the digest mismatch and fail the apply...
	waitMetricDelta(t, victimAPI, "spedsql_snapshot_apply_failures_total", 0, 1, 30*time.Second)
	// ...via the digest path specifically: the manifest and first chunk
	// passed structural checks (received counters moved)...
	waitMetricDelta(t, victimAPI, "spedsql_repl_snapshot_manifests_received_total", 0, 1, 15*time.Second)
	waitMetricDelta(t, victimAPI, "spedsql_repl_snapshot_chunks_received_total", 0, 1, 15*time.Second)
	// ...while no snapshot completed and nothing was partially published.
	time.Sleep(2 * time.Second)
	if got := metricValue(t, victimAPI, "spedsql_repl_snapshots_received_total"); got != 0 {
		t.Fatalf("snapshots_received = %v after corrupt transfer, want 0 (corrupt data published)", got)
	}
	if n, err := cluster.QueryRowCount(stale, "snap_rows"); err != nil || n != 5 {
		t.Fatalf("stale node rows = %d, err = %v after corrupt transfer, want 5 (partial publication)", n, err)
	}
	if d, err := cluster.ComputeTableDigest(stale, "snap_rows", "id"); err != nil || d != preStopDigest {
		t.Fatalf("stale node digest changed by corrupt transfer: %q -> %q, err = %v", preStopDigest, d, err)
	}
	if got := metricValue(t, victimAPI, "spedsql_state_generation"); got != genBeforeAttack {
		t.Fatalf("state generation moved %v -> %v on corrupt transfer (partial publication)", genBeforeAttack, got)
	}

	// Phase 4: honest peers return; the node retries from them and converges.
	for _, pair := range [][2]int{{2, 0}, {2, 1}, {0, 2}, {1, 2}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			t.Fatalf("honest AddPeer(%d,%d): %v", pair[0], pair[1], err)
		}
	}
	waitConverged(t, cluster, "snap_rows", 65, 120*time.Second)
	// The rejoin completed via an honest snapshot, not the corrupt one.
	waitMetricDelta(t, victimAPI, "spedsql_repl_snapshots_received_total", 0, 1, 30*time.Second)
	if got := metricValue(t, victimAPI, "spedsql_state_generation"); got < genBeforeAttack {
		t.Fatalf("state generation regressed %v -> %v (non-monotonic publication)", genBeforeAttack, got)
	}
	// The stale node's acknowledged pre-stop state survives the merge.
	res, err := cluster.QuerySQL(stale, "SELECT name FROM snap_rows WHERE name LIKE 'stale-%'")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 5 {
		t.Fatalf("stale rows after honest resync = %d, want 5", len(res.Rows))
	}
}

// serveCorrupt pushes the forged manifest and both chunks on a fresh
// snapshot stream, then drains a beat so node replies never wedge the
// session while the test asserts.
func serveCorrupt(t *testing.T, mal *evilSession, forged *corruptSnapshot) {
	t.Helper()
	stream := mal.openStream(t)
	defer stream.Close()
	_ = stream.SetWriteDeadline(time.Now().Add(15 * time.Second))
	if err := replication.WriteFrame(stream, replication.MsgSnapshotManifest, 0, codec.EncodeManifest(nil, forged.manifest)); err != nil {
		t.Fatalf("serve corrupt manifest: %v", err)
	}
	if err := replication.WriteFrame(stream, replication.MsgSnapshotChunk, 0, forged.chunk0); err != nil {
		t.Fatalf("serve corrupt chunk0: %v", err)
	}
	if err := replication.WriteFrame(stream, replication.MsgSnapshotChunk, 0, forged.chunk1); err != nil {
		t.Fatalf("serve corrupt chunk1: %v", err)
	}
	_ = stream.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_ = replication.WriteFrame(stream, replication.MsgSnapshotDone, 0, nil)
	mal.collectUntil(3*time.Second, func(*replication.Frame) bool { return false })
}

// gcWait mirrors snapshot-resync's expiry window; overridable for
// faster development runs via SPEDSQL_CORRUPT_SNAPSHOT_GC_WAIT_SECONDS.
func gcWait() time.Duration {
	if v := os.Getenv("SPEDSQL_CORRUPT_SNAPSHOT_GC_WAIT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 55 * time.Second
}

func anyFrame(frames []*replication.Frame, want func(*replication.Frame) bool) bool {
	for _, fr := range frames {
		if want(fr) {
			return true
		}
	}
	return false
}

func waitRowCount(t *testing.T, c *harness.Cluster, idx int, table string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n, err := c.QueryRowCount(idx, table)
		if err == nil && n == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	n, _ := c.QueryRowCount(idx, table)
	t.Fatalf("node %d %s count = %d, want %d within %v", idx, table, n, want, timeout)
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
