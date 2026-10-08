// Stale-node snapshot resync acceptance scenario.
//
// Three spedsql daemon processes mesh with aggressive log retention. One
// node stops, the survivors write past retention and GC expiry, and the
// stale node restarts: its needed log ranges are gone, so it must rejoin
// by merging a snapshot without discarding its acknowledged pre-stop
// state. The snapshot path is proven via the receiver's Prometheus
// snapshot counter, not merely by convergence (which log catch-up would
// also produce).
package snapshotresync_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestStaleNodeRejoinsViaSnapshot(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "snapshot-resync",
		NumNodes:        3,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
		Replication: &harness.ReplicationOptions{
			MinLogRetentionMs:        1000,
			MaxOfflineLogRetentionMs: 20000,
			MinRetainedBatches:       10,
		},
	})

	// Phase 1: node3 writes acknowledged rows, then all three converge.
	for i := 0; i < 5; i++ {
		if err := insertRows(cluster, 2, 1000+i, 1, "stale"); err != nil {
			t.Fatalf("node3 pre-stop write: %v", err)
		}
	}
	waitConverged(t, cluster, 5, 30*time.Second)

	// Phase 2: node3 stops; survivors write far past retention.
	cluster.StopNode(2)
	for i := 0; i < 60; i++ {
		if err := insertRows(cluster, 0, 2000+i, 1, "fresh"); err != nil {
			t.Fatalf("survivor write: %v", err)
		}
	}
	if _, err := waitRowCount(t, cluster, 0, 65, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := waitRowCount(t, cluster, 1, 65, 30*time.Second); err != nil {
		t.Fatal(err)
	}

	// Node3's member deadline (20s) must expire before its ranges stop
	// gating GC, and log GC ticks every 30s: wait out expiry plus one full
	// GC pass so node3's needed ranges are genuinely unrecoverable from
	// logs. Online peers keep each other admitted through replication
	// acks, so only the down peer's pin lapses.
	t.Logf("waiting for member-deadline expiry and a GC pass")
	time.Sleep(55 * time.Second)

	// Phase 3: node3 restarts with the same durable directory and rejoins.
	cluster.StartNode(2)
	cluster.UnlockNode(2, cluster.Nodes[2].KeyHex)
	cluster.WaitNodeReady(2)
	waitConverged(t, cluster, 65, 90*time.Second)

	// The rejoin must have used the snapshot path, not log catch-up.
	if got := snapshotCounter(t, cluster.Nodes[2].APIAddr, "spedsql_repl_snapshots_received_total"); got < 1 {
		t.Fatalf("node3 snapshots received = %v, want >= 1 (log catch-up would hide a snapshot-path regression)", got)
	}

	// Node3's acknowledged pre-stop state survives the snapshot merge.
	rows, err := cluster.TypedContentionRows(2)
	if err != nil {
		t.Fatal(err)
	}
	if countPrefix(rows, "stale-") != 5 {
		t.Fatalf("node3 stale rows = %d, want 5", countPrefix(rows, "stale-"))
	}
}

// Two stale nodes restarting together request snapshots concurrently from
// the same survivor. The source serves one transfer per peer at a time and
// answers the other with an explicit busy deferral; the receiver backs off
// and the stall watchdog re-requests, so both rejoins converge instead of
// stalling on a silently dropped request.
func TestDualStaleNodesRejoinViaSnapshot(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "snapshot-resync-dual",
		NumNodes:        3,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
		Replication: &harness.ReplicationOptions{
			MinLogRetentionMs:        1000,
			MaxOfflineLogRetentionMs: 20000,
			MinRetainedBatches:       10,
		},
	})

	for i := 0; i < 5; i++ {
		if err := insertRows(cluster, 2, 1000+i, 1, "stale"); err != nil {
			t.Fatalf("node3 pre-stop write: %v", err)
		}
	}
	waitConverged(t, cluster, 5, 30*time.Second)

	cluster.StopNode(1)
	cluster.StopNode(2)
	for i := 0; i < 60; i++ {
		if err := insertRows(cluster, 0, 2000+i, 1, "fresh"); err != nil {
			t.Fatalf("survivor write: %v", err)
		}
	}
	if _, err := waitRowCount(t, cluster, 0, 65, 30*time.Second); err != nil {
		t.Fatal(err)
	}

	// Same expiry + GC-pass wait as the single-stale test, now covering
	// two down peers' retention pins.
	t.Logf("waiting for member-deadline expiry and a GC pass")
	time.Sleep(55 * time.Second)

	// Restart both stale nodes back-to-back so their snapshot requests
	// overlap on the survivor.
	cluster.StartNode(1)
	cluster.StartNode(2)
	cluster.UnlockNode(1, cluster.Nodes[1].KeyHex)
	cluster.UnlockNode(2, cluster.Nodes[2].KeyHex)
	cluster.WaitNodeReady(1)
	cluster.WaitNodeReady(2)
	waitConverged(t, cluster, 65, 120*time.Second)

	for _, idx := range []int{1, 2} {
		if got := snapshotCounter(t, cluster.Nodes[idx].APIAddr, "spedsql_repl_snapshots_received_total"); got < 1 {
			t.Fatalf("node%d snapshots received = %v, want >= 1", idx+1, got)
		}
		// The progress/deferral diagnostics surface end to end, even
		// when this run needed no deferral (presence, not value).
		snapshotCounter(t, cluster.Nodes[idx].APIAddr, "spedsql_repl_snapshots_busy_deferred_total")
		snapshotCounter(t, cluster.Nodes[idx].APIAddr, "spedsql_repl_snapshot_busy_received_total")
		snapshotCounter(t, cluster.Nodes[idx].APIAddr, "spedsql_peer_awaiting_snapshot")
		snapshotCounter(t, cluster.Nodes[idx].APIAddr, "spedsql_peer_snapshot_chunks_received")
		snapshotCounter(t, cluster.Nodes[idx].APIAddr, "spedsql_peer_snapshot_chunks_total")
	}
}

func waitRowCount(t *testing.T, c *harness.Cluster, idx int, want int, timeout time.Duration) (int, error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		rows, err := c.TypedContentionRows(idx)
		if err == nil && len(rows) == want {
			return len(rows), nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	rows, _ := c.TypedContentionRows(idx)
	return len(rows), fmt.Errorf("node %d typed row count = %d, want %d within %v", idx, len(rows), want, timeout)
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			rows, err := c.TypedContentionRows(i)
			if err != nil || len(rows) != want {
				ok = false
				break
			}
			d := digestRows(rows)
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
		rows, err := c.TypedContentionRows(i)
		t.Logf("node %d at timeout: count=%d err=%v digest=%s", i, len(rows), err, digestRows(rows))
	}
	t.Fatalf("nodes did not converge on %d typed rows with equal digests within %v", want, timeout)
}

func insertRows(c *harness.Cluster, node, start, count int, prefix string) error {
	rows := make([]harness.TypedContentionRow, 0, count)
	for i := 0; i < count; i++ {
		n := start + i
		rows = append(rows, harness.TypedContentionRow{ID: typedRowID(n), Name: fmt.Sprintf("%s-%d", prefix, n)})
	}
	if count == 1 {
		return c.TypedContentionInsert(node, rows[0])
	}
	return c.TypedContentionInsertMany(node, rows)
}

func typedRowID(n int) string { return fmt.Sprintf("%08x-0000-4000-8000-%012x", n, n) }

func digestRows(rows []harness.TypedContentionRow) string {
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	h := sha256.New()
	for _, row := range rows {
		_, _ = fmt.Fprintf(h, "%q\x00%q\x00%q\x00%d\n", row.ID, row.Name, row.Phone, row.Score)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func countPrefix(rows []harness.TypedContentionRow, prefix string) int {
	n := 0
	for _, row := range rows {
		if strings.HasPrefix(row.Name, prefix) {
			n++
		}
	}
	return n
}

func snapshotCounter(t *testing.T, apiAddr, name string) float64 {
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
	if value, ok := harness.MetricValueFrom(string(raw), name); ok {
		return value
	}
	t.Fatalf("counter %s not present in /metrics", name)
	return 0
}
