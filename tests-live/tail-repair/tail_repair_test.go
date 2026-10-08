// Tail-repair acceptance: a node partitioned mid-write-burst misses a
// contiguous range of its peers' origin logs, then heals and repairs the
// missing ranges from peer logs. The repair must NOT use the snapshot
// path: the healed node's spedsql_repl_snapshots_received_total stays at
// zero while row counts and ordered digests converge everywhere.
package tailrepair_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestPartitionedNodeRepairsTailFromPeerLogs(t *testing.T) {
	burstRows := envInt("MURMUR_TAIL_REPAIR_BURST_ROWS", 100)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "tail-repair",
		NumNodes:        3,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
		// Generous retention: the missed range must stay comfortably
		// inside every peer's origin log, so log repair (not snapshot
		// resync) is the only legitimate healing path.
		Replication: &harness.ReplicationOptions{
			MinLogRetentionMs:        300000,
			MaxOfflineLogRetentionMs: 600000,
			MinRetainedBatches:       10000,
		},
	})

	// Baseline: 10 rows on node1, converged everywhere.
	const baseline = 10
	for i := 0; i < baseline; i++ {
		if err := insertMarker(cluster, 0, i, fmt.Sprintf("base-%d", i)); err != nil {
			t.Fatalf("baseline insert %d: %v", i, err)
		}
	}
	waitConverged(t, cluster, baseline, 30*time.Second)

	snapRecvBefore := metricValue(t, cluster.Nodes[2].APIAddr, "spedsql_repl_snapshots_received_total")
	snapReqBefore := metricValue(t, cluster.Nodes[2].APIAddr, "spedsql_repl_snapshot_required_received_total")
	t.Logf("node3 pre-partition snapshots_received=%v snapshot_required_received=%v", snapRecvBefore, snapReqBefore)

	// Partition node3 away from both peers in both directions.
	for _, pair := range [][2]int{{2, 0}, {0, 2}, {2, 1}, {1, 2}} {
		if err := cluster.RemovePeer(pair[0], pair[1]); err != nil {
			t.Fatalf("remove peer %d->%d: %v", pair[0], pair[1], err)
		}
	}
	waitConnectedPeers(t, cluster, 2, 0, 15*time.Second)

	// Isolation probe: one row on node1 must reach node2 but never node3.
	if err := insertMarker(cluster, 0, 9000, "probe"); err != nil {
		t.Fatalf("probe insert: %v", err)
	}
	waitRowCount(t, cluster, 1, baseline+1, 15*time.Second)
	time.Sleep(2 * time.Second) // negative check needs a settle margin
	if rows, err := cluster.TypedContentionRows(2); err != nil || len(rows) != baseline {
		t.Fatalf("partition leaked: node3 count = %d (err=%v), want %d", len(rows), err, baseline)
	}

	// Write burst on the survivors while node3 is isolated.
	for i := 0; i < burstRows; i++ {
		node := i % 2
		if err := insertMarker(cluster, node, 10000+i, fmt.Sprintf("burst-%d", i)); err != nil {
			t.Fatalf("burst insert %d: %v", i, err)
		}
	}
	want := baseline + 1 + burstRows
	waitRowCount(t, cluster, 0, want, 30*time.Second)
	waitRowCount(t, cluster, 1, want, 30*time.Second)
	// Anti-vacuity: node3 must actually be missing the contiguous range.
	if rows, err := cluster.TypedContentionRows(2); err != nil || len(rows) != baseline {
		t.Fatalf("node3 count = %d (err=%v), want %d (it must miss the burst range)", len(rows), err, baseline)
	}
	t.Logf("node3 missed %d rows while partitioned", want-baseline)

	// Heal and require full convergence.
	for _, pair := range [][2]int{{2, 0}, {0, 2}, {2, 1}, {1, 2}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			t.Fatalf("add peer %d->%d: %v", pair[0], pair[1], err)
		}
	}
	for i := range cluster.Nodes {
		waitConnectedPeers(t, cluster, i, 2, 30*time.Second)
	}
	waitConverged(t, cluster, want, 60*time.Second)

	// The healing must have used log range repair, not the snapshot path.
	if got := metricValue(t, cluster.Nodes[2].APIAddr, "spedsql_repl_snapshots_received_total"); got != snapRecvBefore {
		t.Fatalf("node3 snapshots_received = %v, want %v (healing must not snapshot-resync)", got, snapRecvBefore)
	}
	if got := metricValue(t, cluster.Nodes[2].APIAddr, "spedsql_repl_snapshot_required_received_total"); got != snapReqBefore {
		t.Fatalf("node3 snapshot_required_received = %v, want %v", got, snapReqBefore)
	}
	for i := range cluster.Nodes {
		if got := metricValue(t, cluster.Nodes[i].APIAddr, "spedsql_repl_snapshots_sent_total"); got != 0 {
			t.Fatalf("node%d snapshots_sent = %v, want 0 (no peer may serve a snapshot)", i+1, got)
		}
	}
	// Diagnostic (logged, not asserted): gap-pull activity on the healer.
	t.Logf("node3 gaps_detected=%v needs_sent=%v batches_received=%v",
		metricValue(t, cluster.Nodes[2].APIAddr, "spedsql_repl_gaps_detected_total"),
		metricValue(t, cluster.Nodes[2].APIAddr, "spedsql_repl_needs_sent_total"),
		metricValue(t, cluster.Nodes[2].APIAddr, "spedsql_repl_batches_received_total"))

	// Post-heal write on the healed node replicates everywhere.
	if err := insertMarker(cluster, 2, 20000, "post-heal"); err != nil {
		t.Fatalf("post-heal insert: %v", err)
	}
	waitConverged(t, cluster, want+1, 30*time.Second)
}

func insertMarker(c *harness.Cluster, node, id int, name string) error {
	rowID := fmt.Sprintf("%08x-0000-4000-8000-%012x", id, id)
	return c.TypedContentionInsert(node, harness.TypedContentionRow{ID: rowID, Name: name})
}

func waitRowCount(t *testing.T, c *harness.Cluster, idx int, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if rows, err := c.TypedContentionRows(idx); err == nil && len(rows) == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	rows, _ := c.TypedContentionRows(idx)
	t.Fatalf("node %d typed row count = %d, want %d within %v", idx, len(rows), want, timeout)
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
		time.Sleep(100 * time.Millisecond)
	}
	for i := range c.Nodes {
		rows, _ := c.TypedContentionRows(i)
		t.Logf("node %d at timeout: count=%d digest=%s", i, len(rows), digestRows(rows))
	}
	t.Fatalf("nodes did not converge on %d typed rows with equal digests within %v", want, timeout)
}

func digestRows(rows []harness.TypedContentionRow) string {
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return fmt.Sprintf("%#v", rows)
}

func waitConnectedPeers(t *testing.T, c *harness.Cluster, idx, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if connectedPeers(c.Nodes[idx].APIAddr) == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node %d connected peers = %d, want %d within %v", idx, connectedPeers(c.Nodes[idx].APIAddr), want, timeout)
}

func connectedPeers(apiAddr string) int {
	resp, err := http.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	var status struct {
		ConnectedPeers int `json:"connected_peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return -1
	}
	return status.ConnectedPeers
}

func metricValue(t *testing.T, apiAddr, name string) float64 {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		t.Fatalf("metrics %s: %v", apiAddr, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := harness.MetricValueFrom(string(raw), name); ok {
		return value
	}
	t.Fatalf("counter %s not present in /metrics", name)
	return 0
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(harness.GetEnv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}
