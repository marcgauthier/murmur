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
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestStaleNodeRejoinsViaSnapshot(t *testing.T) {
	// NOTE: the table must ride in the replicated SchemaConfig. Tables
	// created only from schema.sql exist in the local engine but never
	// enter the replication registry, so their writes stay local-only and
	// peers silently skip them as unknown tables.
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "snapshot-resync",
		NumNodes:    3,
		AwaitUnlock: true,
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

	// Phase 1: node3 writes acknowledged rows, then all three converge.
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%032x", 1000+i)
		if err := cluster.ExecSQL(2, "INSERT INTO snap_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("stale-%d", i)); err != nil {
			t.Fatalf("node3 pre-stop write: %v", err)
		}
	}
	waitConverged(t, cluster, "snap_rows", 5, 30*time.Second)

	// Phase 2: node3 stops; survivors write far past retention.
	cluster.StopNode(2)
	for i := 0; i < 60; i++ {
		id := fmt.Sprintf("%032x", 2000+i)
		if err := cluster.ExecSQL(0, "INSERT INTO snap_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("fresh-%d", i)); err != nil {
			t.Fatalf("survivor write: %v", err)
		}
	}
	if _, err := waitRowCount(t, cluster, 0, "snap_rows", 65, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := waitRowCount(t, cluster, 1, "snap_rows", 65, 30*time.Second); err != nil {
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
	waitConverged(t, cluster, "snap_rows", 65, 90*time.Second)

	// The rejoin must have used the snapshot path, not log catch-up.
	if got := snapshotCounter(t, cluster.Nodes[2].APIAddr, "spedsql_repl_snapshots_received_total"); got < 1 {
		t.Fatalf("node3 snapshots received = %v, want >= 1 (log catch-up would hide a snapshot-path regression)", got)
	}

	// Node3's acknowledged pre-stop state survives the snapshot merge.
	res, err := cluster.QuerySQL(2, "SELECT name FROM snap_rows WHERE name LIKE 'stale-%'")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 5 {
		t.Fatalf("node3 stale rows = %d, want 5", len(res.Rows))
	}
}

// Two stale nodes restarting together request snapshots concurrently from
// the same survivor. The source serves one transfer per peer at a time and
// answers the other with an explicit busy deferral; the receiver backs off
// and the stall watchdog re-requests, so both rejoins converge instead of
// stalling on a silently dropped request.
func TestDualStaleNodesRejoinViaSnapshot(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "snapshot-resync-dual",
		NumNodes:    3,
		AwaitUnlock: true,
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

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%032x", 1000+i)
		if err := cluster.ExecSQL(2, "INSERT INTO snap_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("stale-%d", i)); err != nil {
			t.Fatalf("node3 pre-stop write: %v", err)
		}
	}
	waitConverged(t, cluster, "snap_rows", 5, 30*time.Second)

	cluster.StopNode(1)
	cluster.StopNode(2)
	for i := 0; i < 60; i++ {
		id := fmt.Sprintf("%032x", 2000+i)
		if err := cluster.ExecSQL(0, "INSERT INTO snap_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("fresh-%d", i)); err != nil {
			t.Fatalf("survivor write: %v", err)
		}
	}
	if _, err := waitRowCount(t, cluster, 0, "snap_rows", 65, 30*time.Second); err != nil {
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
	waitConverged(t, cluster, "snap_rows", 65, 120*time.Second)

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

func waitRowCount(t *testing.T, c *harness.Cluster, idx int, table string, want int, timeout time.Duration) (int, error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n, err := c.QueryRowCount(idx, table)
		if err == nil && n == want {
			return n, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	n, _ := c.QueryRowCount(idx, table)
	return n, fmt.Errorf("node %d %s count = %d, want %d within %v", idx, table, n, want, timeout)
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
			d, err := c.ComputeTableDigest(i, table, "name")
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
		d, derr := c.ComputeTableDigest(i, table, "name")
		t.Logf("node %d at timeout: count=%d countErr=%v digest=%s digestErr=%v", i, n, nerr, d, derr)
	}
	t.Fatalf("nodes did not converge on %d %s rows with equal digests within %v", want, table, timeout)
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
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || line[0] == '#' {
			continue
		}
		if !strings.HasPrefix(line, name) {
			continue
		}
		rest := strings.TrimSpace(strings.TrimPrefix(line, name))
		// Tolerate `name{labels} value` as well as `name value`.
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
