// Graceful shutdown under load: SIGTERM mid-write and mid-snapshot-apply.
//
// Three daemons mesh with aggressive log retention. Node1 takes SIGTERM
// while a write is in flight; node3 restarts stale (log ranges GC'd, so it
// must snapshot-resync) and takes SIGTERM while the snapshot transfer is
// in progress. Both restarts must be clean (shutdown markers in the log),
// fast, lose no acknowledged rows, and the mesh must reconverge with
// identical digests. The snapshot path is proven via the receiver's
// Prometheus snapshot counter, not merely by convergence.
package gracefulshutdown_test

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestGracefulShutdownMidWriteAndMidSnapshot(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "graceful-shutdown",
		NumNodes:    3,
		AwaitUnlock: true,
		Replication: &harness.ReplicationOptions{
			MinLogRetentionMs:        1000,
			MaxOfflineLogRetentionMs: 20000,
			MinRetainedBatches:       10,
		},
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "gs_rows",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
				{Name: "payload", Type: schema.ColText, Nullable: true},
			},
		}}},
	})

	// Baseline: 5 rows converge before any shutdown.
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%032x", i)
		if err := cluster.ExecSQL(0, "INSERT INTO gs_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("base-%d", i)); err != nil {
			t.Fatalf("baseline write: %v", err)
		}
	}
	waitConverged(t, cluster, "gs_rows", 5, 30*time.Second)

	// Phase A: all three nodes under write load; SIGTERM node1 mid-write.
	acknowledged := map[string]struct{}{}
	for i := 0; i < 5; i++ {
		acknowledged[fmt.Sprintf("base-%d", i)] = struct{}{}
	}
	var ackMu sync.Mutex
	stop := make(chan struct{})
	var writers sync.WaitGroup
	stopWriters := sync.OnceFunc(func() { close(stop); writers.Wait() })
	defer stopWriters()
	var active [3]atomic.Int64
	var seqs [3]atomic.Int64
	for node := range cluster.Nodes {
		writers.Add(1)
		go func(node int) {
			defer writers.Done()
			ticker := time.NewTicker(25 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
				seq := seqs[node].Add(1)
				value := fmt.Sprintf("load-n%d-%08d", node+1, seq)
				id := fmt.Sprintf("%032x", int64(node+1)*1_000_000_000+seq)
				active[node].Add(1)
				err := cluster.ExecSQL(node, "INSERT INTO gs_rows (id, name) VALUES (?, ?)", id, value)
				active[node].Add(-1)
				if err == nil {
					ackMu.Lock()
					acknowledged[value] = struct{}{}
					ackMu.Unlock()
				}
			}
		}(node)
	}
	time.Sleep(4 * time.Second)

	// Prove a write was actually in flight when SIGTERM landed, so a
	// vacuous pass (idle node, clean exit, nothing at stake) is detectable.
	deadline := time.Now().Add(5 * time.Second)
	for active[0].Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if active[0].Load() == 0 {
		t.Fatalf("node1 had no write in flight before SIGTERM")
	}
	t.Logf("SIGTERM node1 with %d writes in flight", active[0].Load())
	markersBefore := countMarkers(t, cluster.Nodes[0].LogFile)
	stopStart := time.Now()
	cluster.StopNode(0)
	stopElapsed := time.Since(stopStart)
	assertCleanShutdown(t, cluster.Nodes[0].LogFile, markersBefore, "node1 mid-write SIGTERM")
	t.Logf("node1 graceful stop took %v", stopElapsed)

	restartStart := time.Now()
	cluster.StartNode(0)
	cluster.UnlockNode(0, cluster.Nodes[0].KeyHex)
	cluster.WaitNodeReady(0)
	restartElapsed := time.Since(restartStart)
	t.Logf("node1 restart took %v", restartElapsed)
	if restartElapsed > 60*time.Second {
		t.Fatalf("node1 restart took %v, want fast restart under 60s", restartElapsed)
	}

	// Positive control: honest load converges across the SIGTERM restart.
	stopWriters()
	ackMu.Lock()
	total := len(acknowledged)
	ackMu.Unlock()
	waitConverged(t, cluster, "gs_rows", total, 90*time.Second)
	assertAcknowledgedPresent(t, cluster, acknowledged)

	// Phase B: node3 goes stale under aggressive retention while the
	// survivors write a fat snapshot payload, then SIGTERM lands
	// mid-snapshot-apply on its restart.
	cluster.StopNode(2)
	const bulkRows = 100
	for i := 0; i < bulkRows; i++ {
		id := fmt.Sprintf("%032x", 5000+i)
		if err := cluster.ExecSQL(0, "INSERT INTO gs_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("fresh-%d", i)); err != nil {
			t.Fatalf("survivor write: %v", err)
		}
		ackMu.Lock()
		acknowledged[fmt.Sprintf("fresh-%d", i)] = struct{}{}
		ackMu.Unlock()
	}
	// Fat values widen the snapshot transfer window so SIGTERM can land
	// inside it deterministically: ~2MB across 8 rows keeps the chunked
	// transfer + SQLite apply busy long past the first 5ms poll, on a
	// fast box as well as under contention.
	big := strings.Repeat("SPeD-SQL-snapshot-filler-", 256*1024/len("SPeD-SQL-snapshot-filler-"))
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("%032x", 6000+i)
		name := fmt.Sprintf("fat-%d", i)
		if err := cluster.ExecSQL(1, "INSERT INTO gs_rows (id, name, payload) VALUES (?, ?, ?)", id, name, big); err != nil {
			t.Fatalf("fat write: %v", err)
		}
		ackMu.Lock()
		acknowledged[name] = struct{}{}
		ackMu.Unlock()
	}
	ackMu.Lock()
	want := len(acknowledged)
	ackMu.Unlock()
	waitRowCount(t, cluster, 0, "gs_rows", want, 30*time.Second)
	waitRowCount(t, cluster, 1, "gs_rows", want, 30*time.Second)

	// Member-deadline expiry plus a polled GC pass (not a fixed sleep:
	// under contention the 30s GC tick slips, so a fixed wait can elapse
	// without any pass collecting node3's ranges, and the rejoin
	// silently takes the log path). First sleep out node3's 20s member
	// deadline so its ranges stop gating GC, then poll a survivor's GC
	// counters until a pass runs after expiry and collects log batches.
	gcRunsBefore := metricValue(t, cluster.Nodes[0].APIAddr, "spedsql_gc_runs_total")
	collectedBefore := metricValue(t, cluster.Nodes[0].APIAddr, "spedsql_gc_log_collected_total")
	deadlineWait := time.Duration(envSeconds("SPEDSQL_GRACEFUL_MEMBER_DEADLINE_SECONDS", 25)) * time.Second
	t.Logf("waiting %v for member-deadline expiry", deadlineWait)
	time.Sleep(deadlineWait)
	waitGCPass(t, cluster.Nodes[0].APIAddr, gcRunsBefore, 120*time.Second)
	collectedAfter := metricValue(t, cluster.Nodes[0].APIAddr, "spedsql_gc_log_collected_total")
	if collectedAfter <= collectedBefore {
		t.Fatalf("a post-expiry GC pass ran but collected no log batches (%.0f -> %.0f); node3's ranges are still pinned and the snapshot path is not forced",
			collectedBefore, collectedAfter)
	}
	t.Logf("GC collected log batches after expiry (%.0f -> %.0f); snapshot path forced", collectedBefore, collectedAfter)

	markersBefore = countMarkers(t, cluster.Nodes[2].LogFile)
	cluster.StartNode(2)
	cluster.UnlockNode(2, cluster.Nodes[2].KeyHex)
	cluster.WaitNodeReady(2)

	// Catch the snapshot transfer in flight: a manifest (or chunks)
	// observed while no snapshot has completed yet. Poll fast; the fat
	// payload keeps the window open.
	caught, detail := waitSnapshotInFlight(t, cluster.Nodes[2].APIAddr, 60*time.Second)
	t.Logf("SIGTERM node3 during snapshot rejoin (midTransfer=%v %s)", caught, detail)
	if !caught {
		t.Fatalf("snapshot transfer completed before SIGTERM could land mid-apply (%s); widen the payload", detail)
	}
	cluster.StopNode(2)
	assertCleanShutdown(t, cluster.Nodes[2].LogFile, markersBefore, "node3 mid-snapshot SIGTERM")

	restartStart = time.Now()
	cluster.StartNode(2)
	cluster.UnlockNode(2, cluster.Nodes[2].KeyHex)
	cluster.WaitNodeReady(2)
	restartElapsed = time.Since(restartStart)
	t.Logf("node3 post-snapshot-interrupt restart took %v", restartElapsed)
	if restartElapsed > 60*time.Second {
		t.Fatalf("node3 restart took %v, want fast restart under 60s", restartElapsed)
	}

	waitConverged(t, cluster, "gs_rows", want, 120*time.Second)
	if got := metricValue(t, cluster.Nodes[2].APIAddr, "spedsql_repl_snapshots_received_total"); got < 1 {
		t.Fatalf("node3 snapshots received = %v, want >= 1 (log catch-up would hide a snapshot-path regression)", got)
	}
	assertAcknowledgedPresent(t, cluster, acknowledged)

	// Post-recovery honest write still replicates everywhere.
	postID := fmt.Sprintf("%032x", 999_999)
	if err := cluster.ExecSQL(0, "INSERT INTO gs_rows (id, name) VALUES (?, ?)", postID, "post-recovery"); err != nil {
		t.Fatalf("post-recovery write: %v", err)
	}
	ackMu.Lock()
	acknowledged["post-recovery"] = struct{}{}
	want = len(acknowledged)
	ackMu.Unlock()
	waitConverged(t, cluster, "gs_rows", want, 60*time.Second)
	assertAcknowledgedPresent(t, cluster, acknowledged)
	t.Logf("graceful shutdown proven: 2 SIGTERM restarts, %d rows intact, digests converge", want)
}

// waitGCPass polls the survivor's GC run counter until a pass completes
// after the recorded baseline, proving the collector actually ran
// (a fixed sleep cannot prove that under a slipping tick).
func waitGCPass(t *testing.T, apiAddr string, baseline float64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if metricValue(t, apiAddr, "spedsql_gc_runs_total") > baseline {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("no GC pass completed within %v (baseline %.0f)", timeout, baseline)
}

// waitSnapshotInFlight polls the receiver's snapshot counters until a
// transfer is observably in progress (manifest/chunks seen, none
// completed) or the deadline passes. It reports whether SIGTERM can
// still land inside the transfer.
func waitSnapshotInFlight(t *testing.T, apiAddr string, timeout time.Duration) (bool, string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		<-ticker.C
		manifests := metricValue(t, apiAddr, "spedsql_repl_snapshot_manifests_received_total")
		chunks := metricValue(t, apiAddr, "spedsql_repl_snapshot_chunks_received_total")
		done := metricValue(t, apiAddr, "spedsql_repl_snapshots_received_total")
		detail := fmt.Sprintf("manifests=%.0f chunks=%.0f completed=%.0f", manifests, chunks, done)
		if done >= 1 {
			return false, detail
		}
		if manifests >= 1 || chunks >= 1 {
			return true, detail
		}
	}
	return false, "no snapshot activity observed"
}

func countMarkers(t *testing.T, logFile string) int {
	t.Helper()
	raw, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log %s: %v", logFile, err)
	}
	return strings.Count(string(raw), "Node shutdown complete.")
}

func assertCleanShutdown(t *testing.T, logFile string, before int, what string) {
	t.Helper()
	raw, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("read log %s: %v", logFile, err)
	}
	after := strings.Count(string(raw), "Node shutdown complete.")
	if after <= before {
		t.Fatalf("%s: no new clean-shutdown marker in %s (before=%d after=%d)", what, logFile, before, after)
	}
	if !strings.Contains(string(raw), "shutting down...") {
		t.Fatalf("%s: no signal-received marker in %s", what, logFile)
	}
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
		n, _ := c.QueryRowCount(i, "gs_rows")
		t.Logf("node %d at timeout: count=%d", i, n)
	}
	t.Fatalf("nodes did not converge on %d %s rows with equal digests within %v", want, table, timeout)
}

func assertAcknowledgedPresent(t *testing.T, c *harness.Cluster, acknowledged map[string]struct{}) {
	t.Helper()
	for _, node := range c.Nodes {
		res, err := c.QuerySQL(node.Index, "SELECT name FROM gs_rows")
		if err != nil {
			t.Fatalf("%s recovery query: %v", node.Label, err)
		}
		present := make(map[string]struct{}, len(res.Rows))
		for _, row := range res.Rows {
			if len(row) > 0 {
				present[fmt.Sprint(row[0])] = struct{}{}
			}
		}
		for value := range acknowledged {
			if _, ok := present[value]; !ok {
				t.Fatalf("%s lost acknowledged write %q after restart", node.Label, value)
			}
		}
	}
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

func envSeconds(name string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(name)); err == nil && value > 0 {
		return value
	}
	return fallback
}
