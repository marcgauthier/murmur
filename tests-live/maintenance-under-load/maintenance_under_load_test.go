// Maintenance-under-load acceptance: sustained concurrent writers run with
// zero failed writes while log-GC maintenance is forced via tight
// retention. Every claimed maintenance action is counter-observed
// (spedsql_gc_runs_total and spedsql_gc_log_collected_total advance on
// every node; spedsql_gc_failures_total stays flat), and afterwards all
// nodes converge on exact counts plus equal id-ordered digests.
//
// Maintenance triggers inventoried (read from product source):
//   - Origin-log GC on a 30s ticker (db.go gcLoop/gcOnce): CLAIMED, forced
//     by MinLogRetentionMs=1000ms with continuous writes, observed via
//     gc_runs_total + gc_log_collected_total on every node.
//   - Receipt collection inside gcOnce every 10th round (~5min): NOT
//     claimed, too slow for the default profile.
//   - Periodic durability sync ticker: NOT claimed, only runs in
//     DurabilityAsync mode (default is synchronous).
//   - FilesGC: NOT claimed, manual DB method with no live endpoint.
//   - Snapshot resync / anti-entropy: NOT claimed, on-demand only (needs a
//     gap or snapshot-required, not steady load).
//   - Spool compaction: NOT claimed, automatic with no action counter
//     (gauges only).
package maintenanceunderload_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestMaintenanceUnderSustainedLoad(t *testing.T) {
	seed := envInt("MURMUR_MAINT_LOAD_SEED", 120)
	maxInserts := envInt("MURMUR_MAINT_LOAD_MAX_INSERTS", 4000)
	gcTimeout := envDur("MURMUR_MAINT_LOAD_GC_TIMEOUT", 150*time.Second)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "maintenance-under-load",
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

	// Seed a base row set and converge before the load phase.
	for i := 0; i < seed; i++ {
		if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{
			ID: rowID(i), Name: fmt.Sprintf("seed-%d", i),
		}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	waitConverged(t, cluster, seed, 60*time.Second)

	runsBefore := metricAll(t, cluster, "spedsql_gc_runs_total")
	collectedBefore := metricAll(t, cluster, "spedsql_gc_log_collected_total")
	failuresBefore := metricAll(t, cluster, "spedsql_gc_failures_total")

	// Sustained concurrent writers on disjoint ranges: inserts until the
	// per-writer cap, then updates cycling their own rows so load never
	// stops while maintenance is awaited. Zero failed writes allowed.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	okCounts := make([]atomic.Int64, len(cluster.Nodes))
	errCounts := make([]atomic.Int64, len(cluster.Nodes))
	for w := range cluster.Nodes {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			base := 1 << 20 * (w + 1)
			n := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				var err error
				if n < maxInserts {
					id := base + n
					err = cluster.TypedContentionInsert(w, harness.TypedContentionRow{
						ID: rowID(id), Name: fmt.Sprintf("w%d-%d", w, n),
					})
				} else {
					id := base + (n % maxInserts)
					err = cluster.TypedContentionUpdate(w, rowID(id), "name", fmt.Sprintf("w%d-u%d", w, n))
				}
				if err != nil {
					errCounts[w].Add(1)
					return
				}
				okCounts[w].Add(1)
				n++
			}
		}(w)
	}

	// Maintenance must run WHILE writers are active: wait until GC passes
	// and batch collection advance on EVERY node, with writers still
	// making progress throughout.
	waitMaintenance(t, cluster, runsBefore, collectedBefore, gcTimeout)
	progressMid := make([]int64, len(cluster.Nodes))
	for w := range cluster.Nodes {
		progressMid[w] = okCounts[w].Load()
	}
	close(stop)
	wg.Wait()
	for w := range cluster.Nodes {
		t.Logf("writer %d: %d ok, %d failed (progress when GC observed: %d)",
			w, okCounts[w].Load(), errCounts[w].Load(), progressMid[w])
		if got := errCounts[w].Load(); got != 0 {
			t.Fatalf("writer %d had %d failed writes under maintenance; want zero", w, got)
		}
		if progressMid[w] == 0 {
			t.Fatalf("writer %d made no progress before GC was observed; load/maintenance did not overlap", w)
		}
	}

	// No GC pass may have failed anywhere.
	failuresAfter := metricAll(t, cluster, "spedsql_gc_failures_total")
	for i := range cluster.Nodes {
		if failuresAfter[i] != failuresBefore[i] {
			t.Fatalf("node %d gc failures %d -> %d during load", i, failuresBefore[i], failuresAfter[i])
		}
	}

	// Full convergence: exact counts plus equal id-ordered digests.
	wantCount := seed
	for w := range cluster.Nodes {
		wantCount += minInt(int(okCounts[w].Load()), maxInserts)
	}
	waitConverged(t, cluster, wantCount, 90*time.Second)

	// A post-maintenance write replicates everywhere.
	if err := cluster.TypedContentionInsert(2, harness.TypedContentionRow{
		ID: rowID(seed + 1<<22), Name: "post-maint",
	}); err != nil {
		t.Fatalf("post-maintenance insert: %v", err)
	}
	waitConverged(t, cluster, wantCount+1, 30*time.Second)
	t.Log("maintenance-under-load proven: GC ran on every node under zero-error load, full convergence")
}

// waitMaintenance polls until gc_runs_total AND gc_log_collected_total
// have advanced past baseline on EVERY node.
func waitMaintenance(t *testing.T, c *harness.Cluster, runsBefore, collectedBefore []int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allRuns, allCollected := true, true
		for i := range c.Nodes {
			if metricInt(t, c.Nodes[i].APIAddr, "spedsql_gc_runs_total") <= runsBefore[i] {
				allRuns = false
			}
			if metricInt(t, c.Nodes[i].APIAddr, "spedsql_gc_log_collected_total") <= collectedBefore[i] {
				allCollected = false
			}
		}
		if allRuns && allCollected {
			for i := range c.Nodes {
				t.Logf("node %d: gc runs %d->%d, collected %d->%d",
					i, runsBefore[i], metricInt(t, c.Nodes[i].APIAddr, "spedsql_gc_runs_total"),
					collectedBefore[i], metricInt(t, c.Nodes[i].APIAddr, "spedsql_gc_log_collected_total"))
			}
			return
		}
		time.Sleep(2 * time.Second)
	}
	for i := range c.Nodes {
		t.Logf("node %d at timeout: runs=%d (base %d) collected=%d (base %d) failures=%d local_commits=%s state=%s",
			i, metricInt(t, c.Nodes[i].APIAddr, "spedsql_gc_runs_total"), runsBefore[i],
			metricInt(t, c.Nodes[i].APIAddr, "spedsql_gc_log_collected_total"), collectedBefore[i],
			metricInt(t, c.Nodes[i].APIAddr, "spedsql_gc_failures_total"),
			metricRaw(t, c.Nodes[i].APIAddr, "spedsql_local_commits_total"),
			nodeState(t, c.Nodes[i].APIAddr))
	}
	t.Fatalf("log-GC maintenance did not run+collect on every node within %v", timeout)
}

func rowID(n int) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", n, n)
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
			d := contentionDigest(rows)
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
		t.Logf("node %d at timeout: count=%d digest=%s", i, len(rows), contentionDigest(rows))
	}
	t.Fatalf("nodes did not converge on %d typed rows with equal digests within %v", want, timeout)
}

func contentionDigest(rows []harness.TypedContentionRow) string {
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	h := sha256.New()
	for _, row := range rows {
		_, _ = fmt.Fprintf(h, "%q\x00%q\x00%q\x00%d\n", row.ID, row.Name, row.Phone, row.Score)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func metricAll(t *testing.T, c *harness.Cluster, name string) []int64 {
	t.Helper()
	out := make([]int64, len(c.Nodes))
	for i := range c.Nodes {
		out[i] = metricInt(t, c.Nodes[i].APIAddr, name)
	}
	return out
}

func metricInt(t *testing.T, apiAddr, name string) int64 {
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
		return int64(value)
	}
	t.Fatalf("counter %s not present in /metrics", name)
	return 0
}

// metricRaw returns the raw exposition line for name (diagnostics only).
func metricRaw(t *testing.T, apiAddr, name string) string {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		return "unreachable: " + err.Error()
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "unreadable: " + err.Error()
	}
	if samples, ok := harness.MetricSamples(string(raw)); ok {
		for _, sample := range samples {
			if sample.Name == name {
				return fmt.Sprintf("%s labels=%v value=%g", sample.Name, sample.Labels, sample.Value)
			}
		}
	}
	return "absent"
}

// nodeState fetches /v1/status state (diagnostics only).
func nodeState(t *testing.T, apiAddr string) string {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		return "unreachable: " + err.Error()
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "unreadable: " + err.Error()
	}
	return string(raw)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(harness.GetEnv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

func envDur(name string, fallback time.Duration) time.Duration {
	if v := harness.GetEnv(name); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}
