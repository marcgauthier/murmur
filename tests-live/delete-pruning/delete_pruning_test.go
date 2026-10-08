// Delete/pruning acceptance: concurrent deletes, resurrections, and
// updates on a shared row set converge under aggressive log retention.
// A log-GC pass is forced to run (observed via spedsql_gc_runs_total)
// while tombstone batches are live, and afterwards all nodes agree on
// typed record contents plus exact row counts.
//
// NOTE on scope: there is no stored-tombstone pruning API in the
// product (tombstone markers stay in state; only origin-log batches
// are GC-collected). This suite therefore proves delete/resurrect
// convergence plus log-GC collection of the delete batches, and does
// not claim stored-tombstone reclamation.
package deletepruning_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestConcurrentDeletesResurrectionsAndUpdatesConverge(t *testing.T) {
	keys := envInt("MURMUR_DELETE_PRUNING_KEYS", 300)
	if keys%3 != 0 {
		t.Fatalf("MURMUR_DELETE_PRUNING_KEYS=%d must be a multiple of 3", keys)
	}
	third := keys / 3
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "delete-pruning",
		NumNodes:    3,
		AwaitUnlock: true,
		Replication: &harness.ReplicationOptions{
			MinLogRetentionMs:        1000,
			MaxOfflineLogRetentionMs: 20000,
			MinRetainedBatches:       10,
		},
		TypedRecords:    true,
		TypedContention: true,
	})

	// Seed the shared row set on node1 and converge.
	for i := 0; i < keys; i++ {
		if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{
			ID: deleteRowID(i), Name: fmt.Sprintf("seed-%d", i),
		}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	waitConverged(t, cluster, keys, 60*time.Second)

	gcRunsBefore := metricInt(t, cluster.Nodes[0].APIAddr, "spedsql_gc_runs_total")
	gcCollectedBefore := metricInt(t, cluster.Nodes[0].APIAddr, "spedsql_gc_log_collected_total")

	// Phase 1, concurrent on disjoint thirds: node1 deletes the first
	// third, node2 updates the middle third, node3 deletes the last
	// third. Only the middle third survives.
	var wg sync.WaitGroup
	errCh := make(chan error, 3)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < third; i++ {
			if err := cluster.TypedContentionDelete(0, deleteRowID(i)); err != nil {
				errCh <- fmt.Errorf("node1 delete %d: %w", i, err)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := third; i < 2*third; i++ {
			if err := cluster.TypedContentionUpdate(1, deleteRowID(i), "name", fmt.Sprintf("upd-%d", i)); err != nil {
				errCh <- fmt.Errorf("node2 update %d: %w", i, err)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 2 * third; i < keys; i++ {
			if err := cluster.TypedContentionDelete(2, deleteRowID(i)); err != nil {
				errCh <- fmt.Errorf("node3 delete %d: %w", i, err)
				return
			}
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	// Anti-vacuity: the deletes must actually remove rows everywhere.
	waitConverged(t, cluster, third, 60*time.Second)
	spot, err := cluster.TypedContentionRead(0, deleteRowID(third))
	if want := fmt.Sprintf("upd-%d", third); err != nil || spot.Name != want {
		t.Fatalf("middle-third value = %q, err=%v; want %q (concurrent update lost)", spot.Name, err, want)
	}

	// Phase 2: resurrect the first third with new values from node2.
	for i := 0; i < third; i++ {
		if err := cluster.TypedContentionInsert(1, harness.TypedContentionRow{
			ID: deleteRowID(i), Name: fmt.Sprintf("res-%d", i),
		}); err != nil {
			t.Fatalf("resurrect %d: %v", i, err)
		}
	}
	waitConverged(t, cluster, 2*third, 60*time.Second)

	// Phase 3: delete the middle third from node1. Survivors: the
	// resurrected first third only.
	for i := third; i < 2*third; i++ {
		if err := cluster.TypedContentionDelete(0, deleteRowID(i)); err != nil {
			t.Fatalf("delete middle %d: %v", i, err)
		}
	}
	waitConverged(t, cluster, third, 60*time.Second)

	// Force a GC pass to run over the tombstone batches: the log GC
	// ticker fires every 30s, so poll until the run counter advances.
	// (There is no admin trigger for an on-demand pass.)
	waitMetricAdvanced(t, cluster.Nodes[0].APIAddr, "spedsql_gc_runs_total", gcRunsBefore, 150*time.Second)
	collectedAfter := metricInt(t, cluster.Nodes[0].APIAddr, "spedsql_gc_log_collected_total")
	t.Logf("gc runs advanced; log batches collected during test: %d", collectedAfter-gcCollectedBefore)
	if collectedAfter <= gcCollectedBefore {
		t.Fatalf("no log batches collected (before=%d after=%d); GC pass did not prune delete batches", gcCollectedBefore, collectedAfter)
	}

	// Identical ordered digests and exact row counts on all nodes.
	wantCount := third
	for i := range cluster.Nodes {
		rows, err := cluster.TypedContentionRows(i)
		if err != nil || len(rows) != wantCount {
			t.Fatalf("node %d count = %d (err=%v), want %d", i, len(rows), err, wantCount)
		}
	}
	wantDigest, err := contentionDigest(cluster, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(cluster.Nodes); i++ {
		d, err := contentionDigest(cluster, i)
		if err != nil || d != wantDigest {
			t.Fatalf("node %d digest = %s (err=%v), want %s", i, d, err, wantDigest)
		}
	}
	// Content proof: resurrected rows present with new values, deleted
	// rows absent, on every node.
	for i := range cluster.Nodes {
		got, err := cluster.TypedContentionRead(i, deleteRowID(0))
		if err != nil || got.Name != "res-0" {
			t.Fatalf("node %d resurrected value = %q (err=%v), want %q", i, got.Name, err, "res-0")
		}
		if absent, err := cluster.TypedContentionRead(i, deleteRowID(third)); err == nil || !strings.Contains(err.Error(), "failed (404)") {
			t.Fatalf("node %d deleted row lookup = %+v, %v; want not found", i, absent, err)
		}
	}

	// A post-prune write replicates everywhere.
	if err := cluster.TypedContentionInsert(2, harness.TypedContentionRow{
		ID: deleteRowID(keys + 1), Name: "post-prune",
	}); err != nil {
		t.Fatalf("post-prune insert: %v", err)
	}
	waitConverged(t, cluster, wantCount+1, 30*time.Second)
}

func deleteRowID(n int) string {
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
			d, err := typedRowsDigest(rows)
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
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge on %d typed rows with equal names within %v", want, timeout)
}

func contentionDigest(c *harness.Cluster, node int) (string, error) {
	rows, err := c.TypedContentionRows(node)
	if err != nil {
		return "", err
	}
	return typedRowsDigest(rows)
}

func typedRowsDigest(rows []harness.TypedContentionRow) (string, error) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	encoded, err := json.Marshal(rows)
	return string(encoded), err
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

func waitMetricAdvanced(t *testing.T, apiAddr, name string, before int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := metricInt(t, apiAddr, name); got > before {
			t.Logf("%s advanced %d -> %d", name, before, got)
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("%s did not advance past %d within %v", name, before, timeout)
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(harness.GetEnv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}
