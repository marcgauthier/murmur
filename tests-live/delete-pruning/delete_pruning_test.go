// Delete/pruning acceptance: concurrent deletes, resurrections, and
// updates on a shared row set converge under aggressive log retention.
// A log-GC pass is forced to run (observed via spedsql_gc_runs_total)
// while tombstone batches are live, and afterwards all nodes agree on
// ordered SHA-256 digests plus exact row counts.
//
// NOTE on scope: there is no stored-tombstone pruning API in the
// product (tombstone markers stay in state; only origin-log batches
// are GC-collected). This suite therefore proves delete/resurrect
// convergence plus log-GC collection of the delete batches, and does
// not claim stored-tombstone reclamation.
package deletepruning_test

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

func TestConcurrentDeletesResurrectionsAndUpdatesConverge(t *testing.T) {
	keys := envInt("SPEDSQL_DELETE_PRUNING_KEYS", 300)
	if keys%3 != 0 {
		t.Fatalf("SPEDSQL_DELETE_PRUNING_KEYS=%d must be a multiple of 3", keys)
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
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "del_rows",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
	})

	// Seed the shared row set on node1 and converge.
	for i := 0; i < keys; i++ {
		if err := cluster.ExecSQL(0, "INSERT INTO del_rows (id, name) VALUES (?, ?)",
			fmt.Sprintf("%032x", i), fmt.Sprintf("seed-%d", i)); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	waitConverged(t, cluster, "del_rows", keys, 60*time.Second)

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
			if err := cluster.ExecSQL(0, "DELETE FROM del_rows WHERE id = ?", fmt.Sprintf("%032x", i)); err != nil {
				errCh <- fmt.Errorf("node1 delete %d: %w", i, err)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := third; i < 2*third; i++ {
			if err := cluster.ExecSQL(1, "UPDATE del_rows SET name = ? WHERE id = ?", fmt.Sprintf("upd-%d", i), fmt.Sprintf("%032x", i)); err != nil {
				errCh <- fmt.Errorf("node2 update %d: %w", i, err)
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 2 * third; i < keys; i++ {
			if err := cluster.ExecSQL(2, "DELETE FROM del_rows WHERE id = ?", fmt.Sprintf("%032x", i)); err != nil {
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
	waitConverged(t, cluster, "del_rows", third, 60*time.Second)
	spotValue := querySingleValue(t, cluster, 0, "SELECT name FROM del_rows WHERE id = ?", fmt.Sprintf("%032x", third))
	if want := fmt.Sprintf("upd-%d", third); spotValue != want {
		t.Fatalf("middle-third value = %q, want %q (concurrent update lost)", spotValue, want)
	}

	// Phase 2: resurrect the first third with new values from node2.
	for i := 0; i < third; i++ {
		if err := cluster.ExecSQL(1, "INSERT INTO del_rows (id, name) VALUES (?, ?)",
			fmt.Sprintf("%032x", i), fmt.Sprintf("res-%d", i)); err != nil {
			t.Fatalf("resurrect %d: %v", i, err)
		}
	}
	waitConverged(t, cluster, "del_rows", 2*third, 60*time.Second)

	// Phase 3: delete the middle third from node1. Survivors: the
	// resurrected first third only.
	for i := third; i < 2*third; i++ {
		if err := cluster.ExecSQL(0, "DELETE FROM del_rows WHERE id = ?", fmt.Sprintf("%032x", i)); err != nil {
			t.Fatalf("delete middle %d: %v", i, err)
		}
	}
	waitConverged(t, cluster, "del_rows", third, 60*time.Second)

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
		n, err := cluster.QueryRowCount(i, "del_rows")
		if err != nil || n != wantCount {
			t.Fatalf("node %d count = %d (err=%v), want %d", i, n, err, wantCount)
		}
	}
	wantDigest, err := cluster.ComputeTableDigest(0, "del_rows", "id")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(cluster.Nodes); i++ {
		d, err := cluster.ComputeTableDigest(i, "del_rows", "id")
		if err != nil || d != wantDigest {
			t.Fatalf("node %d digest = %s (err=%v), want %s", i, d, err, wantDigest)
		}
	}
	// Content proof: resurrected rows present with new values, deleted
	// rows absent, on every node.
	for i := range cluster.Nodes {
		got := querySingleValue(t, cluster, i, "SELECT name FROM del_rows WHERE id = ?", fmt.Sprintf("%032x", 0))
		if got != "res-0" {
			t.Fatalf("node %d resurrected value = %q, want %q", i, got, "res-0")
		}
		res, err := cluster.QuerySQL(i, "SELECT count(*) FROM del_rows WHERE id = ?", fmt.Sprintf("%032x", third))
		if err != nil {
			t.Fatal(err)
		}
		if c := fmt.Sprint(res.Rows[0][0]); c != "0" {
			t.Fatalf("node %d deleted-row count = %s, want 0", i, c)
		}
	}

	// A post-prune write replicates everywhere.
	if err := cluster.ExecSQL(2, "INSERT INTO del_rows (id, name) VALUES (?, ?)",
		fmt.Sprintf("%032x", keys+1), "post-prune"); err != nil {
		t.Fatalf("post-prune insert: %v", err)
	}
	waitConverged(t, cluster, "del_rows", wantCount+1, 30*time.Second)
}

func querySingleValue(t *testing.T, c *harness.Cluster, idx int, query string, args ...any) string {
	t.Helper()
	res, err := c.QuerySQL(idx, query, args...)
	if err != nil {
		t.Fatalf("node %d query: %v", idx, err)
	}
	if len(res.Rows) != 1 || len(res.Rows[0]) == 0 || res.Rows[0][0] == nil {
		t.Fatalf("node %d query returned %v rows, want 1 value", idx, len(res.Rows))
	}
	return fmt.Sprint(res.Rows[0][0])
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
		time.Sleep(100 * time.Millisecond)
	}
	for i := range c.Nodes {
		n, _ := c.QueryRowCount(i, table)
		d, _ := c.ComputeTableDigest(i, table, "id")
		t.Logf("node %d at timeout: count=%d digest=%s", i, n, d)
	}
	t.Fatalf("nodes did not converge on %d %s rows with equal digests within %v", want, table, timeout)
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
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || line[0] == '#' || !strings.HasPrefix(line, name) {
			continue
		}
		rest := strings.TrimSpace(strings.TrimPrefix(line, name))
		if i := strings.LastIndex(rest, " "); i >= 0 {
			rest = rest[i+1:]
		}
		v, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			// Counters render as integers, but tolerate float form.
			f, ferr := strconv.ParseFloat(rest, 64)
			if ferr != nil {
				t.Fatalf("parse %s: %v", name, err)
			}
			return int64(f)
		}
		return v
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
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}
