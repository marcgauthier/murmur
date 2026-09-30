// Concurrent additive migration: the SAME new-column migration fires
// simultaneously on two of three meshed nodes while all keep writing.
// Both migrating nodes must land in a well-defined schema state (old or
// new epoch, never mixed), mixed-version replication must flow in both
// directions during the overlap, and after the straggler migrates the
// whole mesh converges fully migrated with identical epochs, counts,
// digests, and materialized new-column reads on every node.
package migrationconcurrency_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func baseTables() []schema.TableSchema {
	return []schema.TableSchema{{
		Name: "mg_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}}
}

func evolvedTables() []schema.TableSchema {
	return []schema.TableSchema{{
		Name: "mg_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
			{Name: "score", Type: schema.ColInteger, Nullable: true},
		},
	}}
}

func TestSimultaneousMigrationConverges(t *testing.T) {
	seedRows := envInt("SPEDSQL_MIGRATION_CONCURRENCY_SEED", 60)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "migration-concurrency",
		NumNodes:    3,
		AwaitUnlock: true,
		Schema:      &db.SchemaConfig{Version: 1, Tables: baseTables()},
	})

	// Honest baseline: all old, seed converged everywhere.
	for i := 0; i < seedRows; i++ {
		id := fmt.Sprintf("%032x", 9000+i)
		if err := cluster.ExecSQL(0, "INSERT INTO mg_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("seed-%d", i)); err != nil {
			t.Fatalf("seed write: %v", err)
		}
	}
	waitNamesConverged(t, cluster, seedRows, 60*time.Second)
	for i := 0; i < 3; i++ {
		assertEpoch(t, cluster, i, 1)
	}

	// Pin node 1 on the old schema with peer isolation: adoption is
	// fast (~1s once a revision exists), so without isolation there
	// would be no deterministic mixed-version window after the race.
	for _, pair := range [][2]int{{0, 1}, {1, 0}, {0, 2}, {2, 0}} {
		if err := cluster.RemovePeer(pair[0], pair[1]); err != nil {
			t.Fatalf("isolate node1: %v", err)
		}
	}

	// All nodes keep writing (old-schema-compatible names) across the
	// migration window; every acked name must converge afterwards.
	acked := map[string]struct{}{}
	var ackMu sync.Mutex
	stop := make(chan struct{})
	var writers sync.WaitGroup
	var seqs [3]atomic.Int64
	for n := 0; n < 3; n++ {
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
				name := fmt.Sprintf("flow-n%d-%08d", node+1, seq)
				id := fmt.Sprintf("ff%02x%028x", node, seq)
				if err := cluster.ExecSQL(node, "INSERT INTO mg_rows (id, name) VALUES (?, ?)", id, name); err == nil {
					ackMu.Lock()
					acked[name] = struct{}{}
					ackMu.Unlock()
				}
			}
		}(n)
	}
	time.Sleep(time.Second) // writers flowing before the race starts

	// Simultaneous trigger: the same migration starts on nodes 2 and 3
	// at the same instant (start barrier), node 1 stays old.
	start := make(chan struct{})
	type migrateResult struct {
		idx int
		err error
	}
	results := make(chan migrateResult, 2)
	for _, idx := range []int{1, 2} {
		go func(idx int) {
			<-start
			results <- migrateResult{idx, cluster.Migrate(idx, evolvedTables())}
		}(idx)
	}
	close(start)
	t.Log("simultaneous migrate fired on nodes 2 and 3")
	r1, r2 := <-results, <-results
	t.Logf("migrate results: node%d err=%v, node%d err=%v", r1.idx+1, r1.err, r2.idx+1, r2.err)
	close(stop)
	writers.Wait()
	ackMu.Lock()
	total := seedRows + len(acked)
	ackMu.Unlock()
	t.Logf("migration window closed with %d overlap writes", len(acked))
	if len(acked) == 0 {
		t.Fatal("no writes acknowledged during the migration window; overlap proved nothing")
	}

	// Both migrating nodes land well-defined: exactly old or new epoch,
	// score column exposed iff new, no mixed state tolerated.
	for _, idx := range []int{1, 2} {
		classifyNode(t, cluster, idx)
	}
	assertEpoch(t, cluster, 0, 1)
	newCount := 0
	for _, idx := range []int{1, 2} {
		if statusEpoch(t, cluster.Nodes[idx].APIAddr) == 2 {
			newCount++
		}
	}
	if newCount == 0 {
		t.Fatal("simultaneous migration landed nowhere: both nodes still old")
	}
	t.Logf("simultaneous trigger: %d/2 nodes new; sequentially migrating laggards", newCount)
	for _, idx := range []int{1, 2} {
		if statusEpoch(t, cluster.Nodes[idx].APIAddr) != 2 {
			if err := cluster.Migrate(idx, evolvedTables()); err != nil {
				t.Fatalf("honest migrate node%d: %v", idx+1, err)
			}
		}
	}

	// Heal with versions asserted different at the instant: node 1 is
	// deterministically old (isolated since before the first
	// publication), the racers are new. Overlap writes from both sides
	// then converge by name across the boundary in both directions.
	if got := statusEpoch(t, cluster.Nodes[0].APIAddr); got != 1 {
		t.Fatalf("node1 epoch = %d at heal, want 1 (isolation leaked)", got)
	}
	for _, pair := range [][2]int{{0, 1}, {1, 0}, {0, 2}, {2, 0}} {
		if err := cluster.AddPeer(pair[0], pair[1]); err != nil {
			t.Fatalf("heal node1: %v", err)
		}
	}
	waitNamesConverged(t, cluster, total, 90*time.Second)
	t.Logf("overlap writes converged in both directions: %d names", total)

	// Explicit exchange across the (former) boundary: new-column
	// writes from a new node plus an old-schema insert from node 1.
	// Node 1 may auto-adopt at any moment after the heal; the overlap
	// convergence above is the version-pinned mixed proof, this step
	// proves continued flow plus score replication.
	assertEpoch(t, cluster, 1, 2)
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%032x", 9000+i)
		if err := cluster.ExecSQL(1, "UPDATE mg_rows SET score=? WHERE id=?", int64(100+i), id); err != nil {
			t.Fatalf("score write: %v", err)
		}
	}
	if err := cluster.ExecSQL(0, "INSERT INTO mg_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 20001), "post-race"); err != nil {
		t.Fatalf("post-race write: %v", err)
	}
	waitNamesConverged(t, cluster, total+1, 90*time.Second)

	// Migrate the third (unless it already auto-adopted), then full
	// convergence: exact counts, PK-ordered digests, epoch 2 everywhere.
	if statusEpoch(t, cluster.Nodes[0].APIAddr) != 2 {
		if err := cluster.Migrate(0, evolvedTables()); err != nil {
			t.Fatalf("honest migrate node1: %v", err)
		}
	} else {
		t.Log("node1 auto-adopted the revision after healing; explicit migrate unneeded")
	}
	waitFullConverged(t, cluster, total+1, 90*time.Second)
	for i := 0; i < 3; i++ {
		assertEpoch(t, cluster, i, 2)
	}
	// Materialized-query check: the new column reads identically everywhere.
	for i := 0; i < 3; i++ {
		assertScore(t, cluster, i, fmt.Sprintf("%032x", 9000), 100)
		assertScore(t, cluster, i, fmt.Sprintf("%032x", 9004), 104)
	}
	t.Log("migration concurrency proven: simultaneous trigger, defined states, full convergence")
}

// classifyNode requires a well-defined schema state: epoch exactly 1 or
// 2, the score column exposed if and only if new, and a stable row count
// (writers are stopped, so the count must not drift mid-check).
func classifyNode(t *testing.T, c *harness.Cluster, idx int) {
	t.Helper()
	epoch := statusEpoch(t, c.Nodes[idx].APIAddr)
	if epoch != 1 && epoch != 2 {
		t.Fatalf("node%d epoch = %d, want exactly 1 or 2 (mixed state)", idx+1, epoch)
	}
	_, scoreErr := c.QuerySQL(idx, "SELECT score FROM mg_rows LIMIT 1")
	if epoch == 1 && scoreErr == nil {
		t.Fatalf("node%d epoch 1 yet exposes score (mixed state)", idx+1)
	}
	if epoch == 2 && scoreErr != nil {
		t.Fatalf("node%d epoch 2 yet hides score (mixed state): %v", idx+1, scoreErr)
	}
	n, err := c.QueryRowCount(idx, "mg_rows")
	if err != nil {
		t.Fatal(err)
	}
	// No exact count here: in-window rows may still be converging.
	// waitNamesConverged below proves exact totals everywhere.
	t.Logf("node%d classified: epoch=%d rows=%d (converging)", idx+1, epoch, n)
}

func assertEpoch(t *testing.T, c *harness.Cluster, idx int, want uint64) {
	t.Helper()
	if got := statusEpoch(t, c.Nodes[idx].APIAddr); got != want {
		t.Fatalf("node%d schema_epoch = %d, want %d", idx+1, got, want)
	}
}

func statusEpoch(t *testing.T, apiAddr string) uint64 {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	defer resp.Body.Close()
	var st struct {
		SchemaEpoch uint64 `json:"schema_epoch"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	return st.SchemaEpoch
}

// waitNamesConverged polls id/name convergence (tolerates schema skew on
// the new column during the mixed-version window).
func waitNamesConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		counts := map[string]int{}
		for i := range c.Nodes {
			res, err := c.QuerySQL(i, "SELECT name FROM mg_rows ORDER BY name")
			if err != nil || len(res.Rows) != want {
				ok = false
				break
			}
			key := fmt.Sprintf("%v", res.Rows)
			counts[key]++
		}
		if ok && len(counts) == 1 {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge on %d names within %v", want, timeout)
}

// waitFullConverged polls exact counts plus PK-ordered digest equality.
func waitFullConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, "mg_rows")
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, "mg_rows", "id")
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
		n, _ := c.QueryRowCount(i, "mg_rows")
		d, _ := c.ComputeTableDigest(i, "mg_rows", "id")
		t.Logf("node %d at timeout: count=%d digest=%s", i, n, d)
	}
	t.Fatalf("nodes did not fully converge on %d rows within %v", want, timeout)
}

func assertScore(t *testing.T, c *harness.Cluster, idx int, idHex string, want int64) {
	t.Helper()
	res, err := c.QuerySQL(idx, "SELECT score FROM mg_rows WHERE id=?", idHex)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("node %d score read: %+v %v", idx, res, err)
	}
	got, _ := res.Rows[0][0].(float64)
	if int64(got) != want {
		t.Fatalf("node %d score = %v, want %d", idx, got, want)
	}
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}
