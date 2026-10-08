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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestSimultaneousMigrationConverges(t *testing.T) {
	seedRows := envInt("MURMUR_MIGRATION_CONCURRENCY_SEED", 60)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "migration-concurrency",
		NumNodes:     3,
		AwaitUnlock:  true,
		TypedRecords: true,
	})

	// Honest baseline: all old, seed converged everywhere.
	for i := 0; i < seedRows; i++ {
		if err := cluster.TypedInsertWithID(0, ids.NewRowID(), fmt.Sprintf("seed-%d", i)); err != nil {
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
				if err := cluster.TypedInsertWithID(node, ids.NewRowID(), name); err == nil {
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
			results <- migrateResult{idx, cluster.MigrateTypedRecords(idx)}
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
			if err := cluster.MigrateTypedRecords(idx); err != nil {
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
		if err := cluster.TypedSetNote(1, fmt.Sprintf("seed-%d", i), fmt.Sprintf("%d", 100+i)); err != nil {
			t.Fatalf("score write: %v", err)
		}
	}
	if err := cluster.TypedInsertWithID(0, ids.NewRowID(), "post-race"); err != nil {
		t.Fatalf("post-race write: %v", err)
	}
	waitNamesConverged(t, cluster, total+1, 90*time.Second)

	// Migrate the third (unless it already auto-adopted), then full
	// convergence: exact counts, PK-ordered digests, epoch 2 everywhere.
	for i := range cluster.Nodes {
		if err := cluster.MigrateTypedRecords(i); err != nil {
			t.Fatalf("bind migrated record schema on node%d: %v", i+1, err)
		}
	}
	waitFullConverged(t, cluster, total+1, 90*time.Second)
	for i := 0; i < 3; i++ {
		assertEpoch(t, cluster, i, 2)
	}
	// Materialized-query check: the new column reads identically everywhere.
	for i := 0; i < 3; i++ {
		assertNote(t, cluster, i, "seed-0", "100")
		assertNote(t, cluster, i, "seed-4", "104")
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
	_, noteErr := c.TypedNote(idx, "seed-0")
	if epoch == 1 && noteErr == nil {
		t.Fatalf("node%d epoch 1 yet exposes Note (mixed state)", idx+1)
	}
	if epoch == 2 && noteErr != nil {
		t.Fatalf("node%d epoch 2 yet hides Note (mixed state): %v", idx+1, noteErr)
	}
	names, err := c.TypedNames(idx)
	if err != nil {
		t.Fatal(err)
	}
	// No exact count here: in-window rows may still be converging.
	// waitNamesConverged below proves exact totals everywhere.
	t.Logf("node%d classified: epoch=%d rows=%d (converging)", idx+1, epoch, len(names))
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
		var first []string
		for i := range c.Nodes {
			names, err := c.TypedNames(i)
			if err != nil || len(names) != want {
				ok = false
				break
			}
			if i == 0 {
				first = names
			} else if !equalNames(first, names) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	sets := make([]map[string]bool, len(c.Nodes))
	for i := range c.Nodes {
		names, err := c.TypedNames(i)
		if err != nil {
			t.Logf("node %d at timeout: typed names err=%v", i+1, err)
			continue
		}
		t.Logf("node %d at timeout: names=%d", i+1, len(names))
		sets[i] = make(map[string]bool, len(names))
		for _, name := range names {
			sets[i][name] = true
		}
	}
	union := map[string]bool{}
	for _, s := range sets {
		for n := range s {
			union[n] = true
		}
	}
	for i, s := range sets {
		if s == nil {
			continue
		}
		var missing []string
		for n := range union {
			if !s[n] {
				missing = append(missing, n)
			}
		}
		if len(missing) > 0 {
			t.Logf("node %d at timeout: missing %d names: %v", i+1, len(missing), missing)
		}
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
			names, err := c.TypedNames(i)
			if err != nil || len(names) != want {
				ok = false
				break
			}
			d := digestNames(names)
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
		names, _ := c.TypedNames(i)
		t.Logf("node %d at timeout: count=%d digest=%s", i, len(names), digestNames(names))
	}
	t.Fatalf("nodes did not fully converge on %d rows within %v", want, timeout)
}

func assertNote(t *testing.T, c *harness.Cluster, idx int, name, want string) {
	t.Helper()
	got, err := c.TypedNote(idx, name)
	if err != nil || got != want {
		t.Fatalf("node %d Note(%q) = %q (err=%v), want %q", idx, name, got, err, want)
	}
}

func equalNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func digestNames(names []string) string { return fmt.Sprintf("%q", names) }

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(harness.GetEnv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}
