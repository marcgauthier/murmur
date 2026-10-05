// Package abuse_test runs the parameterized Murmur abuse suite: a live
// multi-process mesh hammered through prolonged partitions, extended
// outages with reconnect backlogs, and a simultaneous reconnect storm,
// with continuous writers throughout. The verdict is exact cross-node
// convergence (row counts plus PK-ordered digests), never a sample.
package abuse_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

const tableName = "abuse_rows"

// TestAbuseMesh answers "can I make Murmur fail under realistic abuse?"
// Scale and length are environment-driven so the same test covers a quick
// 3-node smoke pass and a 200-node multi-hour hammering:
//
//	MURMUR_ABUSE_NODES=10               nodes in the mesh (3..256)
//	MURMUR_ABUSE_DURATION_SECONDS=600   total abuse time, split evenly
//	                                    across the four phases
//	MURMUR_ABUSE_SETTLE_SECONDS=120     max convergence wait before verdict
//	MURMUR_ABUSE_WRITE_INTERVAL_MS=50   pacing per writer goroutine
//	MURMUR_ABUSE_SEED=1                 RNG seed for writer targeting
//	MURMUR_ABUSE_STATUS_SECONDS=15      progress heartbeat interval
func TestAbuseMesh(t *testing.T) {
	nodes := harness.EnvInt("MURMUR_ABUSE_NODES", 10)
	nodes = min(max(nodes, 3), 256)
	duration := harness.EnvSeconds("MURMUR_ABUSE_DURATION_SECONDS", 600)
	settle := harness.EnvSeconds("MURMUR_ABUSE_SETTLE_SECONDS", 120)
	writeInterval := time.Duration(harness.EnvInt("MURMUR_ABUSE_WRITE_INTERVAL_MS", 50)) * time.Millisecond
	seed := int64(harness.EnvInt("MURMUR_ABUSE_SEED", 1))
	statusInterval := harness.EnvSeconds("MURMUR_ABUSE_STATUS_SECONDS", 15)

	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "abuse",
		NumNodes:    nodes,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: tableName,
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
	})

	rep := newReporter(t)
	rep.logf("config: nodes=%d duration=%s settle=%s writeInterval=%s seed=%d statusEvery=%s",
		nodes, duration, settle, writeInterval, seed, statusInterval)

	// Baseline: one row on node 0, present everywhere before abuse starts.
	if err := cluster.ExecSQL(0, "INSERT INTO abuse_rows (id, name) VALUES (?, ?)",
		fmt.Sprintf("%032x", 0), "baseline"); err != nil {
		t.Fatalf("baseline insert: %v", err)
	}
	if err := waitAllCount(cluster, nodes, 1, 2*time.Minute); err != nil {
		t.Fatalf("baseline replication: %v", err)
	}
	rep.logf("baseline row converged on all %d nodes", nodes)

	// Alive-set shared between the phase choreography and the writers.
	var aliveMu sync.Mutex
	alive := make([]bool, nodes)
	for i := range alive {
		alive[i] = true
	}
	setAlive := func(idx int, up bool) {
		aliveMu.Lock()
		alive[idx] = up
		aliveMu.Unlock()
	}
	pickAlive := func(rng *rand.Rand) int {
		aliveMu.Lock()
		defer aliveMu.Unlock()
		var up []int
		for i, ok := range alive {
			if ok {
				up = append(up, i)
			}
		}
		if len(up) == 0 {
			return -1
		}
		return up[rng.Intn(len(up))]
	}

	// Continuous writers: at most 8 goroutines, each targeting a random
	// live node per write. Every attempt mints a fresh id, so a retry
	// after a transient failure can never collide on the primary key.
	// Transient errors are counted, never fatal: abuse is expected to
	// break individual writes; only the final convergence verdict fails.
	stopWriters := make(chan struct{})
	var writersWG sync.WaitGroup
	for w := 0; w < min(nodes, 8); w++ {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			rng := rand.New(rand.NewSource(seed*1000 + int64(w)))
			var seq uint64
			for {
				select {
				case <-stopWriters:
					return
				default:
				}
				target := pickAlive(rng)
				if target < 0 {
					time.Sleep(writeInterval)
					continue
				}
				seq++
				// 32 lowercase hex chars: unique per writer/attempt, and
				// accepted by the API's blob-id decoding (opaque text
				// ids are rejected).
				id := fmt.Sprintf("%08x%024x", w, seq)
				err := cluster.ExecSQL(target,
					"INSERT INTO abuse_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("writer-%d", w))
				rep.countAttempt(err == nil)
				if err != nil {
					rep.sampleErr(err)
				}
				select {
				case <-stopWriters:
					return
				case <-time.After(writeInterval):
				}
			}
		}(w)
	}

	stopStatus := make(chan struct{})
	go rep.statusLoop(stopStatus, cluster, nodes, statusInterval)

	quarter := duration / 4

	// Phase 1: healthy-mesh soak. Writers run, nothing breaks.
	rep.phase("mesh-soak", fmt.Sprintf("healthy full mesh for %s", quarter))
	time.Sleep(quarter)

	// Phase 2: prolonged partition. The mesh splits into halves with no
	// cross-links; both sides keep accepting writes (divergence is the
	// point). Healed at the end of the phase.
	rep.phase("partition", fmt.Sprintf("splitting %d nodes into halves for %s", nodes, quarter))
	half := nodes / 2
	cut := cutPartition(t, cluster, half, nodes)
	rep.logf("partition cut: %d cross-links removed (%d nodes isolated)", cut, nodes-half)
	time.Sleep(quarter)
	healed := healPartition(t, cluster, half, nodes)
	rep.logf("partition healed: %d cross-links restored", healed)

	// Phase 3: extended outage. A quarter of the nodes go down (mixed
	// graceful stops and kills) while writers hammer the survivors,
	// building the reconnect backlog. All victims restart together at
	// the end of the phase; no re-peering is needed (membership
	// persists across restarts).
	victims := max(nodes/4, 1)
	rep.phase("outage", fmt.Sprintf("stopping %d of %d nodes for %s", victims, nodes, quarter))
	victimIdx := make([]int, 0, victims)
	for i := nodes - victims; i < nodes; i++ {
		victimIdx = append(victimIdx, i)
	}
	for k, idx := range victimIdx {
		if k%2 == 0 {
			cluster.KillNode(idx)
		} else {
			cluster.StopNode(idx)
		}
		setAlive(idx, false)
	}
	rep.logf("outage: %d nodes down, writers continue on %d survivors", victims, nodes-victims)
	time.Sleep(quarter)
	rep.logf("outage over: restarting %d victims behind one barrier", victims)
	restartBehindBarrier(cluster, victimIdx, setAlive, rep)

	// Phase 4: reconnect storm. Every node is killed and restarted
	// near-simultaneously while writers keep firing into the churn.
	rep.phase("storm", fmt.Sprintf("killing and restarting all %d nodes, then soaking %s", nodes, quarter))
	all := make([]int, 0, nodes)
	for i := 0; i < nodes; i++ {
		all = append(all, i)
		setAlive(i, false)
	}
	killConcurrently(cluster, all)
	rep.logf("storm: all %d nodes killed, restarting behind one barrier", nodes)
	restartBehindBarrier(cluster, all, setAlive, rep)
	time.Sleep(quarter)

	// Writers stop; the mesh gets `settle` to converge, then verdict.
	close(stopWriters)
	writersWG.Wait()
	close(stopStatus)
	rep.logf("writers stopped: attempted=%d acked=%d transient-errors=%d",
		rep.attempted.Load(), rep.succeeded.Load(), rep.failed.Load())

	// Anti-vacuous guard: if no write was ever acked (not even during the
	// healthy soak), the abuse proved nothing and the run must fail.
	if rep.succeeded.Load() == 0 {
		rep.verdict(false, cluster, "no writes acked during the entire run")
		t.Fatalf("abuse verdict: 0 of %d writes acked; see report for distinct errors",
			rep.attempted.Load())
	}

	rep.phase("verdict", fmt.Sprintf("waiting up to %s for exact convergence", settle))
	counts, digests, err := waitConverged(cluster, nodes, settle)
	if err != nil {
		rep.verdict(false, cluster, err.Error())
		t.Fatalf("abuse verdict: %v\ncounts=%v\ndigests=%v", err, counts, digests)
	}
	rep.verdict(true, cluster, fmt.Sprintf("all %d nodes hold %d rows with identical digest %s",
		nodes, counts[0], digests[0]))
}

// cutPartition removes every peering link between [0,half) and [half,nodes)
// in both directions and returns the number of links cut.
func cutPartition(t *testing.T, cluster *harness.Cluster, half, nodes int) int {
	t.Helper()
	type link struct{ from, to int }
	var links []link
	for a := 0; a < half; a++ {
		for b := half; b < nodes; b++ {
			links = append(links, link{a, b}, link{b, a})
		}
	}
	runLinks(links, func(l link) error { return cluster.RemovePeer(l.from, l.to) })
	return len(links)
}

// healPartition restores every peering link cut by cutPartition.
func healPartition(t *testing.T, cluster *harness.Cluster, half, nodes int) int {
	t.Helper()
	type link struct{ from, to int }
	var links []link
	for a := 0; a < half; a++ {
		for b := half; b < nodes; b++ {
			links = append(links, link{a, b}, link{b, a})
		}
	}
	runLinks(links, func(l link) error { return cluster.AddPeer(l.from, l.to) })
	return len(links)
}

// runLinks applies fn to every link with a bounded worker pool so a
// 200-node partition (tens of thousands of links) heals in seconds.
// Link errors are non-fatal: the convergence verdict is the judge.
func runLinks[T any](links []T, fn func(T) error) {
	const workers = 32
	work := make(chan T)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for l := range work {
				_ = fn(l)
			}
		}()
	}
	for _, l := range links {
		work <- l
	}
	close(work)
	wg.Wait()
}

// killConcurrently SIGKILLs every listed node with bounded concurrency.
func killConcurrently(cluster *harness.Cluster, idx []int) {
	runLinks(idx, func(i int) error { cluster.KillNode(i); return nil })
}

// restartBehindBarrier starts every listed node at the same time so their
// catch-up overlaps (a real reconnect storm), then waits for readiness.
func restartBehindBarrier(cluster *harness.Cluster, idx []int, setAlive func(int, bool), rep *reporter) {
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, i := range idx {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			begin := time.Now()
			cluster.StartNode(i)
			cluster.UnlockNode(i, cluster.Nodes[i].KeyHex)
			cluster.WaitNodeReady(i)
			setAlive(i, true)
			rep.logf("node%d ready %s after storm restart", i+1, time.Since(begin).Round(100*time.Millisecond))
		}(i)
	}
	close(start)
	wg.Wait()
}

// waitAllCount polls until every node reports want rows or timeout.
func waitAllCount(cluster *harness.Cluster, nodes, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for i := 0; i < nodes; i++ {
			n, err := cluster.QueryRowCount(i, tableName)
			if err != nil || n != want {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("row count did not reach %d on all %d nodes within %s", want, nodes, timeout)
}

// waitConverged polls until every node is reachable, all row counts are
// equal, and all PK-ordered digests match. It returns the last observed
// per-node counts and digests alongside any error.
func waitConverged(cluster *harness.Cluster, nodes int, timeout time.Duration) (counts []int, digests []string, err error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		counts = make([]int, nodes)
		digests = make([]string, nodes)
		ok := true
		for i := 0; i < nodes; i++ {
			n, qerr := cluster.QueryRowCount(i, tableName)
			d, derr := cluster.ComputeTableDigest(i, tableName, "id")
			if qerr != nil || derr != nil {
				ok = false
				break
			}
			counts[i], digests[i] = n, d
			if n != counts[0] || d != digests[0] {
				ok = false
			}
		}
		if ok {
			return counts, digests, nil
		}
		time.Sleep(2 * time.Second)
	}
	// FinalBestEffort: re-sample once for the failure report.
	counts = make([]int, nodes)
	digests = make([]string, nodes)
	for i := 0; i < nodes; i++ {
		n, qerr := cluster.QueryRowCount(i, tableName)
		d, derr := cluster.ComputeTableDigest(i, tableName, "id")
		if qerr != nil {
			n = -1
		}
		if derr != nil {
			d = "unreachable:" + derr.Error()
		}
		counts[i], digests[i] = n, d
	}
	return counts, digests, fmt.Errorf("nodes did not converge within %s", timeout)
}
