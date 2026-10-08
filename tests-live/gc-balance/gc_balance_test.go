package gcbalance

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// Log GC must keep up with a sustained update stream: with aggressive
// retention (1s log, 20s offline pin, 10 batches) and a fixed 200-key
// working set updated in a tight loop, GC must collect (nearly) every
// produced log batch, so the retained set does not grow. The balance is
// measured directly from daemon counters (local commits produced vs log
// batches collected), not from directory bytes: at this scale nothing
// ever flushes to SSTs, so filesystem size is flat MANIFEST/WAL noise
// regardless of GC behavior. Deletes are out of scope (delete pruning
// is unimplemented); only version churn is asserted.
func TestLogGCKeepsUpWithChurn(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "gc-balance",
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

	const keys = 200
	for i := 0; i < keys; i++ {
		id := fmt.Sprintf("%032x", i)
		if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: id, Name: "v-0"}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	waitAllCounts(t, cluster, keys, 60*time.Second)

	// Eight updaters over disjoint key ranges: a single sequential
	// updater cannot sustain enough churn on a loaded box (a full
	// `go test ./...` left four updaters at ~4300 commits per 40s
	// window), which would make the pressure gate vacuous. Parallelism
	// keeps the window well above the commit floor in every environment.
	const updaters = 8
	var stopped atomic.Bool
	var updateErrs atomic.Int64
	var sweeps [updaters]atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for w := 0; w < updaters; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				round := 1
				for lo := w * keys / updaters; !stopped.Load(); round++ {
					for i := lo; i < lo+keys/updaters && !stopped.Load(); i++ {
						id := fmt.Sprintf("%032x", i)
						if err := cluster.TypedContentionUpdate(0, id, "name", fmt.Sprintf("v-%d-%d", w, round)); err != nil {
							updateErrs.Add(1)
						}
					}
					sweeps[w].Store(int64(round))
				}
			}(w)
		}
		wg.Wait()
	}()

	// Warm up to steady state, then measure one window: every commit in
	// the window appends an origin-log batch, so (commits - collected)
	// is the retained set's growth over the window. GC keeps up iff it
	// collects >= 90% of what the window produced; a dead GC scores 0%.
	// The window is 40s on a quiet box and stretches (to a 150s cap)
	// under parallel-suite load until the commit floor is met, so the
	// ratio proof stays meaningful at any production rate.
	time.Sleep(20 * time.Second)
	before := gcCounters(t, cluster.Nodes[0].APIAddr)
	time.Sleep(40 * time.Second)
	after := gcCounters(t, cluster.Nodes[0].APIAddr)
	for elapsed := 40 * time.Second; after.commits-before.commits < 2000 && elapsed < 150*time.Second; {
		time.Sleep(5 * time.Second)
		elapsed += 5 * time.Second
		after = gcCounters(t, cluster.Nodes[0].APIAddr)
	}
	t.Logf("window commits=%d over the measurement period", after.commits-before.commits)

	stopped.Store(true)
	<-done

	// Drain: the window's tail stays uncollected until a GC pass runs
	// after the updaters stop, so first wait for a post-stop pass and
	// then settle on a stable collected count before sampling `after`.
	// Stability alone is not enough: on a starved ticker no pass may
	// run for a minute, and a prematurely stable sample measures
	// ticker alignment instead of balance. A dead GC exhausts the
	// run wait and still fails via the runs/ratio checks below.
	runDeadline := time.Now().Add(180 * time.Second)
	windowEndRuns := after.runs
	for {
		next := gcCounters(t, cluster.Nodes[0].APIAddr)
		after = next
		if next.runs > windowEndRuns || time.Now().After(runDeadline) {
			break
		}
		time.Sleep(5 * time.Second)
	}
	drainDeadline := time.Now().Add(120 * time.Second)
	for stable := 0; stable < 2 && time.Now().Before(drainDeadline); {
		time.Sleep(5 * time.Second)
		next := gcCounters(t, cluster.Nodes[0].APIAddr)
		if next.collected == after.collected {
			stable++
		} else {
			stable = 0
		}
		after = next
	}

	if got := updateErrs.Load(); got != 0 {
		t.Fatalf("%d update errors during churn", got)
	}
	var totalSweeps int64
	for w := 0; w < updaters; w++ {
		got := sweeps[w].Load()
		totalSweeps += got
		if got < 5 {
			t.Fatalf("updater %d completed only %d sweeps, want >= 5 for GC pressure", w, got)
		}
	}
	commits := after.commits - before.commits
	collected := after.collected - before.collected
	runs := after.runs - before.runs
	failures := after.failures - before.failures
	maintAcq := after.maintAcq - before.maintAcq
	t.Logf("churn: %d updater sweeps, %d commits produced, %d log batches collected, %d gc runs, %d gc failures, %d maintenance acquisitions",
		totalSweeps, commits, collected, runs, failures, maintAcq)

	// The balance assertion is vacuous without proven sustained churn
	// (the retained set scales with the production rate, so the 90%
	// bound stays meaningful at any rate above the floor).
	if commits < 2000 {
		t.Fatalf("only %d commits in the window, want >= 2000 for GC pressure", commits)
	}
	if maintAcq == 0 {
		t.Fatalf("no maintenance acquisitions during %d commits; scheduler starves GC", commits)
	}
	if runs == 0 {
		t.Fatalf("no GC runs during the window; test cannot observe GC")
	}
	if failures != 0 {
		t.Fatalf("%d GC failures during the window", failures)
	}
	if gap := commits - collected; gap > commits/10 {
		t.Fatalf("GC fell behind: produced %d commits but collected %d (retained grew by %d, want <= %d)",
			commits, collected, gap, commits/10)
	}

	// End-to-end update flow: a follower must observe a churned value.
	waitValueChanged(t, cluster, fmt.Sprintf("%032x", 0), "v-0", 30*time.Second)

	// No data loss: exact key set with identical digests everywhere.
	// Counts never change for fixed-key updates, so convergence means
	// digest agreement: poll for it, since followers drain the final
	// backlog after the updaters stop.
	waitAllCounts(t, cluster, keys, 60*time.Second)
	waitDigestsAgreed(t, cluster, 60*time.Second)
	want, err := gcDigest(cluster, 0)
	if err != nil {
		t.Fatal(err)
	}
	for idx := 1; idx < 3; idx++ {
		d, err := gcDigest(cluster, idx)
		if err != nil {
			t.Fatal(err)
		}
		if d != want {
			t.Fatalf("node%d digest differs after GC churn", idx+1)
		}
	}
}

// gcDigest returns the PK-ordered digest of the contention table on one node.
func gcDigest(c *harness.Cluster, idx int) (string, error) {
	rows, err := c.TypedContentionRows(idx)
	if err != nil {
		return "", err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	h := sha256.New()
	for _, row := range rows {
		fmt.Fprintf(h, "%s:%s:%s:%d\n", row.ID, row.Name, row.Phone, row.Score)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func waitAllCounts(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok := true
		for idx := range c.Nodes {
			rows, err := c.TypedContentionRows(idx)
			if err != nil || len(rows) != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("counts did not reach %d within %v", want, timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitDigestsAgreed polls until every node reports the same table digest.
func waitDigestsAgreed(t *testing.T, c *harness.Cluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		want, err := gcDigest(c, 0)
		if err == nil {
			match := true
			for idx := 1; idx < len(c.Nodes); idx++ {
				d, err := gcDigest(c, idx)
				if err != nil || d != want {
					match = false
					break
				}
			}
			if match {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("digests did not agree within %v", timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitValueChanged polls followers until one reports a value other than
// seed for the given key, proving churned updates flow end to end.
func waitValueChanged(t *testing.T, c *harness.Cluster, id, seed string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		for idx := 1; idx < len(c.Nodes); idx++ {
			row, err := c.TypedContentionRead(idx, id)
			if err == nil && row.Name != seed {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no follower observed a churned value for %s within %v (updates may not match)", id, timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// gcSnapshot scrapes the commit/GC balance counters from one daemon.
type gcSnapshot struct {
	commits   int64
	collected int64
	runs      int64
	failures  int64
	// maintAcq proves the maintenance reserve grants tickets under
	// churn; without it (zero with nonzero commits) gcOnce starves in
	// its first Admit and runs stays flat.
	maintAcq int64
}

func gcCounters(t *testing.T, apiAddr string) (out gcSnapshot) {
	t.Helper()
	// Default client: the harness hijacks it with the cluster CA and a
	// per-target client certificate (same mechanism as write-priority).
	resp, err := http.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		t.Fatalf("metrics %s: %v", apiAddr, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("metrics %s: %v", apiAddr, err)
	}
	samples, ok := harness.MetricSamples(string(body))
	if !ok {
		t.Fatal("decode metrics JSON")
	}
	for _, sample := range samples {
		v := int64(sample.Value)
		switch sample.Name {
		case "spedsql_local_commits_total":
			out.commits = v
		case "spedsql_gc_log_collected_total":
			out.collected = v
		case "spedsql_gc_runs_total":
			out.runs = v
		case "spedsql_gc_failures_total":
			out.failures = v
		}
		if sample.Name == "spedsql_sched_acquisitions_total" && sample.Labels["class"] == "maintenance" {
			out.maintAcq = v
		}
	}
	return out
}
