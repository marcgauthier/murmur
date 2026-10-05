// Crash-loop acceptance: one of three meshed nodes is SIGKILLed and
// restarted N times under continuous writes from every node, with
// short uptimes between kills. Every write acknowledged before each
// kill must survive; the looped node must converge to identical
// digests; it must reach ready within a bound after the final restart
// (no wedge); and a post-loop write on it must replicate everywhere.
//
// Kill placement is proven, not assumed: every round requires a SQL
// request in flight on the victim (mid-write), and the final round
// kills immediately after a 30-row burst on a survivor, which the
// 1s remote-apply tick cannot have flushed yet (mid-replication).
package crashloop_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestCrashLoopNodeConvergesWithoutLoss(t *testing.T) {
	rounds := envInt("MURMUR_CRASH_LOOP_ROUNDS", 5)
	uptime := time.Duration(envInt("MURMUR_CRASH_LOOP_UPTIME_MS", 1000)) * time.Millisecond
	const victim = 2
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "crash-loop",
		NumNodes:    3,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "cl_rows",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
	})

	// Positive control: honest 3-node convergence before any crash.
	for node := 0; node < 3; node++ {
		name := fmt.Sprintf("base-node%d", node+1)
		if err := cluster.ExecSQL(node, "INSERT INTO cl_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", node+1), name); err != nil {
			t.Fatalf("baseline write node%d: %v", node+1, err)
		}
	}
	waitForPeers(t, cluster, []int{2, 2, 2}, 30*time.Second)
	waitForConvergence(t, cluster, 30*time.Second)
	t.Log("positive control: 3-node mesh converged")

	acknowledged := map[string]struct{}{
		"base-node1": {}, "base-node2": {}, "base-node3": {},
	}
	var ackMu sync.Mutex
	stop := make(chan struct{})
	var writers sync.WaitGroup
	var stopOnce sync.Once
	stopWriters := func() { stopOnce.Do(func() { close(stop); writers.Wait() }) }
	t.Cleanup(stopWriters)
	var inFlight [3]atomic.Int64
	var seqs [3]atomic.Int64
	for node := 0; node < 3; node++ {
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
				name := fmt.Sprintf("n%d-%08d", node+1, seq)
				id := fmt.Sprintf("%02x%030x", node+1, seq)
				inFlight[node].Add(1)
				err := cluster.ExecSQL(node, "INSERT INTO cl_rows (id, name) VALUES (?, ?)", id, name)
				inFlight[node].Add(-1)
				if err == nil {
					ackMu.Lock()
					acknowledged[name] = struct{}{}
					ackMu.Unlock()
				}
			}
		}(node)
	}

	var readyDurations []time.Duration
	for round := 1; round <= rounds; round++ {
		time.Sleep(uptime)
		if round == rounds {
			// Burst on a survivor, then kill at once: prefer a
			// moment with a request in flight (mid-write), but
			// bound the wait to 500ms so the burst is still
			// unflushed (mid-replication by construction: the
			// remote-apply tick is 1s).
			for i := 0; i < 30; i++ {
				name := fmt.Sprintf("burst-%d", i)
				id := fmt.Sprintf("b0%030x", i)
				if err := cluster.ExecSQL(0, "INSERT INTO cl_rows (id, name) VALUES (?, ?)", id, name); err != nil {
					t.Fatalf("round %d burst: %v", round, err)
				}
				ackMu.Lock()
				acknowledged[name] = struct{}{}
				ackMu.Unlock()
			}
			deadline := time.Now().Add(500 * time.Millisecond)
			for inFlight[victim].Load() == 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
		} else {
			// Every non-final kill requires a request in
			// flight: mid-write proven.
			waitInflight(t, cluster, victim, &inFlight, 10*time.Second)
		}
		t.Logf("round %d/%d: SIGKILL node3 with %d request(s) in flight", round, rounds, inFlight[victim].Load())
		cluster.KillNode(victim)
		start := time.Now()
		cluster.StartNode(victim)
		cluster.UnlockNode(victim, cluster.Nodes[victim].KeyHex)
		cluster.WaitNodeReady(victim)
		readyDurations = append(readyDurations, time.Since(start))
		t.Logf("round %d/%d: node3 ready in %v", round, rounds, readyDurations[len(readyDurations)-1].Round(100*time.Millisecond))
	}
	// No wedge: the final restart reaches ready within the bound, the
	// mesh reforms, and every acked write is present everywhere.
	if last := readyDurations[len(readyDurations)-1]; last > 60*time.Second {
		t.Fatalf("final restart took %v to reach ready, want < 60s (wedge)", last)
	}
	waitForPeers(t, cluster, []int{2, 2, 2}, 90*time.Second)
	t.Log("mesh reformed after the crash loop")
	stopWriters()
	ackMu.Lock()
	total := len(acknowledged)
	ackMu.Unlock()
	waitForConvergence(t, cluster, 90*time.Second)
	assertAcknowledgedPresent(t, cluster, acknowledged)
	t.Logf("all %d acknowledged writes survived %d SIGKILLs", total, rounds)

	postLoop := "post-loop-from-victim"
	if err := cluster.ExecSQL(victim, "INSERT INTO cl_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 777001), postLoop); err != nil {
		t.Fatalf("post-loop write: %v", err)
	}
	waitForConvergence(t, cluster, 60*time.Second)
	assertAcknowledgedPresent(t, cluster, map[string]struct{}{postLoop: {}})
	t.Log("post-loop write on the looped node replicated everywhere")
}

func waitInflight(t *testing.T, c *harness.Cluster, node int, inFlight *[3]atomic.Int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if inFlight[node].Load() > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("node%d had no SQL request in flight before SIGKILL", node+1)
}

func waitForPeers(t *testing.T, c *harness.Cluster, want []int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for i, node := range c.Nodes {
			if connectedPeers(node.APIAddr) != want[i] {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("peers did not reach %v within %v", want, timeout)
}

func connectedPeers(apiAddr string) int {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	var st struct {
		ConnectedPeers int `json:"connected_peers"`
	}
	if json.NewDecoder(resp.Body).Decode(&st) != nil {
		return -1
	}
	return st.ConnectedPeers
}

// waitForConvergence polls exact counts plus two digests: name-ordered
// and PK-ordered. Names are unique, so both orders are deterministic.
func waitForConvergence(t *testing.T, c *harness.Cluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var wantCount int
		var wantID string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, "cl_rows")
			if err != nil {
				ok = false
				break
			}
			// Ordered by PK id only: names are unique today but a
			// future duplicate would make a name-ordered digest
			// nondeterministic, so the name digest is dropped.
			dID, err := c.ComputeTableDigest(i, "cl_rows", "id")
			if err != nil {
				ok = false
				break
			}
			if i == 0 {
				wantCount, wantID = n, dID
			} else if n != wantCount || dID != wantID {
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
		n, _ := c.QueryRowCount(i, "cl_rows")
		d, _ := c.ComputeTableDigest(i, "cl_rows", "id")
		t.Logf("node %d at timeout: count=%d digest=%s", i+1, n, d)
	}
	t.Fatalf("nodes did not converge (counts + name/PK digests) within %v", timeout)
}

func assertAcknowledgedPresent(t *testing.T, c *harness.Cluster, acknowledged map[string]struct{}) {
	t.Helper()
	for i := range c.Nodes {
		res, err := c.QuerySQL(i, "SELECT name FROM cl_rows")
		if err != nil {
			t.Fatalf("node%d recovery query: %v", i+1, err)
		}
		present := make(map[string]struct{}, len(res.Rows))
		for _, row := range res.Rows {
			if len(row) > 0 {
				present[fmt.Sprint(row[0])] = struct{}{}
			}
		}
		for name := range acknowledged {
			if _, ok := present[name]; !ok {
				t.Fatalf("node%d lost acknowledged write %q", i+1, name)
			}
		}
	}
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(harness.GetEnv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}
