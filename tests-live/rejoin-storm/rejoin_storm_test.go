// Rejoin-storm acceptance: two of three meshed nodes are SIGKILLed at
// once while the survivor keeps taking tracked writes, then both dead
// nodes restart simultaneously (thundering-herd rejoin behind a start
// barrier). Both must rejoin un-wedged, every outage write must land
// everywhere, counts plus sorted logical-record digests must match exactly, and
// each node's rejoin path (range repair vs snapshot) is recorded via
// its receiver counter — correctness is mandatory on either path.
package rejoinstorm_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestDualKillSimultaneousRejoin(t *testing.T) {
	outageRows := envInt("MURMUR_REJOIN_STORM_OUTAGE_ROWS", 40)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "rejoin-storm",
		NumNodes:     3,
		AwaitUnlock:  true,
		TypedRecords: true,
	})

	// Positive control: honest 3-node convergence before any crash.
	for node := 0; node < 3; node++ {
		name := fmt.Sprintf("base-node%d", node+1)
		if err := cluster.TypedInsert(node, name); err != nil {
			t.Fatalf("baseline write node%d: %v", node+1, err)
		}
	}
	waitForPeers(t, cluster, []int{2, 2, 2}, 30*time.Second)
	waitForConvergence(t, cluster, 3, 30*time.Second)
	t.Log("positive control: 3-node mesh converged")

	// Double SIGKILL at once; the outage is real only if both APIs
	// actually go dark.
	cluster.KillNode(1)
	cluster.KillNode(2)
	for _, idx := range []int{1, 2} {
		if _, err := cluster.TypedNames(idx); err == nil {
			t.Fatalf("node%d answers queries after SIGKILL; outage not real", idx+1)
		}
	}
	t.Log("nodes 2 and 3 killed; survivor taking outage writes")

	// Survivor keeps taking tracked writes through the whole outage.
	acked := map[string]struct{}{
		"base-node1": {}, "base-node2": {}, "base-node3": {},
	}
	for i := 0; i < outageRows; i++ {
		name := fmt.Sprintf("outage-%04d", i)
		if err := cluster.TypedInsert(0, name); err != nil {
			t.Fatalf("outage write %d: %v", i, err)
		}
		acked[name] = struct{}{}
	}
	if names, err := cluster.TypedNames(0); err != nil || len(names) != 3+outageRows {
		n := len(names)
		t.Fatalf("survivor count = %d (err=%v), want %d", n, err, 3+outageRows)
	}

	// Thundering-herd rejoin: both nodes start behind the same barrier
	// so their catch-up overlaps instead of serializing.
	start := make(chan struct{})
	type readyResult struct {
		idx  int
		took time.Duration
	}
	ready := make(chan readyResult, 2)
	for _, idx := range []int{1, 2} {
		go func(idx int) {
			<-start
			begin := time.Now()
			cluster.StartNode(idx)
			cluster.UnlockNode(idx, cluster.Nodes[idx].KeyHex)
			cluster.WaitNodeReady(idx)
			ready <- readyResult{idx, time.Since(begin)}
		}(idx)
	}
	close(start)
	r1, r2 := <-ready, <-ready
	for _, r := range []readyResult{r1, r2} {
		t.Logf("node%d ready in %v after simultaneous restart", r.idx+1, r.took.Round(100*time.Millisecond))
		if r.took > 60*time.Second {
			t.Fatalf("node%d took %v to reach ready, want < 60s (wedge)", r.idx+1, r.took)
		}
	}

	// Every outage write present on all three; exact counts plus
	// PK-ordered digests identical everywhere.
	waitForConvergence(t, cluster, 3+outageRows, 90*time.Second)
	assertAcknowledgedPresent(t, cluster, acked)
	t.Logf("all %d outage writes present on all 3 nodes with equal digests", outageRows)

	// Record (never mandate) each node's rejoin path. Counters are
	// process-local, so post-restart values describe this rejoin only:
	// snapshots_received >= 1 means snapshot, 0 means log range repair.
	for _, idx := range []int{1, 2} {
		snaps := snapshotCounter(t, cluster.Nodes[idx].APIAddr, "spedsql_repl_snapshots_received_total")
		path := "log range repair"
		if snaps >= 1 {
			path = "snapshot"
		}
		t.Logf("node%d rejoin path: %s (snapshots_received=%v)", idx+1, path, snaps)
	}

	// Post-rejoin writes on each previously-dead node replicate everywhere.
	post := map[string]struct{}{}
	for _, idx := range []int{1, 2} {
		name := fmt.Sprintf("post-rejoin-node%d", idx+1)
		if err := cluster.TypedInsert(idx, name); err != nil {
			t.Fatalf("post-rejoin write node%d: %v", idx+1, err)
		}
		post[name] = struct{}{}
	}
	waitForConvergence(t, cluster, 3+outageRows+2, 60*time.Second)
	assertAcknowledgedPresent(t, cluster, post)
	t.Log("post-rejoin writes on both revived nodes replicated everywhere")
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

func waitForConvergence(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
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
			sort.Strings(names)
			d := strings.Join(names, "\n")
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
		names, _ := c.TypedNames(i)
		sort.Strings(names)
		t.Logf("node %d at timeout: count=%d digest=%s", i+1, len(names), digestNames(names))
	}
	t.Fatalf("nodes did not converge on %d rows with equal PK digests within %v", want, timeout)
}

func assertAcknowledgedPresent(t *testing.T, c *harness.Cluster, acknowledged map[string]struct{}) {
	t.Helper()
	for i := range c.Nodes {
		names, err := c.TypedNames(i)
		if err != nil {
			t.Fatalf("node%d recovery query: %v", i+1, err)
		}
		present := make(map[string]struct{}, len(names))
		for _, name := range names {
			present[name] = struct{}{}
		}
		for name := range acknowledged {
			if _, ok := present[name]; !ok {
				t.Fatalf("node%d lost acknowledged write %q", i+1, name)
			}
		}
	}
}

func digestNames(names []string) string {
	sort.Strings(names)
	return strings.Join(names, "\n")
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
	if value, ok := harness.MetricValueFrom(string(raw), name); ok {
		return value
	}
	t.Fatalf("counter %s not present in /metrics", name)
	return 0
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(harness.GetEnv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}
