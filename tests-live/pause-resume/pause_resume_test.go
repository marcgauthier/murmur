// Pause/resume under replication load: SIGSTOP one node mid-replication,
// then SIGCONT and prove zero data loss.
//
// Three daemons mesh (static sessions plus SWIM membership) while all
// nodes take writes. Node2 is frozen with SIGSTOP for 15-30s: the
// survivors keep writing, SWIM may suspect the frozen member but must not
// permanently evict it, and after SIGCONT the streams must resume, every
// acknowledged write must be present everywhere, and digests must
// converge. The rejoin path (range repair vs snapshot) is recorded from
// counters; correctness is mandatory either way.
package pauseresume_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestPauseResumeLosesNoWrites(t *testing.T) {
	stopSeconds := envSeconds("SPEDSQL_PAUSE_RESUME_STOP_SECONDS", 20)
	if stopSeconds < 15 {
		t.Logf("stop window %ds below the 15s floor; using 15s", stopSeconds)
		stopSeconds = 15
	}
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:           "pause-resume",
		NumNodes:       3,
		AwaitUnlock:    true,
		BootstrapSeeds: []int{0},
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "pr_rows",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
	})

	// Positive control baseline: full mesh (sessions + SWIM) converges.
	waitConnectedPeers(t, cluster, 2, 60*time.Second)
	waitMembership(t, cluster, 3, 60*time.Second)
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%032x", i)
		if err := cluster.ExecSQL(0, "INSERT INTO pr_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("base-%d", i)); err != nil {
			t.Fatalf("baseline write: %v", err)
		}
	}
	waitConverged(t, cluster, 5, 30*time.Second)

	acknowledged := map[string]struct{}{}
	for i := 0; i < 5; i++ {
		acknowledged[fmt.Sprintf("base-%d", i)] = struct{}{}
	}
	var ackMu sync.Mutex
	needsBefore := metricValue(t, cluster.Nodes[1].APIAddr, "spedsql_repl_needs_sent_total")
	snapsBefore := metricValue(t, cluster.Nodes[1].APIAddr, "spedsql_repl_snapshots_received_total")
	gapsBefore := metricValue(t, cluster.Nodes[1].APIAddr, "spedsql_repl_gaps_detected_total")

	// Writers hammer every node continuously; node2's writer pauses while
	// its daemon is frozen (a frozen node accepts nothing by definition).
	var paused [3]atomic.Bool
	stop := make(chan struct{})
	stopWriters := sync.OnceFunc(func() { close(stop) })
	defer stopWriters()
	var wg sync.WaitGroup
	for n := 0; n < 3; n++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			ticker := time.NewTicker(25 * time.Millisecond)
			defer ticker.Stop()
			seq := 0
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					if paused[node].Load() {
						continue
					}
					id := fmt.Sprintf("ff%02x%028x", node, seq)
					seq++
					value := fmt.Sprintf("flow-n%d-%08d", node+1, seq)
					if err := cluster.ExecSQL(node, "INSERT INTO pr_rows (id, name) VALUES (?, ?)", id, value); err == nil {
						ackMu.Lock()
						acknowledged[value] = struct{}{}
						ackMu.Unlock()
					}
				}
			}
		}(n)
	}
	time.Sleep(2 * time.Second)

	// Freeze node2 mid-replication.
	paused[1].Store(true)
	time.Sleep(300 * time.Millisecond) // drain in-flight requests first
	proc := cluster.Nodes[1].Process.Process
	if proc == nil {
		t.Fatalf("node2 has no process to stop")
	}
	ackMu.Lock()
	ackedAtStop := len(acknowledged)
	ackMu.Unlock()
	t.Logf("SIGSTOP node2 with %d rows acknowledged so far", ackedAtStop)
	if err := proc.Signal(syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP node2: %v", err)
	}

	// While frozen: survivors keep writing; sample the SWIM alive view
	// from the outside. The alive gauge counts peers (excludes self), so
	// a healthy 3-node mesh reads 2; detection of the frozen member may
	// dip survivors to 1. (The suspect/dead gauges are hardwired to zero
	// in MembershipService.Stats, so the alive dip is the observable.)
	probeFailuresBefore := map[int]float64{
		0: metricValue(t, cluster.Nodes[0].APIAddr, "spedsql_swim_probe_failures_total"),
		2: metricValue(t, cluster.Nodes[2].APIAddr, "spedsql_swim_probe_failures_total"),
	}
	minAlive := len(cluster.Nodes) - 1
	stopWindow := time.Duration(stopSeconds) * time.Second
	sampleEnd := time.Now().Add(stopWindow)
	for time.Now().Before(sampleEnd) {
		for _, idx := range []int{0, 2} {
			if alive := int(metricValue(t, cluster.Nodes[idx].APIAddr, "spedsql_membership_alive_count")); alive < minAlive {
				minAlive = alive
			}
		}
		time.Sleep(time.Second)
	}
	ackMu.Lock()
	ackedDuringPause := len(acknowledged) - ackedAtStop
	ackMu.Unlock()
	probeFailuresDuring := 0.0
	for idx, before := range probeFailuresBefore {
		probeFailuresDuring += metricValue(t, cluster.Nodes[idx].APIAddr, "spedsql_swim_probe_failures_total") - before
	}
	t.Logf("during %v freeze: minAlivePeers=%d probeFailures=%.0f ackedDuringPause=%d", stopWindow, minAlive, probeFailuresDuring, ackedDuringPause)
	// Freeze detection proof (F3): survivors must notice the frozen
	// member, either as an alive-view dip or as failed SWIM probes.
	// Without this, broken probing would pass silently.
	if minAlive >= len(cluster.Nodes)-1 && probeFailuresDuring <= 0 {
		t.Fatalf("freeze undetected: survivors kept alive=%d with zero probe failures during %v (SWIM probing ineffective?)",
			minAlive, stopWindow)
	}
	if ackedDuringPause <= 0 {
		t.Fatalf("no writes acknowledged while node2 was frozen; pause proved nothing")
	}

	// Resume and prove streams come back.
	if err := proc.Signal(syscall.SIGCONT); err != nil {
		t.Fatalf("SIGCONT node2: %v", err)
	}
	resumeStart := time.Now()
	waitNodeHealthy(t, cluster.Nodes[1].APIAddr, 30*time.Second)
	t.Logf("node2 healthy %v after SIGCONT", time.Since(resumeStart))
	paused[1].Store(false)

	stopWriters()
	wg.Wait()
	ackMu.Lock()
	total := len(acknowledged)
	ackMu.Unlock()
	// 180s bound: post-resume range-repair of ~2k rows normally lands in
	// ~10s, but one modernc-backend run needed past 120s (observed once
	// in 7 runs; pure-Go SQLite + cold page cache). The bound stays
	// finite and the no-loss assertions below are unchanged.
	waitConverged(t, cluster, total, 180*time.Second)
	assertAcknowledgedPresent(t, cluster, acknowledged)

	// Sessions resumed on every link; SWIM re-admitted the member.
	waitConnectedPeers(t, cluster, 2, 90*time.Second)
	waitMembership(t, cluster, 3, 90*time.Second)
	waitAlivePeers(t, cluster, len(cluster.Nodes)-1, 90*time.Second)
	assertNoEviction(t, cluster)

	// Record the rejoin path: range repair (gap-pull needs) or snapshot.
	// Correctness holds either way; the counters say which one ran.
	needsAfter := metricValue(t, cluster.Nodes[1].APIAddr, "spedsql_repl_needs_sent_total")
	snapsAfter := metricValue(t, cluster.Nodes[1].APIAddr, "spedsql_repl_snapshots_received_total")
	gapsAfter := metricValue(t, cluster.Nodes[1].APIAddr, "spedsql_repl_gaps_detected_total")
	path := "range-repair"
	if snapsAfter > snapsBefore {
		path = "snapshot"
	}
	t.Logf("node2 rejoin path: %s (needsSent %.0f->%.0f gaps %.0f->%.0f snapshots %.0f->%.0f)",
		path, needsBefore, needsAfter, gapsBefore, gapsAfter, snapsBefore, snapsAfter)
	if needsAfter <= needsBefore && snapsAfter <= snapsBefore {
		t.Fatalf("node2 rejoined with no gap-pull and no snapshot (needs %.0f->%.0f snapshots %.0f->%.0f); catch-up path unproven",
			needsBefore, needsAfter, snapsBefore, snapsAfter)
	}

	// Post-resume honest write still replicates everywhere.
	postID := fmt.Sprintf("%032x", 999_999)
	if err := cluster.ExecSQL(1, "INSERT INTO pr_rows (id, name) VALUES (?, ?)", postID, "post-resume"); err != nil {
		t.Fatalf("post-resume write: %v", err)
	}
	ackMu.Lock()
	acknowledged["post-resume"] = struct{}{}
	total = len(acknowledged)
	ackMu.Unlock()
	waitConverged(t, cluster, total, 60*time.Second)
	assertAcknowledgedPresent(t, cluster, acknowledged)
	t.Logf("pause/resume proven: %ds freeze, %d rows intact, rejoin via %s, digests converge", stopSeconds, total, path)
}

func waitNodeHealthy(t *testing.T, apiAddr string, timeout time.Duration) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get("https://" + apiAddr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node at %s not healthy within %v after SIGCONT", apiAddr, timeout)
}

func waitConnectedPeers(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, node := range c.Nodes {
			if got := int(metricValue(t, node.APIAddr, "spedsql_connected_peers")); got != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	for _, node := range c.Nodes {
		t.Logf("%s connected_peers=%.0f", node.Label, metricValue(t, node.APIAddr, "spedsql_connected_peers"))
	}
	t.Fatalf("nodes did not reach %d connected peers within %v", want, timeout)
}

func waitAlivePeers(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, node := range c.Nodes {
			if got := int(metricValue(t, node.APIAddr, "spedsql_membership_alive_count")); got != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	for _, node := range c.Nodes {
		t.Logf("%s alive=%.0f members=%.0f",
			node.Label, metricValue(t, node.APIAddr, "spedsql_membership_alive_count"),
			metricValue(t, node.APIAddr, "spedsql_membership_count"))
	}
	t.Fatalf("nodes did not re-admit all peers (alive=%d) within %v", want, timeout)
}

func waitMembership(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, node := range c.Nodes {
			if got := int(metricValue(t, node.APIAddr, "spedsql_membership_count")); got != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	for _, node := range c.Nodes {
		t.Logf("%s membership_count=%.0f alive=%.0f suspect=%.0f dead=%.0f",
			node.Label, metricValue(t, node.APIAddr, "spedsql_membership_count"),
			metricValue(t, node.APIAddr, "spedsql_membership_alive_count"),
			metricValue(t, node.APIAddr, "spedsql_membership_suspect_count"),
			metricValue(t, node.APIAddr, "spedsql_membership_dead_count"))
	}
	t.Fatalf("nodes did not reach membership %d within %v", want, timeout)
}

// assertNoEviction proves the frozen member was not permanently evicted:
// every node holds a live session to both peers, memberlist sees the full
// 3-member view, and every node's SWIM alive set holds both peers again
// (the alive gauge excludes self, so 2 is the healthy value).
func assertNoEviction(t *testing.T, c *harness.Cluster) {
	t.Helper()
	wantPeers := len(c.Nodes) - 1
	for _, node := range c.Nodes {
		peers := debugPeers(t, node.APIAddr)
		if len(peers) != wantPeers {
			t.Fatalf("%s sees %d peers, want %d (a peer was evicted)", node.Label, len(peers), wantPeers)
		}
		for _, p := range peers {
			if !p.Connected {
				t.Fatalf("%s peer %s not connected after resume (evicted?)", node.Label, p.NodeID)
			}
		}
		if got := int(metricValue(t, node.APIAddr, "spedsql_membership_alive_count")); got != wantPeers {
			t.Fatalf("%s alive peers = %d, want %d (frozen member never re-admitted)", node.Label, got, wantPeers)
		}
	}
}

type peerStatus struct {
	NodeID    string `json:"NodeID"`
	Connected bool   `json:"Connected"`
}

func debugPeers(t *testing.T, apiAddr string) []peerStatus {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/v1/debug/peers")
	if err != nil {
		t.Fatalf("debug peers: %v", err)
	}
	defer resp.Body.Close()
	var peers []peerStatus
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		t.Fatalf("decode peers: %v", err)
	}
	return peers
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, "pr_rows")
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, "pr_rows", "id")
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
		n, _ := c.QueryRowCount(i, "pr_rows")
		t.Logf("node %d at timeout: count=%d", i, n)
	}
	t.Fatalf("nodes did not converge on %d pr_rows rows with equal digests within %v", want, timeout)
}

func assertAcknowledgedPresent(t *testing.T, c *harness.Cluster, acknowledged map[string]struct{}) {
	t.Helper()
	for _, node := range c.Nodes {
		res, err := c.QuerySQL(node.Index, "SELECT name FROM pr_rows")
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
				t.Fatalf("%s lost acknowledged write %q after resume", node.Label, value)
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
