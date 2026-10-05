// Impaired-network acceptance.
//
// A 3-node mesh (static peers plus SWIM membership) keeps converging
// while tc/netem impairs ONLY the replication ports: (a) +150ms
// latency, (b) 3% packet loss, (c) a 1Mbit cap during a stale-node
// snapshot resync under sustained writes. Membership must stay stable
// (no false evictions) and per-row visibility latencies are recorded
// with relaxed p95 bounds.
//
// Scoping: impairment attaches to lo as root qdisc 77: (prio) with the
// netem/tbf child on band 77:3; u32 filters steer only packets whose
// source OR destination port is a cluster replication port into the
// impaired band. API, metrics, and all other loopback traffic take the
// default band untouched. The suite refuses to run if lo already has a
// foreign root qdisc, and always removes qdisc 77: on cleanup.
//
// Needs CAP_NET_ADMIN (root or a network namespace): without it the
// suite skips. MURMUR_IMPAIRED_NETWORK_FORCE=1 runs the same
// write/converge workload with no impairment (logic verification only).
package impairednetwork_test

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

const tableName = "imp_rows"

func TestImpairedNetworkConverges(t *testing.T) {
	force := harness.GetEnv("MURMUR_IMPAIRED_NETWORK_FORCE") == "1"
	tcOK, skipReason := tcCapable()
	if !force && !tcOK {
		t.Skipf("impaired-network needs tc/netem + CAP_NET_ADMIN on lo: %s (set MURMUR_IMPAIRED_NETWORK_FORCE=1 to run the workload unimpaired)", skipReason)
	}
	if force && !tcOK {
		t.Logf("FORCE mode: tc unusable (%s); running workload WITHOUT impairment", skipReason)
	}
	rows := envInt("MURMUR_IMPAIRED_NETWORK_ROWS", 8)

	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "impaired-network",
		NumNodes:    3,
		AwaitUnlock: true,
		// Aggressive retention so phase (c) can force a snapshot resync
		// after a short offline window; harmless to phases (a)/(b).
		Replication: &harness.ReplicationOptions{
			MinLogRetentionMs:        1000,
			MaxOfflineLogRetentionMs: 10000,
			MinRetainedBatches:       10,
		},
		BootstrapSeeds: []int{0},
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: tableName,
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
	})
	replPorts := clusterReplPorts(t, cluster)
	t.Cleanup(func() { clearImpairment() })
	waitMembership(t, cluster, 3, 60*time.Second)

	// Baseline converges unimpaired (honest-path control).
	base := 10
	for i := 0; i < base; i++ {
		if err := cluster.ExecSQL(0, "INSERT INTO "+tableName+" (id, name) VALUES (?, ?)",
			fmt.Sprintf("%032x", 1000+i), fmt.Sprintf("base-%d", i)); err != nil {
			t.Fatalf("baseline write: %v", err)
		}
	}
	waitConverged(t, cluster, base, 150*time.Second)

	impair := func(child string, args ...string) {
		t.Helper()
		if force {
			return
		}
		applyImpairment(t, replPorts, child, args...)
	}
	clear := func() {
		if !force {
			clearImpairment()
		}
	}

	t.Run("latency-150ms", func(t *testing.T) {
		if force {
			t.Log("FORCE mode: NO 150ms latency applied; green here proves the workload only, not impairment tolerance")
		}
		impair("netem", "delay", "150ms")
		defer clear()
		lat := timedWrites(t, cluster, "lat", base, rows, 30*time.Second)
		p95 := percentile(lat, 95)
		t.Logf("latency phase: %d rows p95 visibility=%v (bound 15s)", len(lat), p95)
		if p95 > 15*time.Second {
			t.Fatalf("p95 visibility %v exceeds 15s bound", p95)
		}
	})
	base += rows

	t.Run("loss-3pct", func(t *testing.T) {
		if force {
			t.Log("FORCE mode: NO 3% loss applied; green here proves the workload only, not impairment tolerance")
		}
		impair("netem", "loss", "3%")
		defer clear()
		lat := timedWrites(t, cluster, "loss", base, rows, 45*time.Second)
		p95 := percentile(lat, 95)
		t.Logf("loss phase: %d rows p95 visibility=%v (bound 30s)", len(lat), p95)
		if p95 > 30*time.Second {
			t.Fatalf("p95 visibility %v exceeds 30s bound", p95)
		}
	})
	base += rows

	t.Run("capped-snapshot-resync", func(t *testing.T) {
		if force {
			t.Log("FORCE mode: NO bandwidth cap applied; green here proves the workload only, not impairment tolerance")
		}
		// Stale rows on node3, then it stops with acknowledged state.
		for i := 0; i < 5; i++ {
			if err := cluster.ExecSQL(2, "INSERT INTO "+tableName+" (id, name) VALUES (?, ?)",
				fmt.Sprintf("%032x", 5000+i), fmt.Sprintf("stale-%d", i)); err != nil {
				t.Fatalf("stale write: %v", err)
			}
		}
		waitConverged(t, cluster, base+5, 150*time.Second)
		cluster.StopNode(2)

		// Survivors write far past the short retention while node3 is
		// offline (sustained writes start here and continue below).
		for i := 0; i < 60; i++ {
			if err := cluster.ExecSQL(0, "INSERT INTO "+tableName+" (id, name) VALUES (?, ?)",
				fmt.Sprintf("%032x", 6000+i), fmt.Sprintf("fresh-%d", i)); err != nil {
				t.Fatalf("survivor write: %v", err)
			}
		}
		waitRowCount(t, cluster, 0, base+65, 60*time.Second)
		waitRowCount(t, cluster, 1, base+65, 60*time.Second)

		// Node3's 10s retention pin must lapse, then one 30s-tick GC
		// pass must collect its ranges so the rejoin needs a snapshot.
		// Poll the GC counter instead of a fixed 55s sleep.
		time.Sleep(12 * time.Second)
		waitGCPass(t, cluster, 0, 60*time.Second)

		// Cap replication throughput, then restart node3 into it and
		// keep writing while the snapshot transfers.
		impair("tbf", "rate", "1mbit", "burst", "32kbit", "latency", "400ms")
		defer clear()
		cluster.StartNode(2)
		cluster.UnlockNode(2, cluster.Nodes[2].KeyHex)
		cluster.WaitNodeReady(2)
		for i := 0; i < 20; i++ {
			if err := cluster.ExecSQL(1, "INSERT INTO "+tableName+" (id, name) VALUES (?, ?)",
				fmt.Sprintf("%032x", 8000+i), fmt.Sprintf("sustain-%d", i)); err != nil {
				t.Fatalf("sustained write: %v", err)
			}
		}
		waitConverged(t, cluster, base+85, 180*time.Second)
		if got := metricValue(t, cluster.Nodes[2].APIAddr, "spedsql_repl_snapshots_received_total"); got < 1 {
			t.Fatalf("node3 snapshots received = %v, want >= 1 (log catch-up would hide a snapshot-path regression)", got)
		}
		waitMembership(t, cluster, 3, 60*time.Second)
		t.Logf("capped resync converged with snapshot path proven")
	})
}

// timedWrites inserts rows round-robin and records per-row full-mesh
// visibility latency; every row also samples stable SWIM membership.
func timedWrites(t *testing.T, c *harness.Cluster, prefix string, base, n int, perRow time.Duration) []time.Duration {
	t.Helper()
	var lat []time.Duration
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("%s-%d", prefix, i)
		id := fmt.Sprintf("%032x", int64(base+1)*100000+int64(i))
		start := time.Now()
		if err := c.ExecSQL(i%len(c.Nodes), "INSERT INTO "+tableName+" (id, name) VALUES (?, ?)", id, name); err != nil {
			t.Fatalf("%s write %d: %v", prefix, i, err)
		}
		waitRowVisible(t, c, name, perRow)
		lat = append(lat, time.Since(start))
		assertStableMembership(t, c)
	}
	return lat
}

func waitRowVisible(t *testing.T, c *harness.Cluster, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for i := range c.Nodes {
			n, err := countWhere(t, c, i, name)
			if err != nil || n != 1 {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("row %q not visible on all nodes within %v", name, timeout)
}

func countWhere(t *testing.T, c *harness.Cluster, idx int, name string) (int, error) {
	t.Helper()
	res, err := c.QuerySQL(idx, "SELECT count(*) FROM "+tableName+" WHERE name = ?", name)
	if err != nil {
		return 0, err
	}
	if len(res.Rows) == 0 || len(res.Rows[0]) == 0 {
		return 0, nil
	}
	var n int
	_, _ = fmt.Sscanf(fmt.Sprintf("%v", res.Rows[0][0]), "%d", &n)
	return n, nil
}

// assertStableMembership requires a full healthy SWIM view: 3 members,
// all alive. Transient suspicion does not remove members, so any drop
// here is a genuine false eviction.
func assertStableMembership(t *testing.T, c *harness.Cluster) {
	t.Helper()
	for i, node := range c.Nodes {
		if got := metricValue(t, node.APIAddr, "spedsql_membership_count"); got != 3 {
			t.Fatalf("node %d membership_count = %v during impairment, want 3 (false eviction?)", i, got)
		}
		if got := metricValue(t, node.APIAddr, "spedsql_membership_alive_count"); got != 2 {
			t.Fatalf("node %d alive_count = %v during impairment, want 2 (false eviction?)", i, got)
		}
	}
}

func percentile(d []time.Duration, p int) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := append([]time.Duration(nil), d...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	k := (len(s)*p + 99) / 100
	if k < 1 {
		k = 1
	}
	return s[k-1]
}

// --- tc/netem plumbing (lo, replication ports only) ---

func tcCapable() (bool, string) {
	if _, err := exec.LookPath("tc"); err != nil {
		return false, "tc binary not found"
	}
	out, err := exec.Command("tc", "qdisc", "show", "dev", "lo").CombinedOutput()
	if err != nil {
		return false, fmt.Sprintf("tc qdisc show failed: %v", strings.TrimSpace(string(out)))
	}
	show := string(out)
	if strings.Contains(show, "77:") {
		// Our own leftover from a crashed run: remove and continue.
		if out, err := exec.Command("tc", "qdisc", "del", "dev", "lo", "root").CombinedOutput(); err != nil {
			return false, fmt.Sprintf("cannot clear stale qdisc 77:: %v", strings.TrimSpace(string(out)))
		}
		return tcCapable()
	}
	if strings.Contains(show, "qdisc") && !strings.Contains(show, "noqueue") {
		return false, fmt.Sprintf("lo already has a root qdisc (%s); refusing to disturb shared box", strings.TrimSpace(show))
	}
	// Privilege probe: install and immediately remove an empty prio.
	if out, err := exec.Command("tc", "qdisc", "add", "dev", "lo", "root", "handle", "77:", "prio").CombinedOutput(); err != nil {
		return false, fmt.Sprintf("tc qdisc add failed (need CAP_NET_ADMIN): %v", strings.TrimSpace(string(out)))
	}
	if out, err := exec.Command("tc", "qdisc", "del", "dev", "lo", "root").CombinedOutput(); err != nil {
		return false, fmt.Sprintf("tc probe cleanup failed: %v", strings.TrimSpace(string(out)))
	}
	return true, ""
}

func applyImpairment(t *testing.T, ports []int, child string, args ...string) {
	t.Helper()
	run := func(what string, argv ...string) {
		t.Helper()
		if out, err := exec.Command("tc", argv...).CombinedOutput(); err != nil {
			clearImpairment()
			t.Fatalf("tc %s: %v (%s)", what, err, strings.TrimSpace(string(out)))
		}
	}
	run("root", "qdisc", "replace", "dev", "lo", "root", "handle", "77:", "prio", "bands", "3")
	childArgs := append([]string{"qdisc", "replace", "dev", "lo", "parent", "77:3", "handle", "773:", child}, args...)
	run("child", childArgs...)
	for _, port := range ports {
		p := strconv.Itoa(port)
		// Both directions: every replication packet touches a repl
		// port as either source or destination.
		run("filter-dport", "filter", "add", "dev", "lo", "protocol", "ip", "parent", "77:0",
			"prio", "77", "u32", "match", "ip", "dport", p, "0xffff", "flowid", "77:3")
		run("filter-sport", "filter", "add", "dev", "lo", "protocol", "ip", "parent", "77:0",
			"prio", "77", "u32", "match", "ip", "sport", p, "0xffff", "flowid", "77:3")
	}
}

func clearImpairment() {
	out, err := exec.Command("tc", "qdisc", "show", "dev", "lo").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "77:") {
		return
	}
	_ = exec.Command("tc", "qdisc", "del", "dev", "lo", "root").Run()
}

func clusterReplPorts(t *testing.T, c *harness.Cluster) []int {
	t.Helper()
	var ports []int
	for _, node := range c.Nodes {
		_, p, err := net.SplitHostPort(node.ReplAddr)
		if err != nil {
			t.Fatalf("parse repl addr %q: %v", node.ReplAddr, err)
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			t.Fatalf("parse repl port %q: %v", p, err)
		}
		ports = append(ports, v)
	}
	return ports
}

// --- shared wait/scape helpers (self-contained per suite convention) ---

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
	t.Fatalf("nodes did not reach membership %d within %v", want, timeout)
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, tableName)
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, tableName, "id")
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
		if time.Since(lastLog) > 10*time.Second {
			lastLog = time.Now()
			counts := make([]int, len(c.Nodes))
			for i := range c.Nodes {
				n, _ := c.QueryRowCount(i, tableName)
				counts[i] = n
			}
			t.Logf("converge progress: counts=%v want=%d", counts, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge on %d rows with equal digests within %v", want, timeout)
}

func waitRowCount(t *testing.T, c *harness.Cluster, idx int, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n, err := c.QueryRowCount(idx, tableName); err == nil && n == want {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	n, _ := c.QueryRowCount(idx, tableName)
	t.Fatalf("node %d count = %d, want %d within %v", idx, n, want, timeout)
}

func waitGCPass(t *testing.T, c *harness.Cluster, idx int, timeout time.Duration) {
	t.Helper()
	base := metricValue(t, c.Nodes[idx].APIAddr, "spedsql_gc_runs_total")
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := metricValue(t, c.Nodes[idx].APIAddr, "spedsql_gc_runs_total"); got > base {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("no GC pass observed within %v", timeout)
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(harness.GetEnv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}

func metricValue(t *testing.T, apiAddr, name string) float64 {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
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
		if v, err := strconv.ParseFloat(rest, 64); err == nil {
			return v
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}
