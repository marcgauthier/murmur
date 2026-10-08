// Live perf-matrix cells: mesh scaling (1-10 nodes), network impairment
// (LAN/WAN shaping via tc/netem on replication ports only), and reconnect
// backlogs. All cells run real multi-process daemons through the shared
// tests-live harness in typed-records mode; measurements are convergence
// throughput and sampled full-mesh visibility latency.
package benchmark

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func perfMeshNodes(tier string) []int {
	switch tier {
	case "smoke":
		return []int{2}
	case "full":
		return []int{1, 2, 5, 10}
	default:
		return []int{1, 2, 5}
	}
}

func perfBacklogs(tier string) []int {
	switch tier {
	case "smoke":
		return []int{1000}
	case "full":
		return []int{1000, 10000, 50000}
	default:
		return []int{1000, 10000}
	}
}

func perfMeshRows() int {
	if v := os.Getenv("MURMUR_PERF_MESH_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 100 {
			return n
		}
	}
	return 2000
}

// perfRowID derives the deterministic typed-record ID for worker w and
// sequence seq, preserving the pre-migration 16-byte layout (4-byte worker
// prefix, 12-byte big-endian sequence).
func perfRowID(w int, seq int64) (db.RowID, error) {
	raw, err := hex.DecodeString(fmt.Sprintf("%08x%024x", w, seq))
	if err != nil {
		return db.RowID{}, err
	}
	var id db.RowID
	copy(id[:], raw)
	return id, nil
}

func perfDigestNames(names []string) string {
	h := sha256.New()
	for _, n := range names {
		h.Write([]byte(n))
		h.Write([]byte("\n"))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func perfLiveCells(t *testing.T, rep *perfReport, tier, only string) {
	t.Helper()
	want := func(name string) bool {
		return only == "" || strings.Contains(only, strings.ToLower(name))
	}
	if want("mesh") {
		for _, nodes := range perfMeshNodes(tier) {
			perfMeshCell(t, rep, nodes, perfMeshRows())
		}
	}
	if want("impair") && tier != "smoke" {
		perfImpairCells(t, rep)
	}
	if want("reconnect") {
		for _, bl := range perfBacklogs(tier) {
			perfReconnectCell(t, rep, bl)
		}
	}
}

// perfMeshCell blasts M rows from all nodes in parallel, then measures
// time to exact convergence plus sampled per-row visibility latency.
func perfMeshCell(t *testing.T, rep *perfReport, nodes, total int) {
	t.Helper()
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         fmt.Sprintf("perf-mesh-%dn", nodes),
		NumNodes:     nodes,
		AwaitUnlock:  true,
		TypedRecords: true,
	})

	start := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, nodes)
	for w := 0; w < nodes; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var seq int64
			for i := w; i < total; i += nodes {
				seq++
				id, err := perfRowID(w, seq)
				if err != nil {
					errCh <- err
					return
				}
				if err := cluster.TypedInsertWithID(w, id, fmt.Sprintf("m%d", i)); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("mesh-%d blast: %v", nodes, err)
	}
	if err := perfWaitConverged(cluster, nodes, total, 10*time.Minute); err != nil {
		cluster.DumpForensics(fmt.Sprintf("perf-mesh-%d", nodes))
		t.Fatalf("mesh-%d converge: %v", nodes, err)
	}
	blastWall := time.Since(start)

	// Sampled visibility: sequential rows, each timed to full-mesh sight.
	const samples = 100
	var lat perfLat
	for s := 0; s < samples; s++ {
		name := fmt.Sprintf("vis-%d", s)
		id, err := perfRowID(999, int64(s+1))
		if err != nil {
			t.Fatalf("mesh-%d visibility id: %v", nodes, err)
		}
		op := time.Now()
		if err := cluster.TypedInsertWithID(s%nodes, id, name); err != nil {
			t.Fatalf("mesh-%d visibility write: %v", nodes, err)
		}
		perfWaitRowVisible(t, cluster, nodes, name, 2*time.Minute)
		lat.record(time.Since(op))
	}
	if err := perfWaitConverged(cluster, nodes, total+samples, 10*time.Minute); err != nil {
		t.Fatalf("mesh-%d final converge: %v", nodes, err)
	}

	var rssMax int64
	for i := range cluster.Nodes {
		if pid := cluster.Nodes[i].Process; pid != nil && pid.Process != nil {
			if rss := pidPeakRSSMB(pid.Process.Pid); rss > rssMax {
				rssMax = rss
			}
		}
	}
	rep.add(perfCell{
		Name: "mesh_blast", Rows: total, Nodes: nodes, Ops: int64(total), WallMs: blastWall.Milliseconds(),
		PerSec: float64(total) / blastWall.Seconds(), RowsSec: float64(total) / blastWall.Seconds(),
		P50Ms: lat.percentile(50), P95Ms: lat.percentile(95), PeakRSSMB: rssMax,
		Extra: fmt.Sprintf("vis_samples=%d", samples),
	})
	t.Logf("mesh_blast nodes=%d rows=%d rows/s=%.0f vis_p50=%.1fms vis_p95=%.1fms rss=%dMB",
		nodes, total, float64(total)/blastWall.Seconds(), lat.percentile(50), lat.percentile(95), rssMax)
	// Reap now: the harness also defers cleanup to test end, but a
	// matrix run holds many clusters and must not stack idle daemons.
	cluster.Cleanup()
}

// perfImpairCells replays a fixed 500-row blast on a 3-node mesh under
// shaped replication traffic. Skipped loudly without tc privilege.
func perfImpairCells(t *testing.T, rep *perfReport) {
	t.Helper()
	if ok, reason := perfTcCapable(); !ok {
		rep.add(perfCell{Name: "mesh_impair", Nodes: 3, Skipped: reason})
		t.Logf("mesh_impair SKIP: %s", reason)
		return
	}
	const total = 500
	for _, v := range []struct {
		name  string
		child string
		args  []string
	}{
		{"delay50ms", "netem", []string{"delay", "50ms"}},
		{"loss1pct", "netem", []string{"loss", "1%"}},
	} {
		func() {
			cluster := harness.NewCluster(t, harness.ClusterOptions{
				Name:         fmt.Sprintf("perf-impair-%s", v.name),
				NumNodes:     3,
				AwaitUnlock:  true,
				TypedRecords: true,
			})
			ports, err := perfReplPorts(cluster)
			if err != nil {
				t.Fatal(err)
			}
			if err := perfTcApply(ports, v.child, v.args...); err != nil {
				t.Fatalf("impair %s: %v", v.name, err)
			}
			defer perfTcClear()
			start := time.Now()
			for i := 0; i < total; i++ {
				id, err := perfRowID(i%3, int64(i+1))
				if err != nil {
					t.Fatalf("impair %s id: %v", v.name, err)
				}
				if err := cluster.TypedInsertWithID(i%3, id, fmt.Sprintf("w%d", i)); err != nil {
					t.Fatalf("impair %s write: %v", v.name, err)
				}
			}
			if err := perfWaitConverged(cluster, 3, total, 15*time.Minute); err != nil {
				cluster.DumpForensics("perf-impair-" + v.name)
				t.Fatalf("impair %s converge: %v", v.name, err)
			}
			wall := time.Since(start)
			// Visibility sample under the same impairment (fewer rows:
			// each sample pays the shaped round trip).
			var lat perfLat
			for s := 0; s < 20; s++ {
				name := fmt.Sprintf("iv-%d", s)
				id, err := perfRowID(777, int64(s+1))
				if err != nil {
					t.Fatalf("impair %s vis id: %v", v.name, err)
				}
				op := time.Now()
				if err := cluster.TypedInsertWithID(s%3, id, name); err != nil {
					t.Fatalf("impair %s vis write: %v", v.name, err)
				}
				perfWaitRowVisible(t, cluster, 3, name, 5*time.Minute)
				lat.record(time.Since(op))
			}
			rep.add(perfCell{
				Name: "mesh_impair", Rows: total, Nodes: 3, Variant: v.name, Ops: int64(total), WallMs: wall.Milliseconds(),
				PerSec: float64(total) / wall.Seconds(), RowsSec: float64(total) / wall.Seconds(),
				P50Ms: lat.percentile(50), P95Ms: lat.percentile(95),
				Extra: "replication-ports-only",
			})
			t.Logf("mesh_impair variant=%s rows/s=%.1f vis_p50=%.0fms vis_p95=%.0fms",
				v.name, float64(total)/wall.Seconds(), lat.percentile(50), lat.percentile(95))
			cluster.Cleanup()
		}()
	}
}

// perfReconnectCell stops a follower, builds a backlog on the survivor,
// and measures catch-up throughput plus whether the snapshot path fired.
func perfReconnectCell(t *testing.T, rep *perfReport, backlog int) {
	t.Helper()
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         fmt.Sprintf("perf-reconnect-%d", backlog),
		NumNodes:     2,
		AwaitUnlock:  true,
		TypedRecords: true,
	})
	for i := 0; i < 10; i++ {
		id, err := perfRowID(0, int64(i))
		if err != nil {
			t.Fatal(err)
		}
		if err := cluster.TypedInsertWithID(0, id, fmt.Sprintf("base-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := perfWaitConverged(cluster, 2, 10, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	snapBefore := perfCounter(cluster.FetchPath(1, "/metrics"), "spedsql_repl_snapshots_received_total")
	cluster.StopNode(1)
	for i := 0; i < backlog; i++ {
		id, err := perfRowID(5, int64(i+1))
		if err != nil {
			t.Fatalf("backlog id: %v", err)
		}
		if err := cluster.TypedInsertWithID(0, id, fmt.Sprintf("bl-%d", i)); err != nil {
			t.Fatalf("backlog write: %v", err)
		}
	}
	cluster.StartNode(1)
	cluster.UnlockNode(1, cluster.Nodes[1].KeyHex)
	cluster.WaitNodeReady(1)
	start := time.Now()
	if err := perfWaitConverged(cluster, 2, 10+backlog, 15*time.Minute); err != nil {
		cluster.DumpForensics(fmt.Sprintf("perf-reconnect-%d", backlog))
		t.Fatalf("reconnect backlog=%d: %v", backlog, err)
	}
	wall := time.Since(start)
	snapAfter := perfCounter(cluster.FetchPath(1, "/metrics"), "spedsql_repl_snapshots_received_total")
	var rssMax int64
	for i := range cluster.Nodes {
		if pid := cluster.Nodes[i].Process; pid != nil && pid.Process != nil {
			if rss := pidPeakRSSMB(pid.Process.Pid); rss > rssMax {
				rssMax = rss
			}
		}
	}
	rep.add(perfCell{
		Name: "reconnect", Rows: backlog, Nodes: 2, Backlog: backlog, Ops: int64(backlog), WallMs: wall.Milliseconds(),
		PerSec: float64(backlog) / wall.Seconds(), RowsSec: float64(backlog) / wall.Seconds(),
		PeakRSSMB: rssMax,
		Extra:     fmt.Sprintf("snapshot=%v", snapAfter > snapBefore),
	})
	t.Logf("reconnect backlog=%d catchup_rows/s=%.0f snapshot=%v", backlog,
		float64(backlog)/wall.Seconds(), snapAfter > snapBefore)
	cluster.Cleanup()
}

// perfWaitConverged polls until every node holds the same want rows. Names
// are unique per row within a cell and TypedNames returns them sorted, so
// digest equality over the name list implies the same row set everywhere.
func perfWaitConverged(cluster *harness.Cluster, nodes, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := 0; i < nodes; i++ {
			names, err := cluster.TypedNames(i)
			if err != nil || len(names) != want {
				ok = false
				break
			}
			d := perfDigestNames(names)
			if i == 0 {
				first = d
			} else if d != first {
				ok = false
				break
			}
		}
		if ok {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("no convergence on %d rows within %s", want, timeout)
}

func perfWaitRowVisible(t *testing.T, cluster *harness.Cluster, nodes int, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for i := 0; i < nodes; i++ {
			n, err := cluster.TypedCount(i, name)
			if err != nil || n != 1 {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("row %q not visible on all %d nodes within %v", name, nodes, timeout)
}

func perfCounter(metrics, name string) int64 {
	if value, ok := harness.MetricValueFrom(metrics, name); ok {
		return int64(value)
	}
	return 0
}

// --- scoped tc/netem shaping (replication ports only, loopback) ---

func perfTcCapable() (bool, string) {
	if _, err := exec.LookPath("tc"); err != nil {
		return false, "tc binary not found"
	}
	out, err := exec.Command("tc", "qdisc", "show", "dev", "lo").CombinedOutput()
	if err != nil {
		return false, fmt.Sprintf("tc qdisc show failed: %v", strings.TrimSpace(string(out)))
	}
	show := string(out)
	if strings.Contains(show, "77:") {
		if out, err := exec.Command("tc", "qdisc", "del", "dev", "lo", "root").CombinedOutput(); err != nil {
			return false, fmt.Sprintf("cannot clear stale qdisc 77:: %v", strings.TrimSpace(string(out)))
		}
		return perfTcCapable()
	}
	if strings.Contains(show, "qdisc") && !strings.Contains(show, "noqueue") {
		return false, fmt.Sprintf("lo already has a root qdisc (%s); refusing to disturb shared box", strings.TrimSpace(show))
	}
	if out, err := exec.Command("tc", "qdisc", "add", "dev", "lo", "root", "handle", "77:", "prio").CombinedOutput(); err != nil {
		return false, fmt.Sprintf("tc qdisc add failed (need CAP_NET_ADMIN): %v", strings.TrimSpace(string(out)))
	}
	if out, err := exec.Command("tc", "qdisc", "del", "dev", "lo", "root").CombinedOutput(); err != nil {
		return false, fmt.Sprintf("tc probe cleanup failed: %v", strings.TrimSpace(string(out)))
	}
	return true, ""
}

func perfTcApply(replPorts []int, child string, args ...string) error {
	run := func(what string, argv ...string) error {
		if out, err := exec.Command("tc", argv...).CombinedOutput(); err != nil {
			return fmt.Errorf("tc %s: %v (%s)", what, err, strings.TrimSpace(string(out)))
		}
		return nil
	}
	if err := run("root", "qdisc", "replace", "dev", "lo", "root", "handle", "77:", "prio", "bands", "3"); err != nil {
		perfTcClear()
		return err
	}
	childArgs := append([]string{"qdisc", "replace", "dev", "lo", "parent", "77:3", "handle", "773:", child}, args...)
	if err := run("child", childArgs...); err != nil {
		perfTcClear()
		return err
	}
	for _, port := range replPorts {
		p := strconv.Itoa(port)
		if err := run("filter-dport", "filter", "add", "dev", "lo", "protocol", "ip", "parent", "77:0",
			"prio", "77", "u32", "match", "ip", "dport", p, "0xffff", "flowid", "77:3"); err != nil {
			perfTcClear()
			return err
		}
		if err := run("filter-sport", "filter", "add", "dev", "lo", "protocol", "ip", "parent", "77:0",
			"prio", "77", "u32", "match", "ip", "sport", p, "0xffff", "flowid", "77:3"); err != nil {
			perfTcClear()
			return err
		}
	}
	return nil
}

func perfTcClear() {
	out, err := exec.Command("tc", "qdisc", "show", "dev", "lo").CombinedOutput()
	if err != nil || !strings.Contains(string(out), "77:") {
		return
	}
	_ = exec.Command("tc", "qdisc", "del", "dev", "lo", "root").Run()
}

func perfReplPorts(c *harness.Cluster) ([]int, error) {
	var ports []int
	for _, node := range c.Nodes {
		_, p, err := net.SplitHostPort(node.ReplAddr)
		if err != nil {
			return nil, err
		}
		v, err := strconv.Atoi(p)
		if err != nil {
			return nil, err
		}
		ports = append(ports, v)
	}
	return ports, nil
}
