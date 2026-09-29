// Clock-skew acceptance: one of three encrypted QUIC nodes runs with
// its wall clock +10 minutes (via libfaketime). Concurrent writes on
// all nodes must still resolve to deterministic LWW winners identical
// on every node, digests must converge, and mTLS sessions must show no
// cert-validity flapping (stable peering, zero handshake failures).
//
// The suite self-calibrates: it tries known libfaketime offset
// syntaxes and keeps the first that observably advances the skewed
// node's HLC wall component by ~600s (measured, never assumed). When
// no faketime library is available (or the daemon binary is statically
// linked, which LD_PRELOAD cannot intercept) it Skips with a message.
package clockskew_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

const skewed = 2 // node3 runs with a +10min wall clock

func TestClockSkewKeepsLWWDeterministic(t *testing.T) {
	lib := findFaketimeLib()
	if lib == "" {
		t.Skipf("clock-skew needs libfaketime (LD_PRELOAD); no libfaketime found on PATH/ldconfig globs, skipping")
	}
	if daemonIsStatic(t, effectiveBinary(t)) {
		t.Skipf("daemon binary is statically linked; libfaketime cannot intercept it, skipping")
	}
	t.Logf("using libfaketime at %s", lib)

	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "clock-skew",
		NumNodes:    3,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{
			{Name: "skew_rows", Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			}},
			{Name: "skew_probe", Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			}},
		}},
	})
	baseID := fmt.Sprintf("%032x", 1)
	if err := cluster.ExecSQL(0, "INSERT INTO skew_rows (id, name) VALUES (?, ?)", baseID, "baseline"); err != nil {
		t.Fatalf("baseline insert: %v", err)
	}
	waitConverged(t, cluster, "skew_rows", 1, 30*time.Second)

	// Restart node3 under libfaketime, calibrating the offset syntax:
	// the winner is the first spec whose probe write observably moves
	// the node's HLC wall component ~600s past its pre-stop value.
	hlcPre := nodeStatus(t, cluster.Nodes[skewed].APIAddr).HLC
	spec := calibrateSkew(t, cluster, lib, hlcPre)
	t.Logf("node3 skewed +10min via FAKETIME=%q", spec)

	// Phase A: a skewed write deterministically beats a real-time
	// write on the same cell (its HLC leads by ~10min).
	if err := cluster.ExecSQL(0, "UPDATE skew_rows SET name = ? WHERE id = ?", "realtime-1", baseID); err != nil {
		t.Fatalf("realtime write: %v", err)
	}
	waitCellValue(t, cluster, baseID, "realtime-1", 20*time.Second)
	if err := cluster.ExecSQL(skewed, "UPDATE skew_rows SET name = ? WHERE id = ?", "skewed-1", baseID); err != nil {
		t.Fatalf("skewed write: %v", err)
	}
	waitCellValue(t, cluster, baseID, "skewed-1", 20*time.Second)
	t.Logf("skewed write won deterministically on all nodes")

	// Phase B: concurrent writes on all nodes (one shared cell plus
	// disjoint cells) resolve identically everywhere.
	disjointIDs := make([]string, len(cluster.Nodes))
	for i := range cluster.Nodes {
		disjointIDs[i] = fmt.Sprintf("%032x", 100+i)
		if err := cluster.ExecSQL(i, "INSERT INTO skew_rows (id, name) VALUES (?, ?)", disjointIDs[i], "disjoint"); err != nil {
			t.Fatalf("node %d disjoint insert: %v", i, err)
		}
	}
	waitConverged(t, cluster, "skew_rows", 1+len(cluster.Nodes), 30*time.Second)

	const updatesPerNode = 12
	var wg sync.WaitGroup
	errCh := make(chan error, len(cluster.Nodes)*updatesPerNode)
	for node := range cluster.Nodes {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			for seq := 0; seq < updatesPerNode; seq++ {
				v := fmt.Sprintf("node%d-skew-%03d", node+1, seq)
				if err := cluster.ExecSQL(node, "UPDATE skew_rows SET name = ? WHERE id = ?", v, baseID); err != nil {
					errCh <- err
					return
				}
			}
		}(node)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	winner := waitCellConverged(t, cluster, baseID, 30*time.Second)
	if !strings.HasPrefix(winner, "node1-skew-") && !strings.HasPrefix(winner, "node2-skew-") && !strings.HasPrefix(winner, "node3-skew-") {
		t.Fatalf("shared-cell winner %q did not come from the concurrent write set", winner)
	}
	t.Logf("concurrent skewed contention converged to %q on all nodes", winner)
	waitConverged(t, cluster, "skew_rows", 1+len(cluster.Nodes), 30*time.Second)

	// Phase C: no cert-validity flapping. Peering is stable and no
	// handshake/accept failure counter moved on any node.
	for i := range cluster.Nodes {
		waitConnectedPeers(t, cluster, i, 2, 15*time.Second)
	}
	for i := range cluster.Nodes {
		for _, name := range []string{
			"spedsql_repl_handshake_failures_total",
			"spedsql_repl_handshake_identity_refusals_total",
			"spedsql_repl_accept_failures_total",
		} {
			if got := metricValue(t, cluster.Nodes[i].APIAddr, name); got != 0 {
				t.Fatalf("node %d %s = %v, want 0 (cert-validity flapping)", i+1, name, got)
			}
		}
	}

	// Post-skew write on a real-time node replicates everywhere.
	if err := cluster.ExecSQL(0, "INSERT INTO skew_rows (id, name) VALUES (?, ?)", fmt.Sprintf("%032x", 999), "post-skew"); err != nil {
		t.Fatalf("post-skew insert: %v", err)
	}
	waitConverged(t, cluster, "skew_rows", 2+len(cluster.Nodes), 30*time.Second)
}

// calibrateSkew restarts node `skewed` under each candidate FAKETIME
// spec until a probe write moves its HLC wall component into
// [500s, 700s] past hlcPre. It returns the working spec and leaves the
// node running skewed; it fails when the library is present but no
// spec skews the clock (a loud failure, never a vacuous pass).
func calibrateSkew(t *testing.T, c *harness.Cluster, lib string, hlcPre uint64) string {
	t.Helper()
	frozen := time.Now().Add(610 * time.Second).Format("2006-01-02 15:04:05")
	candidates := []string{
		"+0,0,0,0,10,0", // canonical y,mo,d,h,mi,s offset: +10min
		"+10m",
		"+600s",
		"+600",
		"@" + frozen + " x1", // absolute start, normal speed
		"@" + frozen,         // frozen clock fallback
	}
	preMillis := int64(hlcPre >> 16)
	for i, spec := range candidates {
		c.StopNode(skewed)
		startNodeWithEnv(t, c, skewed, []string{
			"LD_PRELOAD=" + lib,
			"FAKETIME=" + spec,
			"FAKETIME_NO_CACHE=1",
		})
		c.UnlockNode(skewed, c.Nodes[skewed].KeyHex)
		c.WaitNodeReady(skewed)
		probeID := fmt.Sprintf("%032x", 5000+i)
		if err := c.ExecSQL(skewed, "INSERT INTO skew_probe (id, name) VALUES (?, ?)", probeID, spec); err != nil {
			t.Logf("spec %q: probe write failed: %v", spec, err)
			continue
		}
		hlc := nodeStatus(t, c.Nodes[skewed].APIAddr).HLC
		delta := int64(hlc>>16) - preMillis
		t.Logf("spec %q: HLC wall delta %+ds", spec, delta)
		if delta >= 500 && delta <= 700 {
			return spec
		}
	}
	wallNow := int64(nodeStatus(t, c.Nodes[0].APIAddr).HLC >> 16)
	t.Fatalf("libfaketime at %s present but no FAKETIME syntax skewed node3 (tried %d specs; node1 wall %d, node3 pre-stop wall %d)",
		lib, len(candidates), wallNow, preMillis)
	return ""
}

// startNodeWithEnv mirrors harness Cluster.StartNode but launches the
// daemon with extra environment (libfaketime). The node stays owned by
// the cluster: StopNode/Cleanup handle it like any other node.
func startNodeWithEnv(t *testing.T, c *harness.Cluster, idx int, env []string) {
	t.Helper()
	node := c.Nodes[idx]
	f, err := os.OpenFile(node.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("open log file %s: %v", node.LogFile, err)
	}
	node.LogFileWriter = f
	bin := node.BinaryPath
	if bin == "" {
		bin = c.BinaryPath
	}
	cmd := exec.Command(bin, "agent", "--config", node.ConfigFile)
	cmd.Stdout = f
	cmd.Stderr = f
	cmd.Env = append(os.Environ(), env...)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start node %s with env: %v", node.Label, err)
	}
	node.Process = cmd
}

func effectiveBinary(t *testing.T) string {
	t.Helper()
	if override := os.Getenv("SPEDSQL_BIN"); override != "" {
		return override
	}
	// Same default the harness resolves (tests-live/bin/testnode).
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(wd, "go.mod")); err == nil {
			return filepath.Join(wd, "tests-live", "bin", "testnode")
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			t.Fatal("could not find repository root")
		}
		wd = parent
	}
}

func daemonIsStatic(t *testing.T, bin string) bool {
	t.Helper()
	out, err := exec.Command("file", "-b", bin).CombinedOutput()
	if err != nil {
		t.Logf("`file` unavailable (%v); attempting faketime anyway", err)
		return false
	}
	return strings.Contains(string(out), "statically linked")
}

func findFaketimeLib() string {
	if env := os.Getenv("FAKETIME_LIB"); env != "" {
		if _, err := os.Stat(env); err == nil {
			return env
		}
	}
	globs := []string{
		"/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so*",
		"/usr/lib/aarch64-linux-gnu/faketime/libfaketime.so*",
		"/usr/lib64/faketime/libfaketime.so*",
		"/usr/lib/faketime/libfaketime.so*",
		"/usr/local/lib/faketime/libfaketime.so*",
		"/opt/*/lib/faketime/libfaketime.so*",
	}
	for _, g := range globs {
		if hits, _ := filepath.Glob(g); len(hits) > 0 {
			// Prefer the versioned .so.1 over the linker stub.
			for _, h := range hits {
				if strings.HasSuffix(h, ".so.1") {
					return h
				}
			}
			return hits[0]
		}
	}
	if out, err := exec.Command("ldconfig", "-p").CombinedOutput(); err == nil {
		for _, line := range strings.Split(string(out), "\n") {
			if !strings.Contains(line, "libfaketime") {
				continue
			}
			fields := strings.Fields(line)
			candidate := fields[len(fields)-1]
			if _, err := os.Stat(candidate); err == nil {
				return candidate
			}
		}
	}
	return ""
}

func waitCellValue(t *testing.T, c *harness.Cluster, id, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for i := range c.Nodes {
			res, err := c.QuerySQL(i, "SELECT name FROM skew_rows WHERE id = ?", id)
			if err != nil || len(res.Rows) != 1 || len(res.Rows[0]) == 0 || fmt.Sprint(res.Rows[0][0]) != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cell %s did not become %q on all nodes within %v", id, want, timeout)
}

func waitCellConverged(t *testing.T, c *harness.Cluster, id string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var winner string
		converged := true
		for i := range c.Nodes {
			res, err := c.QuerySQL(i, "SELECT name FROM skew_rows WHERE id = ?", id)
			if err != nil || len(res.Rows) != 1 || len(res.Rows[0]) == 0 {
				converged = false
				break
			}
			name := fmt.Sprint(res.Rows[0][0])
			if i == 0 {
				winner = name
			} else if name != winner {
				converged = false
				break
			}
		}
		if converged {
			return winner
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cell %s did not converge within %v", id, timeout)
	return ""
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

func waitConnectedPeers(t *testing.T, c *harness.Cluster, idx, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if nodeStatus(t, c.Nodes[idx].APIAddr).ConnectedPeers == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("node %d connected peers = %d, want %d within %v",
		idx, nodeStatus(t, c.Nodes[idx].APIAddr).ConnectedPeers, want, timeout)
}

type nodeStatusJSON struct {
	HLC            uint64 `json:"hlc"`
	ConnectedPeers int    `json:"connected_peers"`
}

func nodeStatus(t *testing.T, apiAddr string) nodeStatusJSON {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		t.Fatalf("status %s: %v", apiAddr, err)
	}
	defer resp.Body.Close()
	var st nodeStatusJSON
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode status %s: %v", apiAddr, err)
	}
	return st
}

func metricValue(t *testing.T, apiAddr, name string) float64 {
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
		v, err := strconv.ParseFloat(rest, 64)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		return v
	}
	t.Fatalf("counter %s not present in /metrics", name)
	return 0
}
