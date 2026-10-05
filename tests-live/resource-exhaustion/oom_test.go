package resourceexhaustion_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// A memory-starved node must stay correct. Two legs:
//
// A. Go-heap cap: small block cache + tight GOMEMLIMIT + concurrent
// large-value churn. Every write must succeed; the node stays
// responsive and exactly converged.
//
// B. RSS cap (best effort): the victim moves into a memory.max cgroup
// sized at its own phase-A peak. Whether the kernel OOM-kills it (then
// it must restart and recover exactly) or it survives (then it must
// converge directly), the verdict is exact convergence. Skips with a
// banner when systemd-run is unavailable.
//
//	MURMUR_RX_OOM_MEMLIMIT=128MiB   Go heap cap for the victim (leg A)
//	MURMUR_RX_OOM_ROWS=2000         small-row churn (2 KiB values)
//	MURMUR_RX_OOM_BURST=20          near-max-value burst (8 MiB values)
//	MURMUR_RX_OOM_CONC=4            concurrent large writers (leg A)
//
// Burst concurrency stays at 4: big commits serialize on the writer
// mutex, and 8 lanes queue past the 15s client timeout by lock wait
// alone (not memory pressure). Heap pressure comes from value size
// times churn, not lane count.
func TestNearOOMSurvivesAndConverges(t *testing.T) {
	memLimit := getenv("MURMUR_RX_OOM_MEMLIMIT", "128MiB")
	rows := harness.EnvInt("MURMUR_RX_OOM_ROWS", 2000)
	burst := harness.EnvInt("MURMUR_RX_OOM_BURST", 20)
	conc := harness.EnvInt("MURMUR_RX_OOM_CONC", 4)

	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "resource-oom",
		NumNodes:    2,
		AwaitUnlock: true,
		Schema:      rxSchema(),
		PebbleByNode: map[int]*harness.PebbleOptions{
			// 16x less cache than default; the victim must serve
			// churn far larger than its hot set.
			1: {CacheBytes: 16 << 20, MemTableBytes: 1 << 20},
		},
		NodeEnv: map[int][]string{
			1: {"GOMEMLIMIT=" + memLimit},
		},
	})
	victimPID := nodePID(t, cluster, 1)
	t.Logf("victim node2 pid=%d GOMEMLIMIT=%s cache=16MiB", victimPID, memLimit)
	peakRSS := sampler(t, victimPID)

	// Leg A1: sustained small-row churn through both nodes.
	val := strings.Repeat("m", 2<<10)
	for i := 0; i < rows; i++ {
		target := i % 2
		if err := cluster.ExecSQL(target, "INSERT INTO rx_rows (id, name) VALUES (?, ?)",
			fmt.Sprintf("%032x", i+1), val); err != nil {
			t.Fatalf("churn insert %d on node %d: %v", i, target+1, err)
		}
		if (i+1)%500 == 0 {
			if _, err := cluster.QueryRowCount(1, tableName); err != nil {
				t.Fatalf("victim unresponsive at churn row %d: %v", i+1, err)
			}
			t.Logf("churn %d/%d rows, victim peak RSS %d MiB", i+1, rows, peakRSS()/1048576)
		}
	}

	// Leg A2: concurrent near-max-value burst aimed at the victim.
	// Simultaneous multi-MB bodies force real Go-heap churn under the cap.
	big := strings.Repeat("B", 8<<20)
	var wg sync.WaitGroup
	errCh := make(chan error, conc*burst)
	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for b := 0; b < burst; b++ {
				id := fmt.Sprintf("%032x", (1<<32)+(w<<20)+b)
				if err := cluster.ExecSQL(1, "INSERT INTO rx_rows (id, name) VALUES (?, ?)", id, big); err != nil {
					errCh <- fmt.Errorf("writer %d burst %d: %w", w, b, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("burst under heap cap: %v", err)
	}
	total := rows + conc*burst
	t.Logf("burst done: %d writers x %d x 8MiB, victim peak RSS %d MiB",
		conc, burst, peakRSS()/1048576)

	if !procAlive(victimPID) {
		t.Fatal("victim process died under heap cap")
	}
	waitCounts(t, cluster, []int{0, 1}, total, 3*time.Minute)
	d := oomWaitDigests(t, cluster, oomSegments(rows, burst, conc, 0), 3*time.Minute)
	t.Logf("leg A PASS: %d rows converged, digest %s, victim peak RSS %d MiB",
		total, d, peakRSS()/1048576)

	// Leg B: hard RSS cap at the victim's own phase-A peak. New churn
	// grows RSS past it; the kernel may OOM-kill. Either outcome must
	// end exactly converged.
	capBytes := peakRSS()
	if capBytes < 256<<20 {
		capBytes = 256 << 20
	}
	if !moveIntoMemoryCap(t, victimPID, capBytes) {
		t.Logf("SKIP leg B: no systemd user session for memory cgroups")
		return
	}
	t.Logf("leg B: victim capped at RSS %d MiB, churning past it", capBytes/1048576)
	edge := strings.Repeat("E", 8<<20)
	more := 0
	// 12 edge rows: enough churn to press past the cap, small enough
	// that the edge digest segment stays ~100MB (15s query budget).
	for i := 0; i < 12; i++ {
		if !procAlive(victimPID) {
			break
		}
		if err := cluster.ExecSQL(1, "INSERT INTO rx_rows (id, name) VALUES (?, ?)",
			fmt.Sprintf("%032x", (2<<32)+i), edge); err != nil {
			// Errors are expected at the edge (kill mid-flight).
			time.Sleep(100 * time.Millisecond)
			continue
		}
		more++
	}
	died := !procAlive(victimPID)
	if died {
		t.Logf("leg B: victim OOM-killed after %d more rows", more)
	} else {
		t.Logf("leg B: victim survived the cap with %d more rows", more)
		// Point reads must keep serving under the cap even though
		// hundred-MB full-table scans cannot (observed: the digest
		// scan fails while capped, so exact convergence is verified
		// after the cap is lifted below).
		if more > 0 {
			probeID := fmt.Sprintf("%032x", 2<<32)
			got := readValue(t, cluster, 1, probeID, time.Minute)
			if len(got) != 8<<20 {
				t.Fatalf("victim point read under cap: len=%d, want %d", len(got), 8<<20)
			}
			t.Logf("leg B: 8MiB point read served under the cap")
		}
	}
	// Lift the cap with a fresh, uncapped process on the same data dir
	// (children of the test process are never in the victim scope), then
	// demand exact convergence: whatever happened under the cap must
	// have left identical bytes on both nodes. Reopening hundreds of MB
	// takes far longer than the harness's 10s unlock budget, so unlock
	// with a patient poll.
	cluster.StopNode(1)
	cluster.StartNode(1)
	oomUnlockNode(t, cluster, 1, 5*time.Minute)
	cluster.WaitNodeReady(1)
	if died {
		t.Logf("recovered from OOM-kill; verifying exact convergence")
	}
	waitCounts(t, cluster, []int{0, 1}, total+more, 3*time.Minute)
	d2 := oomWaitDigests(t, cluster, oomSegments(rows, burst, conc, more), 3*time.Minute)
	t.Logf("PASS: heap cap + RSS cap survived, %d rows, digest %s", total+more, d2)
}

// moveIntoMemoryCap places pid in a transient systemd user scope capped at
// maxBytes RSS (swap disabled). It reports false when the session cannot
// host user cgroups; the caller must skip, not fail.
func moveIntoMemoryCap(t *testing.T, pid int, maxBytes uint64) bool {
	t.Helper()
	if _, err := exec.LookPath("systemd-run"); err != nil {
		return false
	}
	if _, err := exec.LookPath("systemctl"); err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	unit := fmt.Sprintf("murmur-rx-oom-%d.scope", os.Getpid())
	// --scope blocks until the payload exits, so launch without waiting
	// and without a kill context; the sleep bounds the scope lifetime and
	// cleanup stops the unit.
	run := exec.Command("systemd-run", "--user", "--scope",
		"--unit="+unit, "--quiet", "sleep", "600")
	if err := run.Start(); err != nil {
		t.Logf("systemd-run scope: %v", err)
		return false
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = exec.CommandContext(ctx, "systemctl", "--user", "stop", unit).Run()
		_ = run.Wait()
	})
	// The scope needs a moment to appear before its cgroup is queryable.
	var out []byte
	var err error
	for i := 0; i < 50; i++ {
		show := exec.CommandContext(ctx, "systemctl", "--user", "show",
			"--property=ControlGroup", "--value", unit)
		if out, err = show.Output(); err == nil && len(strings.TrimSpace(string(out))) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || len(strings.TrimSpace(string(out))) == 0 {
		t.Logf("scope cgroup lookup: %v", err)
		return false
	}
	cg := filepath.Join("/sys/fs/cgroup", strings.TrimSpace(string(out)))
	write := func(name, val string) error {
		return os.WriteFile(filepath.Join(cg, name), []byte(val), 0644)
	}
	if err := write("memory.max", strconv.FormatUint(maxBytes, 10)); err != nil {
		t.Logf("memory.max write: %v", err)
		return false
	}
	_ = write("memory.swap.max", "0") // best effort: no swap escape
	if err := write("cgroup.procs", strconv.Itoa(pid)); err != nil {
		t.Logf("cgroup.procs write: %v", err)
		return false
	}
	return true
}

// oomSegment is a half-open id range [lo, hi) of the OOM row layout.
type oomSegment struct {
	name   string
	lo, hi uint64
}

// oomSegments partitions the OOM dataset (churn + per-writer bursts +
// leg-B edge rows) so no single digest query exceeds ~100MB: a whole
// 500MB+ SELECT * cannot clear the 15s API client timeout, while each
// segment reads in seconds. Exactness is unchanged: every row sits in
// exactly one segment and all segments must match on both nodes.
func oomSegments(rows, burst, conc, more int) []oomSegment {
	segs := []oomSegment{{name: "churn", lo: 1, hi: uint64(rows) + 1}}
	for w := 0; w < conc; w++ {
		base := (uint64(1) << 32) + (uint64(w) << 20)
		segs = append(segs, oomSegment{
			name: fmt.Sprintf("burst-w%d", w), lo: base, hi: base + uint64(burst),
		})
	}
	if more > 0 {
		base := uint64(2) << 32
		segs = append(segs, oomSegment{name: "edge", lo: base, hi: base + uint64(more)})
	}
	return segs
}

// oomWaitDigests polls until every segment digests identically on both
// nodes and returns the combined digest.
func oomWaitDigests(t *testing.T, cluster *harness.Cluster, segs []oomSegment, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var combined string
	for time.Now().Before(deadline) {
		h := sha256.New()
		ok := true
		for _, s := range segs {
			d0, err0 := oomSegmentDigest(cluster, 0, s)
			d1, err1 := oomSegmentDigest(cluster, 1, s)
			if err0 != nil || err1 != nil {
				t.Logf("segment %s unreadable (n0=%v n1=%v), retrying", s.name, err0, err1)
				ok = false
				break
			}
			if d0 != d1 {
				ok = false
				break
			}
			h.Write([]byte(d0))
		}
		if ok {
			combined = hex.EncodeToString(h.Sum(nil))
			return combined
		}
		time.Sleep(2 * time.Second)
	}
	// Final sample names the offending segment.
	for _, s := range segs {
		d0, err0 := oomSegmentDigest(cluster, 0, s)
		d1, err1 := oomSegmentDigest(cluster, 1, s)
		t.Logf("segment %s: n0=%s err=%v | n1=%s err=%v", s.name, d0, err0, d1, err1)
	}
	t.Fatalf("oom segments did not converge within %s", timeout)
	return ""
}

// oomSegmentDigest hashes one id range on one node (same %v: cell
// encoding as the whole-table digest).
func oomSegmentDigest(cluster *harness.Cluster, idx int, s oomSegment) (string, error) {
	res, err := cluster.QuerySQL(idx, "SELECT * FROM rx_rows WHERE id >= ? AND id < ? ORDER BY id",
		fmt.Sprintf("%032x", s.lo), fmt.Sprintf("%032x", s.hi))
	if err != nil {
		return "", err
	}
	h := sha256.New()
	for _, row := range res.Rows {
		for _, cell := range row {
			h.Write([]byte(fmt.Sprintf("%v:", cell)))
		}
		h.Write([]byte("\n"))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// oomUnlockNode unlocks a node with a generous budget for slow reopens
// (the harness helper allows 10s; hundreds of MB need minutes). It uses
// the default transport, which the harness wires with per-node mTLS.
func oomUnlockNode(t *testing.T, cluster *harness.Cluster, idx int, timeout time.Duration) {
	t.Helper()
	node := cluster.Nodes[idx]
	payload, _ := json.Marshal(map[string]string{"key_hex": node.KeyHex, "cipher": "chacha20"})
	url := fmt.Sprintf("https://%s/v1/admin/unlock", node.APIAddr)
	client := &http.Client{Timeout: 30 * time.Second}
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		resp, err := client.Post(url, "application/json", bytes.NewReader(payload))
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNoContent {
				return
			}
			last = fmt.Sprintf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		} else {
			last = err.Error()
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("unlock node %s: %s", node.Label, last)
}

// sampler returns a func reporting the peak RSS so far for pid (bytes).
// On systems without /proc it reports 0 and never fails.
func sampler(t *testing.T, pid int) func() uint64 {
	t.Helper()
	var peak uint64
	status := filepath.Join("/proc", strconv.Itoa(pid), "status")
	return func() uint64 {
		raw, err := os.ReadFile(status)
		if err != nil {
			return peak
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if v, ok := strings.CutPrefix(line, "VmHWM:"); ok {
				var kb uint64
				_, _ = fmt.Sscanf(strings.TrimSpace(v), "%d", &kb)
				if kb*1024 > peak {
					peak = kb * 1024
				}
				break
			}
		}
		return peak
	}
}
