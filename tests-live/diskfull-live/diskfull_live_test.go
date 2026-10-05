// Disk-full acceptance: with a node's data directory on a size-limited
// tmpfs, writes driven to ENOSPC must fail closed (errors, no panic, no
// partial commits); after freeing space and restarting, acknowledged rows
// must be intact and the mesh must reconverge.
//
// Requires tmpfs mount privilege (root in CI). Without it the suite skips
// with a clear message; it never fails for lack of privilege.
//
// The suite uses Schema (replicated registry) tables: tables created only
// via SchemaSQL DDL are local-only sqlite and would make the test vacuous.
package diskfulllive_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func schemaConfig() *db.SchemaConfig {
	return &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
		Name: "disk_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "val", Type: schema.ColText, Nullable: true},
		},
	}}}
}

func tmpfsMB() int {
	if v := harness.GetEnv("MURMUR_DISKFULL_TMPFS_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 32 {
			return n
		}
	}
	return 64
}

func fillRowCap() int {
	if v := harness.GetEnv("MURMUR_DISKFULL_FILL_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 300
}

func TestDiskFullFailClosedAndRecover(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "diskfull-live",
		NumNodes:    2,
		AwaitUnlock: true,
		Schema:      schemaConfig(),
	})
	node0 := cluster.Nodes[0]

	// Baseline: acked rows on both nodes before any fault.
	const baseline = 20
	for i := 0; i < baseline; i++ {
		if err := cluster.ExecSQL(0, "INSERT INTO disk_rows (id, val) VALUES (?, ?)",
			fmt.Sprintf("%032x", 1000+i), fmt.Sprintf("baseline-%d", i)); err != nil {
			t.Fatalf("baseline write: %v", err)
		}
	}
	waitConverged(t, cluster, baseline, 30*time.Second)

	// Move node0's data directory onto a size-limited tmpfs.
	cluster.StopNode(0)
	staging := t.TempDir()
	if out, err := exec.Command("cp", "-a", node0.Dir+"/.", staging+"/").CombinedOutput(); err != nil {
		t.Fatalf("stage node dir: %v: %s", err, out)
	}
	mountOut, err := exec.Command("mount", "-t", "tmpfs",
		"-o", fmt.Sprintf("size=%dm", tmpfsMB()), "tmpfs", node0.Dir).CombinedOutput()
	if err != nil {
		t.Skipf("SKIP: tmpfs mount requires privilege (mount: %s: %v); run as root to exercise ENOSPC",
			strings.TrimSpace(string(mountOut)), err)
	}
	t.Logf("node0 data dir on %dM tmpfs", tmpfsMB())
	// Unmount before the harness cleanup runs (LIFO): otherwise the mount
	// leaks into later suites. On failure, snapshot the tmpfs contents
	// aside first so retained artifacts keep the evidence.
	t.Cleanup(func() {
		if t.Failed() {
			snap := node0.Dir + "-diskfull-snapshot"
			_ = os.RemoveAll(snap)
			_ = os.MkdirAll(snap, 0o755)
			_, _ = exec.Command("cp", "-a", node0.Dir+"/.", snap+"/").CombinedOutput()
		}
		_, _ = exec.Command("umount", node0.Dir).CombinedOutput()
	})
	if out, err := exec.Command("cp", "-a", staging+"/.", node0.Dir+"/").CombinedOutput(); err != nil {
		t.Fatalf("restore node dir onto tmpfs: %v: %s", err, out)
	}

	cluster.StartNode(0)
	cluster.UnlockNode(0, node0.KeyHex)
	cluster.WaitNodeReady(0)
	waitConverged(t, cluster, baseline, 30*time.Second)

	// Leave only a sliver of free space so the fill loop hits ENOSPC fast.
	const reserveFree = 3 << 20
	var st syscall.Statfs_t
	if err := syscall.Statfs(node0.Dir, &st); err != nil {
		t.Fatal(err)
	}
	free := int64(st.Bavail) * int64(st.Bsize)
	t.Logf("tmpfs free before filler: %d bytes", free)
	fillerPath := filepath.Join(node0.Dir, "enospc-filler.bin")
	fillerSize := free - reserveFree
	if fillerSize < 0 {
		fillerSize = 0
	}
	if err := writeZeros(fillerPath, fillerSize); err != nil {
		t.Fatalf("write filler: %v", err)
	}

	// Drive writes to ENOSPC. Track acked vs errored rows exactly.
	bigVal := strings.Repeat("F", 128*1024)
	acked := make(map[string]string)
	var errored []string
	sawError := false
	for i := 0; i < fillRowCap(); i++ {
		id := fmt.Sprintf("%032x", 5000+i)
		val := fmt.Sprintf("%s-row%d", bigVal, i)
		if err := cluster.ExecSQL(0, "INSERT INTO disk_rows (id, val) VALUES (?, ?)", id, val); err != nil {
			sawError = true
			errored = append(errored, id)
			if len(errored) >= 5 {
				break
			}
			continue
		}
		acked[id] = val
	}
	if !sawError {
		if err := syscall.Statfs(node0.Dir, &st); err == nil {
			t.Logf("tmpfs free after fill: %d bytes", int64(st.Bavail)*int64(st.Bsize))
		}
		t.Fatalf("fill loop never hit an error in %d rows; ENOSPC not reached", fillRowCap())
	}
	t.Logf("ENOSPC reached: %d fill rows acked, %d errored", len(acked), len(errored))

	// Fail-closed: the node must still be alive and answering (errors are
	// fine, hangs and panics are not).
	cluster.WaitNodeReady(0)
	probeErr := cluster.ExecSQL(0, "INSERT INTO disk_rows (id, val) VALUES (?, ?)",
		fmt.Sprintf("%032x", 99999), "liveness-probe")
	t.Logf("liveness probe during ENOSPC: err=%v", probeErr)
	if probeErr == nil {
		acked[fmt.Sprintf("%032x", 99999)] = "liveness-probe"
	}
	raw, _ := os.ReadFile(node0.LogFile)
	if strings.Contains(strings.ToLower(string(raw)), "panic") {
		t.Fatalf("node log contains panic during ENOSPC:\n%s", tailLines(string(raw), 20))
	}

	// Free space and restart; acknowledged rows must be intact.
	_ = os.Remove(fillerPath)
	cluster.StopNode(0)
	cluster.StartNode(0)
	cluster.UnlockNode(0, node0.KeyHex)
	cluster.WaitNodeReady(0)

	for id, want := range acked {
		res, err := cluster.QuerySQL(0, "SELECT val FROM disk_rows WHERE id = ?", id)
		if err != nil || len(res.Rows) != 1 {
			t.Fatalf("acked row %s missing after recovery: err=%v res=%v", id, err, res)
		}
		if got := fmt.Sprintf("%v", res.Rows[0][0]); got != want {
			t.Fatalf("acked row %s corrupted: %d bytes, want %d", id, len(got), len(want))
		}
	}
	// Errored rows must be absent or whole: never a partial commit.
	for _, id := range errored {
		res, err := cluster.QuerySQL(0, "SELECT val FROM disk_rows WHERE id = ?", id)
		if err != nil {
			t.Fatalf("errored row %s query: %v", id, err)
		}
		if len(res.Rows) == 1 {
			got := fmt.Sprintf("%v", res.Rows[0][0])
			if !strings.HasPrefix(got, bigVal) || len(got) <= len(bigVal) {
				t.Fatalf("errored row %s partially committed (%d bytes)", id, len(got))
			}
		}
	}
	t.Logf("recovery: all %d acked rows intact, %d errored rows clean", len(acked), len(errored))

	// Mesh convergence plus post-recovery writes on both nodes.
	wantTotal := baseline + len(acked)
	waitConverged(t, cluster, wantTotal, 90*time.Second)
	for i := 0; i < 5; i++ {
		if err := cluster.ExecSQL(0, "INSERT INTO disk_rows (id, val) VALUES (?, ?)",
			fmt.Sprintf("%032x", 20000+i), "post-a"); err != nil {
			t.Fatalf("post-recovery write node0: %v", err)
		}
		if err := cluster.ExecSQL(1, "INSERT INTO disk_rows (id, val) VALUES (?, ?)",
			fmt.Sprintf("%032x", 21000+i), "post-b"); err != nil {
			t.Fatalf("post-recovery write node1: %v", err)
		}
	}
	waitConverged(t, cluster, wantTotal+10, 90*time.Second)
	t.Logf("PASS: ENOSPC failed closed, recovery intact, mesh converged")
}

func writeZeros(path string, size int64) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 1<<20)
	for size > 0 {
		n := int64(len(buf))
		if n > size {
			n = size
		}
		if _, err := f.Write(buf[:n]); err != nil {
			return err
		}
		size -= n
	}
	return f.Sync()
}

func tailLines(s string, n int) string {
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, "disk_rows")
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, "disk_rows", "val")
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
	t.Fatalf("nodes did not converge on %d rows within %v", want, timeout)
}
