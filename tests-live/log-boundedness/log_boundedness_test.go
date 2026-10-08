// Log boundedness: under sustained write load the daemon's text log must
// not grow per row, and on-disk usage must stay proportional to the data
// written (no runaway duplication or leak).
//
// Mechanism note: the daemon emits no per-write log lines (startup lines
// plus rare errors only); there is no rotation because there is nothing to
// rotate. The ceiling below enforces that property: any per-row log spam
// (~100 bytes/line with timestamps) exceeds the per-row allowance.
//
// The suite writes replicated typed records so the load exercises the
// durable replicated path; node-local writes would make it vacuous.
package logboundedness_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func loadSeconds() int {
	if v := harness.GetEnv("MURMUR_LOG_BOUNDEDNESS_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 75
}

func TestLogBoundednessUnderSustainedLoad(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "log-boundedness",
		NumNodes:        2,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
	})

	duration := time.Duration(loadSeconds()) * time.Second
	startLog := logSizes(t, cluster)
	startDisk := spoolSizes(t, cluster)
	midLog, midDisk := []int64(nil), []int64(nil)

	// Fixed-window sustained write load, alternating nodes.
	deadline := time.Now().Add(duration)
	acked := 0
	seq := 0
	sampled := false
	for time.Now().Before(deadline) {
		val := fmt.Sprintf("payload-%d-%s", seq, strings.Repeat("abcdefgh", 25))
		if err := cluster.TypedContentionInsert(seq%2, harness.TypedContentionRow{ID: fmt.Sprintf("%032x", seq), Name: val}); err != nil {
			t.Fatalf("load write %d: %v", seq, err)
		}
		acked++
		seq++
		if !sampled && time.Until(deadline) < duration/2 {
			midLog, midDisk = logSizes(t, cluster), spoolSizes(t, cluster)
			sampled = true
		}
	}
	t.Logf("load complete: %d acked rows in %v", acked, duration)
	if acked < 100 {
		t.Fatalf("only %d rows acked, load too small to judge boundedness", acked)
	}

	endLog := logSizes(t, cluster)
	endDisk := spoolSizes(t, cluster)
	for i := range cluster.Nodes {
		t.Logf("node%d log bytes start=%d mid=%d end=%d (growth=%d)",
			i, startLog[i], midLog[i], endLog[i], endLog[i]-startLog[i])
		t.Logf("node%d spool bytes start=%d mid=%d end=%d (growth=%d)",
			i, startDisk[i], midDisk[i], endDisk[i], endDisk[i]-startDisk[i])
	}

	// Primary assertion: per-node log growth ceiling. The 128 KiB floor
	// absorbs startup lines and error bursts; the 16 bytes/row slope is
	// far below any per-row log line, so per-row spam fails.
	for i := range cluster.Nodes {
		growth := endLog[i] - startLog[i]
		ceiling := int64(128*1024 + 16*acked)
		if growth > ceiling {
			t.Fatalf("node%d log growth %d bytes over %d rows exceeds ceiling %d (unbounded per-row logging?)",
				i, growth, acked, ceiling)
		}
	}

	// No panic/fatal may be recorded during the load window.
	for i, node := range cluster.Nodes {
		raw, err := os.ReadFile(node.LogFile)
		if err != nil {
			t.Fatal(err)
		}
		lower := strings.ToLower(string(raw))
		if strings.Contains(lower, "panic") || strings.Contains(lower, "fatal") {
			t.Fatalf("node%d log contains panic/fatal:\n%s", i, tailLines(string(raw), 20))
		}
	}

	// Disk usage must stay proportional to data: 200-byte values must not
	// cost more than 8 KiB/row on disk (observed ~2.2 KiB/row; 8 KiB keeps
	// 3.6x headroom for compaction timing while catching 10x blowups),
	// and second-half growth must not accelerate past 2x the first half
	// plus 1 MiB slack (stabilization, not runaway).
	for i := range cluster.Nodes {
		growth := endDisk[i] - startDisk[i]
		if cap := int64(acked) * 8192; growth > cap {
			t.Fatalf("node%d spool growth %d bytes over %d rows exceeds %d (8KiB/row cap)",
				i, growth, acked, cap)
		}
		firstHalf := midDisk[i] - startDisk[i]
		secondHalf := endDisk[i] - midDisk[i]
		if secondHalf > 2*firstHalf+1024*1024 {
			t.Fatalf("node%d spool growth accelerates: first-half=%d second-half=%d",
				i, firstHalf, secondHalf)
		}
	}

	// Positive control: honest behavior works — the mesh converges on
	// exactly the acked rows with identical digests.
	waitConverged(t, cluster, acked, 90*time.Second)
	t.Logf("PASS: log growth bounded, disk proportional, mesh converged on %d rows", acked)
}

func logSizes(t *testing.T, c *harness.Cluster) []int64 {
	t.Helper()
	out := make([]int64, len(c.Nodes))
	for i, n := range c.Nodes {
		fi, err := os.Stat(n.LogFile)
		if err != nil {
			t.Fatal(err)
		}
		out[i] = fi.Size()
	}
	return out
}

func spoolSizes(t *testing.T, c *harness.Cluster) []int64 {
	t.Helper()
	out := make([]int64, len(c.Nodes))
	for i, n := range c.Nodes {
		var total int64
		err := filepath.WalkDir(n.Dir, func(_ string, e os.DirEntry, err error) error {
			if err == nil && !e.IsDir() {
				if fi, err := e.Info(); err == nil {
					total += fi.Size()
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		out[i] = total
	}
	return out
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
			rows, err := c.TypedContentionRows(i)
			if err != nil || len(rows) != want {
				ok = false
				break
			}
			sort.Slice(rows, func(a, b int) bool {
				if rows[a].Name != rows[b].Name {
					return rows[a].Name < rows[b].Name
				}
				return rows[a].ID < rows[b].ID
			})
			h := sha256.New()
			for _, row := range rows {
				fmt.Fprintf(h, "%s:%s:%s:%d\n", row.ID, row.Name, row.Phone, row.Score)
			}
			d := hex.EncodeToString(h.Sum(nil))
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
