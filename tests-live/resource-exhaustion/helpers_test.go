// Shared helpers for the resource-exhaustion suite: near-OOM survival,
// file-descriptor exhaustion, stalled compaction, throttled slow peers,
// and huge transactions at default budgets.
package resourceexhaustion_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// The suite shares the harness's typed contention fixture table
// (live_typed_contention); every cluster sets TypedRecords and
// TypedContention instead of a SchemaConfig.

// rxCount returns the contention-table row count on one node.
func rxCount(cluster *harness.Cluster, idx int) (int, error) {
	rows, err := cluster.TypedContentionRows(idx)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

// rxDigest returns the PK-ordered digest of the contention table on one node.
func rxDigest(cluster *harness.Cluster, idx int) (string, error) {
	rows, err := cluster.TypedContentionRows(idx)
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

func getenv(name, fallback string) string {
	if v := harness.GetEnv(name); v != "" {
		return v
	}
	return fallback
}

// waitCounts polls until every listed node reports want rows or timeout.
func waitCounts(t *testing.T, cluster *harness.Cluster, nodes []int, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, i := range nodes {
			n, err := rxCount(cluster, i)
			if err != nil || n != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("row count did not reach %d on nodes %v within %s", want, nodes, timeout)
}

// waitDigests polls until every listed node reports the same PK-ordered
// digest and returns it. The failure message distinguishes unreadable
// nodes (query errors) from true content divergence.
func waitDigests(t *testing.T, cluster *harness.Cluster, nodes []int, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	last := map[int]string{}
	for time.Now().Before(deadline) {
		var first string
		ok := true
		for k, i := range nodes {
			d, err := rxDigest(cluster, i)
			if err != nil {
				lastErr = fmt.Errorf("node %d: %w", i+1, err)
				ok = false
				break
			}
			last[i] = d[:16]
			if k == 0 {
				first = d
			} else if d != first {
				lastErr = nil
				ok = false
				break
			}
		}
		if ok {
			return first
		}
		time.Sleep(time.Second)
	}
	if lastErr != nil {
		t.Fatalf("digests unreadable on nodes %v within %s: last error %v", nodes, timeout, lastErr)
	}
	t.Fatalf("digest divergence on nodes %v within %s: %v", nodes, timeout, last)
	return ""
}

// countSSTs counts segment and data files under dir (recursive), an observable
// proxy for compaction debt: a node with stalled compactions accumulates
// segment tables while a healthy node folds them away.
func countSSTs(t *testing.T, dir string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() && (strings.HasSuffix(d.Name(), ".seg") || strings.HasSuffix(d.Name(), ".sst")) {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return n
}

// nodePID returns the current daemon PID for a node.
func nodePID(t *testing.T, cluster *harness.Cluster, idx int) int {
	t.Helper()
	pids := cluster.Nodes[idx].Pids
	if len(pids) == 0 {
		t.Fatalf("node %d has no recorded PIDs", idx)
	}
	return pids[len(pids)-1]
}

// procAlive reports whether pid names a live, non-zombie process.
func procAlive(pid int) bool {
	raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return false
	}
	// State is the first field after the comm parenthetical.
	rest, _, _ := strings.Cut(string(raw), ") ")
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return false
	}
	return fields[0] != "Z"
}
