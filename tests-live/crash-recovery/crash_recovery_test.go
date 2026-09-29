package crashrecovery_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb/tests-live/harness"
)

func TestAbruptProcessDeathRecoversCommittedRows(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "crash-recovery",
		NumNodes:    3,
		AwaitUnlock: true,
		SchemaSQL: `CREATE TABLE IF NOT EXISTS crash_rows (
  id BLOB PRIMARY KEY NOT NULL,
  value TEXT NOT NULL DEFAULT ''
);`,
	})
	for _, node := range cluster.Nodes {
		t.Logf("%s pid=%d dir=%s repl=%s", node.Label, node.Process.Process.Pid, node.Dir, node.ReplAddr)
	}

	acknowledged := map[string]struct{}{}
	var ackMu sync.Mutex
	for node := range cluster.Nodes {
		value := fmt.Sprintf("baseline-node%d", node+1)
		if err := cluster.ExecSQL(node, "INSERT INTO crash_rows (id, value) VALUES (?, ?)", fmt.Sprintf("%032x", node+1), value); err != nil {
			t.Fatalf("%s baseline write: %v", cluster.Nodes[node].Label, err)
		}
		acknowledged[value] = struct{}{}
	}
	waitForPeers(t, cluster, []int{2, 2, 2}, 20*time.Second)
	waitForAcknowledged(t, cluster, acknowledged, 20*time.Second)

	stop := make(chan struct{})
	var writers sync.WaitGroup
	var stopWritersOnce sync.Once
	stopWriters := func() {
		stopWritersOnce.Do(func() { close(stop) })
		writers.Wait()
	}
	t.Cleanup(stopWriters)
	var active [3]atomic.Int64
	seqs := [3]atomic.Int64{}
	for node := range cluster.Nodes {
		for worker := 0; worker < 2; worker++ {
			writers.Add(1)
			go func(node, worker int) {
				defer writers.Done()
				ticker := time.NewTicker(50 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-stop:
						return
					case <-ticker.C:
					}
					seq := seqs[node].Add(1)
					value := fmt.Sprintf("node%d-worker%d-write-%08d", node+1, worker, seq)
					id := fmt.Sprintf("%032x", int64(node+1)*1_000_000_000_000+seq*10+int64(worker))
					active[node].Add(1)
					err := cluster.ExecSQL(node, "INSERT INTO crash_rows (id, value) VALUES (?, ?)", id, value)
					active[node].Add(-1)
					if err == nil {
						ackMu.Lock()
						acknowledged[value] = struct{}{}
						ackMu.Unlock()
					} else {
						// Kill/restart and short startup windows can refuse requests;
						// the next unique insert continues the application workload.
						time.Sleep(5 * time.Millisecond)
					}
				}
			}(node, worker)
		}
	}

	writeBurst := time.Duration(envSeconds("SPEDSQL_CRASH_WRITE_SECONDS", 12)) * time.Second
	time.Sleep(writeBurst)
	killDuringWrites(t, cluster, 1, &active)
	time.Sleep(writeBurst)
	restartNode(t, cluster, 1)
	waitForPeers(t, cluster, []int{2, 2, 2}, 30*time.Second)
	t.Log("node2 rejoined under continuing application load")

	time.Sleep(writeBurst)
	killDuringWrites(t, cluster, 2, &active)
	time.Sleep(writeBurst)
	restartNode(t, cluster, 2)
	waitForPeers(t, cluster, []int{2, 2, 2}, 30*time.Second)

	stopWriters()
	waitForConvergence(t, cluster, time.Duration(envSeconds("SPEDSQL_CRASH_SETTLE_SECONDS", 60))*time.Second)
	assertAcknowledgedPresent(t, cluster, acknowledged)
	assertConverged(t, cluster)
	t.Logf("all acknowledged application writes survived two SIGKILL restarts (%d acknowledged rows)", len(acknowledged))

	postCrashValue := "post-recovery-write"
	if err := cluster.ExecSQL(0, "INSERT INTO crash_rows (id, value) VALUES (?, ?)", fmt.Sprintf("%032x", 999_999_999_999), postCrashValue); err != nil {
		t.Fatalf("post-recovery SQL write: %v", err)
	}
	acknowledged[postCrashValue] = struct{}{}
	waitForConvergence(t, cluster, time.Duration(envSeconds("SPEDSQL_CRASH_SETTLE_SECONDS", 20))*time.Second)
	assertAcknowledgedPresent(t, cluster, acknowledged)
	assertConverged(t, cluster)
}

func killDuringWrites(t *testing.T, cluster *harness.Cluster, node int, active *[3]atomic.Int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for active[node].Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if active[node].Load() == 0 {
		t.Fatalf("%s had no SQL request in flight before SIGKILL", cluster.Nodes[node].Label)
	}
	t.Logf("SIGKILL %s with %d SQL requests in flight", cluster.Nodes[node].Label, active[node].Load())
	cluster.KillNode(node)
}

func restartNode(t *testing.T, cluster *harness.Cluster, node int) {
	t.Helper()
	cluster.StartNode(node)
	cluster.UnlockNode(node, cluster.Nodes[node].KeyHex)
	cluster.WaitNodeReady(node)
}

func waitForPeers(t *testing.T, cluster *harness.Cluster, want []int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for i, node := range cluster.Nodes {
			if connectedPeers(node.APIAddr) != want[i] {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	for i, node := range cluster.Nodes {
		t.Fatalf("%s connected peers=%d, want %d", node.Label, connectedPeers(node.APIAddr), want[i])
	}
}

func connectedPeers(apiAddr string) int {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	var status struct {
		ConnectedPeers int `json:"connected_peers"`
	}
	if json.NewDecoder(resp.Body).Decode(&status) != nil {
		return -1
	}
	return status.ConnectedPeers
}

func envSeconds(name string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(name)); err == nil && value > 0 {
		return value
	}
	return fallback
}

func waitForAcknowledged(t *testing.T, cluster *harness.Cluster, acknowledged map[string]struct{}, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if allNodesHaveAcknowledged(cluster, acknowledged) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, node := range cluster.Nodes {
		res, err := cluster.QuerySQL(node.Index, "SELECT value FROM crash_rows")
		if err != nil {
			t.Fatalf("%s recovery query: %v", node.Label, err)
		}
		present := map[string]struct{}{}
		for _, row := range res.Rows {
			if len(row) > 0 {
				present[fmt.Sprint(row[0])] = struct{}{}
			}
		}
		for value := range acknowledged {
			if _, ok := present[value]; !ok {
				t.Fatalf("%s lost acknowledged write %q after restart", node.Label, value)
			}
		}
	}
}

func assertAcknowledgedPresent(t *testing.T, cluster *harness.Cluster, acknowledged map[string]struct{}) {
	t.Helper()
	for _, node := range cluster.Nodes {
		res, err := cluster.QuerySQL(node.Index, "SELECT value FROM crash_rows")
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
				t.Fatalf("%s lost acknowledged write %q after restart", node.Label, value)
			}
		}
	}
}

func allNodesHaveAcknowledged(cluster *harness.Cluster, acknowledged map[string]struct{}) bool {
	for _, node := range cluster.Nodes {
		res, err := cluster.QuerySQL(node.Index, "SELECT value FROM crash_rows")
		if err != nil {
			return false
		}
		present := make(map[string]struct{}, len(res.Rows))
		for _, row := range res.Rows {
			if len(row) > 0 {
				present[fmt.Sprint(row[0])] = struct{}{}
			}
		}
		for value := range acknowledged {
			if _, ok := present[value]; !ok {
				return false
			}
		}
	}
	return true
}

func assertConverged(t *testing.T, cluster *harness.Cluster) {
	t.Helper()
	var digest string
	for i, node := range cluster.Nodes {
		got, err := canonicalTableDigest(cluster, i)
		if err != nil {
			t.Fatalf("%s digest: %v", node.Label, err)
		}
		if digest == "" {
			digest = got
		} else if got != digest {
			t.Fatalf("%s digest %s != %s", node.Label, got, digest)
		}
	}
}

func waitForConvergence(t *testing.T, cluster *harness.Cluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var expectedCount int
		var expectedDigest string
		converged := true
		for i := range cluster.Nodes {
			count, err := cluster.QueryRowCount(i, "crash_rows")
			if err != nil {
				converged = false
				break
			}
			digest, err := canonicalTableDigest(cluster, i)
			if err != nil {
				converged = false
				break
			}
			if i == 0 {
				expectedCount, expectedDigest = count, digest
			} else if count != expectedCount || digest != expectedDigest {
				converged = false
				break
			}
		}
		if converged {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	for i, node := range cluster.Nodes {
		count, _ := cluster.QueryRowCount(i, "crash_rows")
		digest, _ := canonicalTableDigest(cluster, i)
		t.Logf("%s after recovery rows=%d digest=%s", node.Label, count, digest)
	}
	baseline, err := cluster.QuerySQL(0, "SELECT value FROM crash_rows")
	if err == nil {
		base := make(map[string]struct{}, len(baseline.Rows))
		for _, row := range baseline.Rows {
			if len(row) > 0 {
				base[fmt.Sprint(row[0])] = struct{}{}
			}
		}
		for node := 1; node < len(cluster.Nodes); node++ {
			other, queryErr := cluster.QuerySQL(node, "SELECT value FROM crash_rows")
			if queryErr != nil {
				continue
			}
			otherSet := make(map[string]struct{}, len(other.Rows))
			for _, row := range other.Rows {
				if len(row) > 0 {
					otherSet[fmt.Sprint(row[0])] = struct{}{}
				}
			}
			missing, extra := 0, 0
			for value := range otherSet {
				if _, ok := base[value]; !ok {
					missing++
				}
			}
			for value := range base {
				if _, ok := otherSet[value]; !ok {
					extra++
				}
			}
			t.Logf("node1 vs %s content delta: node1-only=%d peer-only=%d", cluster.Nodes[node].Label, extra, missing)
			leftPairs, leftErr := cluster.QuerySQL(0, "SELECT id, value FROM crash_rows")
			rightPairs, rightErr := cluster.QuerySQL(node, "SELECT id, value FROM crash_rows")
			if leftErr == nil && rightErr == nil {
				leftSet := make(map[string]struct{}, len(leftPairs.Rows))
				rightSet := make(map[string]struct{}, len(rightPairs.Rows))
				for _, row := range leftPairs.Rows {
					leftSet[fmt.Sprint(row[0], "|", row[1])] = struct{}{}
				}
				for _, row := range rightPairs.Rows {
					rightSet[fmt.Sprint(row[0], "|", row[1])] = struct{}{}
				}
				leftOnly, rightOnly := 0, 0
				for key := range leftSet {
					if _, ok := rightSet[key]; !ok {
						leftOnly++
					}
				}
				for key := range rightSet {
					if _, ok := leftSet[key]; !ok {
						rightOnly++
					}
				}
				t.Logf("node1 vs %s (id,value) rows: node1-only=%d peer-only=%d", cluster.Nodes[node].Label, leftOnly, rightOnly)
			}
		}
	}
	t.Fatalf("three nodes did not converge after recovery within %v", timeout)
}

func canonicalTableDigest(cluster *harness.Cluster, node int) (string, error) {
	res, err := cluster.QuerySQL(node, "SELECT id, value FROM crash_rows")
	if err != nil {
		return "", err
	}
	rows := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		if len(row) < 2 {
			return "", fmt.Errorf("node %d returned malformed crash_rows row: %v", node, row)
		}
		rows = append(rows, fmt.Sprintf("%q|%q", fmt.Sprint(row[0]), fmt.Sprint(row[1])))
	}
	sort.Strings(rows)
	h := sha256.New()
	for _, row := range rows {
		_, _ = h.Write([]byte(row))
		_, _ = h.Write([]byte("\n"))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
