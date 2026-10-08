package crashrecovery_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestAbruptProcessDeathRecoversCommittedRows(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "crash-recovery",
		NumNodes:     3,
		AwaitUnlock:  true,
		TypedRecords: true,
	})
	for _, node := range cluster.Nodes {
		t.Logf("%s pid=%d dir=%s repl=%s", node.Label, node.Process.Process.Pid, node.Dir, node.ReplAddr)
	}

	acknowledged := map[string]struct{}{}
	var ackMu sync.Mutex
	for node := range cluster.Nodes {
		value := fmt.Sprintf("baseline-node%d", node+1)
		if err := cluster.TypedInsert(node, value); err != nil {
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
					active[node].Add(1)
					err := cluster.TypedInsert(node, value)
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

	writeBurst := time.Duration(envSeconds("MURMUR_CRASH_WRITE_SECONDS", 12)) * time.Second
	time.Sleep(writeBurst)
	killDuringWrites(t, cluster, 1, &active)
	time.Sleep(writeBurst)
	restartNode(t, cluster, 1)
	// 90s, not 30s: under a full parallel `go test ./...` the box runs
	// ~10x slow and a rejoined node legitimately needs over half a
	// minute to redial and resync; the proof (rejoin, no lost writes)
	// is unchanged.
	waitForPeers(t, cluster, []int{2, 2, 2}, 90*time.Second)
	t.Log("node2 rejoined under continuing application load")

	time.Sleep(writeBurst)
	killDuringWrites(t, cluster, 2, &active)
	time.Sleep(writeBurst)
	restartNode(t, cluster, 2)
	waitForPeers(t, cluster, []int{2, 2, 2}, 90*time.Second)

	stopWriters()
	waitForConvergence(t, cluster, time.Duration(envSeconds("MURMUR_CRASH_SETTLE_SECONDS", 60))*time.Second)
	assertAcknowledgedPresent(t, cluster, acknowledged)
	assertConverged(t, cluster)
	t.Logf("all acknowledged application writes survived two SIGKILL restarts (%d acknowledged rows)", len(acknowledged))

	postCrashValue := "post-recovery-write"
	if err := cluster.TypedInsert(0, postCrashValue); err != nil {
		t.Fatalf("post-recovery typed write: %v", err)
	}
	acknowledged[postCrashValue] = struct{}{}
	waitForConvergence(t, cluster, time.Duration(envSeconds("MURMUR_CRASH_SETTLE_SECONDS", 20))*time.Second)
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
		t.Fatalf("%s had no typed write in flight before SIGKILL", cluster.Nodes[node].Label)
	}
	t.Logf("SIGKILL %s with %d typed writes in flight", cluster.Nodes[node].Label, active[node].Load())
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
	var states []string
	for i, node := range cluster.Nodes {
		states = append(states, fmt.Sprintf("%s=%d(want %d)", node.Label, connectedPeers(node.APIAddr), want[i]))
	}
	t.Fatalf("peers did not reach %v within %v: %s", want, timeout, strings.Join(states, " "))
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
	if value, err := strconv.Atoi(harness.GetEnv(name)); err == nil && value > 0 {
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
		got, err := cluster.TypedNames(node.Index)
		if err != nil {
			t.Fatalf("%s typed recovery read: %v", node.Label, err)
		}
		present := nameSet(got)
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
		got, err := cluster.TypedNames(node.Index)
		if err != nil {
			t.Fatalf("%s typed recovery read: %v", node.Label, err)
		}
		present := nameSet(got)
		for value := range acknowledged {
			if _, ok := present[value]; !ok {
				t.Fatalf("%s lost acknowledged write %q after restart", node.Label, value)
			}
		}
	}
}

func allNodesHaveAcknowledged(cluster *harness.Cluster, acknowledged map[string]struct{}) bool {
	for _, node := range cluster.Nodes {
		got, err := cluster.TypedNames(node.Index)
		if err != nil {
			return false
		}
		present := nameSet(got)
		for value := range acknowledged {
			if _, ok := present[value]; !ok {
				return false
			}
		}
	}
	return true
}

func nameSet(names []string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}
	return set
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
			names, err := cluster.TypedNames(i)
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
				expectedCount, expectedDigest = len(names), digest
			} else if len(names) != expectedCount || digest != expectedDigest {
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
		names, _ := cluster.TypedNames(i)
		digest, _ := canonicalTableDigest(cluster, i)
		t.Logf("%s after recovery rows=%d digest=%s", node.Label, len(names), digest)
	}
	t.Fatalf("three nodes did not converge after recovery within %v", timeout)
}

func canonicalTableDigest(cluster *harness.Cluster, node int) (string, error) {
	names, err := cluster.TypedNames(node)
	if err != nil {
		return "", err
	}
	rows := append([]string(nil), names...)
	sort.Strings(rows)
	h := sha256.New()
	for _, row := range rows {
		_, _ = h.Write([]byte(row))
		_, _ = h.Write([]byte("\n"))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
