package fivenodesoak_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

var statusClient = &http.Client{Timeout: 3 * time.Second}

func envSeconds(t *testing.T, key string, fallback int) time.Duration {
	t.Helper()
	if raw := harness.GetEnv(key); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 {
			t.Fatalf("%s must be a positive number of seconds", key)
		}
		return time.Duration(value) * time.Second
	}
	return time.Duration(fallback) * time.Second
}

func TestFiveNodePersistentCapacityAndMeshSoak(t *testing.T) {
	duration := envSeconds(t, "MURMUR_FIVE_NODE_DURATION_SECONDS", 30)
	interval := envMillis(t, "MURMUR_FIVE_NODE_INTERVAL_MS", 50)
	settle := envSeconds(t, "MURMUR_FIVE_NODE_SETTLE_SECONDS", 120)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "long-running-five-node",
		NumNodes:     5,
		AwaitUnlock:  true,
		TypedRecords: true,
	})
	for _, node := range cluster.Nodes {
		t.Logf("%s pid=%d dir=%s repl=%s", node.Label, node.Process.Process.Pid, node.Dir, node.ReplAddr)
	}
	waitForPeers(t, cluster, 4, 45*time.Second)

	var writes [5]atomic.Uint64
	var failures [5]atomic.Uint64
	stop := make(chan struct{})
	var workers sync.WaitGroup
	stopAt := time.Now().Add(duration)
	for node := range cluster.Nodes {
		workers.Add(1)
		go func(node int) {
			defer workers.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
				if time.Now().After(stopAt) {
					return
				}
				seq := writes[node].Load()
				value := fmt.Sprintf("node-%d-capacity-%09d", node+1, seq)
				if err := cluster.TypedInsert(node, value); err != nil {
					failures[node].Add(1)
					continue
				}
				writes[node].Add(1)
			}
		}(node)
	}

	firstRestart := time.Now().Add(duration / 3)
	secondRestart := time.Now().Add(2 * duration / 3)
	time.Sleep(time.Until(firstRestart))
	restartAndCheckPersistence(t, cluster, 1)
	time.Sleep(max(time.Until(secondRestart), 0))
	restartAndCheckPersistence(t, cluster, 3)
	time.Sleep(max(time.Until(stopAt), 0))
	close(stop)
	workers.Wait()

	totalAcked := 0
	for i := range cluster.Nodes {
		totalAcked += int(writes[i].Load())
		if writes[i].Load() < 10 {
			t.Fatalf("%s committed only %d rows during %s", cluster.Nodes[i].Label, writes[i].Load(), duration)
		}
		t.Logf("%s acknowledged=%d transient_http_errors=%d", cluster.Nodes[i].Label, writes[i].Load(), failures[i].Load())
	}
	if totalAcked < 100 {
		t.Fatalf("only %d application writes committed across five nodes", totalAcked)
	}

	deadline := time.Now().Add(settle)
	var finalCount int
	var finalDigest string
	converged := false
	for time.Now().Before(deadline) {
		counts := make([]int, len(cluster.Nodes))
		digests := make([]string, len(cluster.Nodes))
		all := true
		for i := range cluster.Nodes {
			count, err := rowCount(cluster, i)
			if err != nil {
				all = false
				break
			}
			counts[i] = count
			digests[i], err = tableDigest(cluster, i)
			if err != nil {
				all = false
				break
			}
		}
		if all {
			matched := true
			for i := 1; i < len(counts); i++ {
				if counts[i] != counts[0] || digests[i] != digests[0] {
					matched = false
					break
				}
			}
			if matched && counts[0] >= totalAcked {
				finalCount, finalDigest, converged = counts[0], digests[0], true
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !converged {
		for i, node := range cluster.Nodes {
			count, err := rowCount(cluster, i)
			t.Logf("%s count=%d err=%v peers=%d", node.Label, count, err, connectedPeers(node.APIAddr))
		}
		t.Fatalf("five-node mesh did not converge to at least %d acknowledged rows within %s", totalAcked, settle)
	}
	for _, node := range cluster.Nodes {
		metrics, err := scrapeMetrics(node.APIAddr)
		if err != nil {
			t.Fatalf("%s metrics: %v", node.Label, err)
		}
		if metric(metrics, "spedsql_materialized_generation") != metric(metrics, "spedsql_state_generation") {
			t.Fatalf("%s state has not been fully materialized", node.Label)
		}
		if metric(metrics, "spedsql_spool_disk_bytes") > 2<<30 {
			t.Fatalf("%s exceeded 2 GiB persistent capacity limit", node.Label)
		}
	}
	if err := cluster.TypedInsert(4, "post-soak-write"); err != nil {
		t.Fatalf("post-soak application write: %v", err)
	}
	postDigest := waitForConvergence(t, cluster, finalCount+1, settle)
	t.Logf("five encrypted node processes held %s of load, survived two orderly data-directory restarts, and converged %d rows (digest=%s; before-post-write=%s)", duration, finalCount+1, postDigest, finalDigest)
}

func envMillis(t *testing.T, key string, fallback int) time.Duration {
	t.Helper()
	if raw := harness.GetEnv(key); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 {
			t.Fatalf("%s must be a positive number of milliseconds", key)
		}
		return time.Duration(value) * time.Millisecond
	}
	return time.Duration(fallback) * time.Millisecond
}

func restartAndCheckPersistence(t *testing.T, cluster *harness.Cluster, node int) {
	t.Helper()
	started := time.Now()
	before, err := rowCount(cluster, node)
	if err != nil {
		t.Fatalf("%s pre-restart count: %v", cluster.Nodes[node].Label, err)
	}
	cluster.StopNode(node)
	cluster.StartNode(node)
	cluster.UnlockNode(node, cluster.Nodes[node].KeyHex)
	cluster.WaitNodeReady(node)
	waitForPeers(t, cluster, 4, 45*time.Second)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if got, err := rowCount(cluster, node); err == nil && got >= before {
			t.Logf("%s restarted and rejoined in %s with %d rows (had %d before restart)", cluster.Nodes[node].Label, time.Since(started), got, before)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	got, err := rowCount(cluster, node)
	t.Fatalf("%s lost durable rows after restart: got=%d err=%v before=%d", cluster.Nodes[node].Label, got, err, before)
}

func waitForPeers(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, node := range cluster.Nodes {
			if connectedPeers(node.APIAddr) != want {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, node := range cluster.Nodes {
		t.Fatalf("%s connected peers=%d, want %d", node.Label, connectedPeers(node.APIAddr), want)
	}
}

func connectedPeers(apiAddr string) int {
	resp, err := statusClient.Get("https://" + apiAddr + "/v1/status")
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

func rowCount(cluster *harness.Cluster, node int) (int, error) {
	names, err := cluster.TypedNames(node)
	if err != nil {
		return 0, fmt.Errorf("read typed rows: %w", err)
	}
	return len(names), nil
}

func tableDigest(cluster *harness.Cluster, node int) (string, error) {
	rows, err := cluster.TypedNames(node)
	if err != nil {
		return "", err
	}
	sort.Strings(rows)
	h := sha256.New()
	for _, row := range rows {
		_, _ = fmt.Fprintln(h, row)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func waitForConvergence(t *testing.T, cluster *harness.Cluster, count int, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		var digest string
		for node := range cluster.Nodes {
			gotCount, err := rowCount(cluster, node)
			gotDigest, digestErr := tableDigest(cluster, node)
			if err != nil || digestErr != nil || gotCount != count {
				all = false
				break
			}
			if node == 0 {
				digest = gotDigest
			} else if gotDigest != digest {
				all = false
				break
			}
		}
		if all {
			return digest
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("post-soak write did not converge to %d rows", count)
	return ""
}

func scrapeMetrics(apiAddr string) (string, error) {
	resp, err := statusClient.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return string(data), err
}

func metric(metrics, name string) float64 {
	if value, ok := harness.MetricValueFrom(metrics, name); ok {
		return value
	}
	return -1
}
