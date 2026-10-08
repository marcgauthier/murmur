package soakslo_test

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

var scenarioHTTP = &http.Client{Timeout: 3 * time.Second}

func seconds(t *testing.T, name string, fallback int) time.Duration {
	t.Helper()
	if raw := harness.GetEnv(name); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			t.Fatalf("%s must be a positive number of seconds", name)
		}
		return time.Duration(n) * time.Second
	}
	return time.Duration(fallback) * time.Second
}

func TestThreeNodeEncryptedSustainedWriteSLO(t *testing.T) {
	duration := seconds(t, "MURMUR_SLO_DURATION_SECONDS", 5)
	settle := seconds(t, "MURMUR_SLO_SETTLE_SECONDS", 30)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "soak-slo",
		NumNodes:        3,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
	})
	for _, node := range cluster.Nodes {
		t.Logf("%s pid=%d dir=%s repl=%s", node.Label, node.Process.Process.Pid, node.Dir, node.ReplAddr)
	}
	waitForPeers(t, cluster, 2, 30*time.Second)

	var writes [3]atomic.Uint64
	var latencyMu sync.Mutex
	latencies := make([]time.Duration, 0, 4096)
	errCh := make(chan error, len(cluster.Nodes))
	stopAt := time.Now().Add(duration)
	var workers sync.WaitGroup
	for i := range cluster.Nodes {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			for time.Now().Before(stopAt) {
				seq := writes[i].Load()
				rowID := fmt.Sprintf("%032x", int64(i+1)*1_000_000_000_000+int64(seq)+1)
				value := fmt.Sprintf("node-%d-write-%09d", i+1, seq)
				started := time.Now()
				err := cluster.TypedContentionInsert(i, harness.TypedContentionRow{ID: rowID, Name: fmt.Sprintf("node-%d", i+1), Phone: value})
				elapsed := time.Since(started)
				latencyMu.Lock()
				latencies = append(latencies, elapsed)
				latencyMu.Unlock()
				if err != nil {
					errCh <- fmt.Errorf("%s write: %w", cluster.Nodes[i].Label, err)
					return
				}
				writes[i].Add(1)
			}
		}(i)
	}
	workers.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	expected := int(writes[0].Load() + writes[1].Load() + writes[2].Load())
	if expected < 60 {
		t.Fatalf("only %d application rows committed across %s", expected, duration)
	}

	deadline := time.Now().Add(settle)
	var digest string
	converged := false
	for time.Now().Before(deadline) {
		all := true
		var digests [3]string
		for i := range cluster.Nodes {
			count, err := rowCount(cluster, i)
			if err != nil || count != expected {
				all = false
				break
			}
			digests[i], err = tableDigest(cluster, i)
			if err != nil {
				all = false
				break
			}
		}
		if all && digests[0] == digests[1] && digests[1] == digests[2] {
			digest, converged = digests[0], true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !converged {
		for i, node := range cluster.Nodes {
			count, err := rowCount(cluster, i)
			t.Logf("%s count=%d err=%v", node.Label, count, err)
		}
		t.Fatalf("three application processes did not converge to %d rows within %s", expected, settle)
	}

	for _, node := range cluster.Nodes {
		state, err := serviceState(node.APIAddr)
		if err != nil || state != "ready" {
			t.Fatalf("%s service state=%q err=%v, want ready", node.Label, state, err)
		}
		metrics, err := scrapeMetrics(node.APIAddr)
		if err != nil {
			t.Fatalf("%s metrics: %v", node.Label, err)
		}
		stateGen := metricValue(metrics, "spedsql_state_generation")
		materialized := metricValue(metrics, "spedsql_materialized_generation")
		if stateGen < 0 || materialized != stateGen {
			t.Fatalf("%s generation state=%v materialized=%v", node.Label, stateGen, materialized)
		}
		if metricValue(metrics, "spedsql_queued_need") > 5000 || metricValue(metrics, "spedsql_queued_ctrl") > 5000 {
			t.Fatalf("%s exceeded queue SLO: need=%v control=%v", node.Label, metricValue(metrics, "spedsql_queued_need"), metricValue(metrics, "spedsql_queued_ctrl"))
		}
		if metricValue(metrics, "spedsql_spool_disk_bytes") > 512<<20 {
			t.Fatalf("%s Spool size %.0f exceeds 512 MiB SLO", node.Label, metricValue(metrics, "spedsql_spool_disk_bytes"))
		}
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := latencies[(95*len(latencies)+99)/100-1]
	maxLatency := latencies[len(latencies)-1]
	t.Logf("multi-process encrypted write soak duration=%s rows=%d digest=%s writes_per_node=%d/%d/%d write_p95=%s max=%s",
		duration, expected, digest, writes[0].Load(), writes[1].Load(), writes[2].Load(), p95, maxLatency)
	if p95 > 5*time.Second {
		t.Fatalf("write p95 %s exceeded 5s SLO", p95)
	}
	if maxLatency > 20*time.Second {
		t.Fatalf("maximum write latency %s exceeded 20s SLO", maxLatency)
	}
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
		time.Sleep(50 * time.Millisecond)
	}
	for _, node := range cluster.Nodes {
		t.Fatalf("%s connected peers=%d, want %d", node.Label, connectedPeers(node.APIAddr), want)
	}
}

func connectedPeers(apiAddr string) int {
	resp, err := scenarioHTTP.Get("https://" + apiAddr + "/v1/status")
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

func serviceState(apiAddr string) (string, error) {
	resp, err := scenarioHTTP.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var status struct {
		State string `json:"state"`
	}
	err = json.NewDecoder(resp.Body).Decode(&status)
	return status.State, err
}

func scrapeMetrics(apiAddr string) (string, error) {
	resp, err := scenarioHTTP.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return string(data), err
}

func metricValue(metrics, name string) float64 {
	if value, ok := harness.MetricValueFrom(metrics, name); ok {
		return value
	}
	return -1
}

func rowCount(cluster *harness.Cluster, idx int) (int, error) {
	rows, err := cluster.TypedContentionRows(idx)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

func tableDigest(cluster *harness.Cluster, idx int) (string, error) {
	rows, err := cluster.TypedContentionRows(idx)
	if err != nil {
		return "", err
	}
	encoded := make([]string, 0, len(rows))
	for _, row := range rows {
		encoded = append(encoded, fmt.Sprintf("%q:%q:%q", row.ID, row.Name, row.Phone))
	}
	sort.Strings(encoded)
	h := sha256.New()
	for _, row := range encoded {
		_, _ = fmt.Fprintln(h, row)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
