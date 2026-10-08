package chaosload_test

import (
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

func TestContinuousWritesAcrossPartitionAndHealing(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "chaos-load",
		NumNodes:     3,
		AwaitUnlock:  true,
		TypedRecords: true,
	})
	for _, node := range cluster.Nodes {
		t.Logf("%s pid=%d dir=%s repl=%s", node.Label, node.Process.Process.Pid, node.Dir, node.ReplAddr)
	}
	if err := cluster.TypedInsert(0, "baseline"); err != nil {
		t.Fatalf("baseline insert: %v", err)
	}
	waitAllCount(t, cluster, 1, 20*time.Second)

	// Split {node1,node2} from {node3} by removing every cross-partition
	// relationship in both directions. Keep the surviving pair connected.
	for _, edge := range [][2]int{{0, 2}, {2, 0}, {1, 2}, {2, 1}} {
		if err := cluster.RemovePeer(edge[0], edge[1]); err != nil {
			t.Fatalf("remove peer %s -> %s: %v", cluster.Nodes[edge[0]].Label, cluster.Nodes[edge[1]].Label, err)
		}
	}
	waitForPeers(t, cluster, []int{1, 1, 0}, 20*time.Second)

	var writes [3]atomic.Int64
	var active atomic.Int64
	var paused atomic.Bool
	stop := make(chan struct{})
	var writers sync.WaitGroup
	for node := range cluster.Nodes {
		writers.Add(1)
		go func(node int) {
			defer writers.Done()
			var seq int64
			ticker := time.NewTicker(25 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
				active.Add(1)
				if paused.Load() {
					active.Add(-1)
					continue
				}
				seq++
				err := cluster.TypedInsert(node, fmt.Sprintf("node%d-write-%06d", node+1, seq))
				if err != nil {
					cluster.MarkFailed(fmt.Sprintf("%s write %d failed: %v", cluster.Nodes[node].Label, seq, err))
				} else {
					writes[node].Add(1)
				}
				active.Add(-1)
			}
		}(node)
	}

	partitionSeconds := envSeconds("MURMUR_CHAOS_PARTITION_SECONDS", 12)
	healSeconds := envSeconds("MURMUR_CHAOS_HEAL_SECONDS", 12)
	time.Sleep(time.Duration(partitionSeconds) * time.Second)
	pauseAndDrain(t, &paused, &active)
	leftRows := 1 + int(writes[0].Load()+writes[1].Load())
	rightRows := 1 + int(writes[2].Load())
	if leftRows < 2 || rightRows < 1 {
		t.Fatalf("insufficient partition writes: left=%d right=%d", leftRows, rightRows)
	}
	waitNodeCount(t, cluster, 0, leftRows, 15*time.Second)
	waitNodeCount(t, cluster, 1, leftRows, 15*time.Second)
	waitNodeCount(t, cluster, 2, rightRows, 15*time.Second)
	leftDigest, err := namesDigest(cluster, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rightDigest, err := namesDigest(cluster, 2); err != nil {
		t.Fatal(err)
	} else if rightDigest == leftDigest {
		t.Fatal("partitioned groups unexpectedly have identical state digests")
	}
	if got := connectedPeers(cluster.Nodes[2].APIAddr); got != 0 {
		t.Fatalf("isolated node3 has %d connected peers, want 0", got)
	}
	t.Logf("partition held for %ds with divergent states: node1/node2=%d rows, node3=%d rows", partitionSeconds, leftRows, rightRows)

	// Re-admit the cut edges while all application writers are running again.
	resumeWriters(&paused)
	for _, edge := range [][2]int{{0, 2}, {2, 0}, {1, 2}, {2, 1}} {
		if err := cluster.AddPeer(edge[0], edge[1]); err != nil {
			t.Fatalf("add peer %s -> %s: %v", cluster.Nodes[edge[0]].Label, cluster.Nodes[edge[1]].Label, err)
		}
	}
	waitForPeers(t, cluster, []int{2, 2, 2}, 30*time.Second)
	time.Sleep(time.Duration(healSeconds) * time.Second)
	pauseAndDrain(t, &paused, &active)
	close(stop)
	writers.Wait()
	totalExpected := 1 + int(writes[0].Load()+writes[1].Load()+writes[2].Load())
	waitAllCount(t, cluster, totalExpected, 60*time.Second)
	assertConverged(t, cluster, totalExpected)
	t.Logf("healed after %ds with %d writes; all processes converge", healSeconds, totalExpected-1)

	// A new application write after reconciliation must still traverse the
	// healed mesh.
	if err := cluster.TypedInsert(1, "post-heal"); err != nil {
		t.Fatalf("post-heal write: %v", err)
	}
	waitAllCount(t, cluster, totalExpected+1, 20*time.Second)
	assertConverged(t, cluster, totalExpected+1)
}

func pauseAndDrain(t *testing.T, paused *atomic.Bool, active *atomic.Int64) {
	t.Helper()
	paused.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for active.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if active.Load() != 0 {
		t.Fatalf("%d application writes still active at measurement barrier", active.Load())
	}
}

func resumeWriters(paused *atomic.Bool) { paused.Store(false) }

func waitForPeers(t *testing.T, cluster *harness.Cluster, want []int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for i, node := range cluster.Nodes {
			if got := connectedPeers(node.APIAddr); got != want[i] {
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

var statusHTTPClient = &http.Client{Timeout: 5 * time.Second}

func connectedPeers(apiAddr string) int {
	resp, err := statusHTTPClient.Get("https://" + apiAddr + "/v1/status")
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

func waitAllCount(t *testing.T, cluster *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for i := range cluster.Nodes {
			names, err := cluster.TypedNames(i)
			if err != nil || len(names) != want {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	for i, node := range cluster.Nodes {
		names, err := cluster.TypedNames(i)
		t.Logf("%s rows=%d err=%v", node.Label, len(names), err)
	}
	t.Fatalf("cluster failed to converge to %d rows within %v", want, timeout)
}

func waitNodeCount(t *testing.T, cluster *harness.Cluster, node, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if names, err := cluster.TypedNames(node); err == nil && len(names) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	names, err := cluster.TypedNames(node)
	t.Fatalf("%s rows=%d err=%v, want %d", cluster.Nodes[node].Label, len(names), err, want)
}

func assertConverged(t *testing.T, cluster *harness.Cluster, wantRows int) {
	t.Helper()
	var digest string
	for i, node := range cluster.Nodes {
		names, err := cluster.TypedNames(i)
		if err != nil || len(names) != wantRows {
			t.Fatalf("%s rows=%d err=%v, want %d", node.Label, len(names), err, wantRows)
		}
		got, err := namesDigest(cluster, i)
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

func namesDigest(cluster *harness.Cluster, node int) (string, error) {
	names, err := cluster.TypedNames(node)
	if err != nil {
		return "", err
	}
	sort.Strings(names)
	return strings.Join(names, "\n"), nil
}

func envSeconds(name string, fallback int) int {
	if value, err := strconv.Atoi(harness.GetEnv(name)); err == nil && value > 0 {
		return value
	}
	return fallback
}
