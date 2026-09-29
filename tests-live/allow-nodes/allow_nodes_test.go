package allownodes_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb"
	"github.com/nomadsql/replicateddb/tests-live/harness"
)

// TestAllowedNodeIDsAcrossProcesses checks that certificate validity alone
// does not authorize a process whose NodeID is absent from its peers' lists.
func TestAllowedNodeIDsAcrossProcesses(t *testing.T) {
	node1, node2, node3 := replicateddb.NewNodeID(), replicateddb.NewNodeID(), replicateddb.NewNodeID()
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "allow-nodes",
		NumNodes:    3,
		AwaitUnlock: true,
		NodeIDs:     []replicateddb.NodeID{node1, node2, node3},
		SchemaSQL: `CREATE TABLE IF NOT EXISTS items (
  id BLOB PRIMARY KEY NOT NULL,
  name TEXT NOT NULL DEFAULT ''
);`,
		// Node2 accepts only node1. Node1 and node3 have no identity filter.
		AllowedPeersByNode: map[int][]replicateddb.NodeID{1: {node1}},
	})

	for _, node := range cluster.Nodes {
		t.Logf("%s pid=%d node_id=%s dir=%s repl=%s", node.Label,
			node.Process.Process.Pid, node.NodeID, node.Dir, node.ReplAddr)
	}
	waitForPeers(t, cluster, []int{2, 1, 1}, 20*time.Second)

	writeSeconds := envSeconds("SPEDSQL_ALLOW_NODES_WRITE_SECONDS", 180)
	var written [3]atomic.Int64
	stop := make(chan struct{})
	var writers sync.WaitGroup
	for i := range cluster.Nodes {
		writers.Add(1)
		go func(node int) {
			defer writers.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for seq := int64(1); ; seq++ {
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
				id := fmt.Sprintf("%032x", int64(node+1)*1_000_000+seq)
				if err := cluster.ExecSQL(node, "INSERT INTO items (id, name) VALUES (?, ?)", id,
					fmt.Sprintf("node%d-write-%d", node+1, seq)); err != nil {
					cluster.MarkFailed(fmt.Sprintf("node%d write %d: %v", node+1, seq, err))
					return
				}
				written[node].Add(1)
			}
		}(i)
	}
	time.Sleep(time.Duration(writeSeconds) * time.Second)
	close(stop)
	writers.Wait()
	want := int(written[0].Load() + written[1].Load() + written[2].Load())
	if want < 3 {
		t.Fatalf("too few writes completed during %ds window: %d", writeSeconds, want)
	}
	t.Logf("wrote node1=%d node2=%d node3=%d rows; B allow-list accepts only node1", written[0].Load(), written[1].Load(), written[2].Load())

	deadline := time.Now().Add(45 * time.Second)
	var digest string
	for time.Now().Before(deadline) {
		converged := true
		for i := range cluster.Nodes {
			count, err := cluster.QueryRowCount(i, "items")
			if err != nil || count != want {
				converged = false
				break
			}
			got, err := cluster.ComputeTableDigest(i, "items", "name")
			if err != nil {
				converged = false
				break
			}
			if digest == "" {
				digest = got
			} else if got != digest {
				converged = false
				break
			}
		}
		if converged {
			break
		}
		digest = ""
		time.Sleep(100 * time.Millisecond)
	}
	for i, node := range cluster.Nodes {
		count, err := cluster.QueryRowCount(i, "items")
		if err != nil || count != want {
			t.Fatalf("%s rows=%d err=%v, want all %d application writes", node.Label, count, err, want)
		}
		got, err := cluster.ComputeTableDigest(i, "items", "name")
		if err != nil || got != digest {
			t.Fatalf("%s digest=%s err=%v, want %s", node.Label, got, err, digest)
		}
	}
	// B must keep just A as a direct peer throughout the workload. C reaches B's
	// rows through A but cannot establish an unauthorized direct session.
	waitForPeers(t, cluster, []int{2, 1, 1}, 2*time.Second)
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
	resp, err := http.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	var status struct {
		ConnectedPeers int `json:"connected_peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
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
