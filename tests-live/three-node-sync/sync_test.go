package threenodesync_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestThreeNodeConcurrentWritesConverge(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "three-node-sync", NumNodes: 3, AwaitUnlock: true, TypedRecords: true,
	})
	for _, node := range cluster.Nodes {
		t.Logf("Node %s running in directory %s (repl=%s api=%s)",
			node.Label, node.Dir, node.ReplAddr, node.APIAddr)
	}

	var wg sync.WaitGroup
	rowsPerNode := 30
	for i := range cluster.Nodes {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()
			for row := 0; row < rowsPerNode; row++ {
				name := fmt.Sprintf("node-%d-row-%02d", nodeIdx, row)
				if err := cluster.TypedInsert(nodeIdx, name); err != nil {
					cluster.MarkFailed(fmt.Sprintf("node %d insert failed: %v", nodeIdx, err))
					return
				}
			}
		}(i)
	}
	wg.Wait()

	want := make([]string, 0, rowsPerNode*len(cluster.Nodes))
	for node := range len(cluster.Nodes) {
		for row := 0; row < rowsPerNode; row++ {
			want = append(want, fmt.Sprintf("node-%d-row-%02d", node, row))
		}
	}
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		converged := true
		for i := range cluster.Nodes {
			got, err := cluster.TypedNames(i)
			if err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
				converged = false
				break
			}
		}
		if converged {
			t.Logf("Three nodes converged on %d typed records", len(want))
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("three nodes did not converge within deadline (expected %d typed records)", len(want))
}
