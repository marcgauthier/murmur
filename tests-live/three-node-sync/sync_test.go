package threenodesync_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb/tests-live/harness"
)

func TestThreeNodeConcurrentWritesConverge(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "three-node-sync",
		NumNodes:    3,
		AwaitUnlock: true,
		SchemaSQL: `
CREATE TABLE IF NOT EXISTS items (
  id BLOB PRIMARY KEY NOT NULL,
  name TEXT NOT NULL DEFAULT ''
);
`,
	})

	// Verify all 3 discrete node directories exist
	for _, node := range cluster.Nodes {
		t.Logf("Node %s running in directory %s (repl=%s api=%s)",
			node.Label, node.Dir, node.ReplAddr, node.APIAddr)
	}

	// Concurrent writes across all 3 nodes
	var wg sync.WaitGroup
	rowsPerNode := 30
	for i := range cluster.Nodes {
		wg.Add(1)
		go func(nodeIdx int) {
			defer wg.Done()
			for row := 0; row < rowsPerNode; row++ {
				name := fmt.Sprintf("node-%d-row-%02d", nodeIdx, row)
				idHex := fmt.Sprintf("%032x", (nodeIdx+1)*1000+row)
				err := cluster.ExecSQL(nodeIdx,
					"INSERT INTO items (id, name) VALUES (?, ?)",
					idHex, name)
				if err != nil {
					cluster.MarkFailed(fmt.Sprintf("node %d insert failed: %v", nodeIdx, err))
					return
				}
			}
		}(i)
	}
	wg.Wait()

	totalExpectedRows := rowsPerNode * len(cluster.Nodes)

	// Poll until all nodes report totalExpectedRows and bit-identical SHA-256 digests
	deadline := time.Now().Add(25 * time.Second)
	var finalDigest string
	for time.Now().Before(deadline) {
		converged := true
		var digests []string

		for i := range cluster.Nodes {
			count, err := cluster.QueryRowCount(i, "items")
			if err != nil || count != totalExpectedRows {
				converged = false
				break
			}
			digest, err := cluster.ComputeTableDigest(i, "items", "name")
			if err != nil {
				converged = false
				break
			}
			digests = append(digests, digest)
		}

		if converged && len(digests) == len(cluster.Nodes) {
			allMatch := true
			for _, d := range digests {
				if d != digests[0] {
					allMatch = false
					break
				}
			}
			if allMatch {
				finalDigest = digests[0]
				t.Logf("Three nodes converged on %d rows with SHA-256: %s", totalExpectedRows, finalDigest)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("three nodes did not converge within deadline (expected %d rows)", totalExpectedRows)
}
