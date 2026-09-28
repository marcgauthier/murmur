package partition_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb/tests-live/harness"
)

func TestFourNodePartitionIsolationAndHealing(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "partition",
		NumNodes:    4,
		AwaitUnlock: true,
		SchemaSQL: `
CREATE TABLE IF NOT EXISTS partition_rows (
  id BLOB PRIMARY KEY NOT NULL,
  name TEXT NOT NULL DEFAULT ''
);
`,
	})

	// Initial baseline insert on Node 0 (node1)
	baseID := fmt.Sprintf("%032x", 1)
	if err := cluster.ExecSQL(0, "INSERT INTO partition_rows (id, name) VALUES (?, ?)", baseID, "baseline"); err != nil {
		t.Fatalf("baseline insert: %v", err)
	}

	// Verify all 4 nodes receive baseline row
	waitAllCount(t, cluster, "partition_rows", 1, 10*time.Second)

	// Partition mesh into two isolated groups:
	// Group 1: Nodes 0, 1 (node1, node2)
	// Group 2: Nodes 2, 3 (node3, node4)
	for _, g1 := range []int{0, 1} {
		for _, g2 := range []int{2, 3} {
			_ = cluster.RemovePeer(g1, g2)
			_ = cluster.RemovePeer(g2, g1)
		}
	}

	time.Sleep(100 * time.Millisecond)

	// Write into Group 1 (node 0)
	g1ID := fmt.Sprintf("%032x", 100)
	if err := cluster.ExecSQL(0, "INSERT INTO partition_rows (id, name) VALUES (?, ?)", g1ID, "side-ab-write"); err != nil {
		t.Fatalf("side-ab insert: %v", err)
	}

	// Write into Group 2 (node 2)
	g2ID := fmt.Sprintf("%032x", 200)
	if err := cluster.ExecSQL(2, "INSERT INTO partition_rows (id, name) VALUES (?, ?)", g2ID, "side-cd-write"); err != nil {
		t.Fatalf("side-cd insert: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Verify isolation: Group 1 should only have baseline + side-ab (2 rows)
	for _, idx := range []int{0, 1} {
		count, err := cluster.QueryRowCount(idx, "partition_rows")
		if err != nil || count != 2 {
			t.Fatalf("node %d count = %d, want 2 during partition", idx, count)
		}
	}

	// Verify isolation: Group 2 should only have baseline + side-cd (2 rows)
	for _, idx := range []int{2, 3} {
		count, err := cluster.QueryRowCount(idx, "partition_rows")
		if err != nil || count != 2 {
			t.Fatalf("node %d count = %d, want 2 during partition", idx, count)
		}
	}

	// Heal partition: re-connect Group 1 and Group 2
	for _, g1 := range []int{0, 1} {
		for _, g2 := range []int{2, 3} {
			_ = cluster.AddPeer(g1, g2)
			_ = cluster.AddPeer(g2, g1)
		}
	}

	// Verify full convergence on 3 rows across all 4 nodes
	waitAllCount(t, cluster, "partition_rows", 3, 15*time.Second)

	// Verify bit-identical SHA-256 digests across all 4 nodes
	var digests []string
	for i := range cluster.Nodes {
		d, err := cluster.ComputeTableDigest(i, "partition_rows", "name")
		if err != nil {
			t.Fatalf("node %d digest: %v", i, err)
		}
		digests = append(digests, d)
	}
	for i := 1; i < len(digests); i++ {
		if digests[i] != digests[0] {
			t.Fatalf("node %d digest %s != node 0 digest %s", i, digests[i], digests[0])
		}
	}

	// Post-heal write replicates to all
	postID := fmt.Sprintf("%032x", 300)
	if err := cluster.ExecSQL(1, "INSERT INTO partition_rows (id, name) VALUES (?, ?)", postID, "post-heal"); err != nil {
		t.Fatalf("post-heal insert: %v", err)
	}
	waitAllCount(t, cluster, "partition_rows", 4, 10*time.Second)
}

func waitAllCount(t *testing.T, cluster *harness.Cluster, table string, expected int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allMatch := true
		for i := range cluster.Nodes {
			c, err := cluster.QueryRowCount(i, table)
			if err != nil || c != expected {
				allMatch = false
				break
			}
		}
		if allMatch {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cluster failed to converge to %d rows in %s within %v", expected, table, timeout)
}
