package partition_test

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestFourNodePartitionIsolationAndHealing(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "partition",
		NumNodes:        4,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
	})

	// Initial baseline insert on Node 0 (node1)
	if err := insertMarker(cluster, 0, 1, "baseline"); err != nil {
		t.Fatalf("baseline insert: %v", err)
	}

	// Verify all 4 nodes receive baseline row
	waitAllCount(t, cluster, 1, 10*time.Second)

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
	if err := insertMarker(cluster, 0, 100, "side-ab-write"); err != nil {
		t.Fatalf("side-ab insert: %v", err)
	}

	// Write into Group 2 (node 2)
	if err := insertMarker(cluster, 2, 200, "side-cd-write"); err != nil {
		t.Fatalf("side-cd insert: %v", err)
	}

	// Intra-group replication is asynchronous: received rows materialize on
	// the 1s remote-apply interval, so poll for internal convergence instead
	// of asserting on a fixed sleep. Any count above 2 means the partition
	// leaked and fails fast.
	deadline := time.Now().Add(15 * time.Second)
	for {
		counts := make([]int, 4)
		ready := true
		for idx := 0; idx < 4; idx++ {
			rows, err := cluster.TypedContentionRows(idx)
			if err != nil {
				t.Fatalf("node %d typed rows: %v", idx, err)
			}
			counts[idx] = len(rows)
			if len(rows) > 2 {
				t.Fatalf("partition leaked: node %d count = %d, want at most 2", idx, len(rows))
			}
			if len(rows) != 2 {
				ready = false
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("groups failed to converge internally, counts = %v, want all 2", counts)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Verify isolation by content: equal counts but divergent digests prove
	// each group converged internally without cross-group replication.
	groupDigests := make([]string, 4)
	for idx := 0; idx < 4; idx++ {
		rows, err := cluster.TypedContentionRows(idx)
		if err != nil {
			t.Fatalf("node %d typed rows: %v", idx, err)
		}
		groupDigests[idx] = digestRows(rows)
	}
	if groupDigests[0] != groupDigests[1] || groupDigests[2] != groupDigests[3] {
		t.Fatalf("intra-group digests diverge: %v", groupDigests)
	}
	if groupDigests[0] == groupDigests[2] {
		t.Fatalf("cross-group digests unexpectedly match during partition: %v", groupDigests)
	}

	// Heal partition: re-connect Group 1 and Group 2
	for _, g1 := range []int{0, 1} {
		for _, g2 := range []int{2, 3} {
			_ = cluster.AddPeer(g1, g2)
			_ = cluster.AddPeer(g2, g1)
		}
	}

	// Verify full convergence on 3 rows across all 4 nodes
	waitAllCount(t, cluster, 3, 15*time.Second)

	// Verify bit-identical SHA-256 digests across all 4 nodes
	var digests []string
	for i := range cluster.Nodes {
		rows, err := cluster.TypedContentionRows(i)
		if err != nil {
			t.Fatalf("node %d typed rows: %v", i, err)
		}
		digests = append(digests, digestRows(rows))
	}
	for i := 1; i < len(digests); i++ {
		if digests[i] != digests[0] {
			t.Fatalf("node %d digest %s != node 0 digest %s", i, digests[i], digests[0])
		}
	}

	// Post-heal write replicates to all
	if err := insertMarker(cluster, 1, 300, "post-heal"); err != nil {
		t.Fatalf("post-heal insert: %v", err)
	}
	waitAllCount(t, cluster, 4, 10*time.Second)
}

func insertMarker(c *harness.Cluster, node, id int, name string) error {
	return c.TypedContentionInsert(node, harness.TypedContentionRow{ID: fmt.Sprintf("%08x-0000-4000-8000-%012x", id, id), Name: name})
}

func digestRows(rows []harness.TypedContentionRow) string {
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return fmt.Sprintf("%#v", rows)
}

func waitAllCount(t *testing.T, cluster *harness.Cluster, expected int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allMatch := true
		for i := range cluster.Nodes {
			rows, err := cluster.TypedContentionRows(i)
			if err != nil || len(rows) != expected {
				allMatch = false
				break
			}
		}
		if allMatch {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cluster failed to converge to %d typed rows within %v", expected, timeout)
}
