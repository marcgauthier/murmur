// Three-node typed schema evolution keeps old application bindings able to
// update known fields while retaining newly added fields in durable state.
package schemaevolution_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestRollingTypedSchemaMigration(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "schema-evolution", NumNodes: 3, AwaitUnlock: true, TypedRecords: true,
	})
	const rows = 5
	for i := 0; i < rows; i++ {
		name := fmt.Sprintf("typed-row-%d", i)
		if err := cluster.TypedInsert(0, name); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		if err := cluster.TypedCounterAdd(0, name, int64(100+i)); err != nil {
			t.Fatalf("seed counter %s: %v", name, err)
		}
	}
	waitNames(t, cluster, []string{"typed-row-0", "typed-row-1", "typed-row-2", "typed-row-3", "typed-row-4"})

	// Only node 0 registers the additive Note field. Its peers adopt the
	// manifest while retaining their older executable record binding.
	if err := cluster.MigrateTypedRecords(0); err != nil {
		t.Fatalf("migrate node 0: %v", err)
	}
	waitEpoch(t, cluster, 0, 2, 10*time.Second)
	waitEpoch(t, cluster, 1, 2, 30*time.Second)
	waitEpoch(t, cluster, 2, 2, 30*time.Second)
	if err := cluster.TypedSetNote(0, "typed-row-0", "added-field-survives"); err != nil {
		t.Fatalf("write added field: %v", err)
	}
	// An older binding updates its known Name field after learning the newer
	// manifest; this must not erase the new Note field.
	if err := cluster.TypedRename(1, "typed-row-0", "typed-row-renamed"); err != nil {
		t.Fatalf("old binding write: %v", err)
	}
	if err := cluster.TypedInsert(2, "typed-row-late"); err != nil {
		t.Fatalf("old binding insert after adoption: %v", err)
	}
	wantNames := []string{"typed-row-1", "typed-row-2", "typed-row-3", "typed-row-4", "typed-row-late", "typed-row-renamed"}
	waitNames(t, cluster, wantNames)
	waitNote(t, cluster, 0, "typed-row-renamed", "added-field-survives", 30*time.Second)
	for i := 1; i < rows; i++ {
		name := fmt.Sprintf("typed-row-%d", i)
		for node := range cluster.Nodes {
			waitCounter(t, cluster, node, name, int64(100+i), 30*time.Second)
		}
	}

	cluster.StopNode(2)
	cluster.StartNode(2)
	cluster.UnlockNode(2, cluster.Nodes[2].KeyHex)
	cluster.WaitNodeReady(2)
	waitNames(t, cluster, wantNames)
	waitCounter(t, cluster, 2, "typed-row-4", 104, 30*time.Second)
}

func waitNames(t *testing.T, cluster *harness.Cluster, want []string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var latest []string
	for time.Now().Before(deadline) {
		allMatch := true
		latest = latest[:0]
		for node := range cluster.Nodes {
			got, err := cluster.TypedNames(node)
			latest = append(latest, fmt.Sprintf("node%d names=%v err=%v", node, got, err))
			if err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
				allMatch = false
				break
			}
		}
		if allMatch {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("typed records did not converge to %v: %v", want, latest)
}

func waitEpoch(t *testing.T, cluster *harness.Cluster, node int, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got, err := cluster.TypedSchemaEpoch(node); err == nil && got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, err := cluster.TypedSchemaEpoch(node)
	t.Fatalf("node %d schema epoch=%d err=%v, want %d", node, got, err, want)
}

func waitNote(t *testing.T, cluster *harness.Cluster, node int, name, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got, err := cluster.TypedNote(node, name); err == nil && got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, err := cluster.TypedNote(node, name)
	t.Fatalf("node %d Note=%q err=%v, want %q", node, got, err, want)
}

func waitCounter(t *testing.T, cluster *harness.Cluster, node int, name string, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got, err := cluster.TypedCounterValue(node, name); err == nil && got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, err := cluster.TypedCounterValue(node, name)
	t.Fatalf("node %d counter %s=%d err=%v, want %d", node, name, got, err, want)
}
