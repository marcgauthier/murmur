// The application-level view uses managed typed RIME queries over replicated
// records and rebuilds naturally after a node restart.
package views_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestTypedViewTracksReplicatedRowsAndRebuilds(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "views", NumNodes: 3, TypedRecords: true,
	})
	for i, name := range []string{"ann", "bob", "hidden"} {
		if err := cluster.TypedInsert(i, name); err != nil {
			t.Fatalf("insert %s on node %d: %v", name, i, err)
		}
		if name != "hidden" {
			if err := cluster.TypedCounterAdd(i, name, 1); err != nil {
				t.Fatalf("enable %s on node %d: %v", name, i, err)
			}
		}
	}
	waitForViewRows(t, cluster, []int{0, 1, 2}, []string{"ann", "bob"})

	cluster.StopNode(2)
	cluster.StartNode(2)
	cluster.WaitNodeReady(2)
	waitForViewRows(t, cluster, []int{2}, []string{"ann", "bob"})
}

func waitForViewRows(t *testing.T, cluster *harness.Cluster, nodes []int, want []string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		allMatch := true
		for _, i := range nodes {
			got, err := cluster.TypedEnabledNames(i)
			if err != nil || fmt.Sprint(got) != fmt.Sprint(want) {
				allMatch = false
				break
			}
		}
		if allMatch {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for typed view to contain %v", want)
}
