package mergepolicies_test

import (
	"reflect"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestDisconnectedPoliciesForwardAndRestart(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name: "merge-policies", NumNodes: 3, AwaitUnlock: true,
		ManualPeers: true, TypedRecords: true,
	})
	rowID := db.NewRowID()
	const name = "merge-policy-row"
	for node := range cluster.Nodes {
		if err := cluster.TypedInsertWithID(node, rowID, name); err != nil {
			t.Fatalf("seed node %d: %v", node, err)
		}
	}

	// Each node makes independent typed CRDT updates while disconnected.
	if err := cluster.TypedCounterAdd(0, name, 10); err != nil {
		t.Fatal(err)
	}
	if err := cluster.TypedSetAdd(0, name, "shared"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.TypedSetAdd(0, name, "red"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.TypedExtremaUpdate(0, name, 10, 30); err != nil {
		t.Fatal(err)
	}

	if err := cluster.TypedCounterAdd(1, name, 7); err != nil {
		t.Fatal(err)
	}
	// This remove has not observed node 0's add and must not erase it.
	if err := cluster.TypedSetRemove(1, name, "shared"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.TypedSetAdd(1, name, "blue"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.TypedExtremaUpdate(1, name, 20, 15); err != nil {
		t.Fatal(err)
	}

	if err := cluster.TypedCounterAdd(2, name, -2); err != nil {
		t.Fatal(err)
	}
	if err := cluster.TypedSetAdd(2, name, "green"); err != nil {
		t.Fatal(err)
	}
	if err := cluster.TypedExtremaUpdate(2, name, 5, -7); err != nil {
		t.Fatal(err)
	}

	for _, edge := range [][2]int{{0, 1}, {1, 0}} {
		if err := cluster.AddPeer(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	waitCounter(t, cluster, 0, name, 17)
	waitCounter(t, cluster, 1, name, 17)

	cluster.StopNode(0)
	for _, edge := range [][2]int{{1, 2}, {2, 1}} {
		if err := cluster.AddPeer(edge[0], edge[1]); err != nil {
			t.Fatal(err)
		}
	}
	waitCounter(t, cluster, 1, name, 15)
	waitCounter(t, cluster, 2, name, 15)
	waitPolicyProjection(t, cluster, 1, name, []string{"blue", "green", "red", "shared"}, 20, -7)
	waitPolicyProjection(t, cluster, 2, name, []string{"blue", "green", "red", "shared"}, 20, -7)

	cluster.StopNode(2)
	cluster.StartNode(2)
	cluster.UnlockNode(2, cluster.Nodes[2].KeyHex)
	cluster.WaitNodeReady(2)
	waitCounter(t, cluster, 2, name, 15)
	waitPolicyProjection(t, cluster, 2, name, []string{"blue", "green", "red", "shared"}, 20, -7)

	// A later observed remove must propagate after restart.
	if err := cluster.TypedSetRemove(1, name, "shared"); err != nil {
		t.Fatal(err)
	}
	waitPolicyProjection(t, cluster, 2, name, []string{"blue", "green", "red"}, 20, -7)
}

func waitCounter(t *testing.T, cluster *harness.Cluster, node int, name string, want int64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		got, err := cluster.TypedCounterValue(node, name)
		if err == nil && got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	got, err := cluster.TypedCounterValue(node, name)
	t.Fatalf("node %d counter=%d err=%v, want %d", node, got, err, want)
}

func waitPolicyProjection(t *testing.T, cluster *harness.Cluster, node int, name string, wantTags []string, wantPeak int64, wantFloor float64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		tags, tagsErr := cluster.TypedSetValues(node, name)
		peak, floor, extremaErr := cluster.TypedExtremaValues(node, name)
		if tagsErr == nil && extremaErr == nil && reflect.DeepEqual(tags, wantTags) && peak == wantPeak && floor == wantFloor {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	tags, tagsErr := cluster.TypedSetValues(node, name)
	peak, floor, extremaErr := cluster.TypedExtremaValues(node, name)
	t.Fatalf("node %d projection tags=%v err=%v extrema=(%d,%g) err=%v; want tags=%v extrema=(%d,%g)", node, tags, tagsErr, peak, floor, extremaErr, wantTags, wantPeak, wantFloor)
}
