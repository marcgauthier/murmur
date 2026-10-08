package migrationcrash_test

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestMigratedPeerCrashReopensAndConverges(t *testing.T) {
	for _, tc := range []struct {
		phase         string
		exitCode      int
		restartSchema int
		restartEpoch  uint64
	}{
		{phase: "before-store", exitCode: 85, restartSchema: 1, restartEpoch: 1},
		{phase: "after-store", exitCode: 86, restartSchema: 2, restartEpoch: 2},
	} {
		t.Run(tc.phase, func(t *testing.T) { runSchemaCrashCase(t, tc.phase, tc.exitCode, tc.restartSchema, tc.restartEpoch) })
	}
}

func runSchemaCrashCase(t *testing.T, phase string, exitCode, restartSchema int, restartEpoch uint64) {
	t.Helper()
	const seedRows = 24
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "migration-crash-" + phase,
		NumNodes:     3,
		AwaitUnlock:  true,
		TypedRecords: true,
	})

	wantNames := make([]string, seedRows)
	for i := range wantNames {
		wantNames[i] = fmt.Sprintf("seed-%02d", i)
		if err := cluster.TypedInsertWithID(1, ids.NewRowID(), wantNames[i]); err != nil {
			t.Fatalf("seed node2: %v", err)
		}
	}
	sort.Strings(wantNames)
	waitConverged(t, cluster, wantNames, 45*time.Second)
	for idx := range cluster.Nodes {
		assertEpoch(t, cluster, idx, 1)
	}

	// Keep the two old-schema peers connected to each other while isolating
	// node1 so neither can observe its schema-store crash or recovery early.
	for _, peer := range []int{1, 2} {
		if err := cluster.RemovePeer(0, peer); err != nil {
			t.Fatalf("isolate node1 from node%d: %v", peer+1, err)
		}
		if err := cluster.RemovePeer(peer, 0); err != nil {
			t.Fatalf("isolate node%d from node1: %v", peer+1, err)
		}
	}
	// The testnode loads crash-hook configuration at startup, so restart the
	// isolated node with the hook armed before issuing the migration request.
	cluster.StopNode(0)
	if err := cluster.SetTypedSchemaCrashPhase(0, phase); err != nil {
		t.Fatalf("arm %s schema-store crash: %v", phase, err)
	}
	cluster.StartNode(0)
	cluster.UnlockNode(0, cluster.Nodes[0].KeyHex)
	cluster.WaitNodeReady(0)
	if err := cluster.MigrateTypedRecords(0); err == nil {
		t.Fatal("migration returned successfully; injected process exit did not fire")
	}
	gotExit, err := cluster.WaitNodeExit(0, 5*time.Second)
	if err != nil {
		t.Fatalf("wait for schema-store process exit: %v", err)
	}
	if gotExit != exitCode {
		t.Fatalf("schema-store crash exit code = %d, want %d", gotExit, exitCode)
	}
	for _, idx := range []int{1, 2} {
		assertEpoch(t, cluster, idx, 1)
	}
	t.Logf("node1 exited at schema manifest %s boundary while both peers remained on epoch 1", phase)

	if err := cluster.SetTypedSchemaVersion(0, restartSchema); err != nil {
		t.Fatalf("set restarted application schema: %v", err)
	}
	if err := cluster.SetTypedSchemaCrashPhase(0, ""); err != nil {
		t.Fatalf("clear schema crash hook: %v", err)
	}
	cluster.StartNode(0)
	cluster.UnlockNode(0, cluster.Nodes[0].KeyHex)
	cluster.WaitNodeReady(0)
	assertEpoch(t, cluster, 0, restartEpoch)
	got, err := cluster.TypedNames(0)
	if err != nil {
		t.Fatalf("read recovered rows before reconnect: %v", err)
	}
	if !reflect.DeepEqual(got, wantNames) {
		t.Fatalf("recovered rows = %v, want %v", got, wantNames)
	}
	if restartEpoch == 1 {
		if err := cluster.MigrateTypedRecords(0); err != nil {
			t.Fatalf("retry additive migration after pre-store crash: %v", err)
		}
	}
	assertEpoch(t, cluster, 0, 2)
	if note, err := cluster.TypedNote(0, wantNames[0]); err != nil || note != "" {
		t.Fatalf("new field after local recovery = %q, %v; want empty default", note, err)
	}

	for _, peer := range []int{1, 2} {
		if err := cluster.AddPeer(0, peer); err != nil {
			t.Fatalf("rejoin node1 to node%d: %v", peer+1, err)
		}
		if err := cluster.AddPeer(peer, 0); err != nil {
			t.Fatalf("rejoin node%d to node1: %v", peer+1, err)
		}
	}
	waitConverged(t, cluster, wantNames, 90*time.Second)
	waitEpoch(t, cluster, []int{1, 2}, 2, 45*time.Second)
	for _, idx := range []int{1, 2} {
		if err := cluster.MigrateTypedRecords(idx); err != nil {
			t.Fatalf("rebind node%d to the adopted v2 application schema: %v", idx+1, err)
		}
	}
	if err := cluster.TypedSetNote(0, wantNames[0], phase+"-recovered"); err != nil {
		t.Fatalf("write migrated field after restart: %v", err)
	}
	wantNote := phase + "-recovered"
	deadline := time.Now().Add(45 * time.Second)
	var lastState string
	for time.Now().Before(deadline) {
		ready := true
		states := make([]string, 0, len(cluster.Nodes))
		for idx := range cluster.Nodes {
			epoch, epochErr := cluster.TypedSchemaEpoch(idx)
			note, noteErr := cluster.TypedNote(idx, wantNames[0])
			states = append(states, fmt.Sprintf("node%d epoch=%d/%v note=%q/%v", idx+1, epoch, epochErr, note, noteErr))
			if epochErr != nil || epoch != 2 || noteErr != nil || note != wantNote {
				ready = false
			}
		}
		lastState = fmt.Sprint(states)
		if ready {
			t.Logf("%s schema-store crash recovered and converged with both peers", phase)
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("schema epoch and migrated field did not converge after %s crash: %s", phase, lastState)
}

func assertEpoch(t *testing.T, cluster *harness.Cluster, idx int, want uint64) {
	t.Helper()
	got, err := cluster.TypedSchemaEpoch(idx)
	if err != nil {
		t.Fatalf("node%d schema epoch: %v", idx+1, err)
	}
	if got != want {
		t.Fatalf("node%d schema epoch = %d, want %d", idx+1, got, want)
	}
}

func waitConverged(t *testing.T, cluster *harness.Cluster, want []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		converged := true
		for idx := range cluster.Nodes {
			got, err := cluster.TypedNames(idx)
			if err != nil || !reflect.DeepEqual(got, want) {
				converged = false
				break
			}
		}
		if converged {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge on %d managed record names within %s", len(want), timeout)
}

func waitEpoch(t *testing.T, cluster *harness.Cluster, nodes []int, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		for _, idx := range nodes {
			got, err := cluster.TypedSchemaEpoch(idx)
			if err != nil || got != want {
				ready = false
				break
			}
		}
		if ready {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("nodes %v did not adopt schema epoch %d within %s", nodes, want, timeout)
}
