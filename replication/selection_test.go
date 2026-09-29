package replication

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/ids"
	"github.com/marcgauthier/spedsql/state"
)

func TestPeerSelectionBoundedByFanout(t *testing.T) {
	cluster := newTestCluster(t)
	localID := ids.NewNodeID()
	dbid := cluster.dbid

	st, err := state.Open(t.TempDir(), localID, dbid, state.Options{Limits: codec.Limits{MaxValueBytes: 64, MaxMutations: 100}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	mgr, err := NewManager(ManagerConfig{
		Store:   st,
		Applier: &fakeApplier{},
		Creds:   cluster.creds(t, localID),
		Local:   localID,
		DBID:    dbid,
		Fanout:  2,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Add 5 peers
	peerIDs := make([]ids.NodeID, 5)
	for i := 0; i < 5; i++ {
		peerIDs[i] = ids.NewNodeID()
		mgr.AddPeer(peerIDs[i], []string{fmt.Sprintf("127.0.0.1:%d", 9000+i)})
	}

	// Verify exactly Fanout (2) peers are selected
	statuses := mgr.PeerStatus()
	selectedCount := 0
	for _, st := range statuses {
		if st.Selected {
			selectedCount++
		}
	}
	if selectedCount != 2 {
		t.Fatalf("expected 2 selected peers, got %d", selectedCount)
	}

	// Remove one selected peer
	for _, st := range statuses {
		if st.Selected {
			mgr.RemovePeer(st.NodeID)
			break
		}
	}

	// Verify Fanout is maintained from remaining eligible peers
	statuses = mgr.PeerStatus()
	selectedCount = 0
	for _, st := range statuses {
		if st.Selected {
			selectedCount++
		}
	}
	if selectedCount != 2 {
		t.Fatalf("expected 2 selected peers after removal, got %d", selectedCount)
	}
}

func TestPeerRotation(t *testing.T) {
	cluster := newTestCluster(t)
	localID := ids.NewNodeID()
	dbid := cluster.dbid

	st, err := state.Open(t.TempDir(), localID, dbid, state.Options{Limits: codec.Limits{MaxValueBytes: 64, MaxMutations: 100}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	mgr, err := NewManager(ManagerConfig{
		Store:                st,
		Applier:              &fakeApplier{},
		Creds:                cluster.creds(t, localID),
		Local:                localID,
		DBID:                 dbid,
		Fanout:               1,
		PeerRotationInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 4; i++ {
		mgr.AddPeer(ids.NewNodeID(), []string{fmt.Sprintf("127.0.0.1:%d", 9100+i)})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = mgr.Run(ctx)
	}()

	// Wait for several rotations
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		stats := mgr.Stats()
		if stats.PeerRotations >= 3 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	stats := mgr.Stats()
	if stats.PeerRotations < 3 {
		t.Errorf("expected at least 3 peer rotations, got %d", stats.PeerRotations)
	}

	cancel()
	<-runDone
}

func TestMembershipHandlerUpdates(t *testing.T) {
	cluster := newTestCluster(t)
	localID := ids.NewNodeID()
	dbid := cluster.dbid

	st, err := state.Open(t.TempDir(), localID, dbid, state.Options{Limits: codec.Limits{MaxValueBytes: 64, MaxMutations: 100}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	mgr, err := NewManager(ManagerConfig{
		Store:   st,
		Applier: &fakeApplier{},
		Creds:   cluster.creds(t, localID),
		Local:   localID,
		DBID:    dbid,
		Fanout:  2,
	})
	if err != nil {
		t.Fatal(err)
	}

	nodeA := ids.NewNodeID()
	nodeB := ids.NewNodeID()

	// Simulate discovery callbacks from SWIM
	mgr.OnPeerDiscovered(nodeA, "127.0.0.1:9201", NodeMetadata{DBID: dbid})
	mgr.OnPeerDiscovered(nodeB, "127.0.0.1:9202", NodeMetadata{DBID: dbid})

	statuses := mgr.PeerStatus()
	if len(statuses) != 2 {
		t.Fatalf("expected 2 peers discovered, got %d", len(statuses))
	}
	for _, st := range statuses {
		if !st.Selected {
			t.Errorf("peer %s expected to be selected", st.NodeID)
		}
	}

	// Update node A endpoint
	mgr.OnPeerUpdated(nodeA, "127.0.0.1:9203", NodeMetadata{DBID: dbid})
	statuses = mgr.PeerStatus()
	for _, st := range statuses {
		if st.NodeID == nodeA {
			if len(st.Addrs) == 0 || st.Addrs[0] != "127.0.0.1:9203" {
				t.Errorf("expected updated address for nodeA, got %v", st.Addrs)
			}
		}
	}

	// Peer B leaves
	mgr.OnPeerLeft(nodeB)
	if mgr.isPeerSelected(nodeB) {
		t.Errorf("nodeB should not be selected after leaving")
	}
}

func TestAntiEntropyJitterAndExecution(t *testing.T) {
	cluster := newTestCluster(t)
	dbid := cluster.dbid

	nodeA := ids.NewNodeID()
	credsA := cluster.creds(t, nodeA)

	nodeB := ids.NewNodeID()
	credsB := cluster.creds(t, nodeB)

	stB, err := state.Open(t.TempDir(), nodeB, dbid, state.Options{Limits: codec.Limits{MaxValueBytes: 64, MaxMutations: 100}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stB.Close() })

	// Start Node B listener
	mgrB, err := NewManager(ManagerConfig{
		Store:               stB,
		Applier:             &fakeApplier{},
		Local:               nodeB,
		DBID:                dbid,
		Creds:               credsB,
		ListenAddr:          "127.0.0.1:0",
		AntiEntropyInterval: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	doneB := make(chan struct{})
	go func() {
		defer close(doneB)
		_ = mgrB.Run(ctxB)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && mgrB.Addr() == "" {
		time.Sleep(10 * time.Millisecond)
	}
	if mgrB.Addr() == "" {
		t.Fatal("Node B failed to bind listener")
	}

	stA, err := state.Open(t.TempDir(), nodeA, dbid, state.Options{Limits: codec.Limits{MaxValueBytes: 64, MaxMutations: 100}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stA.Close() })

	// Node A config with fast anti-entropy and Fanout=0 so Node B is outside selected subset
	mgrA, err := NewManager(ManagerConfig{
		Store:               stA,
		Applier:             &fakeApplier{},
		Local:               nodeA,
		DBID:                dbid,
		Creds:               credsA,
		Fanout:              0, // 0 fanout means Node B won't be in selected subset
		AntiEntropyInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	mgrA.AddPeer(nodeB, []string{mgrB.Addr()})

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		_ = mgrA.Run(ctxA)
	}()

	// Wait for periodic anti-entropy runs
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats := mgrA.Stats()
		if stats.AntiEntropyRuns > 0 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	stats := mgrA.Stats()
	if stats.AntiEntropyRuns == 0 {
		t.Errorf("expected at least 1 anti-entropy run, got %d (failures: %d)", stats.AntiEntropyRuns, stats.AntiEntropyFailures)
	}

	cancelA()
	<-doneA
	cancelB()
	<-doneB
}
