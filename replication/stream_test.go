package replication

import (
	"context"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/state"
)

func TestSeparateStreamsDataAndControl(t *testing.T) {
	cl := newTestCluster(t)
	nodeA := ids.NewNodeID()
	nodeB := ids.NewNodeID()

	mgrA, _, addrA := cl.testManager(t, nodeA, 1)

	stB, err := openSignedFixture(t.TempDir(), nodeB, cl.dbid, state.Options{})
	if err != nil {
		t.Fatal(err)
	}

	var hash [32]byte
	hash[0] = 1
	mgrB, err := NewManager(ManagerConfig{
		Store:        stB,
		Applier:      &fakeApplier{},
		Creds:        cl.creds(t, nodeB),
		Local:        nodeB,
		DBID:         cl.dbid,
		SchemaEpoch:  1,
		SchemaHash:   hash,
		Peers:        []PeerInfo{{NodeID: nodeA, Addrs: []string{addrA}}},
		SendInterval: 10 * time.Millisecond,
		AckInterval:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = mgrB.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		_ = stB.Close()
	})

	// Wait for peering connection
	for i := 0; i < 100; i++ {
		st := mgrB.PeerStatus()
		if len(st) > 0 && st[0].Connected {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Commit local write on node B
	batch := &codec.MutationBatch{
		ProtocolVersion: 3,
		TxID:            ids.NewTxID(),
		OriginNode:      nodeB,
		Sequence:        1,
		HLC:             100,
		SchemaEpoch:     1,
		SchemaHash:      hash,
		Mutations: []codec.Mutation{
			{
				TableID:  1,
				RowID:    ids.NewRowID(),
				ColumnID: 1,
				Value:    codec.Value{Type: codec.TypeText, S: "hello"},
			},
		},
	}
	if _, err := stB.CommitLocal(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	mgrB.NotifyLocal()

	// Wait for replication and separate data stream initiation
	for i := 0; i < 100; i++ {
		sB := mgrB.Stats()
		sA := mgrA.Stats()
		if sB.DataStreamsOpened > 0 && sA.DataStreamsAccepted > 0 && sB.BatchesSent > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	statsB := mgrB.Stats()
	statsA := mgrA.Stats()

	if statsB.DataStreamsOpened == 0 {
		t.Errorf("expected DataStreamsOpened > 0 on sender B, got %d", statsB.DataStreamsOpened)
	}
	if statsA.DataStreamsAccepted == 0 {
		t.Errorf("expected DataStreamsAccepted > 0 on receiver A, got %d", statsA.DataStreamsAccepted)
	}
	if statsB.BatchesSent == 0 {
		t.Errorf("expected BatchesSent > 0 on B, got %d", statsB.BatchesSent)
	}
	if statsA.BatchesReceived == 0 {
		t.Errorf("expected BatchesReceived > 0 on A, got %d", statsA.BatchesReceived)
	}
}

func TestSeparateSnapshotStream(t *testing.T) {
	cl := newTestCluster(t)
	nodeA := ids.NewNodeID()
	nodeB := ids.NewNodeID()

	mgrA, _, addrA := cl.testManager(t, nodeA, 1)

	stB, err := openSignedFixture(t.TempDir(), nodeB, cl.dbid, state.Options{})
	if err != nil {
		t.Fatal(err)
	}

	var hash [32]byte
	hash[0] = 1
	applierB := &fakeApplier{}
	mgrB, err := NewManager(ManagerConfig{
		Store:        stB,
		Applier:      applierB,
		Creds:        cl.creds(t, nodeB),
		Local:        nodeB,
		DBID:         cl.dbid,
		SchemaEpoch:  1,
		SchemaHash:   hash,
		Peers:        []PeerInfo{{NodeID: nodeA, Addrs: []string{addrA}}},
		SendInterval: 10 * time.Millisecond,
		AckInterval:  50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = mgrB.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		_ = stB.Close()
	})

	// Wait for peer connection
	for i := 0; i < 100; i++ {
		st := mgrB.PeerStatus()
		if len(st) > 0 && st[0].Connected {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Trigger snapshot transfer from node A to node B
	pA := mgrA.peerFor(nodeB, nil, true)
	if pA == nil {
		t.Fatal("peer B not found on A")
	}
	psA := mgrA.sessionFor(pA)
	if psA == nil {
		// Wait for session
		for i := 0; i < 50; i++ {
			psA = mgrA.sessionFor(pA)
			if psA != nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if psA == nil {
		t.Fatal("expected active session on A")
	}

	// Send snapshot over dedicated snapshot stream
	mgrA.sendSnapshot(pA, psA)

	// Wait for snapshot stream stats
	for i := 0; i < 100; i++ {
		sA := mgrA.Stats()
		sB := mgrB.Stats()
		if sA.SnapStreamsOpened > 0 && sB.SnapStreamsAccepted > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	statsA := mgrA.Stats()
	statsB := mgrB.Stats()

	if statsA.SnapStreamsOpened == 0 {
		t.Errorf("expected SnapStreamsOpened > 0 on A, got %d", statsA.SnapStreamsOpened)
	}
	if statsB.SnapStreamsAccepted == 0 {
		t.Errorf("expected SnapStreamsAccepted > 0 on B, got %d", statsB.SnapStreamsAccepted)
	}
}
