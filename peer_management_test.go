package replicateddb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/state"
)


func TestStateStorePeerExclusion(t *testing.T) {
	dir := t.TempDir()
	store, err := state.Open(dir, ids.NewNodeID(), ids.NewDBID(), state.Options{})
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	defer store.Close()


	n1 := ids.NewNodeID()
	n2 := ids.NewNodeID()

	// Initial check: nothing excluded
	if ex, err := store.IsPeerExcluded(n1); err != nil || ex {
		t.Fatalf("expected n1 not excluded, got ex=%v, err=%v", ex, err)
	}
	list, err := store.ListExcludedPeers()
	if err != nil || len(list) != 0 {
		t.Fatalf("expected empty list, got %v, err=%v", list, err)
	}

	// Exclude n1 and n2
	if err := store.SetPeerExcluded(n1, true); err != nil {
		t.Fatalf("SetPeerExcluded n1: %v", err)
	}
	if err := store.SetPeerExcluded(n2, true); err != nil {
		t.Fatalf("SetPeerExcluded n2: %v", err)
	}

	if ex, err := store.IsPeerExcluded(n1); err != nil || !ex {
		t.Fatalf("expected n1 excluded, got %v, err=%v", ex, err)
	}
	if ex, err := store.IsPeerExcluded(n2); err != nil || !ex {
		t.Fatalf("expected n2 excluded, got %v, err=%v", ex, err)
	}

	list, err = store.ListExcludedPeers()
	if err != nil || len(list) != 2 {
		t.Fatalf("expected 2 excluded peers, got %d, err=%v", len(list), err)
	}

	// Unexclude n1
	if err := store.SetPeerExcluded(n1, false); err != nil {
		t.Fatalf("SetPeerExcluded unexclude n1: %v", err)
	}
	if ex, err := store.IsPeerExcluded(n1); err != nil || ex {
		t.Fatalf("expected n1 unexcluded, got %v, err=%v", ex, err)
	}
	if ex, err := store.IsPeerExcluded(n2); err != nil || !ex {
		t.Fatalf("expected n2 still excluded, got %v, err=%v", ex, err)
	}

	// Clear all
	if err := store.ClearAllPeerExclusions(); err != nil {
		t.Fatalf("ClearAllPeerExclusions: %v", err)
	}
	if ex, err := store.IsPeerExcluded(n2); err != nil || ex {
		t.Fatalf("expected n2 cleared, got %v, err=%v", ex, err)
	}
}

func TestPeerExclusionAndRetirement(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	dirA := t.TempDir()
	dirB := t.TempDir()

	dbA, err := Open(ctx, replConfig(dirA, nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)

	cfgB := replConfig(dirB, nodeB, dbid, creds[nodeB], []Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	dbB, err := Open(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	// 1. Initial write on A replicates to B
	row1 := NewRowID()
	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
		row1[:], "Alice", "111-1111"); err != nil {
		t.Fatal(err)
	}
	waitForRows(t, dbB, 1, 5*time.Second)

	// Check PeerStatus on B
	peers := dbB.Peers()
	if len(peers) == 0 {
		t.Fatalf("expected peer status for nodeA on dbB")
	}

	// 2. RemovePeer on B retires and excludes nodeA
	if err := dbB.RemovePeer(ctx, nodeA); err != nil {
		t.Fatalf("RemovePeer: %v", err)
	}

	if !dbB.IsPeerExcluded(nodeA) {
		t.Fatalf("expected nodeA to be excluded on dbB")
	}
	excludedList := dbB.ExcludedPeers()
	if len(excludedList) != 1 || excludedList[0] != nodeA {
		t.Fatalf("expected ExcludedPeers to contain nodeA, got %v", excludedList)
	}

	// ForceSync on excluded peer must fail with ErrPeerExcluded
	if err := dbB.ForceSync(ctx, nodeA); !errors.Is(err, ErrPeerExcluded) {
		t.Fatalf("expected ErrPeerExcluded on ForceSync, got %v", err)
	}

	// 3. Write another row on A; B must NOT receive it while excluded
	row2 := NewRowID()
	if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
		row2[:], "Bob", "222-2222"); err != nil {
		t.Fatal(err)
	}
	// Give replication loop time to attempt delivery
	time.Sleep(300 * time.Millisecond)
	got := queryAll(t, dbB, `SELECT name FROM contacts`)
	if len(got) != 1 {
		t.Fatalf("expected 1 row on dbB while nodeA is excluded, got %d rows", len(got))
	}

	// 4. Restart Node B and verify persistent exclusion survives restart
	if err := dbB.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen dbB with nodeA in initial Peers config
	dbB2, err := Open(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB2.Close()

	if !dbB2.IsPeerExcluded(nodeA) {
		t.Fatalf("expected persistent exclusion to survive restart on dbB2")
	}

	time.Sleep(300 * time.Millisecond)
	got2 := queryAll(t, dbB2, `SELECT name FROM contacts`)
	if len(got2) != 1 {
		t.Fatalf("expected 1 row on dbB2 after restart, got %d rows", len(got2))
	}

	// 5. AddPeer clears exclusion and restores replication
	if err := dbB2.AddPeer(ctx, Peer{NodeID: nodeA, Addrs: []string{addrA}}); err != nil {
		t.Fatalf("AddPeer: %v", err)
	}
	if dbB2.IsPeerExcluded(nodeA) {
		t.Fatalf("expected nodeA exclusion cleared after AddPeer")
	}

	// Now B should converge and have both rows
	waitForRows(t, dbB2, 2, 5*time.Second)
}

func TestPeerManagementValidation(t *testing.T) {
	ctx := context.Background()
	nodeA := NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA)

	db, err := Open(ctx, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Zero NodeID validation
	if err := db.AddPeer(ctx, Peer{}); err == nil {
		t.Fatalf("expected error adding zero NodeID")
	}
	if err := db.RemovePeer(ctx, NodeID{}); err == nil {
		t.Fatalf("expected error removing zero NodeID")
	}
	if err := db.ForceSync(ctx, NodeID{}); err == nil {
		t.Fatalf("expected error forcing sync with zero NodeID")
	}

	// Self NodeID validation
	if err := db.AddPeer(ctx, Peer{NodeID: nodeA, Addrs: []string{"127.0.0.1:1234"}}); err == nil {
		t.Fatalf("expected error adding self as peer")
	}
	if err := db.ForceSync(ctx, nodeA); err == nil {
		t.Fatalf("expected error forcing sync with self")
	}

	// Unknown peer ForceSync
	randomNode := NewNodeID()
	if err := db.ForceSync(ctx, randomNode); !errors.Is(err, ErrPeerNotFound) {
		t.Fatalf("expected ErrPeerNotFound, got %v", err)
	}

	// Closed DB validation
	db2, err := Open(ctx, replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil))
	if err != nil {
		t.Fatal(err)
	}
	_ = db2.Close()


	otherNode := NewNodeID()
	if err := db2.AddPeer(ctx, Peer{NodeID: otherNode}); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on AddPeer, got %v", err)
	}
	if err := db2.RemovePeer(ctx, otherNode); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on RemovePeer, got %v", err)
	}
	if err := db2.ForceSync(ctx, otherNode); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed on ForceSync, got %v", err)
	}
}
