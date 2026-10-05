package state

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

func openStoreAt(t *testing.T, path string, node ids.NodeID, db ids.DBID, restore *RestoreAdoption) *Store {
	t.Helper()
	s, err := openSignedFixture(path, node, db, Options{Limits: codec.DefaultLimits(), Restore: restore})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func closeStore(t *testing.T, s *Store) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func countPrefix(t *testing.T, s *Store, prefix []byte) int {
	t.Helper()
	var n int
	err := s.snapshot(func(snap *pebble.Snapshot) error {
		it, err := snap.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
		if err != nil {
			return err
		}
		defer it.Close()
		for it.SeekGE(prefix); it.Valid(); it.Next() {
			n++
		}
		return it.Error()
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRestoreAdoptionResetsWriterIdentity(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	nodeA, nodeB := ids.NewNodeID(), ids.NewNodeID()
	dbID := ids.NewDBID()

	a := openStoreAt(t, path, nodeA, dbID, nil)
	row := ids.NewRowID()
	if _, err := a.CommitLocal(ctx, localBatch(a, a.ClockNow(),
		codec.Mutation{TableID: 1, RowID: row, ColumnID: 2, Value: codec.Text("v1")})); err != nil {
		t.Fatal(err)
	}
	// Remote history from a third writer.
	nodeX := ids.NewNodeID()
	xb := &codec.MutationBatch{
		ProtocolVersion: 1, TxID: ids.NewTxID(), OriginNode: nodeX,
		Sequence: 1, HLC: a.ClockNow(), SchemaEpoch: 1,
		Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("x")}},
	}
	if _, err := commitRemoteFixture(a, ctx, xb); err != nil {
		t.Fatal(err)
	}
	// Incomplete snapshot staging and peer acks the fresh node must shed.
	if err := a.db.Set(SnapshotKey("recv/active"), []byte("transfer"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := a.db.Set(SnapshotKey("recv/abcd/manifest"), []byte("manifest"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := a.SetPeerAck(nodeX, nodeA, 7); err != nil {
		t.Fatal(err)
	}
	// Inherited member admissions and exclusion policy that must be cleared.
	if _, err := a.EnsureMemberAdmitted(nodeX, 1000, 5000); err != nil {
		t.Fatal(err)
	}
	if err := a.SetPeerExcluded(nodeX, true); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	b := openStoreAt(t, path, nodeB, dbID, &RestoreAdoption{Source: nodeA, Fresh: nodeB, BackupID: "b1"})
	if got := b.NodeID(); got != nodeB {
		t.Fatalf("NodeID = %s, want %s", got, nodeB)
	}
	if seq, err := b.LocalSeq(); err != nil || seq != 0 {
		t.Fatalf("LocalSeq = %d, %v; want 0", seq, err)
	}
	marker, ok, err := b.RestoreMarker()
	if err != nil || !ok {
		t.Fatalf("RestoreMarker = %+v, %v, %v", marker, ok, err)
	}
	if marker.BackupID != "b1" || marker.SourceNodeID != nodeA.String() ||
		marker.FreshNodeID != nodeB.String() || marker.AdoptedMillis == 0 {
		t.Fatalf("bad marker: %+v", marker)
	}
	// Staging and peer acks cleared.
	if n := countPrefix(t, b, SnapshotKey("recv/")); n != 0 {
		t.Fatalf("%d staged snapshot keys survive adoption", n)
	}
	if ack, err := b.PeerAck(nodeX, nodeA); err != nil || ack != 0 {
		t.Fatalf("PeerAck = %d, %v; want cleared", ack, err)
	}
	// Member admissions and peer exclusion policy cleared.
	if mbs, err := b.ListMembers(); err != nil || len(mbs) != 0 {
		t.Fatalf("ListMembers = %+v, %v; want empty after restore", mbs, err)
	}
	if ex, err := b.ListExcludedPeers(); err != nil || len(ex) != 0 {
		t.Fatalf("ListExcludedPeers = %+v, %v; want empty after restore", ex, err)
	}
	if excluded, err := b.IsPeerExcluded(nodeX); err != nil || excluded {
		t.Fatalf("IsPeerExcluded = %v, %v; want false", excluded, err)
	}
	// History preserved: cells, both origins, DBID.
	if st, ok, err := b.GetCell(1, row, 2); err != nil || !ok || st.Value.S != "v1" {
		t.Fatalf("cell lost: %+v %v %v", st, ok, err)
	}
	origins, err := b.KnownOrigins()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[ids.NodeID]bool{}
	for _, o := range origins {
		seen[o] = true
	}
	if !seen[nodeA] || !seen[nodeX] {
		t.Fatalf("origins = %v, want A and X retained", origins)
	}
	if b.DBID() != dbID {
		t.Fatalf("DBID changed: %s", b.DBID())
	}
	// The fresh writer starts its own sequence.
	if _, err := b.CommitLocal(ctx, localBatch(b, b.ClockNow(),
		codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("b1")})); err != nil {
		t.Fatal(err)
	}
	if seq, _ := b.LocalSeq(); seq != 1 {
		t.Fatalf("LocalSeq after first write = %d, want 1", seq)
	}
	closeStore(t, b)
}

func TestRestoreAdoptionRejectsMismatch(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	nodeA := ids.NewNodeID()
	dbID := ids.NewDBID()

	a := openStoreAt(t, path, nodeA, dbID, nil)
	if _, err := a.CommitLocal(ctx, localBatch(a, a.ClockNow(),
		codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("v")})); err != nil {
		t.Fatal(err)
	}
	nodeX := ids.NewNodeID()
	xb := &codec.MutationBatch{
		ProtocolVersion: 1, TxID: ids.NewTxID(), OriginNode: nodeX,
		Sequence: 1, HLC: a.ClockNow(), SchemaEpoch: 1,
		Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("x")}},
	}
	if _, err := commitRemoteFixture(a, ctx, xb); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	nodeB, nodeC := ids.NewNodeID(), ids.NewNodeID()
	open := func(node ids.NodeID, r *RestoreAdoption) error {
		s, err := openSignedFixture(path, node, dbID, Options{Limits: codec.DefaultLimits(), Restore: r})
		if err == nil {
			_ = s.Close()
		}
		return err
	}
	// No intent: ordinary wrong-node open still rejected.
	if err := open(nodeB, nil); err == nil {
		t.Fatal("open as B without intent succeeded")
	}
	// Intent bound to a different fresh identity.
	if err := open(nodeC, &RestoreAdoption{Source: nodeA, Fresh: nodeB}); !errors.Is(err, ErrRestoreIdentityMismatch) {
		t.Fatalf("wrong-fresh err = %v", err)
	}
	// Intent for the wrong source data.
	if err := open(nodeB, &RestoreAdoption{Source: nodeC, Fresh: nodeB}); !errors.Is(err, ErrRestoreIntentMismatch) {
		t.Fatalf("wrong-source err = %v", err)
	}
	// Intent bound to the stored identity but opened as someone else: the
	// fresh-identity check fires before any source comparison.
	// (Fresh==source rollback is rejected earlier, at Restore and at Open
	// intent validation; reaching here with stored==passed is an ordinary
	// open by design, which maintenance reopen relies on.)
	if err := open(nodeC, &RestoreAdoption{Source: nodeA, Fresh: nodeA}); !errors.Is(err, ErrRestoreIdentityMismatch) {
		t.Fatalf("stale-intent err = %v", err)
	}
	// Fresh == other historical writer.
	if err := open(nodeX, &RestoreAdoption{Source: nodeA, Fresh: nodeX}); !errors.Is(err, ErrRestoreIdentityReuse) {
		t.Fatalf("history-reuse err = %v", err)
	}
	// Store untouched by the failed attempts: A still opens cleanly.
	s := openStoreAt(t, path, nodeA, dbID, nil)
	if seq, _ := s.LocalSeq(); seq != 1 {
		t.Fatalf("LocalSeq = %d, want 1 (failed adoptions must not mutate)", seq)
	}
	if _, ok, _ := s.RestoreMarker(); ok {
		t.Fatal("marker written by failed adoption")
	}
	closeStore(t, s)
}

func TestRestoreAdoptionReseedsDBID(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	nodeA, nodeB := ids.NewNodeID(), ids.NewNodeID()
	oldDB, newDB := ids.NewDBID(), ids.NewDBID()

	a := openStoreAt(t, path, nodeA, oldDB, nil)
	row := ids.NewRowID()
	if _, err := a.CommitLocal(ctx, localBatch(a, a.ClockNow(),
		codec.Mutation{TableID: 1, RowID: row, ColumnID: 2, Value: codec.Text("v")})); err != nil {
		t.Fatal(err)
	}
	closeStore(t, a)

	// Wrong source DBID: intent does not match the stored data.
	bad, err := openSignedFixture(path, nodeB, newDB, Options{Limits: codec.DefaultLimits(), Restore: &RestoreAdoption{
		Source: nodeA, Fresh: nodeB, SourceDBID: ids.NewDBID(), NewDBID: newDB, BackupID: "b1",
	}})
	if err == nil {
		_ = bad.Close()
		t.Fatal("reseed with wrong source DBID succeeded")
	}
	if !errors.Is(err, ErrRestoreIntentMismatch) {
		t.Fatalf("wrong-source-DBID err = %v", err)
	}

	// Reseed to the same DBID is not a reseed.
	same, err := openSignedFixture(path, nodeB, oldDB, Options{Limits: codec.DefaultLimits(), Restore: &RestoreAdoption{
		Source: nodeA, Fresh: nodeB, SourceDBID: oldDB, NewDBID: oldDB, BackupID: "b1",
	}})
	if err == nil {
		_ = same.Close()
		t.Fatal("reseed to same DBID succeeded")
	}
	if !errors.Is(err, ErrRestoreIdentityReuse) {
		t.Fatalf("same-DBID err = %v", err)
	}

	b := openStoreAt(t, path, nodeB, newDB, &RestoreAdoption{
		Source: nodeA, Fresh: nodeB, SourceDBID: oldDB, NewDBID: newDB, BackupID: "b1",
	})
	if got := b.DBID(); got != newDB {
		closeStore(t, b)
		t.Fatalf("DBID = %s, want %s", got, newDB)
	}
	if seq, _ := b.LocalSeq(); seq != 0 {
		closeStore(t, b)
		t.Fatalf("LocalSeq = %d, want 0", seq)
	}
	marker, ok, err := b.RestoreMarker()
	if err != nil || !ok || marker.Mode != "reseed" {
		closeStore(t, b)
		t.Fatalf("marker = %+v, %v, %v", marker, ok, err)
	}
	if st, ok, err := b.GetCell(1, row, 2); err != nil || !ok {
		closeStore(t, b)
		t.Fatalf("baseline cell lost: %+v %v %v", st, ok, err)
	}
	closeStore(t, b)

	// The old cluster identity stays unusable on this data.
	s, err := openSignedFixture(path, nodeB, oldDB, Options{Limits: codec.DefaultLimits()})
	if err == nil {
		_ = s.Close()
		t.Fatal("reopen with old DBID succeeded")
	}
}

func TestRestoreAdoptionIsIdempotent(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	nodeA, nodeB := ids.NewNodeID(), ids.NewNodeID()
	dbID := ids.NewDBID()

	a := openStoreAt(t, path, nodeA, dbID, nil)
	if _, err := a.CommitLocal(ctx, localBatch(a, a.ClockNow(),
		codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("v")})); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	// Adopt, write, close: simulates a crash before the intent file is
	// cleared (the file replay is covered at the DB layer).
	b := openStoreAt(t, path, nodeB, dbID, &RestoreAdoption{Source: nodeA, Fresh: nodeB, BackupID: "b1"})
	if _, err := b.CommitLocal(ctx, localBatch(b, b.ClockNow(),
		codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("b")})); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// Ordinary reopen as B: no sequence reset, marker intact.
	b2 := openStoreAt(t, path, nodeB, dbID, nil)
	if seq, _ := b2.LocalSeq(); seq != 1 {
		t.Fatalf("LocalSeq = %d, want 1 (reopen must not reset)", seq)
	}
	marker, ok, err := b2.RestoreMarker()
	if err != nil || !ok || marker.FreshNodeID != nodeB.String() {
		t.Fatalf("marker = %+v, %v, %v", marker, ok, err)
	}
	// The retired identity stays unusable.
	s, err := openSignedFixture(path, nodeA, dbID, Options{Limits: codec.DefaultLimits()})
	if err == nil {
		_ = s.Close()
		t.Fatal("reopen as retired A succeeded")
	}
	closeStore(t, b2)
}
