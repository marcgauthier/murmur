package state

import (
	"context"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

func TestLocalRecordCommitPersistsWithoutReplication(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node := ids.NewNodeID()
	dbID := fixtureDBID
	s, err := openSignedFixture(dir, node, dbID, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	row := ids.NewRowID()
	b := localBatch(s, 42,
		codec.Mutation{Policy: schema.LWW, TableID: 700, RowID: row, ColumnID: 1, Value: codec.Text("node-local")},
		codec.Mutation{Policy: schema.LWW, TableID: 700, RowID: row, ColumnID: 2, Value: codec.Int(9)},
	)
	res, err := s.CommitLocalRecords(ctx, b)
	if err != nil || !res.Applied {
		t.Fatalf("CommitLocalRecords = %+v, %v", res, err)
	}
	if seq, err := s.LocalSeq(); err != nil || seq != 0 {
		t.Fatalf("local-only write advanced replication sequence: %d, %v", seq, err)
	}
	if _, err := s.LogScan(node, 1, 1, 1<<20, func(*codec.MutationBatch) error {
		t.Fatal("local-only write entered replication log")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var replicatedCells int
	if err := s.IterateCells(func(codec.SnapshotCell) error { replicatedCells++; return nil }); err != nil {
		t.Fatal(err)
	}
	if replicatedCells != 0 {
		t.Fatalf("replicated cell scan returned %d local cells", replicatedCells)
	}
	got, tomb, tombPresent, err := s.GetLocalRow(700, row)
	if err != nil || tombPresent || got[1].Value.S != "node-local" || got[2].Value.I != 9 {
		t.Fatalf("local row = %+v tomb=%+v present=%v err=%v", got, tomb, tombPresent, err)
	}
	var iterRows int
	if err := s.IterateLocalTable(700, func(r *Row) error {
		iterRows++
		if r.ID != row || !r.Visible() {
			t.Errorf("iterated local row = %+v", r)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if iterRows != 1 {
		t.Fatalf("local table rows = %d, want 1", iterRows)
	}
	if receipt, err := s.HasReceipt(b.TxID); err != nil || !receipt {
		t.Fatalf("local transaction receipt = %v, %v", receipt, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = openSignedFixture(dir, node, dbID, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatalf("reopen local state: %v", err)
	}
	defer s.Close()
	got, tomb, tombPresent, err = s.GetLocalRow(700, row)
	if err != nil || tombPresent || got[1].Value.S != "node-local" || got[2].Value.I != 9 {
		t.Fatalf("reopened local row = %+v tomb=%+v present=%v err=%v", got, tomb, tombPresent, err)
	}
	// A stale local write is ignored, and a newer tombstone hides the row.
	stale := localBatch(s, 41, codec.Mutation{Policy: schema.LWW, TableID: 700, RowID: row, ColumnID: 1, Value: codec.Text("stale")})
	if _, err := s.CommitLocalRecords(ctx, stale); err != nil {
		t.Fatal(err)
	}
	got, _, _, err = s.GetLocalRow(700, row)
	if err != nil || got[1].Value.S != "node-local" {
		t.Fatalf("stale local write changed row: %+v, %v", got, err)
	}
	deleted := localBatch(s, 43, codec.Mutation{Policy: schema.LWW, TableID: 700, RowID: row, ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone})
	if _, err := s.CommitLocalRecords(ctx, deleted); err != nil {
		t.Fatal(err)
	}
	rows := 0
	if err := s.IterateLocalTable(700, func(r *Row) error {
		rows++
		if r.Visible() || !r.Tomb.Present || crdt.CompareVersion(r.Tomb.Version, crdt.Version{HLC: 43, NodeID: node}) != 0 {
			t.Errorf("local tombstone visibility/version = %+v", r.Tomb)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("tombstoned local row count = %d, want 1", rows)
	}
}

func TestLocalAndReplicatedRecordsCommitAtomically(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	node := ids.NewNodeID()
	s, err := openSignedFixture(dir, node, fixtureDBID, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	replicatedRow, localRow := ids.NewRowID(), ids.NewRowID()
	batch := localBatch(s, 51, codec.Mutation{Policy: schema.LWW, TableID: 701, RowID: replicatedRow, ColumnID: 1, Value: codec.Text("replicated")})
	local := []codec.Mutation{{Policy: schema.LWW, TableID: 702, RowID: localRow, ColumnID: 1, Value: codec.Text("private")}}
	result, err := s.CommitLocalWithRecords(ctx, batch, local)
	if err != nil || !result.Applied {
		t.Fatalf("mixed commit = %+v, %v", result, err)
	}
	row, err := s.GetRow(701, replicatedRow)
	if err != nil || row[1].Value.S != "replicated" {
		t.Fatalf("replicated row = %+v, %v", row, err)
	}
	localCells, _, tomb, err := s.GetLocalRow(702, localRow)
	if err != nil || tomb || localCells[1].Value.S != "private" {
		t.Fatalf("local row = %+v tomb=%v, %v", localCells, tomb, err)
	}
	entries := 0
	if _, err := s.LogScan(node, 1, 1, 1<<20, func(got *codec.MutationBatch) error {
		entries++
		if len(got.Mutations) != 1 || got.Mutations[0].TableID != 701 {
			t.Errorf("replication log leaked local data: %+v", got.Mutations)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if entries != 1 {
		t.Fatalf("replication log entries = %d", entries)
	}
	if ok, err := s.HasReceipt(batch.TxID); err != nil || !ok {
		t.Fatalf("mixed receipt = %v, %v", ok, err)
	}
}
