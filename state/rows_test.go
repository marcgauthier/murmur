package state

import (
	"context"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

func TestGetRowsBoundsSnapshotRead(t *testing.T) {
	node := ids.NewNodeID()
	s, err := openSignedFixture(t.TempDir(), node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	refs := make([]RowRef, MaxRowsPerRead+1)
	if _, err := s.GetRows(refs); err == nil {
		t.Fatal("GetRows accepted a request larger than its bounded read limit")
	}
}

func TestForEachRowSnapshotStreamsPastBulkReadBound(t *testing.T) {
	node := ids.NewNodeID()
	s, err := openSignedFixture(t.TempDir(), node, ids.DBID{}, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	refs := make([]RowRef, MaxRowsPerRead+1)
	for i := range refs {
		refs[i] = RowRef{Table: 7, ID: ids.NewRowID()}
	}
	visited := 0
	err = s.ForEachRowSnapshot(context.Background(), refs, func(i int, row *Row) error {
		if i != visited || row.Table != refs[i].Table || row.ID != refs[i].ID {
			t.Fatalf("stream item %d = %d/%s, want %d/%s", visited, row.Table, row.ID, refs[i].Table, refs[i].ID)
		}
		visited++
		return nil
	})
	if err != nil {
		t.Fatalf("stream %d rows: %v", len(refs), err)
	}
	if visited != len(refs) {
		t.Fatalf("visited %d rows, want %d", visited, len(refs))
	}
}
