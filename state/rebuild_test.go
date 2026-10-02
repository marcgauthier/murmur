package state

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

func TestRebuildSnapshotProgressAndIsolation(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	row := ids.NewRowID()
	_, err := s.CommitLocal(context.Background(), localBatch(s, 100<<16,
		codec.Mutation{TableID: 1, RowID: row, ColumnID: 1, Value: codec.Text("before")},
		codec.Mutation{TableID: 2, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(2)}))
	if err != nil {
		t.Fatal(err)
	}
	var processed uint64
	r, err := s.NewRebuildSnapshot(context.Background(), func(n uint64) { processed += n })
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, err = s.CommitLocal(context.Background(), localBatch(s, 200<<16,
		codec.Mutation{TableID: 1, RowID: row, ColumnID: 1, Value: codec.Text("after")},
		codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Text("new")}))
	if err != nil {
		t.Fatal(err)
	}
	cells, err := r.GetRow(1, row)
	if err != nil || cells[1].Value.ToAny() != "before" {
		t.Fatalf("snapshot cells=%v err=%v", cells, err)
	}
	for _, table := range []uint32{1, 2} {
		if err := r.IterateTable(table, func(*Row) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if processed != 2 {
		t.Fatalf("processed=%d want=2", processed)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRebuildSnapshotCorruptCellNotProcessed(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	key := CellKey(1, ids.NewRowID(), 1)
	if err := s.db.Set(key, []byte{0xff}, nil); err != nil {
		t.Fatal(err)
	}
	var processed uint64
	r, err := s.NewRebuildSnapshot(context.Background(), func(n uint64) { processed += n })
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	err = r.IterateTable(1, func(*Row) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "corrupt cell") || processed != 0 {
		t.Fatalf("corruption err=%v processed=%d", err, processed)
	}
}

func TestRebuildSnapshotCancellationDuringIteration(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	var mutations []codec.Mutation
	for i := 0; i < 1100; i++ {
		mutations = append(mutations, codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(int64(i))})
	}
	if _, err := s.CommitLocal(context.Background(), localBatch(s, 100<<16, mutations...)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var processed uint64
	r, err := s.NewRebuildSnapshot(ctx, func(n uint64) {
		processed += n
		if processed >= 1024 {
			cancel()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	err = r.IterateTable(1, func(*Row) error { return nil })
	if !errors.Is(err, context.Canceled) || processed != 1024 {
		t.Fatalf("processed=%d err=%v", processed, err)
	}
}

func TestRebuildSnapshotCancellation(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	ctx, cancel := context.WithCancel(context.Background())
	r, err := s.NewRebuildSnapshot(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cancel()
	if err := r.IterateTable(1, func(*Row) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("iteration error=%v", err)
	}
}
