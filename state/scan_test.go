package state

import (
	"context"
	"errors"
	"testing"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
)

func TestLogScanPaginationAndByteLimit(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	for i := 0; i < 3; i++ {
		_, err := s.CommitLocal(context.Background(), localBatch(s, uint64(100+i)<<16,
			codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(int64(i))}))
		if err != nil {
			t.Fatal(err)
		}
	}

	var got []uint64
	last, err := s.LogScan(s.NodeID(), 1, 2, 1<<20, func(b *codec.MutationBatch) error {
		got = append(got, b.Sequence)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if last != 2 || len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("first page: last=%d sequences=%v", last, got)
	}

	// A byte limit smaller than the next encoded batch still permits one batch,
	// so a caller can always make progress.
	got = nil
	last, err = s.LogScan(s.NodeID(), 3, 10, 1, func(b *codec.MutationBatch) error {
		got = append(got, b.Sequence)
		return nil
	})
	if err != nil || last != 3 || len(got) != 1 || got[0] != 3 {
		t.Fatalf("byte-limited page: last=%d sequences=%v err=%v", last, got, err)
	}
}

func TestLogScanCallbackErrorDoesNotAdvanceCursor(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	_, err := s.CommitLocal(context.Background(), localBatch(s, 100<<16,
		codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Text("x")}))
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("stop scan")
	last, err := s.LogScan(s.NodeID(), 1, 10, 0, func(*codec.MutationBatch) error { return wantErr })
	if !errors.Is(err, wantErr) || last != 0 {
		t.Fatalf("LogScan returned last=%d err=%v; want 0 and callback error", last, err)
	}
}

func TestReceiveProgressPagePaginatesWithRetainedBounds(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	origins := []ids.NodeID{ids.NewNodeID(), ids.NewNodeID(), ids.NewNodeID()}
	for i, origin := range origins {
		b := localBatch(s, uint64(200+i)<<16,
			codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(int64(i))})
		b.OriginNode = origin
		b.Sequence = 1
		if _, err := s.CommitRemote(context.Background(), b); err != nil {
			t.Fatal(err)
		}
	}

	first, more, err := s.ReceiveProgressPage(ids.NodeID{}, 2)
	if err != nil || !more || len(first) != 2 {
		t.Fatalf("first progress page len=%d more=%t err=%v", len(first), more, err)
	}
	second, more, err := s.ReceiveProgressPage(first[len(first)-1].Origin, 2)
	if err != nil || more || len(second) != 1 {
		t.Fatalf("second progress page len=%d more=%t err=%v", len(second), more, err)
	}
	for _, p := range append(first, second...) {
		if p.Applied != 1 || p.FirstRetained != 1 || p.LastRetained != 1 {
			t.Fatalf("progress=%+v, want applied and retained range 1..1", p)
		}
	}
	if _, _, err := s.ReceiveProgressPage(ids.NodeID{}, 0); err == nil {
		t.Fatal("accepted zero progress page size")
	}
}
