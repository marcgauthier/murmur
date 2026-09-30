package state

import (
	"context"
	"errors"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

func TestCommitRemoteGroupPreservesReceiptsAndCommitsAtomically(t *testing.T) {
	ctx := context.Background()
	source := openTestStore(t, ids.NewNodeID())
	target := openTestStore(t, ids.NewNodeID())
	row := ids.NewRowID()
	first := localBatch(source, 10, codec.Mutation{TableID: 4, RowID: row, ColumnID: 2, Value: codec.Text("first")})
	second := localBatch(source, 20, codec.Mutation{TableID: 4, RowID: row, ColumnID: 2, Value: codec.Text("second")})
	if _, err := source.CommitLocal(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := source.CommitLocal(ctx, second); err != nil {
		t.Fatal(err)
	}
	before, err := target.StateGeneration()
	if err != nil {
		t.Fatal(err)
	}
	result, err := target.CommitRemoteGroup(ctx, []*codec.MutationBatch{first, second})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Applied || result.Generation != before+2 || len(result.Winners) != 1 {
		t.Fatalf("group result = %+v, generation before=%d", result, before)
	}
	if wm, _ := target.ReceiveWatermark(source.NodeID()); wm != 2 {
		t.Fatalf("group watermark = %d", wm)
	}
	for _, batch := range []*codec.MutationBatch{first, second} {
		if _, err := target.getDirect(ReceiptKey(batch.TxID)); err != nil {
			t.Fatalf("receipt %s missing: %v", batch.TxID, err)
		}
	}
	cell, ok, err := target.GetCell(4, row, 2)
	if err != nil || !ok || cell.Value.S != "second" {
		t.Fatalf("group winner = %+v, present=%v, err=%v", cell, ok, err)
	}
	duplicate, err := target.CommitRemoteGroup(ctx, []*codec.MutationBatch{first, second})
	if err != nil || duplicate.Applied || duplicate.Generation != result.Generation {
		t.Fatalf("duplicate group = %+v, err=%v", duplicate, err)
	}
}

func TestCommitRemoteGroupGapLeavesNoPartialProgress(t *testing.T) {
	ctx := context.Background()
	source := openTestStore(t, ids.NewNodeID())
	target := openTestStore(t, ids.NewNodeID())
	first := localBatch(source, 10, codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(1)})
	second := localBatch(source, 20, codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(2)})
	if _, err := source.CommitLocal(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := source.CommitLocal(ctx, second); err != nil {
		t.Fatal(err)
	}
	first.Sequence++ // sequence 2 is a gap at the receiver
	before, _ := target.StateGeneration()
	if _, err := target.CommitRemoteGroup(ctx, []*codec.MutationBatch{first, second}); !errors.Is(err, ErrGap) {
		t.Fatalf("expected gap, got %v", err)
	}
	if wm, _ := target.ReceiveWatermark(source.NodeID()); wm != 0 {
		t.Fatalf("partial group advanced watermark to %d", wm)
	}
	if after, _ := target.StateGeneration(); after != before {
		t.Fatalf("partial group advanced generation from %d to %d", before, after)
	}
}
