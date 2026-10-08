package state

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/origin"
)

// openGroupTestStore opens a store with origin-signing identity provisioned.
func openGroupTestStore(t *testing.T, node ids.NodeID) *Store {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := origin.NewKeyRegistry(map[ids.NodeID]ed25519.PublicKey{node: pub})
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(t.TempDir(), node, ids.DBID{}, withTestKey(Options{
		Limits:        codec.DefaultLimits(),
		OriginSigning: origin.Config{PrivateKey: priv, TrustedKeys: reg},
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// groupLocalBatch builds an unsigned local-origin batch; CommitLocalGroup
// assigns the sequence and signs it, like CommitLocal.
func groupLocalBatch(s *Store, hlc uint64, muts ...codec.Mutation) *codec.MutationBatch {
	return &codec.MutationBatch{
		ProtocolVersion: 1,
		TxID:            ids.NewTxID(),
		OriginNode:      s.NodeID(),
		HLC:             hlc,
		SchemaEpoch:     1,
		Mutations:       muts,
	}
}

func TestCommitLocalGroupAssignsContiguousSequences(t *testing.T) {
	ctx := context.Background()
	s := openGroupTestStore(t, ids.NewNodeID())
	batches := []*codec.MutationBatch{
		groupLocalBatch(s, s.ClockNow(), codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(1)}),
		groupLocalBatch(s, s.ClockNow(), codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(2)}),
		groupLocalBatch(s, s.ClockNow(), codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(3)}),
	}
	before, err := s.StateGeneration()
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.CommitLocalGroup(ctx, batches)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Applied || res.Generation != before+3 || res.GenerationBefore != before {
		t.Fatalf("group result = %+v, before=%d", res.MergeResult, before)
	}
	if len(res.Members) != 3 {
		t.Fatalf("members = %d, want 3", len(res.Members))
	}
	for i, m := range res.Members {
		if !m.Applied || m.Sequence != uint64(i+1) || m.Generation != before+uint64(i+1) {
			t.Fatalf("member %d = %+v, want applied seq=%d gen=%d", i, m, i+1, before+uint64(i+1))
		}
		if batches[i].Sequence != uint64(i+1) {
			t.Fatalf("batch %d sequence = %d", i, batches[i].Sequence)
		}
		if _, err := s.getDirect(ReceiptKey(batches[i].TxID)); err != nil {
			t.Fatalf("receipt %d missing: %v", i, err)
		}
	}
	if seq, _ := s.LocalSeq(); seq != 3 {
		t.Fatalf("local seq = %d, want 3", seq)
	}
	if wm, _ := s.ReceiveWatermark(s.NodeID()); wm != 3 {
		t.Fatalf("self watermark = %d, want 3", wm)
	}
	var scanned int
	if _, err := s.LogScan(s.NodeID(), 1, 10, 1<<20, func(b *codec.MutationBatch) error {
		scanned++
		if err := s.VerifyOrigin(b); err != nil {
			t.Fatalf("stored batch %d fails origin verification: %v", scanned, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if scanned != 3 {
		t.Fatalf("scanned %d log batches, want 3", scanned)
	}
}

func TestCommitLocalGroupLaterMemberWinsSharedCell(t *testing.T) {
	ctx := context.Background()
	s := openGroupTestStore(t, ids.NewNodeID())
	row := ids.NewRowID()
	batches := []*codec.MutationBatch{
		groupLocalBatch(s, s.ClockNow(), codec.Mutation{TableID: 4, RowID: row, ColumnID: 2, Value: codec.Text("first")}),
		groupLocalBatch(s, s.ClockNow(), codec.Mutation{TableID: 4, RowID: row, ColumnID: 2, Value: codec.Text("second")}),
	}
	res, err := s.CommitLocalGroup(ctx, batches)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Applied || len(res.Winners) != 1 {
		t.Fatalf("group result = %+v", res.MergeResult)
	}
	cell, ok, err := s.GetCell(4, row, 2)
	if err != nil || !ok || cell.Value.S != "second" {
		t.Fatalf("group winner = %+v, present=%v, err=%v", cell, ok, err)
	}
}

func TestCommitLocalGroupSkipsDuplicates(t *testing.T) {
	ctx := context.Background()
	s := openGroupTestStore(t, ids.NewNodeID())
	batch := groupLocalBatch(s, s.ClockNow(), codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(1)})
	if _, err := s.CommitLocal(ctx, batch); err != nil {
		t.Fatal(err)
	}
	before, err := s.StateGeneration()
	if err != nil {
		t.Fatal(err)
	}
	fresh := groupLocalBatch(s, s.ClockNow(), codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(2)})
	res, err := s.CommitLocalGroup(ctx, []*codec.MutationBatch{batch, fresh, batch})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Applied || res.Generation != before+1 {
		t.Fatalf("group result = %+v, before=%d", res.MergeResult, before)
	}
	if res.Members[0].Applied || !res.Members[1].Applied || res.Members[2].Applied {
		t.Fatalf("members = %+v, want only index 1 applied", res.Members)
	}
	if res.Members[1].Sequence != 2 {
		t.Fatalf("fresh member sequence = %d, want 2", res.Members[1].Sequence)
	}
}

func TestCommitLocalGroupSingleDelegates(t *testing.T) {
	ctx := context.Background()
	s := openGroupTestStore(t, ids.NewNodeID())
	batch := groupLocalBatch(s, s.ClockNow(), codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(1)})
	before, err := s.StateGeneration()
	if err != nil {
		t.Fatal(err)
	}
	res, err := s.CommitLocalGroup(ctx, []*codec.MutationBatch{batch})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Applied || res.Generation != before+1 || res.GenerationBefore != before {
		t.Fatalf("single result = %+v, before=%d", res.MergeResult, before)
	}
	if len(res.Members) != 1 || !res.Members[0].Applied || res.Members[0].Sequence != 1 {
		t.Fatalf("members = %+v", res.Members)
	}
}

func TestCommitLocalGroupRejectsBadInput(t *testing.T) {
	ctx := context.Background()
	s := openGroupTestStore(t, ids.NewNodeID())
	if _, err := s.CommitLocalGroup(ctx, nil); err == nil {
		t.Fatal("empty group committed")
	}
	foreign := groupLocalBatch(s, s.ClockNow(), codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(1)})
	foreign.OriginNode = ids.NewNodeID()
	if _, err := s.CommitLocalGroup(ctx, []*codec.MutationBatch{foreign}); err == nil {
		t.Fatal("foreign origin committed")
	}
	empty := groupLocalBatch(s, s.ClockNow())
	if _, err := s.CommitLocalGroup(ctx, []*codec.MutationBatch{empty}); err == nil {
		t.Fatal("empty batch committed")
	}
	if _, err := s.CommitLocalGroup(ctx, []*codec.MutationBatch{
		groupLocalBatch(s, s.ClockNow(), codec.Mutation{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Int(1)}),
		empty,
	}); err == nil {
		t.Fatal("group with empty batch committed")
	}
}
