package state

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/ids"
)

func stagedBatch(t *testing.T, origin ids.NodeID) (*codec.MutationBatch, [][]byte) {
	t.Helper()
	b := &codec.MutationBatch{ProtocolVersion: 3, TxID: ids.NewTxID(), OriginNode: origin, Sequence: 1, HLC: 100, SchemaEpoch: 1, Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Text(strings.Repeat("x", 150<<10))}}}
	chunks, err := codec.EncodeTransactionChunks(b, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 2 {
		t.Fatal("fixture should require multiple chunks")
	}
	return b, chunks
}

func TestTransactionChunkStageSurvivesRestartAndAppliesOnlyCompleteBatch(t *testing.T) {
	dir := t.TempDir()
	node, origin := ids.NewNodeID(), ids.NewNodeID()
	dbID := ids.NewDBID()
	open := func() *Store {
		s, err := Open(dir, node, dbID, Options{Limits: codec.DefaultLimits()})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open()
	batch := &codec.MutationBatch{ProtocolVersion: 3, TxID: ids.NewTxID(), OriginNode: origin, Sequence: 1, HLC: 100, SchemaEpoch: 1, Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Text(strings.Repeat("x", 150<<10))}}}
	chunks, err := codec.EncodeTransactionChunks(batch, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	assembled, _, err := s.StageTransactionChunk(context.Background(), chunks[0], 1<<20)
	if err != nil || assembled != nil {
		t.Fatalf("first stage: assembled=%v err=%v", assembled, err)
	}
	present, err := s.StagedTransactionChunks(batch.TxID)
	if err != nil || len(present) != len(chunks) || !present[0] {
		t.Fatalf("staged availability: %v %v", present, err)
	}
	stagedPage, more, err := s.StagedTransactionsPage(ids.TxID{}, 4)
	if err != nil || more || len(stagedPage) != 1 || stagedPage[0].TxID != batch.TxID || !stagedPage[0].Received[0] {
		t.Fatalf("staged progress page: %+v more=%v err=%v", stagedPage, more, err)
	}
	progress, _, err := s.ReceiveProgressPage(ids.NodeID{}, 4)
	if err != nil {
		t.Fatal(err)
	}
	foundObserved := false
	for _, item := range progress {
		if item.Origin == origin && item.Applied == 0 && item.Observed == 1 {
			foundObserved = true
		}
	}
	if !foundObserved {
		t.Fatalf("progress did not advertise staged head: %+v", progress)
	}
	if wm, _ := s.ReceiveWatermark(origin); wm != 0 {
		t.Fatalf("partial transaction advanced watermark to %d", wm)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open()
	defer s.Close()
	for i := len(chunks) - 1; i >= 1; i-- {
		assembled, _, err = s.StageTransactionChunk(context.Background(), chunks[i], 1<<20)
		if err != nil {
			t.Fatal(err)
		}
	}
	if assembled == nil || assembled.TxID != batch.TxID {
		t.Fatal("complete durable chunks did not assemble")
	}
	if wm, _ := s.ReceiveWatermark(origin); wm != 0 {
		t.Fatalf("assembled but unapplied transaction advanced watermark to %d", wm)
	}
	if _, err := s.CommitRemote(context.Background(), assembled); err != nil {
		t.Fatal(err)
	}
	if wm, _ := s.ReceiveWatermark(origin); wm != 1 {
		t.Fatalf("applied watermark = %d", wm)
	}
	if err := s.ClearStagedTransaction(batch.TxID); err != nil {
		t.Fatal(err)
	}
	if have, err := s.StagedTransactionChunks(batch.TxID); err != nil || len(have) != 0 {
		t.Fatalf("staging not cleared: %v %v", have, err)
	}
}

func TestTransactionChunkStageInterruptedBeforeCommitIsRetryable(t *testing.T) {
	dir, node, origin, dbID := t.TempDir(), ids.NewNodeID(), ids.NewNodeID(), ids.NewDBID()
	open := func() *Store {
		s, err := Open(dir, node, dbID, Options{Limits: codec.DefaultLimits()})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open()
	fault := errors.New("simulated interruption before stage commit")
	s.transactionStageFault = func() error { return fault }
	batch, chunks := stagedBatch(t, origin)
	if _, _, err := s.StageTransactionChunk(context.Background(), chunks[0], 1<<20); !errors.Is(err, fault) {
		t.Fatalf("stage error = %v, want injected interruption", err)
	}
	if wm, _ := s.ReceiveWatermark(origin); wm != 0 {
		t.Fatalf("failed stage advanced watermark to %d", wm)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open()
	defer s.Close()
	if got, err := s.StagedTransactionChunks(batch.TxID); err != nil || len(got) != 0 {
		t.Fatalf("pre-commit interruption left durable chunks: %v, %v", got, err)
	}
	s.transactionStageFault = nil
	if _, _, err := s.StageTransactionChunk(context.Background(), chunks[0], 1<<20); err != nil {
		t.Fatal(err)
	}
	if wm, _ := s.ReceiveWatermark(origin); wm != 0 {
		t.Fatalf("partial retry advanced watermark to %d", wm)
	}
}

func TestTransactionChunkStageBudgetEvictionDoesNotAdvanceWatermark(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	origin := ids.NewNodeID()
	batch, chunks := stagedBatch(t, origin)
	s.stagedTransactionByteLimit = int64(len(chunks[0]))
	if _, _, err := s.StageTransactionChunk(context.Background(), chunks[0], 1<<20); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.StageTransactionChunk(context.Background(), chunks[1], 1<<20); !errors.Is(err, ErrStagingOverloaded) {
		t.Fatalf("over-budget stage error = %v, want ErrStagingOverloaded", err)
	}
	if wm, _ := s.ReceiveWatermark(origin); wm != 0 {
		t.Fatalf("budget rejection advanced watermark to %d", wm)
	}
	if err := s.ClearStagedTransaction(batch.TxID); err != nil {
		t.Fatal(err)
	}
	if wm, _ := s.ReceiveWatermark(origin); wm != 0 {
		t.Fatalf("eviction advanced watermark to %d", wm)
	}
	if have, err := s.StagedTransactionChunks(batch.TxID); err != nil || len(have) != 0 {
		t.Fatalf("evicted transfer still staged: %v, %v", have, err)
	}
}
