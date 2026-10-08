package state

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testidentity"
)

func stagedBatch(t *testing.T, origin ids.NodeID) (*codec.MutationBatch, [][]byte) {
	t.Helper()
	b := &codec.MutationBatch{ProtocolVersion: 3, TxID: ids.NewTxID(), OriginNode: origin, Sequence: 1, HLC: 100, SchemaEpoch: 1, Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Text(strings.Repeat("x", 150<<10))}}}
	chunks, err := encodeChunksFixture(b, 1<<20)
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
	dbID := fixtureDBID
	open := func() *Store {
		s, err := openSignedFixture(dir, node, dbID, Options{Limits: codec.DefaultLimits()})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open()
	batch := &codec.MutationBatch{ProtocolVersion: 3, TxID: ids.NewTxID(), OriginNode: origin, Sequence: 1, HLC: 100, SchemaEpoch: 1, Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Text(strings.Repeat("x", 150<<10))}}}
	chunks, err := encodeChunksFixture(batch, 1<<20)
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
	if _, err := commitRemoteFixture(s, context.Background(), assembled); err != nil {
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
	dir, node, origin, dbID := t.TempDir(), ids.NewNodeID(), ids.NewNodeID(), fixtureDBID
	open := func() *Store {
		s, err := openSignedFixture(dir, node, dbID, Options{Limits: codec.DefaultLimits()})
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

func TestThousandsOfPartialChunkTransfersRemainBounded(t *testing.T) {
	s := openTestStore(t, ids.NewNodeID())
	origin := ids.NewNodeID()

	s.stagedTransactionCountLimit = 5000
	s.stagedTransactionByteLimit = 500 << 20

	ctx := context.Background()
	const numTransfers = 2000

	firstChunkPerTx := make([][]byte, numTransfers)
	txIDs := make([]ids.TxID, numTransfers)
	for i := 0; i < numTransfers; i++ {
		txID := ids.NewTxID()
		txIDs[i] = txID
		batch := &codec.MutationBatch{
			ProtocolVersion: 3,
			TxID:            txID,
			OriginNode:      origin,
			Sequence:        uint64(i + 1),
			HLC:             uint64(100 + i),
			SchemaEpoch:     1,
			Mutations: []codec.Mutation{{
				TableID:  1,
				RowID:    ids.NewRowID(),
				ColumnID: 1,
				Value:    codec.Text(strings.Repeat("x", 150<<10)),
			}},
		}
		chunks, err := encodeChunksFixture(batch, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		firstChunkPerTx[i] = chunks[0]
	}

	t0 := time.Now()
	for i := 0; i < 100; i++ {
		if _, _, err := s.StageTransactionChunk(ctx, firstChunkPerTx[i], 1<<20); err != nil {
			t.Fatalf("stage initial chunk %d: %v", i, err)
		}
	}
	earlyDuration := time.Since(t0)

	for i := 100; i < numTransfers-100; i++ {
		if _, _, err := s.StageTransactionChunk(ctx, firstChunkPerTx[i], 1<<20); err != nil {
			t.Fatalf("stage intermediate chunk %d: %v", i, err)
		}
	}

	tLate := time.Now()
	for i := numTransfers - 100; i < numTransfers; i++ {
		if _, _, err := s.StageTransactionChunk(ctx, firstChunkPerTx[i], 1<<20); err != nil {
			t.Fatalf("stage late chunk %d: %v", i, err)
		}
	}
	lateDuration := time.Since(tLate)

	t.Logf("Early 100 chunks duration: %v, Late 100 chunks duration (with %d staged): %v",
		earlyDuration, numTransfers-100, lateDuration)

	if lateDuration > 10*earlyDuration && lateDuration > 500*time.Millisecond {
		t.Fatalf("staging latency grew with staged history: early=%v, late=%v", earlyDuration, lateDuration)
	}

	if s.stagedTransactionCount != numTransfers {
		t.Fatalf("staged transaction count = %d, want %d", s.stagedTransactionCount, numTransfers)
	}

	for i := 0; i < numTransfers/2; i++ {
		if err := s.ClearStagedTransaction(txIDs[i]); err != nil {
			t.Fatalf("clear tx %d: %v", i, err)
		}
	}
	if s.stagedTransactionCount != numTransfers/2 {
		t.Fatalf("staged transaction count after clearing half = %d, want %d", s.stagedTransactionCount, numTransfers/2)
	}
}

// Same TxID with conflicting metadata must fail without poisoning the valid
// transfer or the watermark. The rival chunk below is an equivocating origin:
// same TxID, different sequence, separately valid signature, so it passes
// origin verification and reaches the metadata check.
func TestStagedConflictingMetadataRejectedWithoutPoison(t *testing.T) {
	dir := t.TempDir()
	node, origin := ids.NewNodeID(), ids.NewNodeID()
	s, err := openSignedFixture(dir, node, fixtureDBID, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	b, chunks := stagedBatch(t, origin)
	if _, _, err := s.StageTransactionChunk(ctx, chunks[0], 1<<20); err != nil {
		t.Fatal(err)
	}

	rival := *b
	rival.Sequence = 13
	rival.Mutations = append([]codec.Mutation(nil), b.Mutations...)
	testidentity.Sign(&rival, fixtureDBID)
	rivalChunks, err := encodeChunksFixture(&rival, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.StageTransactionChunk(ctx, rivalChunks[1], 1<<20); err == nil ||
		!strings.Contains(err.Error(), "conflicting staged transaction metadata") {
		t.Fatalf("conflicting metadata err = %v", err)
	}
	if wm, _ := s.ReceiveWatermark(origin); wm != 0 {
		t.Fatalf("conflicting metadata advanced watermark to %d", wm)
	}
	present, err := s.StagedTransactionChunks(b.TxID)
	if err != nil || len(present) != len(chunks) || !present[0] || present[1] {
		t.Fatalf("rejected chunk polluted staging: %v %v", present, err)
	}

	// The valid transfer still completes with the correct chunks.
	var assembled *codec.MutationBatch
	for _, raw := range chunks[1:] {
		if assembled, _, err = s.StageTransactionChunk(ctx, raw, 1<<20); err != nil {
			t.Fatal(err)
		}
	}
	if assembled == nil || assembled.TxID != b.TxID || assembled.Sequence != 1 {
		t.Fatal("valid transfer did not assemble after rejected rival")
	}
}

// A corrupted re-send of an already-staged chunk must fail while the first
// (valid) version stays intact: chunk bytes are not covered by the origin
// signature, so the byte comparison is the defense.
func TestStagedConflictingDuplicateKeepsFirstVersion(t *testing.T) {
	dir := t.TempDir()
	node, origin := ids.NewNodeID(), ids.NewNodeID()
	s, err := openSignedFixture(dir, node, fixtureDBID, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	batch := &codec.MutationBatch{ProtocolVersion: 3, TxID: ids.NewTxID(), OriginNode: origin, Sequence: 1, HLC: 100, SchemaEpoch: 1, Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Text(strings.Repeat("y", 200<<10))}}}
	chunks, err := encodeChunksFixture(batch, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 4 {
		t.Fatalf("fixture needs 4 chunks, got %d", len(chunks))
	}
	if _, _, err := s.StageTransactionChunk(ctx, chunks[3], 1<<20); err != nil {
		t.Fatal(err)
	}

	corrupt, err := codec.DecodeTransactionChunk(chunks[3], 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	corrupt.Data[0] ^= 0xff
	bad, err := codec.EncodeTransactionChunk(nil, corrupt, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.StageTransactionChunk(ctx, bad, 1<<20); err == nil ||
		!strings.Contains(err.Error(), "conflicting duplicate transaction chunk 3") {
		t.Fatalf("corrupted duplicate err = %v", err)
	}

	var assembled *codec.MutationBatch
	for _, raw := range chunks[:3] {
		if assembled, _, err = s.StageTransactionChunk(ctx, raw, 1<<20); err != nil {
			t.Fatal(err)
		}
	}
	if assembled == nil || assembled.MutationDigest != batch.MutationDigest {
		t.Fatal("valid version did not survive the corrupted duplicate")
	}
}
