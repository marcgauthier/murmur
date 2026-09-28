package state

import (
	"context"
	"testing"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
)

func snapshotFixture(t *testing.T) (*Store, *codec.SnapshotManifest, [][]codec.SnapshotCell) {
	t.Helper()
	source := openTestStore(t, ids.NewNodeID())
	for i := 0; i < 3; i++ {
		row := ids.NewRowID()
		if _, err := source.CommitLocal(context.Background(), localBatch(source, source.ClockNow(), codec.Mutation{
			TableID: 1, RowID: row, ColumnID: 1, Value: codec.Text("snapshot-value"),
		})); err != nil {
			t.Fatal(err)
		}
	}
	var manifest *codec.SnapshotManifest
	var chunks [][]codec.SnapshotCell
	if err := source.ExportSnapshot(1, func(m *codec.SnapshotManifest, cells []codec.SnapshotCell, _ bool) error {
		if manifest == nil {
			copy := *m
			manifest = &copy
		}
		chunks = append(chunks, append([]codec.SnapshotCell(nil), cells...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(chunks) < 2 {
		t.Fatalf("fixture chunks=%d, want multiple chunks", len(chunks))
	}
	return source, manifest, chunks
}

func snapshotReceiver(t *testing.T, source *Store) *Store {
	t.Helper()
	receiver, err := Open(t.TempDir(), ids.NewNodeID(), source.DBID(), Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = receiver.Close() })
	return receiver
}

func TestSnapshotCandidateRejectsInvalidAndOverBudgetManifests(t *testing.T) {
	source, manifest, chunks := snapshotFixture(t)
	receiver := snapshotReceiver(t, source)
	ctx := context.Background()

	tooLarge := *manifest
	if _, _, err := receiver.ImportSnapshotChunk(ctx, &tooLarge, 0, chunks[0], false, manifest.EncodedBytes-1); err == nil {
		t.Fatal("snapshot exceeding its independent byte budget was accepted")
	}
	invalid := *manifest
	invalid.ChunkCount = 0
	if _, _, err := receiver.ImportSnapshotChunk(ctx, &invalid, 0, chunks[0], false, 512<<20); err == nil {
		t.Fatal("manifest with zero chunks was accepted")
	}
	if wm, err := receiver.ReceiveWatermark(source.NodeID()); err != nil || wm != 0 {
		t.Fatalf("rejected manifest advanced watermark to %d (err=%v)", wm, err)
	}
	for _, cell := range chunks[0] {
		if _, ok, err := receiver.GetCell(cell.TableID, cell.RowID, cell.ColumnID); err != nil || ok {
			t.Fatalf("rejected candidate published cell: ok=%v err=%v", ok, err)
		}
	}
}

func TestSnapshotCandidateRejectsManifestChangesAndConflictingChunks(t *testing.T) {
	source, manifest, chunks := snapshotFixture(t)
	ctx := context.Background()

	changedReceiver := snapshotReceiver(t, source)
	if _, complete, err := changedReceiver.ImportSnapshotChunk(ctx, manifest, 0, chunks[0], false, 512<<20); err != nil || complete {
		t.Fatalf("stage first chunk: complete=%v err=%v", complete, err)
	}
	changed := *manifest
	changed.SchemaEpoch++
	if _, _, err := changedReceiver.ImportSnapshotChunk(ctx, &changed, 1, chunks[1], false, 512<<20); err == nil {
		t.Fatal("manifest mutation during transfer was accepted")
	}
	if wm, err := changedReceiver.ReceiveWatermark(source.NodeID()); err != nil || wm != 0 {
		t.Fatalf("changed manifest advanced watermark to %d (err=%v)", wm, err)
	}
	conflictReceiver := snapshotReceiver(t, source)
	if _, complete, err := conflictReceiver.ImportSnapshotChunk(ctx, manifest, 0, chunks[0], false, 512<<20); err != nil || complete {
		t.Fatalf("stage duplicate-test chunk: complete=%v err=%v", complete, err)
	}
	conflict := append([]codec.SnapshotCell(nil), chunks[0]...)
	conflict[0].Value = codec.Text("different-chunk")
	if _, _, err := conflictReceiver.ImportSnapshotChunk(ctx, manifest, 0, conflict, false, 512<<20); err == nil {
		t.Fatal("conflicting duplicate chunk was accepted")
	}
	if wm, err := conflictReceiver.ReceiveWatermark(source.NodeID()); err != nil || wm != 0 {
		t.Fatalf("conflicting chunk advanced watermark to %d (err=%v)", wm, err)
	}
}
