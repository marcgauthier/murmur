package state

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

// openChunkedStore opens a store that forces the chunked snapshot merge
// path for any non-trivial snapshot.
func openChunkedStore(t *testing.T, node ids.NodeID, dbid ids.DBID) *Store {
	t.Helper()
	s, err := openSignedFixture(t.TempDir(), node, dbid, Options{
		Limits:                   codec.DefaultLimits(),
		SnapshotAtomicMergeBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// exportCollect exports a's state one cell per chunk and returns the
// manifest plus chunks in order.
func exportCollect(t *testing.T, a *Store) (*codec.SnapshotManifest, [][]codec.SnapshotCell) {
	t.Helper()
	var manifest *codec.SnapshotManifest
	var chunks [][]codec.SnapshotCell
	if err := a.ExportSnapshot(1, func(m *codec.SnapshotManifest, chunk []codec.SnapshotCell, last bool) error {
		manifest = m
		chunks = append(chunks, append([]codec.SnapshotCell(nil), chunk...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if manifest == nil || uint64(len(chunks)) != manifest.ChunkCount {
		t.Fatalf("exported %d chunks, manifest says %d", len(chunks), manifest.ChunkCount)
	}
	return manifest, chunks
}

// seedSource writes cells plus a tombstone and returns the rows.
func seedSource(t *testing.T, ctx context.Context, a *Store) (row1, row2 ids.RowID) {
	t.Helper()
	row1, row2 = ids.NewRowID(), ids.NewRowID()
	if _, err := a.CommitLocal(ctx, localBatch(a, a.ClockNow(),
		codec.Mutation{TableID: 1, RowID: row1, ColumnID: 1, Value: codec.Text("a")},
		codec.Mutation{TableID: 1, RowID: row1, ColumnID: 2, Value: codec.Int(1)},
		codec.Mutation{TableID: 2, RowID: row2, ColumnID: 1, Value: codec.Blob([]byte{9})},
	)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CommitLocal(ctx, localBatch(a, a.ClockNow(),
		codec.Mutation{TableID: 1, RowID: row2, ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone})); err != nil {
		t.Fatal(err)
	}
	return row1, row2
}

func TestMergeProgressCodec(t *testing.T) {
	p, err := decodeMergeProgress(encodeMergeProgress(7, []byte{1, 2, 3}))
	if err != nil {
		t.Fatal(err)
	}
	if p.nextChunk != 7 || string(p.prevKey) != "\x01\x02\x03" {
		t.Fatalf("round trip = %+v", p)
	}
	p, err = decodeMergeProgress(encodeMergeProgress(0, nil))
	if err != nil || p.nextChunk != 0 || len(p.prevKey) != 0 {
		t.Fatalf("empty round trip = %+v, %v", p, err)
	}
	for _, bad := range [][]byte{nil, {1, 2}, make([]byte, 11), append(encodeMergeProgress(1, []byte{9}), 0)} {
		if _, err := decodeMergeProgress(bad); err == nil {
			t.Fatalf("corrupt progress %x accepted", bad)
		}
	}
}

// TestSnapshotChunkedMergePublishesAfterAllChunks proves large snapshots
// merge chunk by chunk, keep watermarks and cells unpublished until every
// chunk merged, then publish atomically with staging cleaned up.
func TestSnapshotChunkedMergePublishesAfterAllChunks(t *testing.T) {
	ctx := context.Background()
	a := openTestStore(t, ids.NewNodeID())
	row1, _ := seedSource(t, ctx, a)
	b := openChunkedStore(t, ids.NewNodeID(), a.DBID())
	manifest, chunks := exportCollect(t, a)
	if len(chunks) < 2 {
		t.Fatalf("need multiple chunks, got %d", len(chunks))
	}
	genBefore, err := b.StateGeneration()
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i+1 < manifest.ChunkCount; i++ {
		_, complete, err := b.ImportSnapshotChunk(ctx, manifest, i, chunks[i], false, 512<<20)
		if err != nil {
			t.Fatal(err)
		}
		if complete {
			t.Fatalf("chunk %d reported complete early", i)
		}
		if wm, _ := b.ReceiveWatermark(a.NodeID()); wm != 0 {
			t.Fatalf("chunk %d advanced watermark: %d", i, wm)
		}
		if _, ok, _ := b.GetCell(1, row1, 1); ok {
			t.Fatalf("chunk %d made cells visible before completion", i)
		}
	}
	last := manifest.ChunkCount - 1
	res, complete, err := b.ImportSnapshotChunk(ctx, manifest, last, chunks[last], true, 512<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || !res.Applied {
		t.Fatalf("complete=%v applied=%v, want both true", complete, res.Applied)
	}
	if st, ok, _ := b.GetCell(1, row1, 2); !ok || st.Value.I != 1 {
		t.Fatalf("merged cell missing: %+v %v", st, ok)
	}
	if wm, _ := b.ReceiveWatermark(a.NodeID()); wm != 2 {
		t.Fatalf("watermark = %d, want 2", wm)
	}
	if gen, _ := b.StateGeneration(); gen != genBefore+1 {
		t.Fatalf("generation = %d, want %d", gen, genBefore+1)
	}
	// Staging, manifest, active marker, and merge progress are gone.
	stagePrefix := "recv/" + string(manifest.SnapshotID[:]) + "/"
	for _, k := range []string{
		fmt.Sprintf("%schunk/%020d", stagePrefix, 0),
		stagePrefix + "manifest",
		stagePrefix + "merge/progress",
		"recv/active",
	} {
		if _, err := b.getDirect(SnapshotKey(k)); !isNotFound(err) {
			t.Fatalf("staging key %q survives publication (err=%v)", k, err)
		}
	}
}

// TestSnapshotChunkedMergeResumesAfterRestart fails the merge mid-way,
// restarts, and proves the transfer resumes from durable progress with
// watermarks still unpublished until the complete candidate merges.
func TestSnapshotChunkedMergeResumesAfterRestart(t *testing.T) {
	ctx := context.Background()
	a := openTestStore(t, ids.NewNodeID())
	seedSource(t, ctx, a)
	manifest, chunks := exportCollect(t, a)
	if len(chunks) < 3 {
		t.Fatalf("need at least 3 chunks, got %d", len(chunks))
	}
	bPath := t.TempDir()
	bNode := ids.NewNodeID()
	b, err := openSignedFixture(bPath, bNode, a.DBID(), Options{Limits: codec.DefaultLimits(), SnapshotAtomicMergeBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	faultErr := errors.New("injected merge fault")
	b.snapshotMergeFault = func(next uint64) error {
		if next >= 1 {
			return faultErr
		}
		return nil
	}
	last := manifest.ChunkCount - 1
	var complete bool
	for i := uint64(0); i < manifest.ChunkCount; i++ {
		_, complete, err = b.ImportSnapshotChunk(ctx, manifest, i, chunks[i], i == last, 512<<20)
		if err != nil {
			break
		}
	}
	if !errors.Is(err, faultErr) {
		t.Fatalf("import err = %v (complete=%v), want injected fault", err, complete)
	}
	// One chunk merged, but watermarks must not publish early.
	if wm, _ := b.ReceiveWatermark(a.NodeID()); wm != 0 {
		t.Fatalf("partial merge advanced watermark: %d", wm)
	}
	stagePrefix := "recv/" + string(manifest.SnapshotID[:]) + "/"
	progRaw, err := b.getDirect(SnapshotKey(stagePrefix + "merge/progress"))
	if err != nil {
		t.Fatalf("merge progress not durable: %v", err)
	}
	if prog, _ := decodeMergeProgress(progRaw); prog.nextChunk != 1 {
		t.Fatalf("progress = %+v, want nextChunk 1", prog)
	}
	// Crash and restart without the fault: the final chunk resumes the
	// transfer from durable progress and publishes.
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b, err = openSignedFixture(bPath, bNode, a.DBID(), Options{Limits: codec.DefaultLimits(), SnapshotAtomicMergeBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if wm, _ := b.ReceiveWatermark(a.NodeID()); wm != 0 {
		t.Fatalf("restart exposed watermark %d before resume", wm)
	}
	res, complete, err := b.ImportSnapshotChunk(ctx, manifest, last, chunks[last], true, 512<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || !res.Applied {
		t.Fatalf("resumed complete=%v applied=%v, want both true", complete, res.Applied)
	}
	if wm, _ := b.ReceiveWatermark(a.NodeID()); wm != 2 {
		t.Fatalf("watermark = %d, want 2", wm)
	}
	if _, err := b.getDirect(SnapshotKey(stagePrefix + "merge/progress")); !isNotFound(err) {
		t.Fatalf("merge progress survives publication (err=%v)", err)
	}
}

// TestSnapshotChunkedMergeCrashBeforePublish fails after every chunk
// merged but before the publication batch, proving watermarks publish
// only after the complete candidate validates and merges.
func TestSnapshotChunkedMergeCrashBeforePublish(t *testing.T) {
	ctx := context.Background()
	a := openTestStore(t, ids.NewNodeID())
	seedSource(t, ctx, a)
	manifest, chunks := exportCollect(t, a)
	bPath := t.TempDir()
	bNode := ids.NewNodeID()
	b, err := openSignedFixture(bPath, bNode, a.DBID(), Options{Limits: codec.DefaultLimits(), SnapshotAtomicMergeBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	faultErr := errors.New("injected pre-publish fault")
	b.snapshotMergeFault = func(next uint64) error {
		if next >= manifest.ChunkCount {
			return faultErr
		}
		return nil
	}
	last := manifest.ChunkCount - 1
	var complete bool
	for i := uint64(0); i < manifest.ChunkCount; i++ {
		_, complete, err = b.ImportSnapshotChunk(ctx, manifest, i, chunks[i], i == last, 512<<20)
		if err != nil {
			break
		}
	}
	if !errors.Is(err, faultErr) {
		t.Fatalf("import err = %v (complete=%v), want pre-publish fault", err, complete)
	}
	if wm, _ := b.ReceiveWatermark(a.NodeID()); wm != 0 {
		t.Fatalf("all-merged snapshot published watermark %d before publication", wm)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b, err = openSignedFixture(bPath, bNode, a.DBID(), Options{Limits: codec.DefaultLimits(), SnapshotAtomicMergeBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	res, complete, err := b.ImportSnapshotChunk(ctx, manifest, last, chunks[last], true, 512<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || !res.Applied {
		t.Fatalf("resumed complete=%v applied=%v, want both true", complete, res.Applied)
	}
	if wm, _ := b.ReceiveWatermark(a.NodeID()); wm != 2 {
		t.Fatalf("watermark = %d, want 2", wm)
	}
}

func TestSnapshotSSTableIngestCrashBeforeProgressResumes(t *testing.T) {
	ctx := context.Background()
	a := openTestStore(t, ids.NewNodeID())
	seedSource(t, ctx, a)
	manifest, chunks := exportCollect(t, a)
	bPath, bNode := t.TempDir(), ids.NewNodeID()
	open := func() *Store {
		s, err := openSignedFixture(bPath, bNode, a.DBID(), Options{Limits: codec.DefaultLimits(), SnapshotAtomicMergeBytes: 1})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	b := open()
	fault := errors.New("interrupted after SSTable ingest")
	faultOnce := true
	b.snapshotIngestFault = func() error {
		if faultOnce {
			faultOnce = false
			return fault
		}
		return nil
	}
	var err error
	for i := uint64(0); i < manifest.ChunkCount; i++ {
		_, _, err = b.ImportSnapshotChunk(ctx, manifest, i, chunks[i], i+1 == manifest.ChunkCount, 512<<20)
		if err != nil {
			break
		}
	}
	if !errors.Is(err, fault) {
		t.Fatalf("import error = %v, want injected post-ingest interruption", err)
	}
	if wm, _ := b.ReceiveWatermark(a.NodeID()); wm != 0 {
		t.Fatalf("interrupted ingest advanced watermark to %d", wm)
	}
	progress, err := b.readMergeProgress(SnapshotKey("recv/" + string(manifest.SnapshotID[:]) + "/merge/progress"))
	if err != nil || progress.nextChunk != 0 {
		t.Fatalf("progress after interrupted ingest = %+v, err=%v; want replay of chunk 0", progress, err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b = open()
	t.Cleanup(func() { _ = b.Close() })
	res, complete, err := b.ImportSnapshotChunk(ctx, manifest, manifest.ChunkCount-1, chunks[manifest.ChunkCount-1], true, 512<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !complete || !res.Applied {
		t.Fatalf("resumed complete=%v applied=%v", complete, res.Applied)
	}
	if wm, _ := b.ReceiveWatermark(a.NodeID()); wm != 2 {
		t.Fatalf("published watermark=%d, want 2", wm)
	}
}

// TestSnapshotChunkedMergeUnderEncryption proves chunked snapshot imports
// persist only ciphertext: a distinctive marker value merged through the
// chunk path must not appear in any segment file.
func TestSnapshotChunkedMergeUnderEncryption(t *testing.T) {
	ctx := context.Background()
	a := openTestStore(t, ids.NewNodeID())
	seedSource(t, ctx, a)
	const marker = "marker-7f3a9c12-plaintext-probe-0042"
	if _, err := a.CommitLocal(ctx, localBatch(a, a.ClockNow(),
		codec.Mutation{TableID: 9, RowID: ids.NewRowID(), ColumnID: 1, Value: codec.Text(marker)})); err != nil {
		t.Fatal(err)
	}
	manifest, chunks := exportCollect(t, a)
	dir := t.TempDir()
	target, err := openSignedFixture(dir+"/data", ids.NewNodeID(), a.DBID(), Options{Limits: codec.DefaultLimits(), SnapshotAtomicMergeBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i, chunk := range chunks {
		if _, _, err := target.ImportSnapshotChunk(ctx, manifest, uint64(i), chunk, i+1 == len(chunks), 512<<20); err != nil {
			t.Fatal(err)
		}
	}
	if wm, _ := target.ReceiveWatermark(a.NodeID()); wm != 3 {
		t.Fatalf("published watermark=%d, want 3", wm)
	}
	if err := target.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir + "/data/segments")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("no segment files to audit")
	}
	for _, e := range entries {
		raw, err := os.ReadFile(filepath.Join(dir+"/data/segments", e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(raw, []byte(marker)) {
			t.Fatalf("segment %s contains plaintext marker", e.Name())
		}
	}
}

// TestSnapshotChunkedMergeSupersededNeverPublishes proves a preempted
// partial merge cannot publish watermarks.
func TestSnapshotChunkedMergeSupersededNeverPublishes(t *testing.T) {
	ctx := context.Background()
	a := openTestStore(t, ids.NewNodeID())
	seedSource(t, ctx, a)
	manifestA, chunksA := exportCollect(t, a)
	b := openChunkedStore(t, ids.NewNodeID(), a.DBID())
	faultErr := errors.New("injected merge fault")
	b.snapshotMergeFault = func(next uint64) error {
		if next >= 1 {
			return faultErr
		}
		return nil
	}
	lastA := manifestA.ChunkCount - 1
	var err error
	for i := uint64(0); i < manifestA.ChunkCount; i++ {
		_, _, err = b.ImportSnapshotChunk(ctx, manifestA, i, chunksA[i], i == lastA, 512<<20)
		if err != nil {
			break
		}
	}
	if !errors.Is(err, faultErr) {
		t.Fatalf("import A err = %v, want injected fault", err)
	}
	b.snapshotMergeFault = nil
	// A newer transfer preempts A and clears its staging.
	manifestB, chunksB := exportCollect(t, a)
	if manifestB.SnapshotID == manifestA.SnapshotID {
		t.Fatal("exports share a snapshot id")
	}
	if _, _, err := b.ImportSnapshotChunk(ctx, manifestB, 0, chunksB[0], false, 512<<20); err != nil {
		t.Fatal(err)
	}
	// Resuming A's merge directly must refuse: its namespace is gone.
	stageA := "recv/" + string(manifestA.SnapshotID[:]) + "/"
	_, _, err = b.mergeSnapshotChunked(ctx, manifestA, stageA,
		SnapshotKey(stageA+"manifest"), SnapshotKey("recv/active"), SnapshotKey(stageA+"merge/progress"))
	if err == nil || err.Error() != "state: snapshot transfer superseded" {
		t.Fatalf("resumed preempted merge err = %v, want superseded", err)
	}
	if wm, _ := b.ReceiveWatermark(a.NodeID()); wm != 0 {
		t.Fatalf("preempted merge published watermark %d", wm)
	}
}

// TestSnapshotIngestCrashReplayWithConcurrentSourceWritesAndGCTailCatchup proves:
// 1. Ingestion of chunk N into SSTable succeeds but progress persistence fails (crash).
// 2. Restart and re-ingestion of chunk N resumes cleanly.
// 3. Source commits concurrent writes and runs aggressive GC with a tail retention lease.
// 4. Receiver publishes snapshot at exact snapshot watermark (without premature tail or missing sequence).
// 5. Tail catch-up replicates remaining log batches and achieves exact sequence/watermark/data convergence.
func TestSnapshotIngestCrashReplayWithConcurrentSourceWritesAndGCTailCatchup(t *testing.T) {
	ctx := context.Background()
	aNode, bNode := ids.NewNodeID(), ids.NewNodeID()
	dbID := ids.NewDBID()

	a, err := openSignedFixture(t.TempDir(), aNode, dbID, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()

	// Seed source with 5 separate transaction commits (sequences 1..5).
	rowsA := make([]ids.RowID, 5)
	for i := 0; i < 5; i++ {
		rowsA[i] = ids.NewRowID()
		if _, err := a.CommitLocal(ctx, localBatch(a, a.ClockNow(),
			codec.Mutation{TableID: 1, RowID: rowsA[i], ColumnID: 1, Value: codec.Int(int64(i + 1))},
			codec.Mutation{TableID: 2, RowID: rowsA[i], ColumnID: 1, Value: codec.Text(fmt.Sprintf("snap-%d", i+1))},
		)); err != nil {
			t.Fatal(err)
		}
	}

	manifest, chunks := exportCollect(t, a)
	if len(chunks) < 3 {
		t.Fatalf("expected at least 3 chunks, got %d", len(chunks))
	}

	bPath := t.TempDir()
	openB := func() *Store {
		s, err := openSignedFixture(bPath, bNode, dbID, Options{
			Limits:                   codec.DefaultLimits(),
			SnapshotAtomicMergeBytes: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}

	b := openB()

	// Inject fault on chunk index 1: SSTable file written, but progress persistence aborted.
	faultErr := errors.New("injected crash after SST ingest before progress commit")
	var ingestedCount int
	b.snapshotIngestFault = func() error {
		ingestedCount++
		if ingestedCount == 2 { // chunk index 1
			return faultErr
		}
		return nil
	}

	for i := uint64(0); i < manifest.ChunkCount; i++ {
		_, _, err = b.ImportSnapshotChunk(ctx, manifest, i, chunks[i], i+1 == manifest.ChunkCount, 512<<20)
		if err != nil {
			break
		}
	}
	if !errors.Is(err, faultErr) {
		t.Fatalf("expected faultErr, got %v", err)
	}

	// Verify no watermarks or generations are exposed before publication.
	if wm, _ := b.ReceiveWatermark(a.NodeID()); wm != 0 {
		t.Fatalf("watermark prematurely exposed during interrupted ingest: %d", wm)
	}
	if gen, _ := b.StateGeneration(); gen != 0 {
		t.Fatalf("generation prematurely exposed during interrupted ingest: %d", gen)
	}

	// Verify progress recorded only chunk 0 (nextChunk = 1), so chunk 1 must be re-ingested on replay.
	progRaw, err := b.getDirect(SnapshotKey("recv/" + string(manifest.SnapshotID[:]) + "/merge/progress"))
	if err != nil {
		t.Fatalf("failed reading merge progress before crash: %v", err)
	}
	if prog, err := decodeMergeProgress(progRaw); err != nil || prog.nextChunk != 1 {
		t.Fatalf("expected nextChunk=1 before crash, got %+v (err=%v)", prog, err)
	}

	// Crash and restart receiver B.
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b = openB()
	defer b.Close()

	// Verify restart did not publish anything.
	if wm, _ := b.ReceiveWatermark(a.NodeID()); wm != 0 {
		t.Fatalf("watermark exposed after restart: %d", wm)
	}
	if gen, _ := b.StateGeneration(); gen != 0 {
		t.Fatalf("generation exposed after restart: %d", gen)
	}

	// Concurrently on source A:
	// Acquire retention lease holding watermark 5.
	releaseLease, err := a.AcquireRetentionLease(b.NodeID().String(), time.Now().Add(time.Hour), map[ids.NodeID]uint64{a.NodeID(): 5})
	if err != nil {
		t.Fatalf("AcquireRetentionLease failed: %v", err)
	}

	// Commit 5 new transactions on source A (sequences 6..10).
	rowsTail := make([]ids.RowID, 5)
	for i := 0; i < 5; i++ {
		rowsTail[i] = ids.NewRowID()
		if _, err := a.CommitLocal(ctx, localBatch(a, a.ClockNow(),
			codec.Mutation{TableID: 1, RowID: rowsTail[i], ColumnID: 1, Value: codec.Int(int64(i + 6))},
			codec.Mutation{TableID: 2, RowID: rowsTail[i], ColumnID: 1, Value: codec.Text(fmt.Sprintf("tail-%d", i+6))},
		)); err != nil {
			t.Fatal(err)
		}
	}

	// Run aggressive log GC on source A.
	// Retention lease at watermark 5 must protect sequences 6..10 while sequences 1..5 are pruned.
	pruned, err := a.CollectLog(a.NodeID(), ^uint64(0), 1<<62, 0)
	if err != nil {
		t.Fatalf("CollectLog on source failed: %v", err)
	}
	if pruned != 5 {
		t.Fatalf("CollectLog pruned %d entries, want 5 (pruned 1..5)", pruned)
	}

	// Verify pruned entries 1..5 cannot be scanned from log on source.
	var oldEntriesScanned int
	_, _ = a.LogScan(a.NodeID(), 1, 5, 1<<20, func(batch *codec.MutationBatch) error {
		oldEntriesScanned++
		return nil
	})
	if oldEntriesScanned != 0 {
		t.Fatalf("expected 0 log entries scanned in 1..5 after GC, got %d", oldEntriesScanned)
	}

	// Receiver B re-ingests and completes the snapshot transfer.
	last := manifest.ChunkCount - 1
	resSnap, complete, err := b.ImportSnapshotChunk(ctx, manifest, last, chunks[last], true, 512<<20)
	if err != nil {
		t.Fatalf("failed resuming snapshot on B: %v", err)
	}
	if !complete || !resSnap.Applied {
		t.Fatalf("snapshot transfer failed to complete on replay: complete=%v, applied=%v", complete, resSnap.Applied)
	}

	// Verify exact watermark and generation semantics immediately upon snapshot publication.
	// 1. Watermark must match snapshot cut (5), NOT 0 and NOT 10.
	wmSnap, err := b.ReceiveWatermark(a.NodeID())
	if err != nil {
		t.Fatal(err)
	}
	if wmSnap != 5 {
		t.Fatalf("snapshot watermark immediately after publication = %d, want exact snapshot cut 5", wmSnap)
	}

	// 2. Snapshot data (rowsA) must be visible on B.
	for i := 0; i < 5; i++ {
		st1, ok1, err1 := b.GetCell(1, rowsA[i], 1)
		if err1 != nil || !ok1 || st1.Value.I != int64(i+1) {
			t.Fatalf("rowA[%d] table 1 missing or mismatch: ok=%v, val=%+v, err=%v", i, ok1, st1, err1)
		}
		st2, ok2, err2 := b.GetCell(2, rowsA[i], 1)
		if err2 != nil || !ok2 || st2.Value.S != fmt.Sprintf("snap-%d", i+1) {
			t.Fatalf("rowA[%d] table 2 missing or mismatch: ok=%v, val=%+v, err=%v", i, ok2, st2, err2)
		}
	}

	// 3. Tail data (rowsTail) must NOT yet be visible on B.
	for i := 0; i < 5; i++ {
		if _, ok, _ := b.GetCell(1, rowsTail[i], 1); ok {
			t.Fatalf("tail row %d unexpectedly present before tail catchup", i)
		}
	}

	// Source tail catch-up: scan sequences 6..10 from source A and apply remotely to B.
	var tailBatches []*codec.MutationBatch
	lastSeq, err := a.LogScan(a.NodeID(), 6, 10, 1<<20, func(batch *codec.MutationBatch) error {
		tailBatches = append(tailBatches, batch)
		return nil
	})
	if err != nil {
		t.Fatalf("LogScan on source tail failed: %v", err)
	}
	if len(tailBatches) != 5 || lastSeq != 10 {
		t.Fatalf("expected 5 tail batches up to seq 10, got %d batches, lastSeq=%d", len(tailBatches), lastSeq)
	}

	for i, batch := range tailBatches {
		res, err := commitRemoteFixture(b, ctx, batch)
		if err != nil {
			t.Fatalf("failed committing tail batch %d (seq %d) to B: %v", i, batch.Sequence, err)
		}
		if !res.Applied {
			t.Fatalf("tail batch %d (seq %d) was not applied", i, batch.Sequence)
		}
		expectedWM := uint64(6 + i)
		currentWM, _ := b.ReceiveWatermark(a.NodeID())
		if currentWM != expectedWM {
			t.Fatalf("after tail batch %d, watermark = %d, want %d", i, currentWM, expectedWM)
		}
	}

	// Release the retention lease on source A.
	releaseLease()

	// Aggressive GC on source A can now prune tail sequences 6..10.
	prunedTail, err := a.CollectLog(a.NodeID(), ^uint64(0), 1<<62, 0)
	if err != nil {
		t.Fatalf("CollectLog tail failed: %v", err)
	}
	if prunedTail != 5 {
		t.Fatalf("CollectLog tail pruned %d entries, want 5", prunedTail)
	}

	// Final assertions: all rows match exactly.
	finalWM, _ := b.ReceiveWatermark(a.NodeID())
	if finalWM != 10 {
		t.Fatalf("final receiver watermark = %d, want 10", finalWM)
	}

	for i := 0; i < 5; i++ {
		st1, ok1, err1 := b.GetCell(1, rowsTail[i], 1)
		if err1 != nil || !ok1 || st1.Value.I != int64(i+6) {
			t.Fatalf("rowsTail[%d] table 1 mismatch on B: ok=%v, val=%+v, err=%v", i, ok1, st1, err1)
		}
		st2, ok2, err2 := b.GetCell(2, rowsTail[i], 1)
		if err2 != nil || !ok2 || st2.Value.S != fmt.Sprintf("tail-%d", i+6) {
			t.Fatalf("rowsTail[%d] table 2 mismatch on B: ok=%v, val=%+v, err=%v", i, ok2, st2, err2)
		}
	}
}
