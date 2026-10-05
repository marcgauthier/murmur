package state

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/marcgauthier/murmur/codec"
	spedsqlcrypto "github.com/marcgauthier/murmur/crypto"
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

func TestSnapshotSSTableIngestUsesEncryptedVFS(t *testing.T) {
	ctx := context.Background()
	a := openTestStore(t, ids.NewNodeID())
	seedSource(t, ctx, a)
	manifest, chunks := exportCollect(t, a)
	dir := t.TempDir()
	var cryptoDBID [16]byte
	stateDBID := a.DBID()
	copy(cryptoDBID[:], stateDBID[:])
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	registry, err := spedsqlcrypto.OpenRegistry(dir+"/keys", spedsqlcrypto.Static(key), cryptoDBID)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	encFS, err := spedsqlcrypto.NewEncryptedFS(spedsqlcrypto.FSOptions{Base: vfs.Default, Registry: registry, DBID: cryptoDBID})
	if err != nil {
		t.Fatal(err)
	}
	target, err := openSignedFixture(dir+"/data", ids.NewNodeID(), a.DBID(), Options{FS: encFS, Limits: codec.DefaultLimits(), SnapshotAtomicMergeBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	for i, chunk := range chunks {
		if _, _, err := target.ImportSnapshotChunk(ctx, manifest, uint64(i), chunk, i+1 == len(chunks), 512<<20); err != nil {
			t.Fatal(err)
		}
	}
	if encFS.Stats().BytesEncrypted == 0 {
		t.Fatal("SSTable ingestion produced no encrypted VFS writes")
	}
	if wm, _ := target.ReceiveWatermark(a.NodeID()); wm != 2 {
		t.Fatalf("published watermark=%d, want 2", wm)
	}
}

func TestOpenRemovesInterruptedSnapshotIngestFile(t *testing.T) {
	dir, node, dbID := t.TempDir(), ids.NewNodeID(), ids.NewDBID()
	s, err := openSignedFixture(dir, node, dbID, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	stagingDir := dir + "/" + snapshotIngestDir
	if err := vfs.Default.MkdirAll(stagingDir, 0o700); err != nil {
		t.Fatal(err)
	}
	orphan, err := vfs.Default.Create(stagingDir+"/partial.sst", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orphan.Write([]byte("incomplete")); err != nil {
		t.Fatal(err)
	}
	if err := orphan.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = openSignedFixture(dir, node, dbID, Options{Limits: codec.DefaultLimits()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := vfs.Default.List(stagingDir); !isMissingFile(err) {
		t.Fatalf("interrupted SSTable staging directory remains: %v", err)
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
