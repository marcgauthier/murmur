package replicateddb

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/nomadsql/replicateddb/codec"
)

// TestSnapshotLargeChunkedMergeEndToEnd imports a real multi-chunk
// snapshot above the default atomic threshold and proves crash-safe
// publication with a gated SQL rebuild: rows stay invisible until the
// final chunk publishes, then the materializer rebuilds once from the
// committed state with watermarks and generation advanced together.
func TestSnapshotLargeChunkedMergeEndToEnd(t *testing.T) {
	ctx := context.Background()
	dbA, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()

	// Fat values keep the row count (and setup time) small while the
	// encoded snapshot clears the 8 MiB atomic threshold.
	fat := strings.Repeat("v", 64<<10)
	const rows = 200
	for i := 0; i < rows; i++ {
		id := NewRowID()
		if _, err := dbA.ExecContext(ctx, `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
			id[:], fmt.Sprintf("n%04d", i), fat); err != nil {
			t.Fatal(err)
		}
	}

	var manifest *codec.SnapshotManifest
	var chunks [][]codec.SnapshotCell
	if err := dbA.store.ExportSnapshot(100, func(m *codec.SnapshotManifest, chunk []codec.SnapshotCell, last bool) error {
		manifest = m
		chunks = append(chunks, append([]codec.SnapshotCell(nil), chunk...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if manifest == nil || uint64(len(chunks)) != manifest.ChunkCount {
		t.Fatalf("exported %d chunks, manifest says %d", len(chunks), manifest.ChunkCount)
	}
	if manifest.EncodedBytes <= 8<<20 {
		t.Fatalf("encoded bytes = %d, want above the 8 MiB atomic threshold", manifest.EncodedBytes)
	}
	t.Logf("snapshot: %d chunks, %d bytes", manifest.ChunkCount, manifest.EncodedBytes)

	cfgB := testConfig(t.TempDir())
	cfgB.DBID = dbA.DBID()
	dbB, err := Open(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	// Before the final chunk, nothing is published and SQL stays gated
	// on the pre-snapshot (empty) materialization.
	for i := uint64(0); i+1 < manifest.ChunkCount; i++ {
		complete, err := dbB.ApplySnapshotChunk(ctx, manifest, i, chunks[i], false)
		if err != nil {
			t.Fatal(err)
		}
		if complete {
			t.Fatalf("chunk %d reported complete early", i)
		}
		if got := queryAll(t, dbB, `SELECT id FROM contacts`); len(got) != 0 {
			t.Fatalf("chunk %d exposed %d rows before publication", i, len(got))
		}
		if wm, _ := dbB.store.ReceiveWatermark(dbA.cfg.NodeID); wm != 0 {
			t.Fatalf("chunk %d advanced watermark: %d", i, wm)
		}
	}

	// The final chunk merges, publishes, and rebuilds SQL once.
	last := manifest.ChunkCount - 1
	complete, err := dbB.ApplySnapshotChunk(ctx, manifest, last, chunks[last], true)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatal("final chunk did not complete")
	}
	if got := queryAll(t, dbB, `SELECT id FROM contacts`); len(got) != rows {
		t.Fatalf("published rows = %d, want %d", len(got), rows)
	}
	if wm, _ := dbB.store.ReceiveWatermark(dbA.cfg.NodeID); wm != rows {
		t.Fatalf("watermark = %d, want %d", wm, rows)
	}
	gen, err := dbB.store.StateGeneration()
	if err != nil {
		t.Fatal(err)
	}
	mgen := dbB.Status().MaterializedGeneration
	if gen == 0 || mgen != gen {
		t.Fatalf("state gen = %d, materialized gen = %d, want equal nonzero", gen, mgen)
	}
	if st := dbB.Status().State; st != StateReady {
		t.Fatalf("state = %s, want ready", st)
	}
}
