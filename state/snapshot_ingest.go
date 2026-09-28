package state

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/nomadsql/replicateddb/ids"
)

const snapshotIngestDir = "snapshot-ingest"

// ingestSnapshotChunkToSST writes only CRDT-winning cells into an encrypted
// external SSTable and ingests it. The caller serializes this operation with
// the state writer lock and persists merge progress after the ingest; a crash
// between the ingest and progress commit is safe because replay is LWW-idempotent.
func (s *Store) ingestSnapshotChunkToSST(ctx context.Context, raw, prevKey []byte) (lastKey []byte, changed bool, err error) {
	fs := s.openOpt.FS
	if fs == nil {
		fs = vfs.Default
	}
	dir := filepath.Join(s.openPath, snapshotIngestDir)
	if err := fs.MkdirAll(dir, 0o700); err != nil {
		return nil, false, err
	}
	path := filepath.Join(dir, fmt.Sprintf("%s.sst", ids.NewTxID()))
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return nil, false, err
	}
	opts := sstable.WriterOptions{Comparer: pebble.DefaultComparer, TableFormat: s.db.TableFormat()}
	if s.openOpt.Compression != nil {
		opts.Compression = s.openOpt.Compression
	}
	w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), opts)
	lastKey, changed, err = s.mergeSnapshotChunkWithSet(func(key, value []byte) error { return w.Set(key, value) }, raw, prevKey)
	closeErr := w.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = fs.Remove(path)
		return nil, false, err
	}
	if !changed {
		_ = fs.Remove(path)
		return lastKey, false, nil
	}
	if err := s.db.Ingest(ctx, []string{path}); err != nil {
		_ = fs.Remove(path)
		return nil, false, err
	}
	// Pebble may consume or retain the source depending on the FS; remove any
	// leftover external path after the table is installed.
	if err := fs.Remove(path); err != nil && !isMissingFile(err) {
		return nil, false, err
	}
	return lastKey, true, nil
}

func cleanupSnapshotIngestTemps(fs vfs.FS, dbPath string) error {
	if fs == nil {
		fs = vfs.Default
	}
	dir := filepath.Join(dbPath, snapshotIngestDir)
	entries, err := fs.List(dir)
	if err != nil {
		if isMissingFile(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if err := fs.Remove(filepath.Join(dir, entry)); err != nil && !isMissingFile(err) {
			return err
		}
	}
	return fs.Remove(dir)
}

func isMissingFile(err error) bool { return isNotFound(err) || errors.Is(err, os.ErrNotExist) }
