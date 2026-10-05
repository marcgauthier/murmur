// Replicated file metadata with node-local content-addressed objects.
//
// File metadata (name, digest, size) replicates through the normal durable
// log as cells of a reserved table that lives outside the application
// schema registry, so enabling files never changes the replicated schema
// identity and mixed clusters stay compatible. Object bytes are stored
// node-locally by the objectstore package (encrypted, chunked, streaming)
// and are published before the metadata commit is acknowledged, so any
// peer that learns the metadata can fetch bytes from the uploader.
//
// Deletes are row tombstones: a re-upload (newer cell versions) resurrects
// the file, and concurrent upload/delete pairs resolve deterministically
// under the standard last-writer-wins visibility rule.
package murmur

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/filefetch"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/objectstore"
	"github.com/marcgauthier/murmur/replication"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/state"
)

// fileTableName is the reserved metadata table. Application schemas must
// not use this name: Open rejects the collision.
const fileTableName = "__replicatedb_files"

// File metadata columns. The primary key column ("id", BLOB) holds the
// row ID bytes; the row ID derives deterministically from the file name so
// every node addresses the same row without coordination.
const (
	fileColID     = "id"
	fileColName   = "name"
	fileColDigest = "digest"
	fileColSize   = "size"
)

// fileIDs are the resolved numeric IDs of the reserved metadata table.
type fileIDs struct {
	table  uint32
	id     uint32
	name   uint32
	digest uint32
	size   uint32
}

var (
	fileIDsOnce sync.Once
	fileIDsVal  fileIDs
	fileIDsErr  error
)

// resolveFileIDs derives the reserved table/column IDs deterministically
// from their names. Derivation is per-name, so the result is identical on
// every node regardless of the application schema.
func resolveFileIDs() (fileIDs, error) {
	fileIDsOnce.Do(func() {
		reg, err := schema.BuildRegistry(0, []schema.TableSchema{{
			Name: fileTableName,
			Columns: []schema.ColumnSchema{
				{Name: fileColID, Type: schema.ColBlob},
				{Name: fileColName, Type: schema.ColText},
				{Name: fileColDigest, Type: schema.ColBlob},
				{Name: fileColSize, Type: schema.ColInteger},
			},
		}})
		if err != nil {
			fileIDsErr = err
			return
		}
		t := reg.Table(fileTableName)
		col := func(name string) uint32 {
			for _, c := range t.Columns {
				if c.Name == name {
					return c.ID
				}
			}
			return 0
		}
		fileIDsVal = fileIDs{
			table:  t.ID,
			id:     col(fileColID),
			name:   col(fileColName),
			digest: col(fileColDigest),
			size:   col(fileColSize),
		}
	})
	return fileIDsVal, fileIDsErr
}

// fileRowID derives the metadata row ID from the file name.
func fileRowID(name string) ids.RowID {
	sum := sha256.Sum256([]byte("replicateddb-file/1:" + name))
	var row ids.RowID
	copy(row[:], sum[:])
	return row
}

// fileStore is the per-DB file subsystem. Nil unless Files.Enabled.
type fileStore struct {
	db      *DB
	objects *objectstore.Store
	ids     fileIDs
	fetch   *fetchState
}

// openFileStore creates the subsystem. It rejects reserved-table ID
// collisions with the application schema (same-name use or an FNV-1a
// derivation collision) so file cells can never alias application rows.
func openFileStore(db *DB) (*fileStore, error) {
	fids, err := resolveFileIDs()
	if err != nil {
		return nil, fmt.Errorf("murmur: file metadata schema: %w", err)
	}
	if t := db.reg.TableByID(fids.table); t != nil {
		return nil, fmt.Errorf("murmur: application table %q collides with the reserved file metadata table %q",
			t.Name, fileTableName)
	}
	filesRoot := filepath.Join(db.cfg.Path, "files")
	var objects *objectstore.Store
	if len(db.cfg.Files.PrevObjectKey) > 0 {
		objects, err = objectstore.OpenWithPrevious(filesRoot, db.cfg.Files.ObjectKey, db.cfg.Files.PrevObjectKey)
	} else {
		objects, err = objectstore.New(filesRoot, db.cfg.Files.ObjectKey)
	}
	if err != nil {
		return nil, fmt.Errorf("murmur: file object store: %w", err)
	}
	fs := &fileStore{db: db, objects: objects, ids: fids}
	if err := fs.openFetch(); err != nil {
		_ = objects.Close()
		return nil, err
	}
	return fs, nil
}

// openFetch prepares fetch staging, the fetch client, and the serving
// endpoint (when FetchAddr is set).
func (fs *fileStore) openFetch() error {
	db := fs.db
	staging := filepath.Join(db.cfg.Path, "files", "staging")
	if err := os.MkdirAll(staging, 0700); err != nil {
		return fmt.Errorf("murmur: file staging: %w", err)
	}
	fetch := &fetchState{
		staging: staging,
		sem:     make(chan struct{}, db.cfg.Files.MaxConcurrentFetches),
		trigger: make(chan struct{}, 1),
		active:  make(map[objectstore.Digest]int),
	}
	if db.cfg.Files.FetchAddr != "" || len(db.cfg.Files.FetchPeers) > 0 {
		creds, err := db.meshCreds()
		if err != nil {
			return fmt.Errorf("murmur: file fetch: %w", err)
		}
		fetch.client = filefetch.NewClient(creds)
	}
	if db.cfg.Files.FetchAddr != "" {
		creds, err := db.meshCreds()
		if err != nil {
			return fmt.Errorf("murmur: file fetch: %w", err)
		}
		server, err := filefetch.NewServer(filefetch.ServerConfig{
			Objects:        fs.objects,
			Creds:          creds,
			DBID:           db.store.DBID(),
			Addr:           db.cfg.Files.FetchAddr,
			MaxConns:       db.cfg.Files.MaxFetchConns,
			RequestTimeout: db.cfg.Files.FetchTimeout,
		})
		if err != nil {
			return err
		}
		fetch.server = server
		// Advertise the bound (possibly ephemeral) fetch endpoint via SWIM
		// so peers discover this source dynamically. Without a membership
		// service only static FetchPeers apply, as before.
		if repl := db.replManager(); repl != nil {
			if ms := repl.Membership(); ms != nil {
				ms.SetFetchEndpoint(server.Addr())
			}
		}
	}
	fs.fetch = fetch
	return nil
}

func (fs *fileStore) close() error {
	if fs == nil {
		return nil
	}
	var first error
	if fs.fetch != nil && fs.fetch.server != nil {
		if err := fs.fetch.server.Close(); err != nil && first == nil {
			first = err
		}
		fs.fetch.server = nil
	}
	if fs.objects != nil {
		if err := fs.objects.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// filesForWrite gates mutating file operations.
func (db *DB) filesForWrite() (*fileStore, error) {
	if db.files == nil {
		return nil, ErrFilesDisabled
	}
	if err := db.requireWrite(); err != nil {
		return nil, err
	}
	return db.files, nil
}

// filesForRead gates file reads. Unlike SQL reads (served from the
// in-memory materialization), file metadata reads hit the durable store
// directly, so they additionally require a usable store.
func (db *DB) filesForRead() (*fileStore, error) {
	if db.files == nil {
		return nil, ErrFilesDisabled
	}
	if err := db.requireRead(); err != nil {
		return nil, err
	}
	db.mu.Lock()
	usable := db.storeUsable
	db.mu.Unlock()
	if !usable {
		return nil, ErrNotReady
	}
	return db.files, nil
}

// FileInfo describes an uploaded file.
type FileInfo struct {
	Name   string
	Digest objectstore.Digest
	Size   int64
	Chunks uint64
}

// chunkCount reports the object chunk count for a plaintext length.
func chunkCount(size int64) uint64 {
	if size <= 0 {
		return 0
	}
	return uint64((size + int64(objectstore.ChunkSize) - 1) / int64(objectstore.ChunkSize))
}

// UploadFile streams src into the node-local object store and replicates
// the file metadata. The object is published before the metadata commit,
// so the commit acknowledgement implies local byte availability.
//
// Uploading an existing name replaces its metadata; uploading a deleted
// name resurrects it. Concurrent uploads of one name converge on one
// winner; every copy's bytes remain content-addressed locally.
func (db *DB) UploadFile(ctx context.Context, name string, src io.Reader) (FileInfo, error) {
	fs, err := db.filesForWrite()
	if err != nil {
		return FileInfo{}, err
	}
	if name == "" {
		return FileInfo{}, fmt.Errorf("murmur: file name is required")
	}
	if len(name) > db.cfg.MaxReplicatedValueBytes {
		return FileInfo{}, fmt.Errorf("%w: file name %d bytes", ErrValueTooLarge, len(name))
	}
	maxBytes := db.cfg.Files.MaxFileBytes
	if maxBytes < 0 {
		return FileInfo{}, fmt.Errorf("%w: uploads disabled", ErrFileTooLarge)
	}
	if src == nil {
		return FileInfo{}, fmt.Errorf("murmur: file content reader is required")
	}
	// Cap the stream so an unbounded reader cannot fill the disk before
	// the size check runs. A rejected oversize object is left
	// unreferenced; FilesGC reclaims it.
	info, err := fs.objects.Put(ctx, io.LimitReader(src, maxBytes+1))
	if err != nil {
		return FileInfo{}, fmt.Errorf("murmur: upload %q: %w", name, err)
	}
	if info.Length > maxBytes {
		return FileInfo{}, fmt.Errorf("%w: %q exceeds %d bytes", ErrFileTooLarge, name, maxBytes)
	}
	row := fileRowID(name)
	mutations := []codec.Mutation{
		{TableID: fs.ids.table, RowID: row, ColumnID: fs.ids.id, Value: codec.Blob(row[:])},
		{TableID: fs.ids.table, RowID: row, ColumnID: fs.ids.name, Value: codec.Text(name)},
		{TableID: fs.ids.table, RowID: row, ColumnID: fs.ids.digest, Value: codec.Blob(info.Digest[:])},
		{TableID: fs.ids.table, RowID: row, ColumnID: fs.ids.size, Value: codec.Int(info.Length)},
	}
	if err := fs.commit(mutations); err != nil {
		return FileInfo{}, fmt.Errorf("murmur: upload %q: %w", name, err)
	}
	return FileInfo{Name: name, Digest: info.Digest, Size: info.Length, Chunks: chunkCount(info.Length)}, nil
}

// DeleteFile replicates a row tombstone for the named file. The delete is
// idempotent: deleting an unknown name still commits (converging every
// replica on "deleted"). A later upload resurrects the name. Local object
// bytes are dereferenced, not removed; FilesGC reclaims them.
func (db *DB) DeleteFile(ctx context.Context, name string) error {
	fs, err := db.filesForWrite()
	if err != nil {
		return err
	}
	if name == "" {
		return fmt.Errorf("murmur: file name is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	mutations := []codec.Mutation{
		{TableID: fs.ids.table, RowID: fileRowID(name), ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone},
	}
	if err := fs.commit(mutations); err != nil {
		return fmt.Errorf("murmur: delete %q: %w", name, err)
	}
	return nil
}

// commit durably records one file-metadata transaction and notifies
// replication. It mirrors the SQL commit tail: serialized under applyMu,
// in-memory materialized generation advanced (file tables have no SQL rows, so no
// repair pass applies), then NotifyLocal.
func (fs *fileStore) commit(mutations []codec.Mutation) error {
	db := fs.db
	schemaID := db.schemaIdentity()
	batch := &codec.MutationBatch{
		ProtocolVersion: replication.ProtocolVersion,
		TxID:            ids.NewTxID(),
		OriginNode:      db.cfg.NodeID,
		HLC:             db.store.ClockNow(),
		SchemaEpoch:     schemaID.Epoch,
		SchemaHash:      schemaID.Hash,
		Mutations:       mutations,
	}
	// Local file writes record High ownership exactly like local SQL
	// writes, so later Low imports protect High-uploaded files.
	localPolicy, err := policyMutationsForTx(db, &Tx{txID: batch.TxID}, mutations)
	if err != nil {
		return fmt.Errorf("murmur: bridge policy: %w", err)
	}
	batch.Mutations = append(batch.Mutations, localPolicy...)
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	if _, err := db.store.CommitLocal(context.Background(), batch); err != nil {
		if errors.Is(err, state.ErrTooBig) {
			return fmt.Errorf("%w: %w", ErrBatchTooLarge, err)
		}
		return err
	}
	gen, err := db.store.StateGeneration()
	if err != nil {
		return err
	}
	if len(db.remoteRows) == 0 {
		db.materializedGeneration.Store(gen)
	}
	if repl := db.replManager(); repl != nil {
		repl.NotifyLocal()
	}
	return nil
}

// FileStatus describes the replicated metadata and local availability of
// one file.
type FileStatus struct {
	Name   string
	Digest objectstore.Digest
	Size   int64
	Chunks uint64
	// Exists reports whether any metadata cells are stored. Deleted
	// reports a tombstone newer than the newest cell.
	Exists  bool
	Deleted bool
	// Available reports whether the content-addressed object is present
	// locally. Known-but-absent metadata (Exists, !Available) means the
	// bytes were never fetched to this node.
	Available bool
}

// FileStatus returns the metadata and local availability of one file. It
// returns ErrFileUnavailable only for open/read gating; unknown names
// report Exists=false with a nil error.
func (db *DB) FileStatus(ctx context.Context, name string) (FileStatus, error) {
	fs, err := db.filesForRead()
	if err != nil {
		return FileStatus{}, err
	}
	if name == "" {
		return FileStatus{}, fmt.Errorf("murmur: file name is required")
	}
	if err := ctx.Err(); err != nil {
		return FileStatus{}, err
	}
	return fs.status(fileRowID(name))
}

func (fs *fileStore) status(row ids.RowID) (FileStatus, error) {
	cells, err := fs.db.store.GetRow(fs.ids.table, row)
	if err != nil {
		return FileStatus{}, err
	}
	st := FileStatus{}
	if len(cells) == 0 {
		return st, nil
	}
	tomb, hasTomb, err := fs.db.store.GetTombstone(fs.ids.table, row)
	if err != nil {
		return FileStatus{}, err
	}
	eff, visible, err := effectiveFileVisible(fs.db, cells, tomb, hasTomb)
	if err != nil {
		return FileStatus{}, err
	}
	name, digest, size, err := fs.decode(eff)
	if err != nil {
		return FileStatus{}, err
	}
	st.Name, st.Digest, st.Size = name, digest, size
	st.Chunks = chunkCount(size)
	st.Exists = true
	st.Deleted = !visible
	st.Available = fs.objects.Has(digest)
	return st, nil
}

// decode extracts typed metadata from stored cells.
func (fs *fileStore) decode(cells map[uint32]codec.CellState) (string, objectstore.Digest, int64, error) {
	var digest objectstore.Digest
	nameCell, ok := cells[fs.ids.name]
	if !ok || nameCell.Value.Type != codec.TypeText {
		return "", digest, 0, fmt.Errorf("murmur: file metadata missing name")
	}
	digestCell, ok := cells[fs.ids.digest]
	if !ok || digestCell.Value.Type != codec.TypeBlob || len(digestCell.Value.B) != len(digest) {
		return "", digest, 0, fmt.Errorf("murmur: file metadata missing digest")
	}
	sizeCell, ok := cells[fs.ids.size]
	if !ok || sizeCell.Value.Type != codec.TypeInteger || sizeCell.Value.I < 0 {
		return "", digest, 0, fmt.Errorf("murmur: file metadata missing size")
	}
	copy(digest[:], digestCell.Value.B)
	return nameCell.Value.S, digest, sizeCell.Value.I, nil
}

// ListFiles returns visible (non-deleted) files whose names carry prefix,
// ordered by name. Limit <= 0 means no limit.
func (db *DB) ListFiles(ctx context.Context, prefix string, limit int) ([]FileStatus, error) {
	return db.scanFiles(ctx, prefix, limit, func(name, arg string) bool {
		return strings.HasPrefix(name, arg)
	})
}

// SearchFiles returns visible (non-deleted) files whose names contain
// substr, ordered by name. Limit <= 0 means no limit.
func (db *DB) SearchFiles(ctx context.Context, substr string, limit int) ([]FileStatus, error) {
	return db.scanFiles(ctx, substr, limit, strings.Contains)
}

func (db *DB) scanFiles(ctx context.Context, arg string, limit int, match func(name, arg string) bool) ([]FileStatus, error) {
	fs, err := db.filesForRead()
	if err != nil {
		return nil, err
	}
	var out []FileStatus
	err = db.store.IterateTable(fs.ids.table, func(r *state.Row) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		eff, visible, err := effectiveFileVisible(db, r.Cells, r.Tomb.Version, r.Tomb.Present)
		if err != nil {
			return err
		}
		if !visible {
			return nil
		}
		name, digest, size, err := fs.decode(eff)
		if err != nil {
			return err
		}
		if !match(name, arg) {
			return nil
		}
		out = append(out, FileStatus{
			Name:      name,
			Digest:    digest,
			Size:      size,
			Chunks:    chunkCount(size),
			Exists:    true,
			Available: fs.objects.Has(digest),
		})
		if limit > 0 && len(out) >= limit {
			return errScanDone
		}
		return nil
	})
	if errors.Is(err, errScanDone) {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// errScanDone aborts a metadata scan once the caller limit is reached.
var errScanDone = errors.New("murmur: file scan limit reached")

// FileReader streams verified object bytes. Reads fail closed on any
// integrity error. Close releases the retention pin; the reader must be
// closed to let FilesGC reclaim the object.
type FileReader struct {
	pipe   *io.PipeReader
	pin    *objectstore.Pin
	done   chan struct{}
	name   string
	digest objectstore.Digest
	size   int64
	once   sync.Once
}

// Name, Digest, and Size describe the open file.
func (r *FileReader) Name() string               { return r.name }
func (r *FileReader) Digest() objectstore.Digest { return r.digest }
func (r *FileReader) Size() int64                { return r.size }

// Read streams the next verified bytes.
func (r *FileReader) Read(p []byte) (int, error) { return r.pipe.Read(p) }

// Close stops the stream and releases the retention pin.
func (r *FileReader) Close() error {
	r.once.Do(func() {
		_ = r.pipe.Close()
		<-r.done
		_ = r.pin.Close()
	})
	return nil
}

// OpenFile opens a verified streaming reader for the named file. Unknown
// or deleted names, and names whose bytes are absent locally, return
// ErrFileUnavailable.
func (db *DB) OpenFile(ctx context.Context, name string) (*FileReader, error) {
	fs, err := db.filesForRead()
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("murmur: file name is required")
	}
	st, err := fs.status(fileRowID(name))
	if err != nil {
		return nil, err
	}
	if !st.Exists || st.Deleted {
		return nil, fmt.Errorf("%w: no such file %q", ErrFileUnavailable, name)
	}
	if !st.Available {
		return nil, fmt.Errorf("%w: %q has no local object bytes", ErrFileUnavailable, name)
	}
	// Pin before streaming so concurrent FilesGC cannot reclaim the
	// object mid-read.
	pin, err := fs.objects.Pin(st.Digest)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %q has no local object bytes", ErrFileUnavailable, name)
		}
		return nil, fmt.Errorf("murmur: open %q: %w", name, err)
	}
	if err := ctx.Err(); err != nil {
		_ = pin.Close()
		return nil, err
	}
	pr, pw := io.Pipe()
	r := &FileReader{pipe: pr, pin: pin, done: make(chan struct{}), name: name, digest: st.Digest, size: st.Size}
	// Detached context: the reader's lifetime, not the Open call's,
	// bounds the stream. Close terminates the pump via the pipe.
	go func() {
		defer close(r.done)
		_, rerr := fs.objects.Read(context.Background(), st.Digest, pw)
		_ = pw.CloseWithError(mapFileReadErr(name, rerr))
	}()
	return r, nil
}

// mapFileReadErr translates stream failures. A vanishing object (removed
// between the availability check and the read) reports unavailable.
func mapFileReadErr(name string, err error) error {
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		return fmt.Errorf("%w: %q has no local object bytes", ErrFileUnavailable, name)
	}
	return fmt.Errorf("murmur: read %q: %w", name, err)
}

// FilesGC removes node-local objects that are older than grace, unpinned,
// and unreferenced by any visible file metadata. It returns the digests
// removed. Use a grace comfortably longer than the slowest upload: objects
// publish before their metadata commits, so a zero grace during an active
// upload could reclaim bytes the pending commit is about to reference.
func (db *DB) FilesGC(ctx context.Context, grace time.Duration) ([]objectstore.Digest, error) {
	fs, err := db.filesForWrite()
	if err != nil {
		return nil, err
	}
	var referenced []objectstore.Digest
	err = db.store.IterateTable(fs.ids.table, func(r *state.Row) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		eff, visible, err := effectiveFileVisible(db, r.Cells, r.Tomb.Version, r.Tomb.Present)
		if err != nil {
			return err
		}
		if !visible {
			return nil
		}
		_, digest, _, err := fs.decode(eff)
		if err != nil {
			return err
		}
		referenced = append(referenced, digest)
		return nil
	})
	if err != nil {
		return nil, err
	}
	removed, err := fs.objects.Collect(referenced, grace)
	if err != nil {
		return removed, fmt.Errorf("murmur: file GC: %w", err)
	}
	if err := fs.sweepStaging(grace); err != nil {
		return removed, fmt.Errorf("murmur: file GC: %w", err)
	}
	return removed, nil
}

// RotateFileObjectKey re-encrypts every node-local object under newKey
// (32 bytes) and advances the object-key generation. Objects created
// concurrently land on the new key and are never missed; reads run
// throughout. The operator must persist newKey as Files.ObjectKey before
// the next restart (the old key cannot read rotated objects), and must
// install the same key on every node: mesh fetch verifies with the local
// key, so mixed generations fail fetches closed until each node rotates.
// Progress reports completed/total objects and may be nil.
func (db *DB) RotateFileObjectKey(ctx context.Context, newKey []byte, progress func(done, total int)) error {
	fs, err := db.filesForWrite()
	if err != nil {
		return err
	}
	if len(newKey) != 32 {
		return fmt.Errorf("murmur: replacement object key must be 32 bytes")
	}
	if err := fs.objects.RotateKey(ctx, newKey, progress); err != nil {
		return fmt.Errorf("murmur: rotate object key: %w", err)
	}
	return nil
}

// FileObjectKeyGeneration reports the node-local object-key generation
// (1 before the first rotation). Operators compare it across nodes to
// confirm a cluster-wide rotation finished.
func (db *DB) FileObjectKeyGeneration() (uint32, error) {
	fs, err := db.filesForRead()
	if err != nil {
		return 0, err
	}
	return fs.objects.CurrentGeneration(), nil
}

// sweepStaging removes staged fetch prefixes that are no longer needed:
// prefixes of installed objects, and (past the grace) orphaned prefixes
// with no active fetch. Active fetches are never disturbed.
func (fs *fileStore) sweepStaging(grace time.Duration) error {
	entries, err := os.ReadDir(fs.fetch.staging)
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-grace)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".part") {
			continue
		}
		hexPart := strings.TrimSuffix(name, ".part")
		if i := strings.IndexByte(hexPart, '-'); i >= 0 {
			hexPart = hexPart[:i]
		}
		var digest objectstore.Digest
		if len(hexPart) != len(digest)*2 {
			continue
		}
		n, herr := hex.DecodeString(hexPart)
		if herr != nil || len(n) != len(digest) {
			continue
		}
		copy(digest[:], n)
		fs.fetch.mu.Lock()
		active := fs.fetch.active[digest] > 0
		fs.fetch.mu.Unlock()
		if active {
			continue
		}
		if fs.objects.Has(digest) {
			_ = os.Remove(filepath.Join(fs.fetch.staging, name))
			continue
		}
		if grace <= 0 {
			_ = os.Remove(filepath.Join(fs.fetch.staging, name))
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		if fi.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(fs.fetch.staging, name))
		}
	}
	return nil
}
