package crypto

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// Logger mirrors the root Logger to avoid an import cycle.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Debug(string, ...any) {}
func (discardLogger) Info(string, ...any)  {}
func (discardLogger) Warn(string, ...any)  {}
func (discardLogger) Error(string, ...any) {}

func bgCtx() context.Context { return context.Background() }

func nowUnix() int64 { return time.Now().Unix() }

// FSStats counts encrypted-FS activity. All fields are atomic.
type FSStats struct {
	FilesCreated   atomic.Uint64
	FilesOpened    atomic.Uint64
	ShortFiles     atomic.Uint64 // zero-byte files opened as empty
	BytesEncrypted atomic.Uint64
	BytesDecrypted atomic.Uint64
	Compactions    atomic.Uint64
	CompactErrors  atomic.Uint64
	BytesReclaimed atomic.Uint64
	Rewrites       atomic.Uint64
	RewriteSkipped atomic.Uint64
	RewriteBytes   atomic.Uint64
}

// FSCounters is a plain snapshot of FSStats.
type FSCounters struct {
	FilesCreated   uint64
	FilesOpened    uint64
	ShortFiles     uint64
	BytesEncrypted uint64
	BytesDecrypted uint64
	Compactions    uint64
	CompactErrors  uint64
	BytesReclaimed uint64
	Rewrites       uint64
	RewriteSkipped uint64
	RewriteBytes   uint64
}

// Snapshot copies the counters.
func (s *FSStats) Snapshot() FSCounters {
	return FSCounters{
		FilesCreated:   s.FilesCreated.Load(),
		FilesOpened:    s.FilesOpened.Load(),
		ShortFiles:     s.ShortFiles.Load(),
		BytesEncrypted: s.BytesEncrypted.Load(),
		BytesDecrypted: s.BytesDecrypted.Load(),
		Compactions:    s.Compactions.Load(),
		CompactErrors:  s.CompactErrors.Load(),
		BytesReclaimed: s.BytesReclaimed.Load(),
		Rewrites:       s.Rewrites.Load(),
		RewriteSkipped: s.RewriteSkipped.Load(),
		RewriteBytes:   s.RewriteBytes.Load(),
	}
}

// FSOptions configures the encrypted filesystem.
type FSOptions struct {
	// Base is the underlying filesystem (vfs.Default or vfs.NewMem).
	Base vfs.FS
	// Registry holds the per-file data keys.
	Registry *Registry
	// DBID binds every container to one database.
	DBID [16]byte
	// Logger receives warnings (compaction failures). Nil discards.
	Logger Logger
	// RotationCheck, when set, runs before a new file adopts the active
	// key so an expired generation rotates first. Errors are logged and
	// creation proceeds with the current key (availability first).
	RotationCheck func() error
}

// EncryptedFS implements vfs.FS, transparently encrypting every file handle
// with the authenticated container format. Directories, locks, links,
// renames, and path queries pass through to the base filesystem.
//
// The only writers tracked are open write handles: a second concurrent
// write-open of one path fails closed (Pebble never does this).
type EncryptedFS struct {
	base vfs.FS
	reg  *Registry
	dbID [16]byte
	log  Logger

	rotationCheck func() error

	mu      sync.Mutex
	writers map[string]struct{}
	// openFiles tracks live keyed handles for reference inventories.
	openFiles map[*encryptedFile][KeyIDLen]byte

	stats FSStats
}

var _ vfs.FS = (*EncryptedFS)(nil)

// NewEncryptedFS wraps base. Registry must already be open.
func NewEncryptedFS(opt FSOptions) (*EncryptedFS, error) {
	if opt.Base == nil {
		return nil, fmt.Errorf("crypto: encrypted FS requires a base FS")
	}
	if opt.Registry == nil {
		return nil, fmt.Errorf("crypto: encrypted FS requires a registry")
	}
	log := opt.Logger
	if log == nil {
		log = discardLogger{}
	}
	return &EncryptedFS{
		base: opt.Base, reg: opt.Registry, dbID: opt.DBID, log: log,
		rotationCheck: opt.RotationCheck,
		writers:       make(map[string]struct{}),
		openFiles:     make(map[*encryptedFile][KeyIDLen]byte),
	}, nil
}

// SetRotationCheck installs the pre-create expiry hook. It must be called
// before the FS serves traffic (no concurrent mutation afterwards).
func (fs *EncryptedFS) SetRotationCheck(fn func() error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.rotationCheck = fn
}

// trackOpen records a live keyed handle; untrackOpen removes it.
func (fs *EncryptedFS) trackOpen(ef *encryptedFile) {
	if id, ok := ef.keyIDOf(); ok {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		fs.openFiles[ef] = id
	}
}

func (fs *EncryptedFS) untrackOpen(ef *encryptedFile) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.openFiles, ef)
}

// OpenHandleCounts counts live keyed handles per key id.
func (fs *EncryptedFS) OpenHandleCounts() map[string]uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	out := make(map[string]uint64, len(fs.openFiles))
	for _, id := range fs.openFiles {
		out[string(id[:])]++
	}
	return out
}

// Stats returns a snapshot of the FS counters.
func (fs *EncryptedFS) Stats() FSCounters { return fs.stats.Snapshot() }

// Registry exposes the key registry.
func (fs *EncryptedFS) Registry() *Registry { return fs.reg }

// markWriter registers a write handle; false means one is already open.
func (fs *EncryptedFS) markWriter(name string) bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, ok := fs.writers[name]; ok {
		return false
	}
	fs.writers[name] = struct{}{}
	return true
}

func (fs *EncryptedFS) unmarkWriter(name string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.writers, name)
}

// isWriteOpen reports whether a write handle is open for name.
func (fs *EncryptedFS) isWriteOpen(name string) bool {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	_, ok := fs.writers[name]
	return ok
}

// Create implements vfs.FS.
func (fs *EncryptedFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	if !fs.markWriter(name) {
		return nil, fmt.Errorf("crypto: %s already open for write", name)
	}
	f, err := fs.base.Create(name, category)
	if err != nil {
		fs.unmarkWriter(name)
		return nil, err
	}
	ef := &encryptedFile{
		fs: fs, f: f, name: name, category: category,
		chunks: make(map[uint64]*chunkState), epochs: make(map[uint64]*epochAEAD),
		stats: &fs.stats,
	}
	if err := ef.startFresh(); err != nil {
		f.Close()
		fs.unmarkWriter(name)
		return nil, err
	}
	fs.trackOpen(ef)
	fs.stats.FilesCreated.Add(1)
	return ef, nil
}

// Open implements vfs.FS (read-only).
func (fs *EncryptedFS) Open(name string, _ ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.base.Open(name)
	if err != nil {
		return nil, err
	}
	size, err := fileSize(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	ef, err := openEncrypted(fs, f, name, "", size, false)
	if err != nil {
		return nil, err
	}
	fs.trackOpen(ef)
	fs.stats.FilesOpened.Add(1)
	return ef, nil
}

// OpenReadWrite implements vfs.FS.
func (fs *EncryptedFS) OpenReadWrite(name string, category vfs.DiskWriteCategory, _ ...vfs.OpenOption) (vfs.File, error) {
	if !fs.markWriter(name) {
		return nil, fmt.Errorf("crypto: %s already open for write", name)
	}
	f, err := fs.base.OpenReadWrite(name, category)
	if err != nil {
		fs.unmarkWriter(name)
		return nil, err
	}
	size, err := fileSize(f)
	if err != nil {
		f.Close()
		fs.unmarkWriter(name)
		return nil, err
	}
	ef, err := openEncrypted(fs, f, name, category, size, true)
	if err != nil {
		fs.unmarkWriter(name)
		return nil, err
	}
	fs.trackOpen(ef)
	fs.stats.FilesOpened.Add(1)
	return ef, nil
}

// OpenDir implements vfs.FS (passthrough: directories are not encrypted).
func (fs *EncryptedFS) OpenDir(name string) (vfs.File, error) {
	return fs.base.OpenDir(name)
}

// Remove implements vfs.FS. Keys are never removed here: retirement happens
// only after rebuilding the full reference inventory (live files, open
// handles, checkpoints), so checkpoint-pinned keys survive live deletion.
func (fs *EncryptedFS) Remove(name string) error {
	if isRegistryArtifact(name) {
		return fs.base.Remove(name)
	}
	_, _, openErr := fs.peekKeyID(name)
	if err := fs.base.Remove(name); err != nil {
		return err
	}
	if openErr != nil {
		// The bytes are gone; report the earlier parse failure loudly.
		return fmt.Errorf("remove %s: %w", name, openErr)
	}
	return nil
}

// peekKeyID opens name read-only to learn its key id without registering
// anything. known=false for zero-byte files and registry artifacts.
func (fs *EncryptedFS) peekKeyID(name string) ([KeyIDLen]byte, bool, error) {
	var zero [KeyIDLen]byte
	f, err := fs.base.Open(name)
	if err != nil {
		return zero, false, err
	}
	defer f.Close()
	size, err := fileSize(f)
	if err != nil {
		return zero, false, err
	}
	if size == 0 {
		return zero, false, nil
	}
	if size < headerTotalLen {
		return zero, false, fmt.Errorf("%w: short file", ErrCorrupt)
	}
	hdr, err := readHeader(f, fs.dbID, func(id [KeyIDLen]byte) ([]byte, AlgorithmID, error) {
		return fs.reg.GetKey(bgCtx(), id)
	})
	if err != nil {
		return zero, false, err
	}
	return hdr.keyID, true, nil
}

// RemoveAll implements vfs.FS (passthrough).
func (fs *EncryptedFS) RemoveAll(name string) error { return fs.base.RemoveAll(name) }

// Link implements vfs.FS (passthrough: hard-linked ciphertext shares keys).
func (fs *EncryptedFS) Link(oldname, newname string) error {
	return fs.base.Link(oldname, newname)
}

// Rename implements vfs.FS (passthrough: keys travel inside the container).
func (fs *EncryptedFS) Rename(oldname, newname string) error {
	return fs.base.Rename(oldname, newname)
}

// ReuseForWrite implements vfs.FS. The vfs contract allows declining the
// reuse; do so: the old file is deleted (with its key) and a fresh encrypted
// file is created. This is also how recycled WALs rotate to fresh keys.
func (fs *EncryptedFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	if err := fs.Remove(oldname); err != nil {
		// Best effort: a missing file is fine, anything else is reported
		// but does not block the fresh file (the old bytes are obsolete).
		fs.log.Warn("reuse remove failed", "file", oldname, "err", err.Error())
		_ = fs.base.Remove(oldname)
	}
	return fs.Create(newname, category)
}

// MkdirAll implements vfs.FS.
func (fs *EncryptedFS) MkdirAll(dir string, perm os.FileMode) error {
	return fs.base.MkdirAll(dir, perm)
}

// Lock implements vfs.FS (passthrough).
func (fs *EncryptedFS) Lock(name string) (io.Closer, error) { return fs.base.Lock(name) }

// List implements vfs.FS.
func (fs *EncryptedFS) List(dir string) ([]string, error) { return fs.base.List(dir) }

// Stat implements vfs.FS. Encrypted files report their logical size; anything
// else (registry, LOCK, short files) falls back to the base stat.
func (fs *EncryptedFS) Stat(name string) (vfs.FileInfo, error) {
	fi, err := fs.base.Stat(name)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() || fi.Size() < headerTotalLen || isRegistryArtifact(name) {
		return fi, nil
	}
	f, err := fs.base.Open(name)
	if err != nil {
		return fi, nil
	}
	size := fi.Size()
	hdr, herr := readHeader(f, fs.dbID, func(id [KeyIDLen]byte) ([]byte, AlgorithmID, error) {
		return fs.reg.GetKey(bgCtx(), id)
	})
	f.Close()
	if herr != nil {
		return fi, nil
	}
	_ = size
	return &sizedFileInfo{FileInfo: fi, size: int64(hdr.logicalSize)}, nil
}

// sizedFileInfo overrides Size with the logical container size.
type sizedFileInfo struct {
	vfs.FileInfo
	size int64
}

func (s *sizedFileInfo) Size() int64 { return s.size }

// PathBase implements vfs.FS.
func (fs *EncryptedFS) PathBase(path string) string { return fs.base.PathBase(path) }

// PathJoin implements vfs.FS.
func (fs *EncryptedFS) PathJoin(elem ...string) string { return fs.base.PathJoin(elem...) }

// PathDir implements vfs.FS.
func (fs *EncryptedFS) PathDir(path string) string { return fs.base.PathDir(path) }

// GetDiskUsage implements vfs.FS.
func (fs *EncryptedFS) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	return fs.base.GetDiskUsage(path)
}

// Unwrap implements vfs.FS.
func (fs *EncryptedFS) Unwrap() vfs.FS { return fs.base }

// syncParentDir fsyncs the directory containing name for rename durability.
func (fs *EncryptedFS) syncParentDir(name string) error {
	d, err := fs.base.OpenDir(fs.base.PathDir(name))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// verifyImage fully scrubs a container: headers, checkpoint, every record
// seal (including superseded extents), and the assembled logical range.
func (fs *EncryptedFS) verifyImage(name string) error {
	f, err := fs.base.Open(name)
	if err != nil {
		return err
	}
	size, err := fileSize(f)
	if err != nil {
		f.Close()
		return err
	}
	ef, err := openEncrypted(fs, f, name, "", size, false)
	if err != nil {
		return err
	}
	defer ef.Close()
	if size == 0 {
		return nil
	}
	ef.mu.Lock()
	defer ef.mu.Unlock()
	if err := ef.scrubLocked(ef.committedEnd); err != nil {
		return err
	}
	// Read the whole logical range: assembly is verified too.
	const step = 1 << 20
	buf := make([]byte, step)
	var off int64
	for uint64(off) < ef.logicalSize {
		// ReadAt takes ef.mu; unlock around it.
		ef.mu.Unlock()
		n, rerr := ef.ReadAt(buf, off)
		ef.mu.Lock()
		off += int64(n)
		if rerr != nil && rerr != io.EOF {
			return rerr
		}
		if n == 0 {
			break
		}
	}
	return nil
}

// isRegistryArtifact reports files the registry manages itself.
func isRegistryArtifact(name string) bool {
	base := filepath.Base(name)
	if strings.HasPrefix(base, RegistryFileName) {
		return true
	}
	return false
}

func fileSize(f vfs.File) (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
