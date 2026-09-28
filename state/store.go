package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/crdt"
	"github.com/nomadsql/replicateddb/ids"
)

// Options configures the durable store.
type Options struct {
	// FS is the filesystem Pebble uses (the encrypted VFS in production).
	// Nil means vfs.Default.
	FS vfs.FS
	// WALDir overrides the WAL directory. Empty keeps Pebble's default
	// (the data directory itself).
	WALDir string
	// CacheBytes sizes Pebble's unified block cache. Default 16 MiB.
	CacheBytes int64
	// MemTableSize caps one memtable. Default 4 MiB (small hot set).
	MemTableSize uint64
	// MemTableStopWritesThreshold caps live memtables. Zero keeps default (2).
	MemTableStopWritesThreshold int
	// MaxOpenFiles caps open handles. Zero keeps Pebble's default.
	MaxOpenFiles int
	// CompactionConcurrency caps background compactions as (1, n).
	// Zero keeps Pebble's default (1, 1).
	CompactionConcurrency int
	// Compression overrides every level's block profile. Nil keeps default.
	Compression *sstable.CompressionProfile
	// WALBytesPerSync smooths WAL writes. Zero keeps Pebble's default.
	WALBytesPerSync int
	// Limits bounds decoded values/mutations.
	Limits codec.Limits
	// Restore, when non-nil, adopts a fresh writer identity for restored
	// data: the stored node identity must equal Restore.Source and is
	// replaced by Restore.Fresh. Nil means ordinary open, where a stored
	// identity mismatch is rejected.
	Restore *RestoreAdoption
	// AsyncDurability configures commits to return without waiting for synchronous fsync (pebble.NoSync).
	AsyncDurability bool
	// Logger receives Pebble logs (Info maps to Debug). Nil discards.
	Logger Logger
	// SnapshotAtomicMergeBytes caps the encoded size merged in one atomic
	// Pebble batch during snapshot import. Larger validated snapshots
	// merge chunk by chunk with durable resume progress and publish
	// watermarks/generation in one final atomic batch. Zero selects
	// DefaultSnapshotAtomicMergeBytes.
	SnapshotAtomicMergeBytes uint64
}

// Logger mirrors the root Logger to avoid an import cycle.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type pebbleLogAdapter struct {
	l     Logger
	fatal *fatalCapture
}

func (a pebbleLogAdapter) Infof(f string, v ...any)    { a.l.Debug(fmt.Sprintf(f, v...)) }
func (a pebbleLogAdapter) Warningf(f string, v ...any) { a.l.Warn(fmt.Sprintf(f, v...)) }
func (a pebbleLogAdapter) Errorf(f string, v ...any)   { a.l.Error(fmt.Sprintf(f, v...)) }

// Fatalf records Pebble's terminal errors (commit/WAL/MANIFEST failures)
// into the fail-closed capture. Pebble's contract treats Fatalf as
// non-returning, but a library must not exit the host process; the capture
// converts the signal into a sticky ErrStorageFailed instead.
func (a pebbleLogAdapter) Fatalf(f string, v ...any) {
	msg := fmt.Sprintf(f, v...)
	a.l.Error(msg)
	if a.fatal != nil {
		a.fatal.noteFatal(msg)
	}
}

type discardPebbleLog struct{ fatal *fatalCapture }

func (discardPebbleLog) Infof(string, ...any)    {}
func (discardPebbleLog) Warningf(string, ...any) {}
func (discardPebbleLog) Errorf(string, ...any)   {}
func (d discardPebbleLog) Fatalf(f string, v ...any) {
	if d.fatal != nil {
		d.fatal.noteFatal(fmt.Sprintf(f, v...))
	}
}

// WinningChange is one winner to materialize into the query engine.
type WinningChange struct {
	TableID   uint32
	RowID     ids.RowID
	ColumnID  uint32
	Value     codec.Value
	Tombstone bool
	Version   crdt.Version
}

// MergeResult summarizes a committed batch.
type MergeResult struct {
	// Winners must be applied to the query engine, in order.
	Winners []WinningChange
	// Applied reports whether the batch was durably recorded (false for pure duplicates).
	Applied bool
	// Generation is the state generation after commit.
	Generation uint64
}

// Errors returned by the store.
var (
	ErrGap     = errors.New("state: replication gap")
	ErrLogGone = errors.New("state: log entries already garbage collected")
	ErrTooBig  = errors.New("state: batch too large for one durable commit")
)

// RetentionLease represents an active source log retention lease that pins
// GC floors for snapshot tail repair and background synchronization.
type RetentionLease struct {
	ID         uint64
	Holder     string
	ExpiresAt  time.Time
	Watermarks map[ids.NodeID]uint64
}

type retentionLease struct {
	id         uint64
	holder     string
	expiresAt  time.Time
	watermarks map[ids.NodeID]uint64
}

// Store is the Pebble-backed authoritative state. All read-modify-write
// commits serialize on one writer mutex and apply atomically through a
// Pebble batch; multi-key reads use point-in-time snapshots.
type Store struct {
	db     *pebble.DB
	clock  crdt.Clock
	limits codec.Limits

	nodeID ids.NodeID
	dbID   ids.DBID

	writeMu sync.Mutex
	// gate drains state operations for maintenance: every Pebble-touching
	// method holds RLock; CloseForMaintenance takes Lock across the
	// close/rewrite/reopen window. Lock order is always gate -> writeMu.
	gate sync.RWMutex
	// maintClosed reports that Pebble was closed (or close was attempted
	// past a successful flush) for maintenance, so the holder must call
	// ReopenAfterMaintenance even when CloseForMaintenance failed.
	maintClosed atomic.Bool

	leaseMu      sync.Mutex
	nextLeaseID  uint64
	leases       map[uint64]*retentionLease
	bridgeResume map[ids.NodeID]uint64

	openPath string
	openOpt  Options

	writeOpts *pebble.WriteOptions
	// fatal is the sticky fail-closed capture: once Pebble reports a
	// terminal storage error, every operation returns ErrStorageFailed
	// until the process restarts.
	fatal *fatalCapture
	// snapshotMergeFault, when non-nil, fails chunked snapshot merges
	// after committing the chunk that advances progress to nextChunk.
	// Tests use it to prove crash-resume; production leaves it nil.
	snapshotMergeFault func(nextChunk uint64) error
	// snapshotIngestFault injects a crash window after SST ingestion and
	// before durable merge progress, for idempotent recovery tests.
	snapshotIngestFault func() error
	// remotePrepareFault injects an interruption after the durable remote
	// prepare record and before its atomic final commit.
	remotePrepareFault func() error
	// transactionStageFault injects a pre-commit interruption for staging
	// recovery tests. Production leaves it nil.
	transactionStageFault       func() error
	stagedTransactionByteLimit  int64
	stagedTransactionCountLimit int
}

// Open opens (or creates) the store. nodeID must match any stored identity;
// dbID zero loads the stored id, nonzero must match or initialize.
func Open(path string, nodeID ids.NodeID, dbID ids.DBID, opt Options) (*Store, error) {
	fs := opt.FS
	if fs == nil {
		fs = vfs.Default
	}
	fatal := &fatalCapture{}
	fs = &watchFS{FS: fs, fatal: fatal}
	opt.FS = fs
	if opt.CacheBytes <= 0 {
		opt.CacheBytes = 16 << 20
	}
	if opt.MemTableSize == 0 {
		opt.MemTableSize = 4 << 20
	}
	popt := &pebble.Options{
		FS:              fs,
		WALDir:          opt.WALDir,
		CacheSize:       opt.CacheBytes,
		MemTableSize:    opt.MemTableSize,
		WALBytesPerSync: opt.WALBytesPerSync,
		EventListener:   pebbleFatalListener(fatal),
	}
	if opt.Logger == nil {
		popt.Logger = discardPebbleLog{fatal: fatal}
	} else {
		popt.Logger = pebbleLogAdapter{l: opt.Logger, fatal: fatal}
	}
	if opt.MemTableStopWritesThreshold > 0 {
		popt.MemTableStopWritesThreshold = opt.MemTableStopWritesThreshold
	}
	if opt.MaxOpenFiles > 0 {
		popt.MaxOpenFiles = opt.MaxOpenFiles
	}
	if opt.CompactionConcurrency > 0 {
		n := opt.CompactionConcurrency
		popt.CompactionConcurrencyRange = func() (int, int) { return 1, n }
	}
	popt.EnsureDefaults()
	if opt.Compression != nil {
		for i := range popt.Levels {
			popt.Levels[i].Compression = func() *sstable.CompressionProfile { return opt.Compression }
		}
	}
	db, err := pebble.Open(path, popt)
	if err != nil {
		return nil, fmt.Errorf("state: open pebble: %w", err)
	}
	writeOpts := pebble.Sync
	if opt.AsyncDurability {
		writeOpts = pebble.NoSync
	}
	s := &Store{
		db:                          db,
		limits:                      opt.Limits,
		nodeID:                      nodeID,
		openPath:                    path,
		openOpt:                     opt,
		writeOpts:                   writeOpts,
		stagedTransactionByteLimit:  MaxStagedTransactionBytes,
		stagedTransactionCountLimit: MaxStagedTransactions,
		fatal:                       fatal,
		leases:                      make(map[uint64]*retentionLease),
		bridgeResume:                make(map[ids.NodeID]uint64),
	}
	if s.limits.MaxValueBytes == 0 {
		s.limits = codec.DefaultLimits()
	}
	if err := s.initMeta(nodeID, dbID); err != nil {
		db.Close()
		return nil, err
	}
	if err := cleanupSnapshotIngestTemps(fs, path); err != nil {
		db.Close()
		return nil, fmt.Errorf("state: clean interrupted snapshot ingest: %w", err)
	}
	if err := s.recoverPreparedRemote(); err != nil {
		db.Close()
		return nil, fmt.Errorf("state: recover prepared remote transaction: %w", err)
	}
	return s, nil
}

func (s *Store) initMeta(nodeID ids.NodeID, dbID ids.DBID) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	set := func(k, v []byte) error { return b.Set(k, v, nil) }
	// Format version.
	raw, err := s.getDirect(SysKey(sysFormat))
	if err == nil {
		v, ok := decodeU64(raw)
		if !ok || v != FormatVersion {
			return fmt.Errorf("state: unsupported format version %d (want %d)", v, FormatVersion)
		}
	} else if isNotFound(err) {
		if err := set(SysKey(sysFormat), encodeU64(FormatVersion)); err != nil {
			return err
		}
	} else {
		return err
	}
	// Minimum reader/writer compatibility. A store demanding a newer
	// reader or writer fails closed. Markers absent on pre-marker v2
	// stores default to the format version (this binary).
	for _, mk := range []struct {
		name string
		max  uint64
		role string
	}{
		{sysMinReader, MinReaderVersion, "reader"},
		{sysMinWriter, MinWriterVersion, "writer"},
	} {
		raw, err := s.getDirect(SysKey(mk.name))
		if err == nil {
			v, ok := decodeU64(raw)
			if !ok {
				return fmt.Errorf("state: corrupt %s", mk.name)
			}
			if v > mk.max {
				return fmt.Errorf("state: store requires minimum %s version %d (this binary supports %d)", mk.role, v, mk.max)
			}
		} else if isNotFound(err) {
			if err := set(SysKey(mk.name), encodeU64(FormatVersion)); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	// Node identity. A stored mismatch is either an ordinary wrong-node
	// open (rejected) or a restore adoption (fresh identity swap with a
	// durable marker, staged into this same batch).
	if raw, err := s.getDirect(SysKey(sysLocalNode)); err == nil {
		if len(raw) != 16 || string(raw) != string(nodeID[:]) {
			if err := s.adoptRestoreIdentity(b, nodeID, raw); err != nil {
				return err
			}
		}
	} else if isNotFound(err) {
		if err := set(SysKey(sysLocalNode), nodeID[:]); err != nil {
			return err
		}
	} else {
		return err
	}
	// Cluster identity. A reseed adoption stages the DBID swap in this
	// same batch; the check below then expects the replacement.
	if raw, err := s.getDirect(SysKey(sysDBID)); err == nil {
		if len(raw) != 16 {
			return fmt.Errorf("state: corrupt db id")
		}
		copy(s.dbID[:], raw)
		if !dbID.IsZero() && s.dbID != dbID {
			swap, ok := s.reseedSwap(raw)
			if !ok || swap != dbID {
				return fmt.Errorf("state: db id mismatch")
			}
			// Normally already staged by adoption; setting again is a
			// harmless no-op that keeps direct API misuse consistent.
			if err := set(SysKey(sysDBID), swap[:]); err != nil {
				return err
			}
			s.dbID = swap
		}
	} else if isNotFound(err) {
		if dbID.IsZero() {
			dbID = ids.NewDBID()
		}
		s.dbID = dbID
		if err := set(SysKey(sysDBID), dbID[:]); err != nil {
			return err
		}
	} else {
		return err
	}
	// Restore HLC floor so the clock cannot move backwards.
	if raw, err := s.getDirect(SysKey(sysHLC)); err == nil {
		if v, ok := decodeU64(raw); ok {
			s.clock.Restore(v)
		}
	} else if !isNotFound(err) {
		return err
	}
	if b.Len() == 0 {
		return nil
	}
	return s.commitBatch(b, pebble.Sync)
}

// DBID returns the cluster identity.
func (s *Store) DBID() ids.DBID { return s.dbID }

// NodeID returns the local node identity.
func (s *Store) NodeID() ids.NodeID { return s.nodeID }

// ClockNow allocates the HLC timestamp for a local write.
func (s *Store) ClockNow() uint64 { return s.clock.Now() }

// ClockMax returns the highest issued/observed HLC timestamp.
func (s *Store) ClockMax() uint64 { return s.clock.Max() }

// Close closes Pebble, waiting out any maintenance window.
func (s *Store) Close() error {
	s.gate.Lock()
	defer s.gate.Unlock()
	return s.db.Close()
}

// CloseForMaintenance drains state operations, flushes, and closes Pebble.
// The gate stays write-locked: every operation blocks until
// ReopenAfterMaintenance. On flush failure nothing is closed and the gate
// is released; past a successful flush the handle is dead either way, the
// gate stays locked, and the caller must still call ReopenAfterMaintenance
// to recover. MaintenanceClosed distinguishes the two outcomes.
func (s *Store) CloseForMaintenance() error {
	s.gate.Lock()
	if err := s.db.Flush(); err != nil {
		s.gate.Unlock()
		return err
	}
	s.maintClosed.Store(true)
	if err := s.db.Close(); err != nil {
		return err
	}
	return nil
}

// MaintenanceClosed reports whether the last CloseForMaintenance closed (or
// attempted to close past flush) Pebble: true means ReopenAfterMaintenance
// is required before the store is usable again.
func (s *Store) MaintenanceClosed() bool { return s.maintClosed.Load() }

// ReopenAfterMaintenance reopens Pebble with the original parameters,
// verifies identity, and resumes operations.
func (s *Store) ReopenAfterMaintenance() error {
	popt := &pebble.Options{
		FS:              s.openOpt.FS,
		WALDir:          s.openOpt.WALDir,
		CacheSize:       s.openOpt.CacheBytes,
		MemTableSize:    s.openOpt.MemTableSize,
		WALBytesPerSync: s.openOpt.WALBytesPerSync,
		EventListener:   pebbleFatalListener(s.fatal),
	}
	if s.openOpt.Logger == nil {
		popt.Logger = discardPebbleLog{fatal: s.fatal}
	} else {
		popt.Logger = pebbleLogAdapter{l: s.openOpt.Logger, fatal: s.fatal}
	}
	if s.openOpt.MemTableStopWritesThreshold > 0 {
		popt.MemTableStopWritesThreshold = s.openOpt.MemTableStopWritesThreshold
	}
	if s.openOpt.MaxOpenFiles > 0 {
		popt.MaxOpenFiles = s.openOpt.MaxOpenFiles
	}
	if s.openOpt.CompactionConcurrency > 0 {
		n := s.openOpt.CompactionConcurrency
		popt.CompactionConcurrencyRange = func() (int, int) { return 1, n }
	}
	popt.EnsureDefaults()
	if s.openOpt.Compression != nil {
		for i := range popt.Levels {
			popt.Levels[i].Compression = func() *sstable.CompressionProfile { return s.openOpt.Compression }
		}
	}
	db, err := pebble.Open(s.openPath, popt)
	if err != nil {
		return fmt.Errorf("state: reopen pebble: %w (maintenance gate still held)", err)
	}
	s.db = db
	if err := s.initMeta(s.nodeID, s.dbID); err != nil {
		// Release the handle (and its LOCK) so a retry or a fresh Open
		// can proceed; the gate stays held until a successful reopen.
		_ = db.Close()
		return fmt.Errorf("state: reopen verify: %w (maintenance gate still held)", err)
	}
	if err := cleanupSnapshotIngestTemps(s.openOpt.FS, s.openPath); err != nil {
		_ = db.Close()
		return fmt.Errorf("state: reopen cleanup: %w (maintenance gate still held)", err)
	}
	s.maintClosed.Store(false)
	s.gate.Unlock()
	return nil
}

// Flush flushes the memtable (manual hook for tests and operations).
func (s *Store) Flush() error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return err
	}
	return s.db.Flush()
}

// Compact runs a manual compaction over [start, end).
func (s *Store) Compact(ctx context.Context, start, end []byte, parallelize bool) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return err
	}
	return s.db.Compact(ctx, start, end, parallelize)
}

// Checkpoint snapshots the database files into destDir (backup primitive;
// the key registry must be copied alongside by the caller).
func (s *Store) Checkpoint(destDir string) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return err
	}
	return s.db.Checkpoint(destDir)
}

// PebbleMetrics is a focused subset of Pebble metrics for status.
type PebbleMetrics struct {
	DiskBytes     uint64
	MemTableBytes uint64
	CacheHits     int64
	CacheMisses   int64
}

// Metrics returns current Pebble metrics.
func (s *Store) Metrics() PebbleMetrics {
	s.gate.RLock()
	defer s.gate.RUnlock()
	m := s.db.Metrics()
	return PebbleMetrics{
		DiskBytes:     m.DiskSpaceUsage(),
		MemTableBytes: m.MemTable.Size,
		CacheHits:     m.BlockCache.Hits,
		CacheMisses:   m.BlockCache.Misses,
	}
}

// getDirect reads one key straight from the DB. Writers call it under
// writeMu, where no concurrent commit can interleave.
// Failed reports the sticky fail-closed storage error, or nil while the
// store is healthy. Once non-nil it never clears: the node must restart
// after the operator resolves the underlying problem.
func (s *Store) Failed() error { return s.fatal.err() }

// failedErr is the entry gate: every Pebble-touching operation fails fast
// once the store has failed closed.
func (s *Store) failedErr() error { return s.fatal.err() }

// commitBatch commits a batch and converts a Pebble commit-pipeline failure
// into a returned error. Pebble reports WAL sync failures through
// Logger.Fatalf and still returns nil from Commit; the post-commit capture
// check turns that silent data loss into an explicit failure.
func (s *Store) commitBatch(b *pebble.Batch, o *pebble.WriteOptions) error {
	if err := b.Commit(o); err != nil {
		return err
	}
	return s.failedErr()
}

// dbSet is s.db.Set plus the post-write fail-closed check.
func (s *Store) dbSet(key, value []byte, o *pebble.WriteOptions) error {
	if err := s.db.Set(key, value, o); err != nil {
		return err
	}
	return s.failedErr()
}

func (s *Store) getDirect(key []byte) ([]byte, error) {
	if err := s.failedErr(); err != nil {
		return nil, err
	}
	v, closer, err := s.db.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return append([]byte(nil), v...), nil
}

func isNotFound(err error) bool { return errors.Is(err, pebble.ErrNotFound) }

// prefixEnd returns the exclusive upper bound for prefix scans, or nil when
// the prefix is all 0xFF (unbounded above).
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// snapshot runs fn with a point-in-time snapshot for consistent multi-key reads.
func (s *Store) snapshot(fn func(snap *pebble.Snapshot) error) error {
	if err := s.failedErr(); err != nil {
		return err
	}
	snap := s.db.NewSnapshot()
	defer snap.Close()
	return fn(snap)
}

func snapGet(snap *pebble.Snapshot, key []byte) ([]byte, error) {
	v, closer, err := snap.Get(key)
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return append([]byte(nil), v...), nil
}

// readU64Direct reads a system counter; missing means zero.
func (s *Store) readU64Direct(name string) (uint64, error) {
	raw, err := s.getDirect(SysKey(name))
	if err != nil {
		if isNotFound(err) {
			return 0, nil
		}
		return 0, err
	}
	v, ok := decodeU64(raw)
	if !ok {
		return 0, fmt.Errorf("state: corrupt counter %s", name)
	}
	return v, nil
}

func readU64Snap(snap *pebble.Snapshot, name string) (uint64, error) {
	raw, err := snapGet(snap, SysKey(name))
	if err != nil {
		if isNotFound(err) {
			return 0, nil
		}
		return 0, err
	}
	v, ok := decodeU64(raw)
	if !ok {
		return 0, fmt.Errorf("state: corrupt counter %s", name)
	}
	return v, nil
}

// CommitLocal durably records a local transaction's batch. It assigns the
// origin sequence and performs the merge, log write, receipt, HLC persist,
// and generation bump in one atomic Pebble batch.
func (s *Store) CommitLocal(_ context.Context, batch *codec.MutationBatch) (MergeResult, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if batch.OriginNode != s.nodeID {
		return MergeResult{}, fmt.Errorf("state: CommitLocal with foreign origin")
	}
	if len(batch.Mutations) == 0 {
		return MergeResult{}, fmt.Errorf("state: empty batch")
	}
	if err := checkBatchLimits(batch, s.limits); err != nil {
		return MergeResult{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var res MergeResult
	// Idempotency: a retried TxID returns the previous outcome.
	if _, err := s.getDirect(ReceiptKey(batch.TxID)); err == nil {
		gen, err := s.readU64Direct(sysGeneration)
		if err != nil {
			return MergeResult{}, err
		}
		res.Generation = gen
		return res, nil
	} else if !isNotFound(err) {
		return MergeResult{}, err
	}
	seq, err := s.readU64Direct(sysLocalSeq)
	if err != nil {
		return MergeResult{}, err
	}
	seq++
	batch.Sequence = seq
	ver := batch.Version()
	b := s.db.NewBatch()
	defer b.Close()
	winners, err := s.mergeIntoBatch(b, batch, ver)
	if err != nil {
		return MergeResult{}, err
	}
	res.Winners = winners
	// Log under local origin.
	if err := b.Set(LogKey(s.nodeID, seq), codec.EncodeBatch(nil, batch), nil); err != nil {
		return MergeResult{}, err
	}
	// Receive watermark for our own origin advances with the log.
	if err := b.Set(RecvKey(s.nodeID), encodeU64(seq), nil); err != nil {
		return MergeResult{}, err
	}
	if err := b.Set(SysKey(sysLocalSeq), encodeU64(seq), nil); err != nil {
		return MergeResult{}, err
	}
	if err := b.Set(SysKey(sysHLC), encodeU64(maxU64(batch.HLC, s.clock.Max())), nil); err != nil {
		return MergeResult{}, err
	}
	var receipt [24]byte
	copy(receipt[:16], s.nodeID[:])
	binary.BigEndian.PutUint64(receipt[16:], seq)
	if err := b.Set(ReceiptKey(batch.TxID), receipt[:], nil); err != nil {
		return MergeResult{}, err
	}
	gen, err := s.readU64Direct(sysGeneration)
	if err != nil {
		return MergeResult{}, err
	}
	gen++
	if err := b.Set(SysKey(sysGeneration), encodeU64(gen), nil); err != nil {
		return MergeResult{}, err
	}
	if err := s.commitBatch(b, s.writeOpts); err != nil {
		return MergeResult{}, err
	}
	res.Applied = true
	res.Generation = gen
	return res, nil
}

// CommitRemote durably merges a batch received from a peer. The batch keeps
// its original origin/sequence identity for multi-origin forwarding.
func (s *Store) CommitRemote(_ context.Context, batch *codec.MutationBatch) (MergeResult, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if len(batch.Mutations) == 0 {
		return MergeResult{}, fmt.Errorf("state: empty batch")
	}
	if err := checkBatchLimits(batch, s.limits); err != nil {
		return MergeResult{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var res MergeResult
	// Duplicate TxID: acknowledge without reapplying.
	if _, err := s.getDirect(ReceiptKey(batch.TxID)); err == nil {
		gen, err := s.readU64Direct(sysGeneration)
		if err != nil {
			return MergeResult{}, err
		}
		res.Generation = gen
		return res, nil
	} else if !isNotFound(err) {
		return MergeResult{}, err
	}
	wm, err := s.recvWatermarkDirect(batch.OriginNode)
	if err != nil {
		return MergeResult{}, err
	}
	if batch.Sequence <= wm {
		// A redelivery under a new TxID needs only a receipt, which is already
		// atomic by itself and must not strand an intent on duplicate input.
		b := s.db.NewBatch()
		defer b.Close()
		var receipt [24]byte
		copy(receipt[:16], batch.OriginNode[:])
		binary.BigEndian.PutUint64(receipt[16:], batch.Sequence)
		if err := b.Set(ReceiptKey(batch.TxID), receipt[:], nil); err != nil {
			return MergeResult{}, err
		}
		gen, err := s.readU64Direct(sysGeneration)
		if err != nil {
			return MergeResult{}, err
		}
		if err := s.commitBatch(b, s.writeOpts); err != nil {
			return MergeResult{}, err
		}
		res.Generation = gen
		return res, nil
	}
	if batch.Sequence > wm+1 {
		return MergeResult{}, fmt.Errorf("%w: origin %s want %d got %d", ErrGap, batch.OriginNode, wm+1, batch.Sequence)
	}
	encoded := codec.EncodeBatch(nil, batch)
	prepared, err := s.getDirect(SysKey(sysRemotePrepare))
	if err == nil {
		if !bytes.Equal(prepared, encoded) {
			return MergeResult{}, fmt.Errorf("state: another remote transaction requires recovery")
		}
	} else if isNotFound(err) {
		prepare := s.db.NewBatch()
		if err := prepare.Set(SysKey(sysRemotePrepare), encoded, nil); err != nil {
			prepare.Close()
			return MergeResult{}, err
		}
		if err := s.commitBatch(prepare, pebble.Sync); err != nil {
			prepare.Close()
			return MergeResult{}, err
		}
		prepare.Close()
		if s.remotePrepareFault != nil {
			if err := s.remotePrepareFault(); err != nil {
				return MergeResult{}, err
			}
		}
	} else {
		return MergeResult{}, err
	}
	b := s.db.NewBatch()
	defer b.Close()
	ver := batch.Version()
	winners, err := s.mergeIntoBatch(b, batch, ver)
	if err != nil {
		return MergeResult{}, err
	}
	res.Winners = winners
	if err := b.Set(LogKey(batch.OriginNode, batch.Sequence), codec.EncodeBatch(nil, batch), nil); err != nil {
		return MergeResult{}, err
	}
	if err := b.Set(RecvKey(batch.OriginNode), encodeU64(batch.Sequence), nil); err != nil {
		return MergeResult{}, err
	}
	var receipt [24]byte
	copy(receipt[:16], batch.OriginNode[:])
	binary.BigEndian.PutUint64(receipt[16:], batch.Sequence)
	if err := b.Set(ReceiptKey(batch.TxID), receipt[:], nil); err != nil {
		return MergeResult{}, err
	}
	s.clock.Observe(batch.HLC)
	if err := b.Set(SysKey(sysHLC), encodeU64(maxU64(batch.HLC, s.clock.Max())), nil); err != nil {
		return MergeResult{}, err
	}
	gen, err := s.readU64Direct(sysGeneration)
	if err != nil {
		return MergeResult{}, err
	}
	gen++
	if err := b.Set(SysKey(sysGeneration), encodeU64(gen), nil); err != nil {
		return MergeResult{}, err
	}
	if err := b.Delete(SysKey(sysRemotePrepare), nil); err != nil {
		return MergeResult{}, err
	}
	if err := s.commitBatch(b, s.writeOpts); err != nil {
		return MergeResult{}, err
	}
	res.Applied = true
	res.Generation = gen
	return res, nil
}

// mergeIntoBatch LWW-merges every mutation against stored state, staging
// winner writes into b. Callers hold writeMu.
func (s *Store) mergeIntoBatch(b *pebble.Batch, batch *codec.MutationBatch, ver crdt.Version) ([]WinningChange, error) {
	winners := make([]WinningChange, 0, len(batch.Mutations))
	// Pebble batches are write-only until commit; staged tracks cells
	// already decided in this batch so intra-batch duplicates keep the
	// first write (the old read-your-writes semantics).
	staged := make(map[string]struct{}, len(batch.Mutations))
	for i := range batch.Mutations {
		m := &batch.Mutations[i]
		if m.IsTombstone() {
			key := string(TombKey(m.TableID, m.RowID))
			if _, ok := staged[key]; ok {
				continue
			}
			staged[key] = struct{}{}
			stored, present, err := s.getTombDirect(m.TableID, m.RowID)
			if err != nil {
				return nil, err
			}
			switch crdt.MergeCell(ver, stored, present) {
			case crdt.MergeTake:
				if err := b.Set(TombKey(m.TableID, m.RowID), codec.EncodeTombstone(nil, ver), nil); err != nil {
					return nil, err
				}
				winners = append(winners, WinningChange{
					TableID: m.TableID, RowID: m.RowID,
					Tombstone: true, Version: ver,
				})
			case crdt.MergeKeep, crdt.MergeEqual:
			}
			continue
		}
		key := string(CellKey(m.TableID, m.RowID, m.ColumnID))
		if _, ok := staged[key]; ok {
			continue
		}
		staged[key] = struct{}{}
		stored, present, err := s.getCellDirect(m.TableID, m.RowID, m.ColumnID)
		if err != nil {
			return nil, err
		}
		switch crdt.MergeCell(ver, stored.Version, present) {
		case crdt.MergeTake:
			st := codec.CellState{Version: ver, Value: m.Value}
			if err := b.Set(CellKey(m.TableID, m.RowID, m.ColumnID), codec.EncodeCellState(nil, st), nil); err != nil {
				return nil, err
			}
			winners = append(winners, WinningChange{
				TableID: m.TableID, RowID: m.RowID, ColumnID: m.ColumnID,
				Value: m.Value, Version: ver,
			})
		case crdt.MergeKeep, crdt.MergeEqual:
		}
	}
	return winners, nil
}

func (s *Store) getCellDirect(table uint32, row ids.RowID, col uint32) (codec.CellState, bool, error) {
	raw, err := s.getDirect(CellKey(table, row, col))
	if err != nil {
		if isNotFound(err) {
			return codec.CellState{}, false, nil
		}
		return codec.CellState{}, false, err
	}
	st, err := codec.DecodeCellState(raw, s.limits)
	if err != nil {
		return codec.CellState{}, false, fmt.Errorf("state: corrupt cell: %w", err)
	}
	return st, true, nil
}

func (s *Store) getTombDirect(table uint32, row ids.RowID) (crdt.Version, bool, error) {
	raw, err := s.getDirect(TombKey(table, row))
	if err != nil {
		if isNotFound(err) {
			return crdt.Version{}, false, nil
		}
		return crdt.Version{}, false, err
	}
	v, err := codec.DecodeTombstone(raw)
	if err != nil {
		return crdt.Version{}, false, fmt.Errorf("state: corrupt tombstone: %w", err)
	}
	return v, true, nil
}

func getCellSnap(snap *pebble.Snapshot, limits codec.Limits, table uint32, row ids.RowID, col uint32) (codec.CellState, bool, error) {
	raw, err := snapGet(snap, CellKey(table, row, col))
	if err != nil {
		if isNotFound(err) {
			return codec.CellState{}, false, nil
		}
		return codec.CellState{}, false, err
	}
	st, err := codec.DecodeCellState(raw, limits)
	if err != nil {
		return codec.CellState{}, false, fmt.Errorf("state: corrupt cell: %w", err)
	}
	return st, true, nil
}

func getTombSnap(snap *pebble.Snapshot, table uint32, row ids.RowID) (crdt.Version, bool, error) {
	raw, err := snapGet(snap, TombKey(table, row))
	if err != nil {
		if isNotFound(err) {
			return crdt.Version{}, false, nil
		}
		return crdt.Version{}, false, err
	}
	v, err := codec.DecodeTombstone(raw)
	if err != nil {
		return crdt.Version{}, false, fmt.Errorf("state: corrupt tombstone: %w", err)
	}
	return v, true, nil
}

// GetCell reads one stored cell.
func (s *Store) GetCell(table uint32, row ids.RowID, col uint32) (codec.CellState, bool, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.getCellDirect(table, row, col)
}

// GetTombstone reads one stored row tombstone.
func (s *Store) GetTombstone(table uint32, row ids.RowID) (crdt.Version, bool, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.getTombDirect(table, row)
}

func (s *Store) recvWatermarkDirect(origin ids.NodeID) (uint64, error) {
	raw, err := s.getDirect(RecvKey(origin))
	if err != nil {
		if isNotFound(err) {
			return 0, nil
		}
		return 0, err
	}
	v, ok := decodeU64(raw)
	if !ok {
		return 0, fmt.Errorf("state: corrupt watermark")
	}
	return v, nil
}

func recvWatermarkSnap(snap *pebble.Snapshot, origin ids.NodeID) (uint64, error) {
	raw, err := snapGet(snap, RecvKey(origin))
	if err != nil {
		if isNotFound(err) {
			return 0, nil
		}
		return 0, err
	}
	v, ok := decodeU64(raw)
	if !ok {
		return 0, fmt.Errorf("state: corrupt watermark")
	}
	return v, nil
}

// ReceiveWatermark returns the highest contiguous sequence stored for origin.
func (s *Store) ReceiveWatermark(origin ids.NodeID) (uint64, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.recvWatermarkDirect(origin)
}

// ReceiveWatermarks lists all known origin watermarks.
func (s *Store) ReceiveWatermarks() ([]codec.OriginWatermark, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.receiveWatermarksCore()
}

func (s *Store) receiveWatermarksCore() ([]codec.OriginWatermark, error) {
	var out []codec.OriginWatermark
	err := s.snapshot(func(snap *pebble.Snapshot) error {
		prefix := []byte{prefixRecv}
		it, err := snap.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
		if err != nil {
			return err
		}
		defer it.Close()
		for it.SeekGE(prefix); it.Valid(); it.Next() {
			k := it.Key()
			if len(k) != 17 {
				continue
			}
			var origin ids.NodeID
			copy(origin[:], k[1:17])
			seq, ok := decodeU64(append([]byte(nil), it.Value()...))
			if !ok {
				return fmt.Errorf("state: corrupt watermark")
			}
			out = append(out, codec.OriginWatermark{Origin: origin, Sequence: seq})
		}
		return it.Error()
	})
	return out, err
}

// LocalSeq returns the local origin sequence.
func (s *Store) LocalSeq() (uint64, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.readU64Direct(sysLocalSeq)
}

// StateGeneration returns the monotonic mutation generation.
func (s *Store) StateGeneration() (uint64, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.readU64Direct(sysGeneration)
}

// SchemaEpoch returns the stored schema epoch and hash.
func (s *Store) SchemaEpoch() (uint64, [32]byte, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.schemaEpochCore()
}

func (s *Store) schemaEpochCore() (uint64, [32]byte, error) {
	var (
		epoch uint64
		hash  [32]byte
	)
	err := s.snapshot(func(snap *pebble.Snapshot) error {
		e, err := readU64Snap(snap, sysSchemaEpoch)
		if err != nil {
			return err
		}
		epoch = e
		raw, err := snapGet(snap, SysKey(sysSchemaHash))
		if err != nil {
			if isNotFound(err) {
				return nil
			}
			return err
		}
		if len(raw) != 32 {
			return fmt.Errorf("state: corrupt schema hash")
		}
		copy(hash[:], raw)
		return nil
	})
	return epoch, hash, err
}

// SetSchemaEpoch stores the schema epoch and hash.
func (s *Store) SetSchemaEpoch(epoch uint64, hash [32]byte) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(SysKey(sysSchemaEpoch), encodeU64(epoch), nil); err != nil {
		return err
	}
	if err := b.Set(SysKey(sysSchemaHash), hash[:], nil); err != nil {
		return err
	}
	return s.commitBatch(b, pebble.Sync)
}

// PeerAck returns the highest contiguous sequence peer confirmed for origin.
func (s *Store) PeerAck(peer, origin ids.NodeID) (uint64, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	raw, err := s.getDirect(PeerAckKey(peer, origin))
	if err != nil {
		if isNotFound(err) {
			return 0, nil
		}
		return 0, err
	}
	v, ok := decodeU64(raw)
	if !ok {
		return 0, fmt.Errorf("state: corrupt peer ack")
	}
	return v, nil
}

// SetPeerAck records a peer's durable watermark (max wins).
func (s *Store) SetPeerAck(peer, origin ids.NodeID, seq uint64) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	raw, err := s.getDirect(PeerAckKey(peer, origin))
	if err == nil {
		cur, ok := decodeU64(raw)
		if !ok {
			return fmt.Errorf("state: corrupt peer ack")
		}
		if cur >= seq {
			return nil
		}
	} else if !isNotFound(err) {
		return err
	}
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(PeerAckKey(peer, origin), encodeU64(seq), nil); err != nil {
		return err
	}
	return s.commitBatch(b, s.writeOpts)
}

// DurabilityWriteOptions returns the WriteOptions configured for this store.
func (s *Store) DurabilityWriteOptions() *pebble.WriteOptions {
	return s.writeOpts
}

// AsyncDurability reports whether the store was opened in asynchronous durability mode.
func (s *Store) AsyncDurability() bool {
	return s.writeOpts == pebble.NoSync
}

// Sync appends a WAL-only record and syncs it, ensuring durability of all
// previously committed transactions. Pebble skips an empty batch entirely.
func (s *Store) Sync() error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.db.LogData(nil, pebble.Sync); err != nil {
		return err
	}
	return s.failedErr()
}

// Size returns the database's total disk usage in bytes.
func (s *Store) Size() (uint64, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return 0, err
	}
	return s.db.Metrics().DiskSpaceUsage(), nil
}

// FormatInfo returns the persisted format, minimum-reader, and
// minimum-writer versions. Absent minima (pre-marker stores) report the
// format version, matching open-time defaults.
func (s *Store) FormatInfo() (format, minReader, minWriter uint64, err error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	read := func(name string) (uint64, error) {
		raw, err := s.getDirect(SysKey(name))
		if err != nil {
			if isNotFound(err) {
				return format, nil
			}
			return 0, err
		}
		v, ok := decodeU64(raw)
		if !ok {
			return 0, fmt.Errorf("state: corrupt %s", name)
		}
		return v, nil
	}
	if format, err = read(sysFormat); err != nil {
		return 0, 0, 0, err
	}
	if minReader, err = read(sysMinReader); err != nil {
		return 0, 0, 0, err
	}
	if minWriter, err = read(sysMinWriter); err != nil {
		return 0, 0, 0, err
	}
	return format, minReader, minWriter, nil
}

// SetPeerExcluded marks or clears local persistent exclusion for a peer.
func (s *Store) SetPeerExcluded(node ids.NodeID, excluded bool) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	k := PeerExcludedKey(node)
	if excluded {
		if err := b.Set(k, []byte{1}, nil); err != nil {
			return err
		}
	} else {
		if err := b.Delete(k, nil); err != nil {
			return err
		}
	}
	return s.commitBatch(b, pebble.Sync)
}

// IsPeerExcluded reports whether the peer is locally excluded in persistent store.
func (s *Store) IsPeerExcluded(node ids.NodeID) (bool, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return false, err
	}
	_, closer, err := s.db.Get(PeerExcludedKey(node))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	_ = closer.Close()
	return true, nil
}

// ListExcludedPeers returns all persistently excluded peer NodeIDs.
func (s *Store) ListExcludedPeers() ([]ids.NodeID, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return nil, err
	}
	prefix := PeerExcludedPrefix()
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixEnd(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []ids.NodeID
	for iter.First(); iter.Valid(); iter.Next() {
		node, ok := ParsePeerExcludedKey(iter.Key())
		if ok {
			out = append(out, node)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

// ClearAllPeerExclusions clears all persisted peer exclusion records.
func (s *Store) ClearAllPeerExclusions() error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.failedErr(); err != nil {
		return err
	}
	prefix := PeerExcludedPrefix()
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixEnd(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	b := s.db.NewBatch()
	defer b.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := b.Delete(iter.Key(), nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return s.commitBatch(b, pebble.Sync)
}

func checkBatchLimits(batch *codec.MutationBatch, lim codec.Limits) error {
	if lim.MaxMutations > 0 && len(batch.Mutations) > lim.MaxMutations {
		return fmt.Errorf("state: batch of %d mutations exceeds limit %d: %w",
			len(batch.Mutations), lim.MaxMutations, ErrTooBig)
	}
	if lim.MaxValueBytes > 0 {
		for i := range batch.Mutations {
			m := &batch.Mutations[i]
			if m.IsTombstone() {
				continue
			}
			var n int
			switch m.Value.Type {
			case codec.TypeText:
				n = len(m.Value.S)
			case codec.TypeBlob:
				n = len(m.Value.B)
			}
			if n > lim.MaxValueBytes {
				return fmt.Errorf("state: mutation %d value of %d bytes exceeds %d: %w",
					i, n, lim.MaxValueBytes, ErrTooBig)
			}
		}
	}
	if lim.MaxTransactionBytes > 0 {
		sz := int64(codec.EncodedBatchSize(batch))
		if sz > lim.MaxTransactionBytes {
			return fmt.Errorf("state: batch encoded size %d bytes exceeds MaxTransactionBytes %d: %w",
				sz, lim.MaxTransactionBytes, ErrTooBig)
		}
	}
	return nil
}

// IsConflict reports whether err is a transaction conflict (safe to retry).
// The Pebble port serializes writers, so commits never conflict; this is
// kept for API compatibility and always reports false.
func IsConflict(err error) bool { return false }

// IsGap reports whether err is a replication gap.
func IsGap(err error) bool { return errors.Is(err, ErrGap) }

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// AcquireRetentionLease registers a bounded log retention lease holding the given
// per-origin watermarks as GC floors until expiresAt or until the returned release
// function is invoked.
func (s *Store) AcquireRetentionLease(holder string, expiresAt time.Time, watermarks map[ids.NodeID]uint64) (release func(), err error) {
	if holder == "" {
		holder = "unnamed"
	}
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	s.nextLeaseID++
	id := s.nextLeaseID
	wms := make(map[ids.NodeID]uint64, len(watermarks))
	for k, v := range watermarks {
		wms[k] = v
	}
	rl := &retentionLease{
		id:         id,
		holder:     holder,
		expiresAt:  expiresAt,
		watermarks: wms,
	}
	if s.leases == nil {
		s.leases = make(map[uint64]*retentionLease)
	}
	s.leases[id] = rl
	var once sync.Once
	return func() {
		once.Do(func() {
			s.leaseMu.Lock()
			delete(s.leases, id)
			s.leaseMu.Unlock()
		})
	}, nil
}

// ActiveRetentionLeases returns a copy of all active, unexpired log retention leases.
// Expired leases are pruned during the inspection.
func (s *Store) ActiveRetentionLeases() []RetentionLease {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	now := time.Now()
	var active []RetentionLease
	for id, l := range s.leases {
		if !l.expiresAt.IsZero() && now.After(l.expiresAt) {
			delete(s.leases, id)
			continue
		}
		wms := make(map[ids.NodeID]uint64, len(l.watermarks))
		for k, v := range l.watermarks {
			wms[k] = v
		}
		active = append(active, RetentionLease{
			ID:         l.id,
			Holder:     l.holder,
			ExpiresAt:  l.expiresAt,
			Watermarks: wms,
		})
	}
	return active
}

// RetentionFloor returns the minimum pinned sequence across all active unexpired leases
// for origin, if any.
func (s *Store) RetentionFloor(origin ids.NodeID) (uint64, bool) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	now := time.Now()
	var (
		minFloor uint64
		found    bool
	)
	for id, l := range s.leases {
		if !l.expiresAt.IsZero() && now.After(l.expiresAt) {
			delete(s.leases, id)
			continue
		}
		if seq, ok := l.watermarks[origin]; ok {
			if !found || seq < minFloor {
				minFloor = seq
				found = true
			}
		}
	}
	return minFloor, found
}

// SetBridgeExportResume records the highest captured sequence for origin.
// Uncaptured sequences (> seq) are protected from log GC.
func (s *Store) SetBridgeExportResume(origin ids.NodeID, seq uint64) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if s.bridgeResume == nil {
		s.bridgeResume = make(map[ids.NodeID]uint64)
	}
	s.bridgeResume[origin] = seq
}

// ClearBridgeExportResume removes bridge export protection for origin.
func (s *Store) ClearBridgeExportResume(origin ids.NodeID) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if s.bridgeResume != nil {
		delete(s.bridgeResume, origin)
	}
}

// BridgeExportFloor returns the lowest uncaptured sequence for origin,
// protecting uncaptured log entries from GC.
func (s *Store) BridgeExportFloor(origin ids.NodeID) (uint64, bool) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if s.bridgeResume == nil {
		return 0, false
	}
	seq, ok := s.bridgeResume[origin]
	return seq, ok
}

// HasReceipt reports whether a transaction receipt exists in authoritative storage.
func (s *Store) HasReceipt(txID ids.TxID) (bool, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	_, err := s.getDirect(ReceiptKey(txID))
	if err == nil {
		return true, nil
	}
	if isNotFound(err) {
		return false, nil
	}
	return false, err
}

// RecordReceipt records a durable receipt for a transaction ID without adding mutations.
func (s *Store) RecordReceipt(txID ids.TxID) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var receipt [24]byte
	copy(receipt[:16], s.nodeID[:])
	return s.dbSet(ReceiptKey(txID), receipt[:], s.writeOpts)
}

// SetBridgeStreamProgress records the highest contiguous applied sequence for a bridge stream.
func (s *Store) SetBridgeStreamProgress(stream string, applied uint64) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.dbSet(BridgeProgressKey(stream), encodeU64(applied), s.writeOpts)
}

// BridgeStreamProgress returns the highest contiguous applied sequence for a bridge stream.
func (s *Store) BridgeStreamProgress(stream string) (uint64, bool, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	val, err := s.getDirect(BridgeProgressKey(stream))
	if err != nil {
		if isNotFound(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	seq, ok := decodeU64(val)
	if !ok {
		return 0, false, fmt.Errorf("state: invalid bridge stream progress encoding for %q", stream)
	}
	return seq, true, nil
}
