package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/origin"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/spool"
)

// Options configures the durable store.
type Options struct {
	OriginSigning origin.Config
	// MigrateUnsignedBaseline authorizes offline conversion of trusted legacy state.
	// Conversion finishes before Open returns; no unsigned runtime mode is exposed.
	MigrateUnsignedBaseline bool
	MigrateMergePolicies    bool
	// Spool configures the persistence backend: buffering, blocks,
	// segments, workers, pending-memory bounds, compaction, compression,
	// and key material. Open fills Path (the state directory itself)
	// and ContextID (the cluster DBID); callers must not set them.
	// MasterKey or Passphrase is required unless Encryption is
	// EncryptionNone; WrappingKeyID selects provider material.
	Spool spool.Options
	// Limits bounds decoded values/mutations.
	Limits codec.Limits
	// Restore, when non-nil, adopts a fresh writer identity for restored
	// data: the stored node identity must equal Restore.Source and is
	// replaced by Restore.Fresh. Nil means ordinary open, where a stored
	// identity mismatch is rejected.
	Restore *RestoreAdoption
	// AsyncDurability configures commits to return once Spool accepts
	// them, without waiting for synchronous fsync.
	AsyncDurability bool
	// SnapshotAtomicMergeBytes caps the encoded size merged in one atomic
	// Spool commit during snapshot import. Larger validated snapshots
	// merge chunk by chunk with durable resume progress and publish
	// watermarks/generation in one final atomic commit. Zero selects
	// DefaultSnapshotAtomicMergeBytes.
	SnapshotAtomicMergeBytes uint64
	Logger                   Logger
}

// Logger mirrors the root Logger to avoid an import cycle.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
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

// Store is the Spool-backed authoritative state. Current state lives in
// an in-memory radix tree; all read-modify-write commits serialize on
// one writer mutex, build a candidate root, persist the mutations
// through one atomic Spool commit, and publish the root only after
// Spool acknowledges it. Multi-key reads use pinned immutable roots.
type Store struct {
	policyMergeAttempts atomic.Uint64
	policyMergeNanos    atomic.Uint64
	policyRejected      atomic.Uint64
	mergeRegistry       atomic.Pointer[schema.Registry]
	migratingOrigin     bool
	spool               *spool.Store
	mem                 *memStore
	clock               crdt.Clock
	limits              codec.Limits

	nodeID ids.NodeID
	dbID   ids.DBID

	writeMu sync.Mutex
	// gate drains state operations for maintenance: every
	// storage-touching method holds RLock; CloseForMaintenance takes
	// Lock across the maintenance window. Lock order is always
	// gate -> writeMu.
	gate sync.RWMutex
	// maintClosed reports that a maintenance window is open (state
	// synced and the gate held), so the holder must call
	// ReopenAfterMaintenance before the store is usable again.
	maintClosed atomic.Bool

	leaseMu      sync.Mutex
	nextLeaseID  uint64
	leases       map[uint64]*retentionLease
	bridgeResume map[ids.NodeID]uint64

	openPath string
	openOpt  Options

	// syncCommits selects synchronous Spool durability for ordinary
	// commits. Critical paths (identity, schema, prepare records,
	// snapshot progress) always commit synchronously.
	syncCommits bool
	// unsyncedBytes estimates bytes committed without a sync. Every
	// async commit through commitBatch/dbSet adds its size; Sync
	// subtracts the pre-sync total. Reads are approximate (mutation
	// bytes, not exact storage framing) and exist only to drive the
	// asynchronous size-triggered durability sync.
	unsyncedBytes atomic.Uint64
	// fatal is the sticky fail-closed capture: once Spool reports a
	// terminal storage error, every operation returns ErrStorageFailed
	// until the process restarts.
	fatal *fatalCapture
	// bindSpoolContext requests a Spool context rebind to the cluster
	// DBID after identity resolution: fresh stores and reseed adoptions
	// persist a DBID the Spool context does not know yet.
	bindSpoolContext bool
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
	stagedTransactionBytes      int64
	stagedTransactionCount      int
	closed                      bool
}

// Open opens (or creates) the store. nodeID must match any stored identity;
// dbID zero loads the stored id, nonzero must match or initialize.
// The state directory holds Spool files directly; a directory holding
// a legacy Pebble database is rejected without modification.
func Open(path string, nodeID ids.NodeID, dbID ids.DBID, opt Options) (*Store, error) {
	if err := opt.OriginSigning.Validate(nodeID); err != nil {
		return nil, err
	}
	opt.OriginSigning.PrivateKey = append([]byte(nil), opt.OriginSigning.PrivateKey...)
	if err := rejectLegacyStore(path); err != nil {
		return nil, err
	}
	fatal := &fatalCapture{}
	spopt := opt.Spool
	spopt.Path = path
	// A reseed adoption legitimately disagrees with the persisted
	// context: the intent validation below authorizes the swap and the
	// store rebinds to the new DBID afterwards.
	if opt.Restore != nil && !opt.Restore.NewDBID.IsZero() {
		spopt.ContextID = nil
	} else if !dbID.IsZero() {
		spopt.ContextID = append([]byte(nil), dbID[:]...)
	} else {
		spopt.ContextID = nil
	}
	prevOnErr := spopt.OnStorageError
	spopt.OnStorageError = func(err error) {
		fatal.noteTerminal("spool storage failure", err)
		if prevOnErr != nil {
			prevOnErr(err)
		}
	}
	mem := newMemStore()
	txn := mem.load().Txn()
	sp, err := spool.OpenAndLoad(spopt, func(records []spool.Record) error {
		for i := range records {
			r := &records[i]
			if len(r.Key) == 0 {
				return fmt.Errorf("state: spool load: empty key")
			}
			if r.Deleted {
				continue
			}
			txn.Insert(append([]byte(nil), r.Key...), append([]byte(nil), r.Value...))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("state: open spool: %w", err)
	}
	mem.publish(txn.Commit())
	s := &Store{
		spool:                       sp,
		mem:                         mem,
		limits:                      opt.Limits,
		nodeID:                      nodeID,
		openPath:                    path,
		openOpt:                     opt,
		syncCommits:                 !opt.AsyncDurability,
		stagedTransactionByteLimit:  MaxStagedTransactionBytes,
		stagedTransactionCountLimit: MaxStagedTransactions,
		fatal:                       fatal,
		leases:                      make(map[uint64]*retentionLease),
		bridgeResume:                make(map[ids.NodeID]uint64),
	}
	if s.limits.MaxValueBytes == 0 {
		s.limits = codec.DefaultLimits()
	}
	if _, err := s.mem.get(SysKey(sysDBID)); err != nil {
		if !isNotFound(err) {
			sp.Close()
			return nil, err
		}
		s.bindSpoolContext = true
	}
	fail := func(err error) (*Store, error) {
		sp.Close()
		return nil, err
	}
	if err := s.initMeta(nodeID, dbID); err != nil {
		return fail(err)
	}
	if s.bindSpoolContext {
		if err := sp.RebindContext(s.dbID); err != nil {
			return fail(fmt.Errorf("state: bind spool context: %w", err))
		}
		s.bindSpoolContext = false
	}
	if err := s.loadMergeRegistry(); err != nil {
		return fail(err)
	}
	if err := s.recoverPreparedRemote(); err != nil {
		return fail(fmt.Errorf("state: recover prepared remote transaction: %w", err))
	}
	if s.migratingOrigin {
		if err := s.finishOriginBaseline(); err != nil {
			return fail(err)
		}
	}
	if err := s.initStagedAccounting(); err != nil {
		return fail(fmt.Errorf("state: init staged accounting: %w", err))
	}
	return s, nil
}

// rejectLegacyStore refuses to open a directory holding a legacy Pebble
// database. There is no migration path: operators start a fresh
// database (optionally reseeding from a Spool backup or a peer).
func rejectLegacyStore(path string) error {
	paths := []string{path}
	if dir := filepath.Dir(path); dir != "" && dir != path && dir != "." && dir != "/" {
		paths = append(paths, dir)
	}
	for _, p := range paths {
		entries, err := os.ReadDir(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}
		for _, e := range entries {
			name := e.Name()
			switch {
			// Note: a bare LOCK file is not evidence; Spool locks its own
			// store directory the same way. Every non-empty Pebble
			// directory also carries CURRENT/MANIFEST/OPTIONS markers.
			case name == "CURRENT" || name == "OPTIONS" || name == "pebble" || name == "pebble-wal":
				return fmt.Errorf("state: %s holds a legacy Pebble database; start fresh (no migration path)", p)
			case strings.HasPrefix(name, "MANIFEST-") || strings.HasPrefix(name, "OPTIONS-"):
				return fmt.Errorf("state: %s holds a legacy Pebble database; start fresh (no migration path)", p)
			case strings.HasSuffix(name, ".sst") || (strings.HasSuffix(name, ".log") && !strings.Contains(name, "testnode")):
				return fmt.Errorf("state: %s holds a legacy Pebble database; start fresh (no migration path)", p)
			}
		}
	}
	return nil
}

func (s *Store) initMeta(nodeID ids.NodeID, dbID ids.DBID) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.mem.newBatch()
	defer b.Close()
	set := func(k, v []byte) error { return b.Set(k, v) }
	// Format version: format 5 belongs to the removed SQL runtime and is never
	// opened or rewritten by the RIME cutover. Older origin-policy formats
	// remain available only through their explicit offline migration options.
	raw, err := s.getDirect(SysKey(sysFormat))
	var storedFormat uint64
	if err == nil {
		v, ok := decodeU64(raw)
		if !ok || v < MinFormatVersion || v > FormatVersion {
			return fmt.Errorf("state: unsupported format version %d (want %d..%d)", v, MinFormatVersion, FormatVersion)
		}
		if v == 5 {
			return fmt.Errorf("state: format 5 belongs to the removed SQL runtime; start with a fresh directory")
		}
		if v == 4 {
			if !s.openOpt.MigrateMergePolicies {
				return fmt.Errorf("state: signed format 4 requires explicit MigrateMergePolicies")
			}
			for _, name := range []string{sysFormat, sysMinReader, sysMinWriter} {
				if err := set(SysKey(name), encodeU64(FormatVersion)); err != nil {
					return err
				}
			}
		}
		if v < 4 {
			if !s.openOpt.MigrateUnsignedBaseline {
				return fmt.Errorf("state: unsigned legacy store requires explicit MigrateOriginBaseline")
			}
			s.migratingOrigin = true
		}
		storedFormat = v
	} else if isNotFound(err) {
		storedFormat = FormatVersion
		if err := set(SysKey(sysFormat), encodeU64(FormatVersion)); err != nil {
			return err
		}
	} else {
		return err
	}
	// Minimum reader/writer compatibility. A store demanding a newer
	// reader or writer fails closed. Markers absent on pre-marker stores
	// default to the store's own format version.
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
			if err := set(SysKey(mk.name), encodeU64(storedFormat)); err != nil {
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
			s.bindSpoolContext = true
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
	if !s.migratingOrigin {
		adopting := false
		if old, e := s.getDirect(SysKey(sysLocalNode)); e == nil {
			adopting = !bytes.Equal(old, nodeID[:])
		}
		if err := s.pinOriginKey(b, adopting); err != nil {
			return err
		}
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
	return s.commitBatch(b, true)
}

// DBID returns the cluster identity.
func (s *Store) DBID() ids.DBID { return s.dbID }

// NodeID returns the local node identity.
func (s *Store) NodeID() ids.NodeID { return s.nodeID }

// ClockNow allocates the HLC timestamp for a local write.
func (s *Store) ClockNow() uint64 { return s.clock.Now() }

// ClockMax returns the highest issued/observed HLC timestamp.
func (s *Store) ClockMax() uint64 { return s.clock.Max() }

// Close closes Spool, waiting out any maintenance window.
func (s *Store) Close() error {
	s.gate.Lock()
	defer s.gate.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.spool.Close()
}

// CloseForMaintenance drains state operations and syncs the store for
// maintenance. The gate stays write-locked: every operation blocks
// until ReopenAfterMaintenance. Spool stays open (rotation and rewrite
// run online); on sync failure the gate is released and nothing is
// held. MaintenanceClosed distinguishes the two outcomes.
func (s *Store) CloseForMaintenance() error {
	s.gate.Lock()
	if err := s.spool.Sync(); err != nil {
		s.gate.Unlock()
		return s.noteTerminal("maintenance sync", err)
	}
	s.maintClosed.Store(true)
	return nil
}

// MaintenanceClosed reports whether the last CloseForMaintenance opened
// a maintenance window: true means ReopenAfterMaintenance is required
// before the store is usable again.
func (s *Store) MaintenanceClosed() bool { return s.maintClosed.Load() }

// ReopenAfterMaintenance verifies identity and resumes operations after
// a maintenance window. The gate stays held until it succeeds.
func (s *Store) ReopenAfterMaintenance() error {
	s.bindSpoolContext = false
	if err := s.initMeta(s.nodeID, s.dbID); err != nil {
		return fmt.Errorf("state: reopen verify: %w (maintenance gate still held)", err)
	}
	if s.bindSpoolContext {
		if err := s.spool.RebindContext(s.dbID); err != nil {
			return fmt.Errorf("state: reopen bind context: %w (maintenance gate still held)", err)
		}
		s.bindSpoolContext = false
	}
	s.maintClosed.Store(false)
	s.gate.Unlock()
	return nil
}

// Flush persists buffered writes (manual hook for tests and operations).
func (s *Store) Flush() error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return err
	}
	if err := s.spool.Flush(); err != nil {
		return s.noteTerminal("flush", err)
	}
	return nil
}

// Compact runs one manual reclamation pass: tombstone cleanup, dead-file
// deletion, and at most one compaction rewrite.
func (s *Store) Compact() error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return err
	}
	if err := s.spool.Reclaim(); err != nil {
		return s.noteTerminal("compact", err)
	}
	return nil
}

// Checkpoint captures a point-in-time copy of the store files into
// destDir (backup primitive). The copy is standalone: it includes the
// encrypted manifest, keyring, and segments.
func (s *Store) Checkpoint(destDir string) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return err
	}
	cp, err := s.spool.Checkpoint(context.Background(), destDir)
	if err != nil {
		return s.noteTerminal("checkpoint", err)
	}
	return cp.Release()
}

// StorageMetrics is a focused subset of storage stats for status.
type StorageMetrics struct {
	DiskBytes      uint64
	Keys           uint64
	PendingBytes   uint64
	PendingRecords uint64
	BlocksWritten  uint64
	BytesWritten   uint64
	Compactions    uint64
	StorageFailure bool
}

// Metrics returns current storage metrics.
func (s *Store) Metrics() StorageMetrics {
	s.gate.RLock()
	defer s.gate.RUnlock()
	m := s.spool.Stats()
	return StorageMetrics{
		DiskBytes:      m.DiskBytes,
		Keys:           m.Keys,
		PendingBytes:   m.PendingBytes,
		PendingRecords: m.PendingRecords,
		BlocksWritten:  m.BlocksWritten,
		BytesWritten:   m.BytesWritten,
		Compactions:    m.Compactions,
		StorageFailure: m.StorageFailures > 0,
	}
}

// getDirect reads one key straight from the DB. Writers call it under
// writeMu, where no concurrent commit can interleave.
// Failed reports the sticky fail-closed storage error, or nil while the
// store is healthy. Once non-nil it never clears: the node must restart
// after the operator resolves the underlying problem.
func (s *Store) Failed() error { return s.fatal.err() }

// failedErr is the entry gate: every storage-touching operation fails
// fast once the store has failed closed. Spool's terminal failure is
// authoritative: if Spool failed without tripping the capture yet (an
// error path that bypassed commitBatch), trip it here so the node
// fails closed instead of serving a diverged memory image.
func (s *Store) failedErr() error {
	if err := s.fatal.err(); err != nil {
		return err
	}
	if s.spool != nil {
		if serr := s.spool.StorageError(); serr != nil {
			s.fatal.noteTerminal("spool storage failure", serr)
			return s.fatal.err()
		}
	}
	return nil
}

// noteTerminal converts a Spool error into the sticky fail-closed error
// when Spool reports a terminal storage failure, and returns the
// original error otherwise. Failed commits publish nothing.
func (s *Store) noteTerminal(op string, err error) error {
	if s.spool != nil {
		if serr := s.spool.StorageError(); serr != nil {
			s.fatal.noteTerminal(op+": spool storage failure", serr)
			return s.fatal.err()
		}
	}
	return err
}

// commitBatch persists a batch through one atomic Spool commit and
// publishes the candidate root only after Spool acknowledges it.
// Async publication follows acceptance; synchronous publication follows
// durable acknowledgement. A failed commit publishes nothing and, when
// the failure is terminal, fails the store closed.
func (s *Store) commitBatch(b *batch, sync bool) error {
	if err := s.failedErr(); err != nil {
		return err
	}
	if len(b.muts) == 0 {
		return nil
	}
	candidate := b.candidate()
	d := spool.DurabilityAsync
	if sync {
		d = spool.DurabilitySync
	}
	n := int64(b.Len())
	if err := s.spool.Commit(b.muts, d); err != nil {
		return s.noteTerminal("commit", err)
	}
	s.mem.publish(candidate)
	if !sync {
		s.unsyncedBytes.Add(uint64(n))
	}
	return nil
}

// dbSet commits one key through the standard batch path plus the
// post-write fail-closed check.
func (s *Store) dbSet(key, value []byte, sync bool) error {
	if err := s.failedErr(); err != nil {
		return err
	}
	b := s.mem.newBatch()
	defer b.Close()
	if err := b.Set(key, value); err != nil {
		return err
	}
	return s.commitBatch(b, sync)
}

func (s *Store) getDirect(key []byte) ([]byte, error) {
	if err := s.failedErr(); err != nil {
		return nil, err
	}
	return s.mem.get(key)
}

// deletePrefixRange removes every key under prefix in bounded synchronous
// commits. Open-time use only: the caller holds writeMu and no other
// operations run concurrently, so the multi-commit clear is safe and a
// crash simply re-runs the idempotent clear on the next Open.
func (s *Store) deletePrefixRange(prefix []byte) error {
	const keysPerCommit = 20000
	upper := prefixEnd(prefix)
	for {
		it, err := s.mem.newIter(&iterOptions{LowerBound: prefix, UpperBound: upper})
		if err != nil {
			return err
		}
		var keys [][]byte
		for it.SeekGE(prefix); it.Valid() && len(keys) < keysPerCommit; it.Next() {
			keys = append(keys, append([]byte(nil), it.Key()...))
		}
		ierr := it.Error()
		it.Close()
		if ierr != nil {
			return ierr
		}
		if len(keys) == 0 {
			return nil
		}
		b := s.mem.newBatch()
		for _, k := range keys {
			if err := b.Delete(k); err != nil {
				b.Close()
				return err
			}
		}
		if err := s.commitBatch(b, true); err != nil {
			b.Close()
			return err
		}
		b.Close()
	}
}

func isNotFound(err error) bool { return errors.Is(err, errNotFound) }

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
func (s *Store) snapshot(fn func(snap *snapshot) error) error {
	if err := s.failedErr(); err != nil {
		return err
	}
	snap := s.mem.newSnapshot()
	defer snap.Close()
	return fn(snap)
}

func snapGet(snap *snapshot, key []byte) ([]byte, error) {
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

func readU64Snap(snap *snapshot, name string) (uint64, error) {
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
// and generation bump in one atomic Spool commit.
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
	batch.ProtocolVersion = 6
	if err := s.finalizeLocalPolicies(batch, make(map[string]*remoteGroupCell), new([]string)); err != nil {
		return MergeResult{}, err
	}
	if err := checkBatchLimits(batch, s.limits); err != nil {
		return MergeResult{}, err
	}
	if err := codec.SignOrigin(batch, s.dbID, s.openOpt.OriginSigning.PrivateKey); err != nil {
		return MergeResult{}, err
	}
	ver := batch.Version()
	b := s.mem.newBatch()
	defer b.Close()
	winners, err := s.mergeIntoBatch(b, batch, ver)
	if err != nil {
		return MergeResult{}, err
	}
	res.Winners = winners
	// Log under local origin.
	if err := b.Set(LogKey(s.nodeID, seq), codec.EncodeBatch(nil, batch)); err != nil {
		return MergeResult{}, err
	}
	// Receive watermark for our own origin advances with the log.
	if err := b.Set(RecvKey(s.nodeID), encodeU64(seq)); err != nil {
		return MergeResult{}, err
	}
	if err := b.Set(SysKey(sysLocalSeq), encodeU64(seq)); err != nil {
		return MergeResult{}, err
	}
	if err := b.Set(SysKey(sysHLC), encodeU64(maxU64(batch.HLC, s.clock.Max()))); err != nil {
		return MergeResult{}, err
	}
	var receipt [24]byte
	copy(receipt[:16], s.nodeID[:])
	binary.BigEndian.PutUint64(receipt[16:], seq)
	if err := b.Set(ReceiptKey(batch.TxID), receipt[:]); err != nil {
		return MergeResult{}, err
	}
	gen, err := s.readU64Direct(sysGeneration)
	if err != nil {
		return MergeResult{}, err
	}
	gen++
	if err := b.Set(SysKey(sysGeneration), encodeU64(gen)); err != nil {
		return MergeResult{}, err
	}
	if err := s.commitBatch(b, s.syncCommits); err != nil {
		return MergeResult{}, err
	}
	res.Applied = true
	res.Generation = gen
	return res, nil
}

// CommitRemote durably merges a batch received from a peer. The batch keeps
// its original origin/sequence identity for multi-origin forwarding.
func (s *Store) CommitRemote(_ context.Context, batch *codec.MutationBatch) (MergeResult, error) {
	if !s.migratingOrigin {
		if err := s.VerifyOrigin(batch); err != nil {
			return MergeResult{}, err
		}
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	if !s.migratingOrigin {
		if err := s.validatePolicyBatch(batch); err != nil {
			return MergeResult{}, err
		}
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
	if !s.migratingOrigin {
		if err := s.checkRemoteIdentity(batch); err != nil {
			return MergeResult{}, err
		}
	}
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
		b := s.mem.newBatch()
		defer b.Close()
		var receipt [24]byte
		copy(receipt[:16], batch.OriginNode[:])
		binary.BigEndian.PutUint64(receipt[16:], batch.Sequence)
		if err := b.Set(ReceiptKey(batch.TxID), receipt[:]); err != nil {
			return MergeResult{}, err
		}
		gen, err := s.readU64Direct(sysGeneration)
		if err != nil {
			return MergeResult{}, err
		}
		if err := s.commitBatch(b, s.syncCommits); err != nil {
			return MergeResult{}, err
		}
		res.Generation = gen
		return res, nil
	}
	if batch.Sequence > wm+1 {
		return MergeResult{}, fmt.Errorf("%w: origin %s want %d got %d", ErrGap, batch.OriginNode, wm+1, batch.Sequence)
	}
	encoded := codec.EncodeBatch(nil, batch)
	if s.migratingOrigin {
		encoded = append(append([]byte(nil), encoded[:94]...), encoded[codec.BatchHeaderSize:]...)
	}
	prepared, err := s.getDirect(SysKey(sysRemotePrepare))
	if err == nil {
		if !bytes.Equal(prepared, encoded) {
			return MergeResult{}, fmt.Errorf("state: another remote transaction requires recovery")
		}
	} else if isNotFound(err) {
		prepare := s.mem.newBatch()
		if err := prepare.Set(SysKey(sysRemotePrepare), encoded); err != nil {
			prepare.Close()
			return MergeResult{}, err
		}
		if err := s.commitBatch(prepare, true); err != nil {
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
	b := s.mem.newBatch()
	defer b.Close()
	ver := batch.Version()
	winners, err := s.mergeIntoBatch(b, batch, ver)
	if err != nil {
		return MergeResult{}, err
	}
	res.Winners = winners
	if err := b.Set(LogKey(batch.OriginNode, batch.Sequence), codec.EncodeBatch(nil, batch)); err != nil {
		return MergeResult{}, err
	}
	if err := b.Set(RecvKey(batch.OriginNode), encodeU64(batch.Sequence)); err != nil {
		return MergeResult{}, err
	}
	var receipt [24]byte
	copy(receipt[:16], batch.OriginNode[:])
	binary.BigEndian.PutUint64(receipt[16:], batch.Sequence)
	if err := b.Set(ReceiptKey(batch.TxID), receipt[:]); err != nil {
		return MergeResult{}, err
	}
	s.clock.Observe(batch.HLC)
	if err := b.Set(SysKey(sysHLC), encodeU64(maxU64(batch.HLC, s.clock.Max()))); err != nil {
		return MergeResult{}, err
	}
	gen, err := s.readU64Direct(sysGeneration)
	if err != nil {
		return MergeResult{}, err
	}
	gen++
	if err := b.Set(SysKey(sysGeneration), encodeU64(gen)); err != nil {
		return MergeResult{}, err
	}
	if err := b.Delete(SysKey(sysRemotePrepare)); err != nil {
		return MergeResult{}, err
	}
	if err := s.commitBatch(b, s.syncCommits); err != nil {
		return MergeResult{}, err
	}
	res.Applied = true
	res.Generation = gen
	return res, nil
}

// mergeIntoBatch LWW-merges every mutation against stored state, staging
// winner writes into b. Callers hold writeMu.
func (s *Store) mergeIntoBatch(b *batch, batch *codec.MutationBatch, ver crdt.Version) ([]WinningChange, error) {
	staged := make(map[string]*remoteGroupCell)
	var order []string
	if err := s.mergeRemoteGroupBatch(b, batch, staged, &order); err != nil {
		return nil, err
	}
	return writeStagedCells(b, staged, order)
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

func getCellSnap(snap *snapshot, limits codec.Limits, table uint32, row ids.RowID, col uint32) (codec.CellState, bool, error) {
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

func getTombSnap(snap *snapshot, table uint32, row ids.RowID) (crdt.Version, bool, error) {
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

func recvWatermarkSnap(snap *snapshot, origin ids.NodeID) (uint64, error) {
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
	err := s.snapshot(func(snap *snapshot) error {
		prefix := []byte{prefixRecv}
		it, err := snap.NewIter(&iterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
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
	err := s.snapshot(func(snap *snapshot) error {
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
	b := s.mem.newBatch()
	defer b.Close()
	if err := b.Set(SysKey(sysSchemaEpoch), encodeU64(epoch)); err != nil {
		return err
	}
	if err := b.Set(SysKey(sysSchemaHash), hash[:]); err != nil {
		return err
	}
	return s.commitBatch(b, true)
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
	b := s.mem.newBatch()
	defer b.Close()
	if err := b.Set(PeerAckKey(peer, origin), encodeU64(seq)); err != nil {
		return err
	}
	return s.commitBatch(b, s.syncCommits)
}

// AsyncDurability reports whether the store was opened in asynchronous durability mode.
func (s *Store) AsyncDurability() bool {
	return !s.syncCommits
}

// UnsyncedBytes estimates bytes committed without a sync (see
// unsyncedBytes). It drives the asynchronous size-triggered durability
// sync and is always zero in synchronous mode.
func (s *Store) UnsyncedBytes() uint64 {
	return s.unsyncedBytes.Load()
}

// Sync fences all previously committed transactions to durable storage.
func (s *Store) Sync() error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	before := s.unsyncedBytes.Load()
	if err := s.spool.Sync(); err != nil {
		return s.noteTerminal("sync", err)
	}
	if err := s.failedErr(); err != nil {
		return err
	}
	// Subtract only the pre-sync total so bytes appended concurrently
	// with the sync stay counted toward the next window. The counter only
	// grows between the load above and here, so this never underflows.
	s.unsyncedBytes.Add(-before)
	return nil
}

// Size returns the database's total disk usage in bytes.
func (s *Store) Size() (uint64, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return 0, err
	}
	return s.spool.Stats().DiskBytes, nil
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
	b := s.mem.newBatch()
	defer b.Close()
	k := PeerExcludedKey(node)
	if excluded {
		if err := b.Set(k, []byte{1}); err != nil {
			return err
		}
	} else {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return s.commitBatch(b, true)
}

// IsPeerExcluded reports whether the peer is locally excluded in persistent store.
func (s *Store) IsPeerExcluded(node ids.NodeID) (bool, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := s.failedErr(); err != nil {
		return false, err
	}
	_, err := s.mem.get(PeerExcludedKey(node))
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
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
	iter, err := s.mem.newIter(&iterOptions{
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
	iter, err := s.mem.newIter(&iterOptions{
		LowerBound: prefix,
		UpperBound: prefixEnd(prefix),
	})
	if err != nil {
		return err
	}
	defer iter.Close()
	b := s.mem.newBatch()
	defer b.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := b.Delete(iter.Key()); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return s.commitBatch(b, true)
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
			for _, r := range m.Records {
				if len(r.Key) > lim.MaxValueBytes || len(r.Data) > lim.MaxValueBytes {
					return fmt.Errorf("state: CRDT record exceeds value limit: %w", ErrTooBig)
				}
			}
			if lim.MaxMutations > 0 && len(m.Records) > lim.MaxMutations {
				return fmt.Errorf("state: CRDT record count exceeds limit: %w", ErrTooBig)
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
// The store serializes writers, so commits never conflict; this is
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
	if !isNotFound(err) {
		return false, err
	}
	_, err = s.getDirect(LocalReceiptKey(txID))
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
	// Administrative receipt recording must not overwrite an authenticated
	// mesh or atomic bridge receipt with a local placeholder.
	if _, err := s.getDirect(ReceiptKey(txID)); err == nil {
		return nil
	} else if !isNotFound(err) {
		return err
	}
	var receipt [24]byte
	copy(receipt[:16], s.nodeID[:])
	return s.dbSet(ReceiptKey(txID), receipt[:], s.syncCommits)
}

// SetBridgeStreamProgress records the highest contiguous applied sequence for a bridge stream.
func (s *Store) SetBridgeStreamProgress(stream string, applied uint64) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.dbSet(BridgeProgressKey(stream), encodeU64(applied), s.syncCommits)
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
