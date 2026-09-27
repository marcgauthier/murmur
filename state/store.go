package state

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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
	// Logger receives Pebble logs (Info maps to Debug). Nil discards.
	Logger Logger
}

// Logger mirrors the root Logger to avoid an import cycle.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type pebbleLogAdapter struct{ l Logger }

func (a pebbleLogAdapter) Infof(f string, v ...any)    { a.l.Debug(fmt.Sprintf(f, v...)) }
func (a pebbleLogAdapter) Warningf(f string, v ...any) { a.l.Warn(fmt.Sprintf(f, v...)) }
func (a pebbleLogAdapter) Errorf(f string, v ...any)   { a.l.Error(fmt.Sprintf(f, v...)) }
func (a pebbleLogAdapter) Fatalf(f string, v ...any)   { a.l.Error(fmt.Sprintf(f, v...)) }

type discardPebbleLog struct{}

func (discardPebbleLog) Infof(string, ...any)    {}
func (discardPebbleLog) Warningf(string, ...any) {}
func (discardPebbleLog) Errorf(string, ...any)   {}
func (discardPebbleLog) Fatalf(string, ...any)   {}

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

	openPath string
	openOpt  Options
}

// Open opens (or creates) the store. nodeID must match any stored identity;
// dbID zero loads the stored id, nonzero must match or initialize.
func Open(path string, nodeID ids.NodeID, dbID ids.DBID, opt Options) (*Store, error) {
	fs := opt.FS
	if fs == nil {
		fs = vfs.Default
	}
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
	}
	if opt.Logger == nil {
		popt.Logger = discardPebbleLog{}
	} else {
		popt.Logger = pebbleLogAdapter{opt.Logger}
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
	s := &Store{db: db, limits: opt.Limits, nodeID: nodeID, openPath: path, openOpt: opt}
	if s.limits.MaxValueBytes == 0 {
		s.limits = codec.DefaultLimits()
	}
	if err := s.initMeta(nodeID, dbID); err != nil {
		db.Close()
		return nil, err
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
	// Node identity.
	if raw, err := s.getDirect(SysKey(sysLocalNode)); err == nil {
		if len(raw) != 16 || string(raw) != string(nodeID[:]) {
			return fmt.Errorf("state: data directory belongs to another node")
		}
	} else if isNotFound(err) {
		if err := set(SysKey(sysLocalNode), nodeID[:]); err != nil {
			return err
		}
	} else {
		return err
	}
	// Cluster identity.
	if raw, err := s.getDirect(SysKey(sysDBID)); err == nil {
		if len(raw) != 16 {
			return fmt.Errorf("state: corrupt db id")
		}
		copy(s.dbID[:], raw)
		if !dbID.IsZero() && s.dbID != dbID {
			return fmt.Errorf("state: db id mismatch")
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
	return b.Commit(pebble.Sync)
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
	}
	if s.openOpt.Logger == nil {
		popt.Logger = discardPebbleLog{}
	} else {
		popt.Logger = pebbleLogAdapter{s.openOpt.Logger}
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
	s.maintClosed.Store(false)
	s.gate.Unlock()
	return nil
}

// Flush flushes the memtable (manual hook for tests and operations).
func (s *Store) Flush() error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.db.Flush()
}

// Compact runs a manual compaction over [start, end).
func (s *Store) Compact(ctx context.Context, start, end []byte, parallelize bool) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.db.Compact(ctx, start, end, parallelize)
}

// Checkpoint snapshots the database files into destDir (backup primitive;
// the key registry must be copied alongside by the caller).
func (s *Store) Checkpoint(destDir string) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
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
func (s *Store) getDirect(key []byte) ([]byte, error) {
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
	if err := checkValueSizes(batch, s.limits.MaxValueBytes); err != nil {
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
	if err := b.Commit(pebble.Sync); err != nil {
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
	if err := checkValueSizes(batch, s.limits.MaxValueBytes); err != nil {
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
	b := s.db.NewBatch()
	defer b.Close()
	switch {
	case batch.Sequence <= wm:
		// Already have this sequence (redelivery under a new TxID):
		// record the receipt so we recognize it, but do not reapply.
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
		if err := b.Commit(pebble.Sync); err != nil {
			return MergeResult{}, err
		}
		res.Generation = gen
		return res, nil
	case batch.Sequence > wm+1:
		return MergeResult{}, fmt.Errorf("%w: origin %s want %d got %d",
			ErrGap, batch.OriginNode, wm+1, batch.Sequence)
	}
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
	if err := b.Commit(pebble.Sync); err != nil {
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

// MaterializedGeneration returns the last generation applied to the query engine.
func (s *Store) MaterializedGeneration() (uint64, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.readU64Direct(sysMaterial)
}

// SetMaterializedGeneration records the query engine's generation.
func (s *Store) SetMaterializedGeneration(gen uint64) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(SysKey(sysMaterial), encodeU64(gen), nil); err != nil {
		return err
	}
	return b.Commit(pebble.Sync)
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
	return b.Commit(pebble.Sync)
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
	return b.Commit(pebble.Sync)
}

// Size returns the database's total disk usage in bytes.
func (s *Store) Size() (uint64, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.db.Metrics().DiskSpaceUsage(), nil
}

func checkValueSizes(batch *codec.MutationBatch, max int) error {
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
		if n > max {
			return fmt.Errorf("state: mutation %d value of %d bytes exceeds %d: %w",
				i, n, max, ErrTooBig)
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
