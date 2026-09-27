package replicateddb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/nomadsql/replicateddb/backup"
	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/crypto"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/replication"
	"github.com/nomadsql/replicateddb/schema"
	"github.com/nomadsql/replicateddb/sqlengine"
	"github.com/nomadsql/replicateddb/state"
	"github.com/nomadsql/replicateddb/transport"
)

// DB is an embedded replicated database. All methods are safe for concurrent
// use; local writes are serialized by an internal coordinator.
type DB struct {
	cfg Config
	log Logger
	reg *schema.Registry

	store  *state.Store
	engine *sqlengine.Engine
	repl   *replication.Manager

	keyReg *crypto.Registry
	encMgr *crypto.Manager

	backupWorker *backup.Worker
	backupMu     sync.Mutex

	replCreds *transport.Credentials
	replDone  chan struct{}

	mu      sync.Mutex
	dbState DBState
	// encPhase is the encryption phase (idle, rotating, rewriting,
	// recovering). storeUsable is false across the maintenance close
	// window (and after a failed reopen); Status serves cached store
	// fields while false.
	encPhase    string
	storeUsable bool
	// lastStatus caches store-dependent Status fields for maintenance.
	lastStatus   Status
	lastStatusOK bool

	writeMu sync.Mutex // serialized local write coordinator
	applyMu sync.Mutex // serializes all durable commits + materialization

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	openedAt time.Time

	crash *crashHooks // failure injection; nil in production
}

// Open opens or creates the database, rebuilds the in-memory query database
// from durable state, and starts replication and maintenance workers.
func Open(ctx context.Context, cfg Config) (*DB, error) {
	cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	reg, err := schema.BuildRegistry(cfg.Schema.Version, cfg.Schema.Tables)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnsupportedSchema, err)
	}
	db := &DB{cfg: cfg, log: cfg.Logger, reg: reg, dbState: StateOpening, openedAt: time.Now()}
	db.ctx, db.cancel = context.WithCancel(context.Background())

	if err := db.recoverRotationLeftovers(); err != nil {
		return nil, err
	}
	// Mandatory at-rest encryption: storage key, registry, encrypted VFS,
	// and rotation/rewrite manager. Encryption is not optional.
	storageProvider := cfg.Encryption.storageProvider()
	mat, err := storageProvider.Current(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: storage key: %w", ErrEncryptionKey, err)
	}
	if verr := checkOpenKeyMaterial(mat); verr != nil {
		crypto.Zero(mat.Key)
		return nil, fmt.Errorf("%w: %w", ErrEncryptionKey, verr)
	}
	crypto.Zero(mat.Key)

	regDir := filepath.Join(cfg.Path, "keys")
	dataPath := filepath.Join(cfg.Path, "data")
	var dID [16]byte
	copy(dID[:], cfg.DBID[:])
	_, statErr := os.Stat(filepath.Join(regDir, crypto.RegistryFileName))
	registryExisted := statErr == nil
	keyReg, err := crypto.OpenRegistry(regDir, storageProvider, dID)
	if err != nil {
		return nil, fmt.Errorf("%w: open key registry: %w", ErrEncryptionKey, err)
	}
	// The persisted write algorithm is authoritative: a fresh registry
	// adopts the configured algorithm, a reopen must agree with it.
	writeAlg := cfg.Encryption.writeAlgorithm()
	if !registryExisted {
		if err := keyReg.SetDefaultAlgorithm(ctx, writeAlg); err != nil {
			keyReg.Close()
			return nil, fmt.Errorf("%w: set write algorithm: %w", ErrEncryptionKey, err)
		}
	} else if got := keyReg.DefaultAlgorithm(); got != writeAlg {
		keyReg.Close()
		return nil, fmt.Errorf("%w: configured algorithm %s disagrees with persisted %s",
			ErrEncryptionKey, writeAlg, got)
	}
	db.keyReg = keyReg
	db.encPhase = crypto.PhaseIdle

	efs, err := crypto.NewEncryptedFS(crypto.FSOptions{
		Base:     vfs.Default,
		Registry: keyReg,
		DBID:     dID,
		Logger:   cfg.Logger,
	})
	if err != nil {
		db.closeStore()
		return nil, fmt.Errorf("crypto: new encrypted fs: %w", err)
	}
	encMgr, err := crypto.NewManager(crypto.ManagerOptions{
		FS:             efs,
		Roots:          []string{dataPath},
		MaxKeyLifetime: cfg.Encryption.DataKeyRotation,
	})
	if err != nil {
		db.closeStore()
		return nil, fmt.Errorf("crypto: new encryption manager: %w", err)
	}
	db.encMgr = encMgr
	efs.SetRotationCheck(encMgr.MaybeRotateOnExpiry)
	// Resume an interrupted file rewrite before Pebble opens: the journal
	// requires a closed store, which holds this early in Open.
	if encMgr.HasJournal() {
		db.encPhase = crypto.PhaseRecovering
		if err := encMgr.ResumeRewrite(ctx); err != nil {
			db.closeStore()
			return nil, fmt.Errorf("%w: resume rewrite: %w", ErrEncryptionKey, err)
		}
		db.encPhase = crypto.PhaseIdle
	}
	var compProfile *sstable.CompressionProfile
	switch cfg.Pebble.Compression.Algorithm {
	case CompressionNone:
		compProfile = block.NoCompression
	case CompressionSnappy:
		compProfile = block.CompressionProfileByName("snappy")
	case CompressionZstd:
		compProfile = block.CompressionProfileByName("zstd")
	}

	pebbleFS := vfs.FS(efs)
	store, err := state.Open(dataPath, cfg.NodeID, cfg.DBID, state.Options{
		FS:                          pebbleFS,
		CacheBytes:                  cfg.Pebble.CacheBytes,
		MemTableSize:                cfg.Pebble.MemTableBytes,
		MemTableStopWritesThreshold: cfg.Pebble.MemTableCount,
		MaxOpenFiles:                cfg.Pebble.MaxOpenFiles,
		CompactionConcurrency:       cfg.Pebble.MaxConcurrentCompactions,
		Compression:                 compProfile,
		Limits:                      codec.Limits{MaxValueBytes: cfg.MaxReplicatedValueBytes, MaxMutations: cfg.MaxBatchMutations},
		Logger:                      cfg.Logger,
	})
	if err != nil {
		if db.keyReg != nil {
			db.keyReg.Close()
		}
		db.cancel()
		return nil, err
	}
	db.store = store
	db.storeUsable = true
	// Schema epoch/hash must match exactly (v1: no online migration).
	if epoch, hash, err := store.SchemaEpoch(); err != nil {
		db.closeStore()
		db.cancel()
		return nil, err
	} else if epoch == 0 && hash == ([32]byte{}) {
		if err := store.SetSchemaEpoch(reg.Epoch, reg.Hash); err != nil {
			db.closeStore()
			db.cancel()
			return nil, err
		}
	} else if epoch != reg.Epoch || hash != reg.Hash {
		db.closeStore()
		db.cancel()
		return nil, fmt.Errorf("%w: stored epoch %d != %d", ErrSchemaMismatch, epoch, reg.Epoch)
	}
	engine, err := sqlengine.Open(reg, cfg.Schema.DDL, cfg.Schema.LocalDDL, cfg.Cache.StatementCacheEntries)
	if err != nil {
		db.closeStore()
		db.cancel()
		return nil, err
	}
	db.engine = engine

	db.setState(StateRebuilding)
	db.applyMu.Lock()
	if err := engine.Rebuild(store); err != nil {
		db.applyMu.Unlock()
		db.setState(StateFailed)
		_ = engine.Close()
		db.closeStore()
		db.cancel()
		return nil, fmt.Errorf("replicateddb: rebuild: %w", err)
	}
	if gen, err := store.StateGeneration(); err != nil {
		db.applyMu.Unlock()
		db.setState(StateFailed)
		_ = engine.Close()
		db.closeStore()
		db.cancel()
		return nil, err
	} else if err := store.SetMaterializedGeneration(gen); err != nil {
		db.applyMu.Unlock()
		db.setState(StateFailed)
		_ = engine.Close()
		db.closeStore()
		db.cancel()
		return nil, err
	}
	db.applyMu.Unlock()

	// Replication (optional; requires TLS credentials).
	if cfg.Replication.ListenAddr != "" || len(cfg.Replication.Peers) > 0 {
		mgr, err := db.newReplicationManager(nil)
		if err != nil {
			db.Close()
			return nil, err
		}
		db.repl = mgr
	}

	// Rotate on open when the active data key already expired; a failure
	// is non-fatal (availability first, the expiry worker retries).
	if err := db.encMgr.MaybeRotateOnExpiry(); err != nil {
		db.log.Warn("replicateddb: data-key rotation on open failed", "err", err.Error())
	}

	db.setState(StateReady)
	if db.repl != nil {
		db.startReplication(db.repl)
	}
	db.wg.Add(1)
	go db.gcLoop()
	db.wg.Add(1)
	go db.valueGCloop()
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		db.encMgr.RunExpiryWorker(db.ctx)
	}()

	if cfg.Backup.Enabled && cfg.Backup.Destination != nil {
		db.backupWorker = backup.NewWorker(backup.ScheduleConfig{
			Enabled:       true,
			Interval:      cfg.Backup.Interval,
			Destination:   cfg.Backup.Destination,
			Compression:   cfg.Backup.Compression,
			RetentionDays: cfg.Backup.RetentionDays,
			MaxBackups:    cfg.Backup.MaxBackups,
			Logger:        cfg.Logger,
		}, db)
		if err := db.backupWorker.Start(db.ctx); err != nil {
			db.log.Warn("replicateddb: start backup worker failed", "err", err.Error())
		}
	}

	return db, nil
}

func (db *DB) closeStore() {
	if db.store != nil {
		_ = db.store.Close()
		db.store = nil
	}
	if db.keyReg != nil {
		db.keyReg.Close()
		db.keyReg = nil
	}
}

// --- state ---

func (db *DB) setState(s DBState) {
	db.mu.Lock()
	db.dbState = s
	db.mu.Unlock()
}

func (db *DB) getState() DBState {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.dbState
}

func (db *DB) requireWrite() error {
	switch db.getState() {
	case StateReady:
		return nil
	case StateClosed, StateClosing:
		return ErrClosed
	case StateMaterializerDirty:
		return ErrMaterializerDirty
	case StateMaintenance:
		return ErrMaintenance
	default:
		return ErrNotReady
	}
}

func (db *DB) requireRead() error {
	// Reads serve the in-memory materialization, so they stay available
	// during rotation and maintenance; Status reports the state explicitly.
	switch db.getState() {
	case StateReady, StateRotatingKey, StateMaintenance:
		return nil
	case StateClosed, StateClosing:
		return ErrClosed
	case StateMaterializerDirty:
		return ErrMaterializerDirty
	default:
		return ErrNotReady
	}
}

// --- writes ---

// ExecContext executes a statement. Writes run as an implicit transaction:
// SQL COMMIT, then one atomic Pebble commit; success is acknowledged only
// after Pebble durability. Read-only statements are executed directly.
func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if isReadOnlyStatement(query) {
		if err := db.requireRead(); err != nil {
			return nil, err
		}
		rows, err := db.engine.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		_ = rows.Close()
		return sqlengine.EmptyResult{}, nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return res, nil
}

// QueryContext runs a read. Rows must be closed promptly; an open Rows
// stalls writers.
func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	if err := db.requireRead(); err != nil {
		return nil, err
	}
	rows, err := db.engine.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Rows{rows: rows}, nil
}

// QueryRowContext runs a single-row read.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *Row {
	return &Row{db: db, ctx: ctx, query: query, args: args}
}

// PrepareContext returns a statement handle. Statements execute through the
// normal implicit-transaction paths; Close is a no-op (the engine owns a
// shared prepared-statement cache).
func (db *DB) PrepareContext(_ context.Context, query string) (*Stmt, error) {
	if err := db.requireRead(); err != nil {
		return nil, err
	}
	return &Stmt{db: db, query: query}, nil
}

// BeginTx starts an explicit local transaction. The serialized write
// coordinator is held until Commit or Rollback.
func (db *DB) BeginTx(_ context.Context, _ *TxOptions) (*Tx, error) {
	if err := db.requireWrite(); err != nil {
		return nil, err
	}
	db.writeMu.Lock()
	if err := db.requireWrite(); err != nil {
		db.writeMu.Unlock()
		return nil, err
	}
	stx, err := db.engine.Begin(context.Background())
	if err != nil {
		db.writeMu.Unlock()
		return nil, err
	}
	return &Tx{db: db, stx: stx, txID: ids.NewTxID()}, nil
}

// crashHooks injects failures at commit boundaries. It is nil in production;
// white-box crash tests set it to prove every boundary converges to durable state
// authoritative state.
type crashHooks struct {
	beforeSQLCommit   func() error
	afterSQLCommit    func() error
	beforeDurable     func() error
	afterDurable      func() error
	remoteMaterialize func() error
}

// fireCrash runs one crash hook (nil-safe).
func (db *DB) fireCrash(sel func(*crashHooks) func() error) error {
	if db.crash == nil {
		return nil
	}
	if fn := sel(db.crash); fn != nil {
		return fn()
	}
	return nil
}

// commitTx implements SQL COMMIT -> Pebble COMMIT ordering for explicit and
// implicit transactions. writeMu is held by the caller.
func (db *DB) commitTx(tx *Tx) error {
	// Validate/coalesce before SQL COMMIT so oversize transactions roll back
	// cleanly instead of dirtying the materializer.
	delta := sqlengine.NewDelta()
	for _, ev := range tx.stx.Pending() {
		if err := delta.Add(ev); err != nil {
			_ = tx.stx.Rollback()
			return err
		}
	}
	mutations, err := delta.Build(db.cfg.MaxReplicatedValueBytes)
	if err != nil {
		_ = tx.stx.Rollback()
		return fmt.Errorf("%w: %w", ErrValueTooLarge, err)
	}
	if len(mutations) > db.cfg.MaxBatchMutations {
		_ = tx.stx.Rollback()
		return fmt.Errorf("%w: %d mutations", ErrBatchTooLarge, len(mutations))
	}
	if err := db.fireCrash(func(h *crashHooks) func() error { return h.beforeSQLCommit }); err != nil {
		_ = tx.stx.Rollback()
		return err
	}
	// Generation probe: remote commits between here and our Pebble commit
	// may have touched the same SQL rows (delete/resurrect races), in which
	// case the touched rows are repaired below.
	genBefore, _ := db.store.StateGeneration()
	if _, err := tx.stx.Commit(); err != nil {
		return err // SQL rejected; nothing durable (capture discarded)
	}
	if err := db.fireCrash(func(h *crashHooks) func() error { return h.afterSQLCommit }); err != nil {
		// SQL committed but durability did not: recover via rebuild.
		db.applyMu.Lock()
		defer db.applyMu.Unlock()
		if rerr := db.rebuildLocked(); rerr != nil {
			db.log.Error("rebuild after failed commit failed", "err", rerr.Error())
		}
		return err
	}
	if len(mutations) == 0 {
		return nil // no net change; nothing to replicate
	}
	batch := &codec.MutationBatch{
		ProtocolVersion: replication.ProtocolVersion,
		TxID:            tx.txID,
		OriginNode:      db.cfg.NodeID,
		HLC:             db.store.ClockNow(),
		SchemaEpoch:     db.reg.Epoch,
		Mutations:       mutations,
	}
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	if err := db.fireCrash(func(h *crashHooks) func() error { return h.beforeDurable }); err != nil {
		if rerr := db.rebuildLocked(); rerr != nil {
			db.log.Error("rebuild after failed commit failed", "err", rerr.Error())
		}
		return err
	}
	res, err := db.store.CommitLocal(context.Background(), batch)
	if err != nil {
		// SQL is ahead of durable state: mark dirty, rebuild, report.
		if rerr := db.rebuildLocked(); rerr != nil {
			db.log.Error("rebuild after failed commit failed", "err", rerr.Error())
		}
		if errors.Is(err, state.ErrTooBig) {
			return fmt.Errorf("%w: %w", ErrBatchTooLarge, err)
		}
		return err
	}
	gen, err := db.store.StateGeneration()
	if err != nil {
		return err
	}
	_ = res
	if gen != genBefore+1 {
		// Concurrent durable commits interleaved with our SQL commit and
		// may have deleted/resurrected our rows in SQL: reconcile the
		// touched rows with durable visibility before acknowledging.
		seen := make(map[sqlengine.RowKey]bool, len(mutations))
		var touched []sqlengine.RowKey
		for i := range mutations {
			k := sqlengine.RowKey{TableID: mutations[i].TableID, RowID: mutations[i].RowID}
			if !seen[k] {
				seen[k] = true
				touched = append(touched, k)
			}
		}
		if err := db.engine.RepairRows(db.store, touched); err != nil {
			db.log.Warn("local repair failed; rebuilding materializer", "err", err.Error())
			if rerr := db.rebuildLocked(); rerr != nil {
				return rerr
			}
			if gen, err = db.store.StateGeneration(); err != nil {
				return err
			}
		}
	}
	if err := db.store.SetMaterializedGeneration(gen); err != nil {
		return err
	}
	if err := db.fireCrash(func(h *crashHooks) func() error { return h.afterDurable }); err != nil {
		// Ambiguous commit: durable and materialized, acknowledgement lost.
		if repl := db.replManager(); repl != nil {
			repl.NotifyLocal()
		}
		return fmt.Errorf("%w: %w", ErrAmbiguousCommit, err)
	}
	if repl := db.replManager(); repl != nil {
		repl.NotifyLocal()
	}
	return nil
}

// rebuildLocked rebuilds the query database from durable state. applyMu held.
func (db *DB) rebuildLocked() error {
	db.setState(StateMaterializerDirty)
	defer db.setState(StateReady)
	if err := db.engine.Rebuild(db.store); err != nil {
		db.setState(StateFailed)
		return err
	}
	gen, err := db.store.StateGeneration()
	if err != nil {
		db.setState(StateFailed)
		return err
	}
	if err := db.store.SetMaterializedGeneration(gen); err != nil {
		db.setState(StateFailed)
		return err
	}
	return nil
}

// --- replication applier ---

// ApplyRemote durably merges a received batch and materializes winners.
// It satisfies replication.Applier. Acknowledgements are sent by the
// replication manager only after this returns nil.
func (db *DB) ApplyRemote(ctx context.Context, batch *codec.MutationBatch) error {
	if st := db.getState(); st == StateClosed || st == StateClosing || st == StateFailed {
		return fmt.Errorf("replicateddb: apply in state %s", st)
	}
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	res, err := db.store.CommitRemote(ctx, batch)
	if err != nil {
		return err
	}
	if err := db.fireCrash(func(h *crashHooks) func() error { return h.remoteMaterialize }); err != nil {
		// Simulated materialization failure: durable state is correct, rebuild.
		db.log.Warn("remote apply failed; rebuilding materializer", "err", err.Error())
		if rerr := db.rebuildLocked(); rerr != nil {
			db.log.Error("rebuild failed", "err", rerr.Error())
		}
		return nil
	}
	if len(res.Winners) > 0 {
		if err := db.engine.ApplyWinners(db.store, res.Winners); err != nil {
			// Durable state is correct; the materializer is stale. Rebuild inline
			// (we hold applyMu) and ack anyway: the batch is durable.
			db.log.Warn("remote apply failed; rebuilding materializer", "err", err.Error())
			if rerr := db.rebuildLocked(); rerr != nil {
				db.log.Error("rebuild failed", "err", rerr.Error())
			}
			return nil
		}
	}
	return db.store.SetMaterializedGeneration(res.Generation)
}

// ApplySnapshotChunk merges one snapshot chunk and materializes winners.
func (db *DB) ApplySnapshotChunk(ctx context.Context, manifest *codec.SnapshotManifest, cells []codec.SnapshotCell, _ bool) error {
	if st := db.getState(); st == StateClosed || st == StateClosing || st == StateFailed {
		return fmt.Errorf("replicateddb: snapshot apply in state %s", st)
	}
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	res, err := db.store.ImportSnapshotChunk(ctx, manifest, cells)
	if err != nil {
		return err
	}
	if len(res.Winners) > 0 {
		if err := db.engine.ApplyWinners(db.store, res.Winners); err != nil {
			db.log.Warn("snapshot apply failed; rebuilding materializer", "err", err.Error())
			if rerr := db.rebuildLocked(); rerr != nil {
				db.log.Error("rebuild failed", "err", rerr.Error())
			}
			return nil
		}
	}
	return db.store.SetMaterializedGeneration(res.Generation)
}

// --- peers ---

// AddPeer adds or updates a replication peer.
func (db *DB) AddPeer(_ context.Context, peer Peer) error {
	repl := db.replManager()
	if repl == nil {
		return fmt.Errorf("replicateddb: replication not configured")
	}
	repl.AddPeer(peer.NodeID, peer.Addrs)
	return nil
}

// RemovePeer retires a replication peer.
func (db *DB) RemovePeer(_ context.Context, nodeID NodeID) error {
	repl := db.replManager()
	if repl == nil {
		return fmt.Errorf("replicateddb: replication not configured")
	}
	repl.RemovePeer(nodeID)
	return nil
}

// ForceSync triggers an immediate sync round with the peer.
func (db *DB) ForceSync(_ context.Context, nodeID NodeID) error {
	repl := db.replManager()
	if repl == nil {
		return fmt.Errorf("replicateddb: replication not configured")
	}
	repl.ForceSync(nodeID)
	return nil
}

// --- status ---

// Status returns a diagnostic snapshot.
func (db *DB) Status() Status {
	db.mu.Lock()
	usable := db.storeUsable
	cached, ok := db.lastStatus, db.lastStatusOK
	db.mu.Unlock()
	if !usable && ok {
		// Maintenance window (or after Close): serve the snapshot with a
		// fresh state and uptime; the store handle is closed.
		cached.State = db.getState()
		cached.Uptime = time.Since(db.openedAt)
		return cached
	}
	return db.statusLive()
}

// statusLive reads the open store. Callers must ensure the store is usable.
func (db *DB) statusLive() Status {
	st := Status{
		State:  db.getState(),
		NodeID: db.cfg.NodeID,
		Uptime: time.Since(db.openedAt),
	}
	if db.store == nil {
		return st
	}
	st.DBID = db.store.DBID()
	st.HLC = db.store.ClockMax()
	if v, err := db.store.LocalSeq(); err == nil {
		st.LocalSeq = v
	}
	if v, err := db.store.StateGeneration(); err == nil {
		st.StateGeneration = v
	}
	if v, err := db.store.MaterializedGeneration(); err == nil {
		st.MaterializedGeneration = v
	}
	if e, h, err := db.store.SchemaEpoch(); err == nil {
		st.SchemaEpoch = e
		st.SchemaHash = h
	}
	if v, err := db.store.Size(); err == nil {
		st.PebbleSizeBytes = v
	}
	if repl := db.replManager(); repl != nil {
		for _, p := range repl.PeerStatus() {
			st.PeerCount++
			if p.Connected {
				st.ConnectedPeers++
			}
		}
	}
	return st
}

// replManager returns the live replication manager (nil when unconfigured
// or across the maintenance stop window). db.repl is only mutated under
// db.mu once Open has published the DB.
func (db *DB) replManager() *replication.Manager {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.repl
}

// NodeID returns the local node identity.
func (db *DB) NodeID() NodeID { return db.cfg.NodeID }

// DBID returns the cluster identity (zero before Open completes).
func (db *DB) DBID() DBID {
	if db.store == nil {
		return DBID{}
	}
	return db.store.DBID()
}

// --- maintenance workers ---

func (db *DB) gcLoop() {
	defer db.wg.Done()
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	round := 0
	for {
		select {
		case <-db.ctx.Done():
			return
		case <-t.C:
			round++
			db.gcOnce(round%10 == 0)
		}
	}
}

func (db *DB) gcOnce(withReceipts bool) {
	if db.getState() != StateReady {
		return
	}
	origins, err := db.store.KnownOrigins()
	if err != nil {
		return
	}
	// Peers seen within the offline window gate log GC; older/absent peers
	// must snapshot-resync.
	var gating []NodeID
	lastSeen := make(map[NodeID]time.Time)
	if repl := db.replManager(); repl != nil {
		for _, p := range repl.PeerStatus() {
			lastSeen[p.NodeID] = p.LastSeen
		}
		for _, p := range repl.ConfiguredPeers() {
			if ls := lastSeen[p.NodeID]; !ls.IsZero() && time.Since(ls) < db.cfg.Replication.MaxOfflineLogRetention {
				gating = append(gating, p.NodeID)
			}
		}
	}
	floors := make(map[NodeID]uint64, len(origins))
	cutoff := time.Now().Add(-db.cfg.Replication.MinLogRetention).UnixMilli()
	for _, origin := range origins {
		floor := ^uint64(0)
		for _, peer := range gating {
			ack, err := db.store.PeerAck(peer, origin)
			if err != nil || ack < floor {
				if err == nil {
					floor = ack
				} else {
					floor = 0
				}
			}
		}
		if _, err := db.store.CollectLog(origin, floor, cutoff, db.cfg.Replication.MinRetainedBatches); err != nil {
			db.log.Debug("log GC failed", "origin", origin.String(), "err", err.Error())
		}
		floors[origin] = floor
	}
	if withReceipts {
		if _, err := db.store.CollectReceipts(floors); err != nil {
			db.log.Debug("receipt GC failed", "err", err.Error())
		}
	}
}

func (db *DB) valueGCloop() {
	defer db.wg.Done()
	// Pebble automatically compacts SSTables in background; valueGCloop is kept for loop draining.
	<-db.ctx.Done()
}

// newReplicationManager builds a manager over the current store. extraPeers
// (used after key rotation) are added to the configured set.
func (db *DB) newReplicationManager(extraPeers []replication.PeerInfo) (*replication.Manager, error) {
	if db.cfg.Replication.TLS == nil {
		return nil, fmt.Errorf("replicateddb: replication requires TLS credentials")
	}
	if db.replCreds == nil {
		creds, err := transport.CredentialsFromPEM(
			db.cfg.Replication.TLS.CertPEM, db.cfg.Replication.TLS.KeyPEM, db.cfg.Replication.TLS.CAPEM,
			db.cfg.Replication.AllowedPeers)
		if err != nil {
			return nil, err
		}
		db.replCreds = creds
	}
	seen := make(map[NodeID]bool)
	var peers []replication.PeerInfo
	for _, p := range db.cfg.Replication.Peers {
		peers = append(peers, replication.PeerInfo{NodeID: p.NodeID, Addrs: p.Addrs})
		seen[p.NodeID] = true
	}
	for _, p := range extraPeers {
		if !seen[p.NodeID] {
			peers = append(peers, p)
			seen[p.NodeID] = true
		}
	}
	return replication.NewManager(replication.ManagerConfig{
		Store:              db.store,
		Applier:            db,
		Creds:              db.replCreds,
		Local:              db.cfg.NodeID,
		DBID:               db.store.DBID(),
		SchemaEpoch:        db.reg.Epoch,
		SchemaHash:         db.reg.Hash,
		ListenAddr:         db.cfg.Replication.ListenAddr,
		Peers:              peers,
		MaxBatchBytes:      db.cfg.Replication.MaxBatchBytes,
		MaxBatchMutations:  db.cfg.Replication.MaxBatchMutations,
		SendInterval:       db.cfg.Replication.SendInterval,
		DialInterval:       db.cfg.Replication.DialInterval,
		AckInterval:        db.cfg.Replication.AckInterval,
		SnapshotChunkCells: db.cfg.Replication.SnapshotChunkCells,
		Limits:             codec.Limits{MaxValueBytes: db.cfg.MaxReplicatedValueBytes, MaxMutations: db.cfg.MaxBatchMutations},
		Logger:             db.cfg.Logger,
	})
}

// startReplication runs a manager to completion.
func (db *DB) startReplication(mgr *replication.Manager) {
	done := make(chan struct{})
	db.replDone = done
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		defer close(done)
		_ = mgr.Run(db.ctx)
	}()
}

// --- backup & restore ---

// Checkpoint implements backup.SourceDB, taking an online snapshot via hard links.
func (db *DB) Checkpoint(stagingDataDir string) error {
	return db.store.Checkpoint(stagingDataDir)
}

// Pin implements backup.SourceDB, pinning data keys during backup.
func (db *DB) Pin(ctx context.Context, path, kind string) error {
	if db.keyReg != nil {
		return db.keyReg.Pin(ctx, path, kind)
	}
	return nil
}

// Unpin implements backup.SourceDB, unpinning data keys after backup.
func (db *DB) Unpin(ctx context.Context, path string) error {
	if db.keyReg != nil {
		return db.keyReg.Unpin(ctx, path)
	}
	return nil
}

// KeysDir implements backup.SourceDB.
func (db *DB) KeysDir() string {
	return filepath.Join(db.cfg.Path, "keys")
}

// ClusterID implements backup.SourceDB.
func (db *DB) ClusterID() string {
	return db.DBID().String()
}

// LocalNodeID implements backup.SourceDB.
func (db *DB) LocalNodeID() string {
	return db.NodeID().String()
}

// SchemaInfo implements backup.SourceDB.
func (db *DB) SchemaInfo() (epoch uint64, version uint64, hash string) {
	if db.reg == nil {
		return 0, 0, ""
	}
	return db.reg.Epoch, db.reg.Epoch, fmt.Sprintf("%x", db.reg.Hash)
}

// DataDir implements backup.SourceDB.
func (db *DB) DataDir() string {
	return filepath.Join(db.cfg.Path, "data")
}

// Backup creates an online, zero-downtime, pure-ciphertext backup.
func (db *DB) Backup(ctx context.Context, cfg backup.Config) (*backup.Metadata, error) {
	if err := db.requireRead(); err != nil {
		return nil, err
	}
	db.backupMu.Lock()
	defer db.backupMu.Unlock()
	return backup.CreateBackup(ctx, db, cfg)
}

// Restore restores a backup bundle from cfg.Source into TargetPath and KeysPath.
func Restore(ctx context.Context, cfg backup.RestoreConfig) (*backup.Metadata, error) {
	return backup.Restore(ctx, cfg)
}

// recoverRotation leftovers completes or rolls back an interrupted rotation's
// directory moves without guessing keys. See RotateStorageKey.
func (db *DB) recoverRotationLeftovers() error {
	path := db.cfg.Path
	shadow, prev := path+".rekey", path+".prev"
	has := func(p string) bool { _, err := os.Stat(p); return err == nil }
	hasPath, hasShadow, hasPrev := has(path), has(shadow), has(prev)
	switch {
	case hasShadow && hasPath:
		// Pre-cutover crash: shadow incomplete; drop it.
		if err := os.RemoveAll(shadow); err != nil {
			return fmt.Errorf("replicateddb: remove stale %s: %w", shadow, err)
		}
	case !hasPath && hasPrev && !hasShadow:
		// Crash after first rename: roll back to old store (old key).
		if err := os.Rename(prev, path); err != nil {
			return fmt.Errorf("replicateddb: rollback rotation: %w", err)
		}
	case !hasPath && hasShadow:
		// Crash between renames with both spares, or shadow-only: the two
		// directories need different keys; refuse to guess.
		return fmt.Errorf("replicateddb: interrupted rotation: %s and/or %s exist without %s; move the correct store into place manually",
			shadow, prev, path)
	case hasPath && hasPrev && !hasShadow:
		// Cutover completed; cleanup did not. The live store is complete.
		if err := os.RemoveAll(prev); err != nil {
			return fmt.Errorf("replicateddb: remove stale %s: %w", prev, err)
		}
	case hasPath && hasPrev && hasShadow:
		return fmt.Errorf("replicateddb: interrupted rotation: both %s and %s exist; resolve manually", shadow, prev)
	}
	return nil
}

// --- close ---

// Close stops workers and replication, then closes the query engine and the
// durable store. It waits for in-flight local transactions and applies to
// drain; new operations are rejected.
func (db *DB) Close() error {
	db.setState(StateClosing)
	db.cancel()
	// Drain in-flight work (new work is rejected by state checks). The
	// drains also synchronize with an in-progress key rotation or rewrite
	// so the manager read below cannot race a maintenance restart.
	db.writeMu.Lock()
	db.writeMu.Unlock()
	db.applyMu.Lock()
	db.applyMu.Unlock()
	if repl := db.replManager(); repl != nil {
		_ = repl.Close()
	}
	// Snapshot status before the store closes so post-Close Status serves
	// the final snapshot instead of touching a closed handle. db.store is
	// deliberately not nilled: the pointer stays valid for DBID().
	if db.store != nil {
		snap := db.statusLive()
		db.mu.Lock()
		db.lastStatus, db.lastStatusOK, db.storeUsable = snap, true, false
		db.mu.Unlock()
	}
	db.wg.Wait()
	var first error
	if db.engine != nil {
		if err := db.engine.Close(); err != nil && first == nil {
			first = err
		}
		db.engine = nil
	}
	if db.backupWorker != nil {
		db.backupWorker.Stop()
		db.backupWorker = nil
	}
	if db.store != nil {
		if err := db.store.Close(); err != nil && first == nil {
			first = err
		}
	}
	if db.keyReg != nil {
		db.keyReg.Close()
		db.keyReg = nil
	}
	db.setState(StateClosed)
	return first
}

// isReadOnlyStatement reports whether q is a read-only statement. Unknown or
// write-capable statements return false (safe default: write path).
func isReadOnlyStatement(q string) bool {
	s := strings.TrimSpace(q)
	for {
		if strings.HasPrefix(s, "--") {
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = strings.TrimSpace(s[i+1:])
				continue
			}
			return true
		}
		if strings.HasPrefix(s, "/*") {
			if i := strings.Index(s, "*/"); i >= 0 {
				s = strings.TrimSpace(s[i+2:])
				continue
			}
			return false
		}
		break
	}
	word := s
	if i := strings.IndexAny(s, " \t\n\r(;"); i >= 0 {
		word = s[:i]
	}
	switch strings.ToUpper(word) {
	case "SELECT", "EXPLAIN", "PRAGMA", "VALUES", "TABLE":
		return true
	default:
		return false
	}
}

var _ replication.Applier = (*DB)(nil)
