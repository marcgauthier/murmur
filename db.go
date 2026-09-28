package replicateddb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	subMgr *subscriptionManager
	files  *fileStore // nil unless Files.Enabled

	keyReg *crypto.Registry
	encMgr *crypto.Manager

	backupWorker *backup.Worker
	backupMu     sync.Mutex

	replCreds *transport.Credentials
	replDone  chan struct{}

	mu      sync.Mutex
	dbState DBState
	// schemaId caches the published schema identity for handshakes and
	// batch provenance. It tracks the current manifest; reg/manifest
	// swaps on migration/adoption update it under mu.
	schemaId replication.SchemaIdentity
	// encPhase is the encryption phase (idle, rotating, rewriting,
	// recovering). storeUsable is false across the maintenance close
	// window (and after a failed reopen); Status serves cached store
	// fields while false.
	encPhase    string
	storeUsable bool
	// lastStatus caches store-dependent Status fields for maintenance.
	lastStatus   Status
	lastStatusOK bool

	writeMu                sync.Mutex                    // serialized local write coordinator
	applyMu                sync.Mutex                    // serializes all durable commits + materialization
	remoteRows             map[sqlengine.RowKey]struct{} // durable remote rows awaiting SQLite
	remoteTxnCount         int                           // received transactions since the last SQLite flush
	remoteFlushWake        chan struct{}                 // transaction-count threshold wakes the bulk flush worker
	materializedGeneration atomic.Uint64                 // query-visible generation; rebuilt on every open

	sched *writerScheduler // fair writer admission (see scheduler.go)

	metrics dbMetrics // node-local diagnostics counters (see metrics.go)

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
	db := &DB{cfg: cfg, log: cfg.Logger, reg: reg, dbState: StateOpening, openedAt: time.Now(), sched: newWriterScheduler(cfg.Scheduling), remoteFlushWake: make(chan struct{}, 1)}
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
	// Coordinated reseed: the restored ciphertext is bound to the source
	// DBID, so rebind it to the new DBID before anything opens it under
	// the new identity. The rebind is idempotent and crash-safe, and the
	// restore intent persists until post-rebuild cleanup, so a crash at
	// any point retries to convergence on the next Open.
	if err := maybeRebindReseedStore(ctx, cfg, storageProvider, regDir, dataPath); err != nil {
		return nil, err
	}
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

	baseFS := cfg.Pebble.BaseFS
	if baseFS == nil {
		baseFS = vfs.Default
	}
	efs, err := crypto.NewEncryptedFS(crypto.FSOptions{
		Base:     baseFS,
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
	// Restore intent: restored data must be opened with its fresh writer
	// identity (same-identity rollback is rejected here and again during
	// adoption). The adoption swaps the stored identity atomically with a
	// durable restore marker.
	restoreAdoption, intentPath, err := restoreAdoptionFor(cfg)
	if err != nil {
		if db.keyReg != nil {
			db.keyReg.Close()
		}
		db.cancel()
		return nil, err
	}
	store, err := state.Open(dataPath, cfg.NodeID, cfg.DBID, state.Options{
		FS:                          pebbleFS,
		CacheBytes:                  cfg.Pebble.CacheBytes,
		MemTableSize:                cfg.Pebble.MemTableBytes,
		MemTableStopWritesThreshold: cfg.Pebble.MemTableCount,
		MaxOpenFiles:                cfg.Pebble.MaxOpenFiles,
		CompactionConcurrency:       cfg.Pebble.MaxConcurrentCompactions,
		Compression:                 compProfile,
		Limits:                      codec.Limits{MaxValueBytes: cfg.MaxReplicatedValueBytes, MaxMutations: cfg.MaxBatchMutations, MaxTransactionBytes: cfg.MaxTransactionBytes},
		Logger:                      cfg.Logger,
		AsyncDurability:             cfg.Durability.Mode == DurabilityAsync,
		Restore:                     restoreAdoption,
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
	// Schema manifests: fresh databases publish the configuration as the
	// genesis revision; reopens require an exact match (migrations go
	// through Migrate, never config drift).
	liveReg, manifest, err := openSchemaManifest(store, cfg)
	if err != nil {
		db.closeStore()
		db.cancel()
		return nil, err
	}
	reg = liveReg
	db.reg = reg
	db.schemaId = replication.SchemaIdentity{
		Epoch:       manifest.Version,
		Hash:        manifest.Hash,
		Author:      manifest.CreatedOnNode,
		TimeCreated: manifest.TimeCreated,
	}
	var engine *sqlengine.Engine
	if cfg.QueryStore.Mode == QueryStoreMMap {
		engine, err = sqlengine.OpenMMap(reg, cfg.Schema.DDL, cfg.Schema.LocalDDL,
			cfg.Cache.StatementCacheEntries, cfg.QueryStore.TempDir, cfg.QueryStore.MMapBytes)
	} else {
		engine, err = sqlengine.Open(reg, cfg.Schema.DDL, cfg.Schema.LocalDDL, cfg.Cache.StatementCacheEntries)
	}
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
	} else {
		db.materializedGeneration.Store(gen)
	}
	db.applyMu.Unlock()

	// A restore adoption (if any) is durable and the rebuild validated the
	// adopted baseline: clear the intent before networking starts. A crash
	// before this point replays the adoption idempotently on reopen.
	if intentPath != "" {
		if err := os.Remove(intentPath); err != nil && !os.IsNotExist(err) {
			db.log.Warn("replicateddb: remove restore intent failed", "err", err.Error())
		}
	}

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

	db.subMgr = newSubscriptionManager(db, cfg.Subscription)
	db.subMgr.start()

	if cfg.Files.Enabled {
		files, err := openFileStore(db)
		if err != nil {
			db.setState(StateFailed)
			_ = engine.Close()
			db.closeStore()
			db.cancel()
			return nil, err
		}
		db.files = files
	}

	db.setState(StateReady)
	db.wg.Add(1)
	go db.remoteMaterializationLoop()
	if db.repl != nil {
		db.startReplication(db.repl)
	}
	if cfg.Durability.SyncInterval > 0 {
		db.wg.Add(1)
		go db.periodicSyncLoop()
	}
	db.wg.Add(1)
	go db.gcLoop()
	db.wg.Add(1)
	go db.valueGCloop()
	if db.files != nil && len(db.files.sources()) > 0 && cfg.Files.FetchInterval >= 0 {
		db.wg.Add(1)
		go db.fetchLoop()
	}
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
			IncludeFiles:  cfg.Backup.IncludeFiles,
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

// restoreAdoptionFor loads a pending restore intent for cfg.Path. It returns
// the adoption for state.Open plus the intent path to clear after a
// successful rebuild, or (nil, "", nil) for ordinary databases.
func restoreAdoptionFor(cfg Config) (*state.RestoreAdoption, string, error) {
	intent, err := backup.ReadRestoreIntent(cfg.Path)
	if err != nil {
		return nil, "", err
	}
	if intent == nil {
		return nil, "", nil
	}
	if intent.Mode != string(backup.RestoreClone) && intent.Mode != string(backup.RestoreReseed) {
		return nil, "", fmt.Errorf("%w: restore mode %q is not implemented", ErrRestoreIdentity, intent.Mode)
	}
	fresh, err := ids.ParseNodeID(intent.FreshNodeID)
	if err != nil {
		return nil, "", fmt.Errorf("%w: corrupt fresh identity %q: %v", ErrRestoreIdentity, intent.FreshNodeID, err)
	}
	if cfg.NodeID != fresh {
		return nil, "", fmt.Errorf("%w: have %s, intent requires %s (same-identity rollback rejected)",
			ErrRestoreIdentity, cfg.NodeID, fresh)
	}
	source, err := ids.ParseNodeID(intent.SourceNodeID)
	if err != nil {
		return nil, "", fmt.Errorf("%w: corrupt source identity %q: %v", ErrRestoreIdentity, intent.SourceNodeID, err)
	}
	adoption := &state.RestoreAdoption{Source: source, Fresh: fresh, BackupID: intent.BackupID}
	if intent.Mode == string(backup.RestoreReseed) {
		// Coordinated reseed: the operator assigns the same new DBID on
		// every node. It must be explicit in config (never defaulted) and
		// must match the intent.
		newDB, err := ids.ParseDBID(intent.NewDBID)
		if err != nil || newDB.IsZero() {
			return nil, "", fmt.Errorf("%w: corrupt reseed DBID %q", ErrRestoreIdentity, intent.NewDBID)
		}
		if cfg.DBID.IsZero() {
			return nil, "", fmt.Errorf("%w: reseed requires the new DBID in config", ErrRestoreIdentity)
		}
		if cfg.DBID != newDB {
			return nil, "", fmt.Errorf("%w: config DBID %s != reseed DBID %s",
				ErrRestoreIdentity, cfg.DBID, newDB)
		}
		sourceDB, err := ids.ParseDBID(intent.SourceDBID)
		if err != nil {
			return nil, "", fmt.Errorf("%w: corrupt source DBID %q: %v", ErrRestoreIdentity, intent.SourceDBID, err)
		}
		if newDB == sourceDB {
			return nil, "", fmt.Errorf("%w: reseed DBID equals the source cluster", ErrRestoreIdentity)
		}
		adoption.SourceDBID = sourceDB
		adoption.NewDBID = newDB
	}
	return adoption, filepath.Join(cfg.Path, backup.RestoreIntentFileName), nil
}

// maybeRebindReseedStore runs the crash-safe ciphertext rebind when a
// reseed intent moves the store to a new DBID. Ordinary opens, fresh
// databases, and same-DBID clones are no-ops.
func maybeRebindReseedStore(ctx context.Context, cfg Config, provider crypto.KeyProvider, regDir, dataPath string) error {
	adoption, _, err := restoreAdoptionFor(cfg)
	if err != nil {
		return err
	}
	if adoption == nil || adoption.NewDBID.IsZero() || adoption.NewDBID == adoption.SourceDBID {
		return nil
	}
	base := cfg.Pebble.BaseFS
	if base == nil {
		base = vfs.Default
	}
	var source, target [16]byte
	copy(source[:], adoption.SourceDBID[:])
	copy(target[:], adoption.NewDBID[:])
	return crypto.RebindStore(ctx, crypto.RebindOptions{
		RegDir:     regDir,
		Roots:      []string{dataPath},
		Provider:   provider,
		SourceDBID: source,
		NewDBID:    target,
		Base:       base,
		Logger:     cfg.Logger,
	})
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

// storeFailed returns the sticky fail-closed storage error when the store
// has failed (disk full, unrecoverable I/O error). Nil store (maintenance
// window) means no failure to report.
func (db *DB) storeFailed() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.store == nil {
		return nil
	}
	return db.store.Failed()
}

func (db *DB) requireWrite() error {
	if err := db.storeFailed(); err != nil {
		return err
	}
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
	// A failed store rejects reads too: the node has failed closed and the
	// materialization can no longer be verified against durable state.
	if err := db.storeFailed(); err != nil {
		return err
	}
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

// Subscribe opens a reactive query subscription for a read-only query.
// It delivers an initial query result followed by updates on committed changes.
func (db *DB) Subscribe(ctx context.Context, query string, args ...any) (*Subscription, error) {
	return db.SubscribeWithOptions(ctx, query, SubscriptionOptions{}, args...)
}

// SubscribeWithOptions opens a reactive query subscription with custom options.
func (db *DB) SubscribeWithOptions(ctx context.Context, query string, opts SubscriptionOptions, args ...any) (*Subscription, error) {
	if err := db.requireRead(); err != nil {
		return nil, err
	}
	if !isReadOnlyStatement(query) {
		return nil, ErrReadOnlyRequired
	}
	if db.subMgr == nil {
		return nil, ErrNotReady
	}
	return db.subMgr.subscribe(ctx, query, opts, args...)
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
func (db *DB) BeginTx(ctx context.Context, opts *TxOptions) (*Tx, error) {
	return db.BeginTxWithID(ctx, ids.NewTxID(), opts)
}

// BeginTxWithID starts an explicit local transaction with a designated TxID.
// This is used by importers to preserve stable source transaction identities
// and enable deduplication across replays and concurrent receivers.
func (db *DB) BeginTxWithID(ctx context.Context, txID ids.TxID, _ *TxOptions) (*Tx, error) {
	if err := db.requireWrite(); err != nil {
		return nil, err
	}
	if txID.IsZero() {
		txID = ids.NewTxID()
	}
	// Scheduler admission precedes the write lock; the ticket covers the
	// whole transaction (SQL plus durable commit) and releases on Commit
	// or Rollback.
	ticket, err := db.sched.Admit(ctx, WriterLocal)
	if err != nil {
		return nil, fmt.Errorf("replicateddb: writer admission: %w", err)
	}
	waitStart := time.Now()
	db.writeMu.Lock()
	db.metrics.writeAcquisitions.Add(1)
	db.metrics.writeQueueWaitNanos.Add(uint64(time.Since(waitStart)))
	if err := db.requireWrite(); err != nil {
		db.writeMu.Unlock()
		ticket.Release()
		return nil, err
	}
	db.applyMu.Lock()
	flushErr := db.flushRemoteLocked()
	db.applyMu.Unlock()
	if flushErr != nil {
		db.writeMu.Unlock()
		ticket.Release()
		return nil, flushErr
	}
	stx, err := db.engine.Begin(context.Background())
	if err != nil {
		db.writeMu.Unlock()
		ticket.Release()
		return nil, err
	}
	return &Tx{db: db, stx: stx, txID: txID, ticket: ticket}, nil
}

// HasTransactionReceipt reports whether a transaction ID already has a durable
// receipt in authoritative storage.
func (db *DB) HasTransactionReceipt(txID ids.TxID) (bool, error) {
	if err := db.requireRead(); err != nil {
		return false, err
	}
	return db.store.HasReceipt(txID)
}

// RecordTransactionReceipt records a durable receipt for a transaction ID in authoritative storage.
func (db *DB) RecordTransactionReceipt(txID ids.TxID) error {
	if err := db.requireWrite(); err != nil {
		return err
	}
	return db.store.RecordReceipt(txID)
}

// SetBridgeStreamProgress records the contiguous applied sequence for a stream in authoritative storage.
func (db *DB) SetBridgeStreamProgress(stream string, applied uint64) error {
	if err := db.requireWrite(); err != nil {
		return err
	}
	return db.store.SetBridgeStreamProgress(stream, applied)
}

// BridgeStreamProgress returns the contiguous applied sequence for a stream from authoritative storage.
func (db *DB) BridgeStreamProgress(stream string) (uint64, bool, error) {
	if err := db.requireRead(); err != nil {
		return 0, false, err
	}
	return db.store.BridgeStreamProgress(stream)
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
	start := time.Now()
	noteCommit := func(mutations int) {
		db.metrics.localCommits.Add(1)
		db.metrics.localCommitMutations.Add(uint64(mutations))
		db.metrics.localCommitLatencyNanos.Add(uint64(time.Since(start)))
	}
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
	policyMutations, err := policyMutationsForTx(db, tx, mutations)
	if err != nil {
		_ = tx.stx.Rollback()
		return fmt.Errorf("replicateddb: bridge policy: %w", err)
	}
	mutations = append(mutations, policyMutations...)
	if len(mutations) > db.cfg.MaxBatchMutations {
		_ = tx.stx.Rollback()
		return fmt.Errorf("%w: %d mutations", ErrBatchTooLarge, len(mutations))
	}
	if encodedSize := int64(codec.EncodedMutationsSize(mutations)); encodedSize > db.cfg.MaxTransactionBytes {
		_ = tx.stx.Rollback()
		return fmt.Errorf("%w: transaction encoded size %d bytes exceeds MaxTransactionBytes %d: %w", ErrTransactionTooLarge, encodedSize, db.cfg.MaxTransactionBytes, ErrBatchTooLarge)
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
		db.applyMu.Lock()
		err := db.flushRemoteLocked()
		db.applyMu.Unlock()
		if err != nil {
			return err
		}
		noteCommit(0)
		return nil // no net change; nothing to replicate
	}
	schemaId := db.schemaIdentity()
	batch := &codec.MutationBatch{
		ProtocolVersion: replication.ProtocolVersion,
		TxID:            tx.txID,
		OriginNode:      db.cfg.NodeID,
		HLC:             db.store.ClockNow(),
		SchemaEpoch:     schemaId.Epoch,
		SchemaHash:      schemaId.Hash,
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
	materializedByFlush := len(db.remoteRows) > 0
	if err := db.flushRemoteLocked(); err != nil {
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
		db.metrics.repairs.Add(1)
		seen := make(map[sqlengine.RowKey]bool, len(mutations))
		var touched []sqlengine.RowKey
		for i := range mutations {
			k := sqlengine.RowKey{TableID: mutations[i].TableID, RowID: mutations[i].RowID}
			if !seen[k] {
				seen[k] = true
				touched = append(touched, k)
			}
		}
		if err := db.engine.RepairRows(db.shadowReader(), touched); err != nil {
			db.log.Warn("local repair failed; rebuilding materializer", "err", err.Error())
			if rerr := db.rebuildLocked(); rerr != nil {
				return rerr
			}
			if gen, err = db.store.StateGeneration(); err != nil {
				return err
			}
		}
	}
	if !materializedByFlush {
		db.materializedGeneration.Store(gen)
	}
	if err := db.fireCrash(func(h *crashHooks) func() error { return h.afterDurable }); err != nil {
		// Ambiguous commit: durable and materialized, acknowledgement lost.
		noteCommit(len(mutations))
		if repl := db.replManager(); repl != nil {
			repl.NotifyLocal()
		}
		return fmt.Errorf("%w: %w", ErrAmbiguousCommit, err)
	}
	if db.subMgr != nil {
		db.subMgr.notifyChange(false)
	}
	if repl := db.replManager(); repl != nil {
		repl.NotifyLocal()
	}
	noteCommit(len(mutations))
	return nil
}

// rebuildLocked rebuilds the query database from durable state. applyMu held.
//
// On success the node returns to Ready. On failure the node fails closed:
// the materializer cannot be trusted, so reads and writes stay rejected
// until the operator restarts (Open rebuilds from Pebble-authoritative
// state). Callers must not overwrite the Failed state.
func (db *DB) rebuildLocked() error {
	start := time.Now()
	db.setState(StateMaterializerDirty)
	ok := false
	defer func() {
		db.metrics.rebuilds.Add(1)
		db.metrics.rebuildNanos.Add(uint64(time.Since(start)))
		if ok {
			db.setState(StateReady)
		} else {
			db.setState(StateFailed)
		}
	}()
	if err := db.engine.Rebuild(db.shadowReader()); err != nil {
		db.log.Error("rebuild failed; node failed closed", "err", err.Error())
		return err
	}
	gen, err := db.store.StateGeneration()
	if err != nil {
		db.log.Error("rebuild failed; node failed closed", "err", err.Error())
		return err
	}
	db.materializedGeneration.Store(gen)
	db.remoteRows = nil
	db.remoteTxnCount = 0
	if db.subMgr != nil {
		db.subMgr.notifyChange(true)
	}
	ok = true
	return nil
}

// --- replication applier ---

// ApplyRemote durably merges a received batch and queues its winning rows for
// bulk SQLite materialization.
// It satisfies replication.Applier. Acknowledgements are sent by the
// replication manager only after this returns nil.
func (db *DB) ApplyRemote(ctx context.Context, batch *codec.MutationBatch) error {
	if st := db.getState(); st == StateClosed || st == StateClosing || st == StateFailed {
		return fmt.Errorf("replicateddb: apply in state %s", st)
	}
	start := time.Now()
	db.metrics.applyInflight.Add(1)
	defer db.metrics.applyInflight.Add(-1)
	db.metrics.remoteApplyMutations.Add(uint64(len(batch.Mutations)))
	failed := true
	defer func() {
		db.metrics.remoteApplyLatencyNanos.Add(uint64(time.Since(start)))
		if failed {
			db.metrics.remoteApplyFailures.Add(1)
		} else {
			db.metrics.remoteApplies.Add(1)
		}
	}()
	ticket, err := db.sched.Admit(ctx, WriterRemote)
	if err != nil {
		return fmt.Errorf("replicateddb: writer admission: %w", err)
	}
	defer ticket.Release()
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	res, err := db.store.CommitRemote(ctx, batch)
	if err != nil {
		if state.IsStorageFailure(err) {
			db.log.Error("remote apply failed; storage failed closed", "err", err.Error())
			db.setState(StateFailed)
		}
		return err
	}
	if err := db.fireCrash(func(h *crashHooks) func() error { return h.remoteMaterialize }); err != nil {
		// Simulated materialization failure: durable state is correct, rebuild.
		db.log.Warn("remote apply failed; rebuilding materializer", "err", err.Error())
		if rerr := db.rebuildLocked(); rerr != nil {
			db.log.Error("rebuild failed", "err", rerr.Error())
		}
		failed = false
		return nil
	}
	if err := db.queueRemoteLocked(res.Winners, res.Generation, 1); err != nil {
		if state.IsStorageFailure(err) {
			db.log.Error("remote apply failed; storage failed closed", "err", err.Error())
			db.setState(StateFailed)
		}
		return err
	}
	if db.files != nil {
		// New file metadata may need object bytes: wake the fetch scan.
		for i := range batch.Mutations {
			if batch.Mutations[i].TableID == db.files.ids.table {
				db.files.triggerFetchScan()
				break
			}
		}
	}
	failed = false
	return nil
}

// ApplyRemoteGroup commits an ordered group of remote transactions with one
// writer admission and one Pebble sync. Transaction IDs, receipts, and origin
// sequence positions remain independent. Query visibility follows the next
// bulk SQLite flush; acknowledgement follows the durable Pebble commit.
func (db *DB) ApplyRemoteGroup(ctx context.Context, batches []*codec.MutationBatch) error {
	if len(batches) == 0 {
		return nil
	}
	if len(batches) == 1 {
		return db.ApplyRemote(ctx, batches[0])
	}
	if st := db.getState(); st == StateClosed || st == StateClosing || st == StateFailed {
		return fmt.Errorf("replicateddb: apply in state %s", st)
	}
	start := time.Now()
	db.metrics.applyInflight.Add(1)
	defer db.metrics.applyInflight.Add(-1)
	var mutations uint64
	for _, batch := range batches {
		mutations += uint64(len(batch.Mutations))
	}
	db.metrics.remoteApplyMutations.Add(mutations)
	failed := true
	defer func() {
		db.metrics.remoteApplyLatencyNanos.Add(uint64(time.Since(start)))
		if failed {
			db.metrics.remoteApplyFailures.Add(1)
		} else {
			db.metrics.remoteApplies.Add(uint64(len(batches)))
		}
	}()
	ticket, err := db.sched.Admit(ctx, WriterRemote)
	if err != nil {
		return fmt.Errorf("replicateddb: writer admission: %w", err)
	}
	defer ticket.Release()
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	res, err := db.store.CommitRemoteGroup(ctx, batches)
	if err != nil {
		if state.IsStorageFailure(err) {
			db.log.Error("remote group apply failed; storage failed closed", "err", err.Error())
			db.setState(StateFailed)
		}
		return err
	}
	if err := db.fireCrash(func(h *crashHooks) func() error { return h.remoteMaterialize }); err != nil {
		db.log.Warn("remote group apply failed; rebuilding materializer", "err", err.Error())
		if rerr := db.rebuildLocked(); rerr != nil {
			db.log.Error("rebuild failed", "err", rerr.Error())
		}
		failed = false
		return nil
	}
	if err := db.queueRemoteLocked(res.Winners, res.Generation, len(batches)); err != nil {
		if state.IsStorageFailure(err) {
			db.log.Error("remote group apply failed; storage failed closed", "err", err.Error())
			db.setState(StateFailed)
		}
		return err
	}
	failed = false
	return nil
}

// ApplySnapshotChunk merges one snapshot chunk and materializes winners.
func (db *DB) ApplySnapshotChunk(ctx context.Context, manifest *codec.SnapshotManifest, index uint64, cells []codec.SnapshotCell, last bool) (bool, error) {
	if st := db.getState(); st == StateClosed || st == StateClosing || st == StateFailed {
		return false, fmt.Errorf("replicateddb: snapshot apply in state %s", st)
	}
	db.metrics.applyInflight.Add(1)
	defer db.metrics.applyInflight.Add(-1)
	ticket, err := db.sched.Admit(ctx, WriterRemote)
	if err != nil {
		return false, fmt.Errorf("replicateddb: writer admission: %w", err)
	}
	defer ticket.Release()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	res, complete, err := db.store.ImportSnapshotChunk(ctx, manifest, index, cells, last, uint64(db.cfg.Replication.MaxSnapshotBytes))
	if err != nil {
		db.metrics.snapshotApplyFailures.Add(1)
		if state.IsStorageFailure(err) {
			db.log.Error("snapshot apply failed; storage failed closed", "err", err.Error())
			db.setState(StateFailed)
		}
		return false, err
	}
	db.metrics.snapshotChunksApplied.Add(1)
	if complete && res.Applied {
		if err := db.rebuildLocked(); err != nil {
			db.metrics.snapshotApplyFailures.Add(1)
			return false, err
		}
		db.metrics.snapshotAppliesComplete.Add(1)
	}
	return complete, nil
}

// --- peers ---

// AddPeer adds or updates a replication peer as a bootstrap candidate,
// clearing any previous local persistent exclusion.
func (db *DB) AddPeer(_ context.Context, peer Peer) error {
	if st := db.getState(); st == StateClosed || st == StateClosing || st == StateFailed {
		return ErrClosed
	}
	if peer.NodeID == (ids.NodeID{}) {
		return fmt.Errorf("replicateddb: peer NodeID cannot be zero")
	}
	if peer.NodeID == db.cfg.NodeID {
		return fmt.Errorf("replicateddb: cannot add self as peer")
	}
	repl := db.replManager()
	if repl == nil {
		return fmt.Errorf("replicateddb: replication not configured")
	}
	repl.AddPeer(peer.NodeID, peer.Addrs)
	// Clear any persisted retirement so the next successful
	// authentication starts a fresh admission obligation. Exclusion is
	// cleared by the manager above.
	if db.store != nil {
		if err := db.store.ReadmitMember(peer.NodeID); err != nil {
			db.log.Warn("replicateddb: readmit member failed", "err", err.Error())
		}
	}
	db.metrics.peersAdded.Add(1)
	return nil
}

// RemovePeer explicitly retires and persistently excludes a replication peer locally.
// Active replication sessions close and subsequent discovery will not recreate the obligation.
func (db *DB) RemovePeer(_ context.Context, nodeID NodeID) error {
	if st := db.getState(); st == StateClosed || st == StateClosing || st == StateFailed {
		return ErrClosed
	}
	if nodeID == (ids.NodeID{}) {
		return fmt.Errorf("replicateddb: peer NodeID cannot be zero")
	}
	repl := db.replManager()
	if repl == nil {
		return fmt.Errorf("replicateddb: replication not configured")
	}
	repl.RemovePeer(nodeID)
	// Persist retirement alongside the exclusion the manager records:
	// the member holds no retention obligation and handshake traffic
	// cannot re-admit it.
	if db.store != nil {
		if err := db.store.RetireMember(nodeID); err != nil {
			db.log.Warn("replicateddb: retire member failed", "err", err.Error())
		}
	}
	db.metrics.peersRemoved.Add(1)
	return nil
}

// ForceSync schedules immediate synchronization with the specified peer subject
// to session and connection limits.
func (db *DB) ForceSync(ctx context.Context, nodeID NodeID) error {
	if st := db.getState(); st == StateClosed || st == StateClosing || st == StateFailed {
		return ErrClosed
	}
	if nodeID == (ids.NodeID{}) {
		return fmt.Errorf("replicateddb: peer NodeID cannot be zero")
	}
	if nodeID == db.cfg.NodeID {
		return fmt.Errorf("replicateddb: cannot force sync with self")
	}
	repl := db.replManager()
	if repl == nil {
		return fmt.Errorf("replicateddb: replication not configured")
	}
	if err := repl.ForceSync(ctx, nodeID); err != nil {
		if errors.Is(err, replication.ErrPeerExcluded) {
			return fmt.Errorf("replicateddb: peer %s is locally excluded: %w", nodeID, ErrPeerExcluded)
		}
		if errors.Is(err, replication.ErrPeerNotFound) {
			return fmt.Errorf("replicateddb: unknown peer %s: %w", nodeID, ErrPeerNotFound)
		}
		return err
	}
	db.metrics.forceSyncs.Add(1)
	return nil
}

// Peers returns current replication peer statuses.
func (db *DB) Peers() []PeerStatus {
	repl := db.replManager()
	if repl == nil {
		return nil
	}
	return repl.PeerStatus()
}

// ExcludedPeers returns a list of locally excluded / retired peer NodeIDs.
func (db *DB) ExcludedPeers() []NodeID {
	repl := db.replManager()
	if repl == nil {
		if db.store != nil {
			list, _ := db.store.ListExcludedPeers()
			return list
		}
		return nil
	}
	return repl.ExcludedPeers()
}

// IsPeerExcluded reports whether nodeID is locally excluded.
func (db *DB) IsPeerExcluded(nodeID NodeID) bool {
	repl := db.replManager()
	if repl == nil {
		if db.store != nil {
			ex, _ := db.store.IsPeerExcluded(nodeID)
			return ex
		}
		return false
	}
	return repl.IsPeerExcluded(nodeID)
}

// DurabilityMode returns the configured durability mode.
func (db *DB) DurabilityMode() DurabilityMode {
	return db.cfg.Durability.Mode
}

// Sync flushes all pending memory-buffered writes to disk.
// Under DurabilityAsync, calling Sync establishes an explicit durable sync point
// across all previously acknowledged transactions.
func (db *DB) Sync(ctx context.Context) error {
	if !db.storeUsable {
		return ErrClosed
	}
	return db.store.Sync()
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
	st.Metrics = db.Metrics()
	st.PendingApply = int(db.metrics.applyInflight.Load())
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
	st.MaterializedGeneration = db.materializedGeneration.Load()
	if e, h, err := db.store.SchemaEpoch(); err == nil {
		st.SchemaEpoch = e
		st.SchemaHash = h
	}
	if format, _, _, err := db.store.FormatInfo(); err == nil {
		st.FormatFormat = format
	}
	pm := db.store.Metrics()
	st.PebbleSizeBytes = pm.DiskBytes
	st.PebbleCacheHits = pm.CacheHits
	st.PebbleCacheMisses = pm.CacheMisses
	st.PebbleMemTableBytes = pm.MemTableBytes
	if repl := db.replManager(); repl != nil {
		rs := repl.Stats()
		st.Replication = rs
		if pool := repl.Pool(); pool != nil {
			st.Pool = pool.Stats()
			if st.Pool.ActiveConnections > 0 {
				st.QUICConnections = st.Pool.ActiveConnections
			} else {
				st.QUICConnections = rs.ConnectedPeers
			}
		} else {
			st.QUICConnections = rs.ConnectedPeers
		}
		memSvc := repl.Membership()
		if memSvc != nil {
			st.Membership = memSvc.Stats()
			st.MembershipCount = st.Membership.NumMembers
		} else {
			st.MembershipCount = rs.PeerCount
		}
		st.PeerCount = rs.PeerCount
		st.ConnectedPeers = rs.ConnectedPeers
		st.SelectedPeers = rs.SelectedPeers
		st.PendingSend = rs.QueuedNeed + rs.QueuedCtrl + rs.QueuedSchemaReq + rs.QueuedSchemaResp
		applied := make(map[NodeID]uint64)
		if wms, err := db.store.ReceiveWatermarks(); err == nil {
			for _, w := range wms {
				applied[w.Origin] = w.Sequence
			}
		}
		peers := repl.PeerStatus()
		sort.Slice(peers, func(i, j int) bool { return peers[i].NodeID.Compare(peers[j].NodeID) < 0 })
		for _, p := range peers {
			if !p.Connected && !p.Dynamic {
				st.PendingDials++
			}
			var retired, excluded bool
			var deadline time.Time
			if rec, err := db.store.GetMember(p.NodeID); err == nil {
				retired = rec.Status == state.MemberRetired
				excluded = rec.Excluded
				if rec.RetentionDeadline > 0 {
					deadline = time.UnixMilli(rec.RetentionDeadline)
				}
			}
			memState := "unknown"
			if memSvc != nil {
				memState = memSvc.MemberState(p.NodeID)
			} else if p.Connected {
				memState = "alive"
			}
			pd := PeerDiagnostics{
				NodeID:             p.NodeID,
				Addrs:              p.Addrs,
				Connected:          p.Connected,
				Dynamic:            p.Dynamic,
				SchemaAgreed:       p.SchemaAgreed,
				SnapshotRequired:   p.SnapshotRequired,
				AwaitingSnapshot:   p.AwaitingSnapshot,
				Retired:            retired,
				Excluded:           excluded,
				Selected:           p.Selected,
				MembershipState:    memState,
				RetirementDeadline: deadline,
				RTT:                p.RTT,
				LastSeen:           p.LastSeen,
				LastHandshake:      p.LastHandshake,
				LastSend:           p.LastSend,
				LastRecv:           p.LastRecv,
				LastAntiEntropy:    p.LastAntiEntropy,
				RemoteSchemaEpoch:  p.RemoteSchemaEpoch,
				RemoteSchemaHash:   p.RemoteSchemaHash,
				BytesSent:          p.BytesSent,
				BytesReceived:      p.BytesReceived,
				QueuedNeed:         p.QueuedNeed,
				QueuedCtrl:         p.QueuedCtrl,
				QueuedSchema:       p.QueuedSchema,
				Have:               p.Have,
				Sent:               p.Sent,
				LagByOrigin:        make(map[NodeID]uint64, len(applied)),
			}
			for origin, seq := range applied {
				if have := p.Have[origin]; seq > have {
					pd.LagByOrigin[origin] = seq - have
				}
			}
			st.Peers = append(st.Peers, pd)
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

func (db *DB) periodicSyncLoop() {
	defer db.wg.Done()
	ticker := time.NewTicker(db.cfg.Durability.SyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-db.ctx.Done():
			return
		case <-ticker.C:
			if err := db.store.Sync(); err != nil {
				db.metrics.periodicSyncFailures.Add(1)
				db.log.Error("replicateddb: periodic durability sync failed", "err", err.Error())
				db.setState(StateFailed)
				db.cancel()
				return
			}
			db.metrics.periodicSyncs.Add(1)
		}
	}
}

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

// admitLegacyAckPeers grants peers with pre-upgrade durable acknowledgement
// progress (but no admission record) one explicit retention window. Without
// it, upgrading would silently drop their obligation and collect history
// they may still need. Fresh deployments no-op: every acking peer already
// holds a record from its admission handshake.
func (db *DB) admitLegacyAckPeers(now int64) {
	peers, err := db.store.PeersWithAcks()
	if err != nil || len(peers) == 0 {
		return
	}
	retention := db.cfg.Replication.MaxOfflineLogRetention.Milliseconds()
	for _, peer := range peers {
		rec, err := db.store.GetMember(peer)
		if err != nil || rec.Status != state.MemberUnknown {
			continue
		}
		if _, err := db.store.EnsureMemberAdmitted(peer, now, retention); err != nil {
			db.log.Debug("legacy member admission failed", "err", err.Error())
		}
	}
}

func (db *DB) gcOnce(withReceipts bool) {
	if db.getState() != StateReady {
		return
	}
	origins, err := db.store.KnownOrigins()
	if err != nil {
		db.metrics.gcFailures.Add(1)
		return
	}
	start := time.Now()
	defer func() {
		db.metrics.gcRuns.Add(1)
		db.metrics.gcNanos.Add(uint64(time.Since(start)))
	}()
	// GC gates on persisted member retention deadlines, independent of
	// session lastSeen, SWIM liveness, and the selected replication
	// subset. Members whose obligation expired (or never existed) do not
	// pin history; their return path is snapshot resync.
	now := time.Now().UnixMilli()
	if ticket, err := db.sched.Admit(db.ctx, WriterMaintenance); err != nil {
		return
	} else {
		db.admitLegacyAckPeers(now)
		ticket.Release()
	}
	var gating []NodeID
	if members, err := db.store.ListMembers(); err != nil {
		db.metrics.gcFailures.Add(1)
		db.log.Debug("member list failed; GC runs without gating", "err", err.Error())
	} else {
		for _, mb := range members {
			if mb.Gating(now) {
				gating = append(gating, mb.NodeID)
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
		// GC is maintenance-class work in bounded per-origin units: each
		// collection re-admits so interactive writers interleave.
		ticket, err := db.sched.Admit(db.ctx, WriterMaintenance)
		if err != nil {
			return
		}
		n, cerr := db.store.CollectLog(origin, floor, cutoff, db.cfg.Replication.MinRetainedBatches)
		ticket.Release()
		if cerr != nil {
			db.metrics.gcFailures.Add(1)
			db.log.Debug("log GC failed", "origin", origin.String(), "err", cerr.Error())
		} else {
			db.metrics.gcLogCollected.Add(uint64(n))
		}
		floors[origin] = floor
	}
	if withReceipts {
		ticket, err := db.sched.Admit(db.ctx, WriterMaintenance)
		if err != nil {
			return
		}
		n, cerr := db.store.CollectReceipts(floors)
		ticket.Release()
		if cerr != nil {
			db.metrics.gcFailures.Add(1)
			db.log.Debug("receipt GC failed", "err", cerr.Error())
		} else {
			db.metrics.gcReceiptsCollected.Add(uint64(n))
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
// meshCreds builds (once) the cluster mTLS credentials shared by the
// replication manager and the file-fetch endpoint.
func (db *DB) meshCreds() (*transport.Credentials, error) {
	if db.replCreds != nil {
		return db.replCreds, nil
	}
	if db.cfg.Replication.TLS == nil {
		return nil, fmt.Errorf("replicateddb: replication requires TLS credentials")
	}
	creds, err := transport.CredentialsFromPEM(
		db.cfg.Replication.TLS.CertPEM, db.cfg.Replication.TLS.KeyPEM, db.cfg.Replication.TLS.CAPEM,
		db.cfg.Replication.AllowedPeers)
	if err != nil {
		return nil, err
	}
	addrPolicy, err := transport.ParseAddressPolicy(db.cfg.Replication.AllowedNetworks)
	if err != nil {
		return nil, err
	}
	creds.AddressPolicy = addrPolicy
	// The replication certificate must belong to the configured node
	// (in particular the fresh identity after a restore) before any
	// writable replication starts; peers would refuse a mismatched
	// identity at handshake, so fail fast here instead.
	if localID, err := creds.LocalNodeID(); err != nil {
		return nil, err
	} else if localID != db.cfg.NodeID {
		return nil, fmt.Errorf("%w: certificate is for node %s, config NodeID is %s",
			ErrLocalIdentityMismatch, localID, db.cfg.NodeID)
	}
	db.replCreds = creds
	return creds, nil
}

func (db *DB) newReplicationManager(extraPeers []replication.PeerInfo) (*replication.Manager, error) {
	if _, err := db.meshCreds(); err != nil {
		return nil, err
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
	schemaId := db.schemaIdentity()
	return replication.NewManager(replication.ManagerConfig{
		Store:                   db.store,
		Applier:                 db,
		Creds:                   db.replCreds,
		Local:                   db.cfg.NodeID,
		DBID:                    db.store.DBID(),
		SchemaEpoch:             schemaId.Epoch,
		SchemaHash:              schemaId.Hash,
		SchemaAuthor:            schemaId.Author,
		SchemaTime:              schemaId.TimeCreated,
		AcceptRemoteSchema:      db.cfg.Schema.acceptRemoteSchema(),
		SchemaSync:              db,
		ListenAddr:              db.cfg.Replication.ListenAddr,
		Peers:                   peers,
		MaxBatchBytes:           db.cfg.Replication.MaxBatchBytes,
		MaxBatchMutations:       db.cfg.Replication.MaxBatchMutations,
		SendInterval:            db.cfg.Replication.SendInterval,
		DialInterval:            db.cfg.Replication.DialInterval,
		AckInterval:             db.cfg.Replication.AckInterval,
		AckRetention:            db.cfg.Replication.MaxOfflineLogRetention,
		SnapshotChunkCells:      db.cfg.Replication.SnapshotChunkCells,
		MaxSnapshotBytes:        uint64(db.cfg.Replication.MaxSnapshotBytes),
		SnapshotTransferTimeout: db.cfg.Replication.SnapshotTransferTimeout,
		Fanout:                  db.cfg.Replication.Fanout,
		PeerRotationInterval:    db.cfg.Replication.PeerRotationInterval,
		AntiEntropyInterval:     db.cfg.Replication.AntiEntropyInterval,
		MaxConcurrentRepairs:    db.cfg.Replication.MaxConcurrentRepairs,
		MaxReplicationSessions:  db.cfg.Replication.MaxReplicationSessions,
		MaxQUICConnections:      db.cfg.Replication.MaxQUICConnections,
		EnablePlumtree:          db.cfg.Replication.Dissemination == DisseminationPlumtree,
		MaxTransactionBytes:     db.cfg.MaxTransactionBytes,
		Limits:                  codec.Limits{MaxValueBytes: db.cfg.MaxReplicatedValueBytes, MaxMutations: db.cfg.MaxBatchMutations, MaxTransactionBytes: db.cfg.MaxTransactionBytes},
		Logger:                  db.cfg.Logger,
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
	reg := db.schemaRegistry()
	if reg == nil {
		return 0, 0, ""
	}
	return reg.Epoch, reg.Epoch, fmt.Sprintf("%x", reg.Hash)
}

// DataDir implements backup.SourceDB.
func (db *DB) DataDir() string {
	return filepath.Join(db.cfg.Path, "data")
}

// FilesDir implements backup.SourceDB, or "" when files are disabled.
func (db *DB) FilesDir() string {
	if db.files == nil {
		return ""
	}
	return filepath.Join(db.cfg.Path, "files")
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
	if st := db.getState(); st == StateClosed || st == StateClosing {
		return nil
	}
	db.setState(StateClosing)
	db.cancel()
	// Wake scheduler waiters first (in-flight ticket holders run to
	// completion), then drain the locks below.
	db.sched.Close()
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
		db.applyMu.Lock()
		if err := db.flushRemoteLocked(); err != nil {
			first = fmt.Errorf("replicateddb: final remote materialization: %w", err)
		}
		db.applyMu.Unlock()
	}
	if db.cfg.Durability.SyncInterval > 0 && db.store != nil {
		if err := db.store.Sync(); err != nil {
			first = fmt.Errorf("replicateddb: final durability sync: %w", err)
		}
	}
	if db.subMgr != nil {
		db.subMgr.close()
		db.subMgr = nil
	}
	if db.engine != nil {
		if err := db.engine.Close(); err != nil && first == nil {
			first = err
		}
		db.engine = nil
	}
	if db.files != nil {
		if err := db.files.close(); err != nil && first == nil {
			first = err
		}
		db.files = nil
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
