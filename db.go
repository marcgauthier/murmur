package murmur

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crypto"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/recordcodec"
	"github.com/marcgauthier/murmur/internal/rimeadapter"
	"github.com/marcgauthier/murmur/replication"
	"github.com/marcgauthier/murmur/rime"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/spool"
	"github.com/marcgauthier/murmur/state"
	"github.com/marcgauthier/murmur/transport"
)

// DB is an embedded replicated database. All methods are safe for concurrent
// use; local writes are serialized by an internal coordinator.
type DB struct {
	mergePendingMu sync.Mutex
	mergePending   map[*codec.MutationBatch]pendingMergeBatch
	cfg            Config
	log            Logger
	reg            *schema.Registry

	store *state.Store
	// recordDB and recordAdapter are the private native-record materializer.
	recordMu          sync.RWMutex
	recordGeneration  uint64
	recordDB          *rime.DB
	recordAdapter     *rimeadapter.Adapter
	recordTables      map[string]any
	recordDefinitions []TableDefinition // guarded by recordMu; runtime Go bindings for the durable typed schema
	retiredRecordDBs  []*rime.DB
	repl              *replication.Manager
	subMgr            *subscriptionManager
	files             *fileStore // nil unless Files.Enabled

	backupWorker *backup.Worker
	backupMu     sync.Mutex

	replCreds *transport.Credentials
	replDone  chan struct{}
	// replExitErr records the replication manager's Run error, if any.
	// A listen failure at startup otherwise leaves a Ready database
	// with permanently dead replication and zero counters.
	replExitErr error

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

	writeMu                sync.Mutex // serialized local write coordinator
	applyMu                sync.Mutex // serializes all durable commits + materialization
	recordPrepMu           sync.Mutex // tracks typed callbacks staged before writer admission
	recordPrepCond         *sync.Cond
	recordPreparers        int
	recordPrepDrain        chan struct{}
	recordPrepGate         chan struct{}
	recordBarrierMu        sync.Mutex      // serializes snapshot publication barriers
	gcRunMu                sync.Mutex      // serializes periodic and operator-triggered GC passes
	grouper                *groupCommitter // synchronous group commit queue; nil when disabled
	groupWG                sync.WaitGroup  // phase-2 (post-writeMu) group commits in flight
	materializedGeneration atomic.Uint64   // query-visible generation; rebuilt on every open

	sched *writerScheduler // fair writer admission (see scheduler.go)

	metrics dbMetrics // node-local diagnostics counters (see metrics.go)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	openedAt     time.Time
	openProgress *openProgressReporter

	crash *crashHooks // failure injection; nil in production
}

// Open opens or creates the database, rebuilds the in-memory query database
// from durable state, and starts replication and maintenance workers.
func Open(ctx context.Context, cfg Config) (result *DB, openErr error) {
	progress := newOpenProgressReporter(cfg.OnOpenProgress)
	defer func() {
		if result == nil && openErr == nil {
			// A panic must not emit a successful terminal event.
			progress.finish(errors.New("murmur: open interrupted"))
		} else {
			progress.finish(openErr)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg.withDefaults()
	if err := cfg.applyTableDefinitions(); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if len(cfg.Tables) == 0 {
		return nil, fmt.Errorf("murmur: SQL schemas are no longer supported; define managed tables with Config.Tables: %w", ErrUnsupportedSchema)
	}
	reg, err := schema.BuildRegistry(cfg.Schema.Version, cfg.Schema.Tables)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnsupportedSchema, err)
	}
	db := &DB{cfg: cfg, log: cfg.Logger, reg: reg, dbState: StateOpening, openedAt: time.Now(), sched: newWriterScheduler(cfg.Scheduling)}
	db.recordPrepCond = sync.NewCond(&db.recordPrepMu)
	db.recordDefinitions = append([]TableDefinition(nil), cfg.Tables...)
	if cfg.Durability.Mode == DurabilitySynchronous && cfg.Durability.GroupCommit.MaxDelay > 0 {
		db.grouper = newGroupCommitter(cfg.Durability.GroupCommit, db.commitRecordGroup)
	}
	db.openProgress = progress
	db.ctx, db.cancel = context.WithCancel(context.Background())

	// Mandatory at-rest encryption: storage key from provider or direct material.
	storageProvider := cfg.Encryption.storageProvider()
	mat, err := storageProvider.Current(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: storage key: %w", ErrEncryptionKey, err)
	}
	if verr := checkOpenKeyMaterial(mat); verr != nil {
		crypto.Zero(mat.Key)
		return nil, fmt.Errorf("%w: %w", ErrEncryptionKey, verr)
	}
	defer crypto.Zero(mat.Key)

	dataPath := filepath.Join(cfg.Path, "data")
	var spoolComp spool.Compression
	switch cfg.Spool.Compression {
	case CompressionNone:
		spoolComp = spool.CompressionNone
	default:
		spoolComp = spool.CompressionDeflate
	}

	spoolDur := spool.DurabilitySync
	if cfg.Durability.Mode == DurabilityAsync {
		spoolDur = spool.DurabilityAsync
	}

	spoolOpt := spool.Options{
		Path:                   dataPath,
		MasterKey:              append([]byte(nil), mat.Key...),
		WrappingKeyID:          mat.ID,
		Encryption:             spool.EncryptionAES256GCM,
		Compression:            spoolComp,
		CompressionSet:         true,
		Codec:                  cfg.Spool.Codec,
		Codecs:                 cfg.Codecs,
		Durability:             spoolDur,
		WriteShards:            cfg.Spool.WriteShards,
		IndexShards:            cfg.Spool.IndexShards,
		TargetBlockBytes:       cfg.Spool.TargetBlockBytes,
		MaxBlockBytes:          cfg.Spool.MaxBlockBytes,
		MaxRecordsPerBlock:     cfg.Spool.MaxRecordsPerBlock,
		MaxAtomicBatchBytes:    cfg.Spool.MaxAtomicBatchBytes,
		MaxSegmentSize:         cfg.Spool.MaxSegmentSize,
		MaxPendingBytes:        cfg.Spool.MaxPendingBytes,
		Workers:                cfg.Spool.Workers,
		CompactionThreshold:    cfg.Spool.CompactionThreshold,
		CompactionMinFreeBytes: cfg.Spool.CompactionMinFreeBytes,
		TombProofThreshold:     cfg.Spool.TombProofThreshold,
		ReclaimInterval:        cfg.Spool.ReclaimInterval,
		DataKeyMaxAge:          cfg.Encryption.DataKeyRotation,
		Flush:                  cfg.Spool.Flush,
		Faults:                 cfg.Spool.Faults,
	}

	// Restore intent: restored data must be opened with its fresh writer
	// identity (same-identity rollback is rejected here and again during
	// adoption). The adoption swaps the stored identity atomically with a
	// durable restore marker.
	restoreAdoption, intentPath, err := restoreAdoptionFor(cfg)
	if err != nil {
		db.cancel()
		return nil, err
	}
	store, err := state.Open(dataPath, cfg.NodeID, cfg.DBID, state.Options{
		OriginSigning:            cfg.OriginSigning,
		SnapshotAtomicMergeBytes: cfg.Replication.SnapshotAtomicMergeBytes,
		MigrateUnsignedBaseline:  cfg.originBaselineMigration,
		MigrateMergePolicies:     cfg.mergePolicyMigration,
		Spool:                    spoolOpt,
		Limits:                   codec.Limits{MaxValueBytes: cfg.MaxReplicatedValueBytes, MaxMutations: cfg.MaxBatchMutations, MaxTransactionBytes: cfg.MaxTransactionBytes},
		Logger:                   cfg.Logger,
		AsyncDurability:          cfg.Durability.Mode == DurabilityAsync,
		Restore:                  restoreAdoption,
	})
	if err != nil {
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
	if len(cfg.Tables) > 0 {
		db.recordDB = rime.New()
		db.recordGeneration = 1
		db.recordAdapter, err = rimeadapter.New(store, db.recordDB, manifest, recordcodec.NewCodecRegistry(), recordcodec.Limits{
			MaxBytes: uint64(cfg.MaxReplicatedValueBytes),
		})
		if err != nil {
			db.recordDB.Close()
			db.recordDB = nil
			db.closeStore()
			db.cancel()
			return nil, err
		}
		if err := db.configureRecordAdapter(db.recordAdapter); err != nil {
			db.recordDB.Close()
			db.recordDB = nil
			db.closeStore()
			db.cancel()
			return nil, err
		}
		db.recordTables = make(map[string]any, len(cfg.Tables))
		for _, definition := range cfg.Tables {
			table, regErr := definition.register(db.recordAdapter)
			if regErr != nil {
				db.recordDB.Close()
				db.recordDB = nil
				db.closeStore()
				db.cancel()
				return nil, regErr
			}
			db.recordTables[strings.ToLower(definition.name)] = table
		}
	}
	db.setState(StateRebuilding)
	db.applyMu.Lock()
	progress.phase(OpenRebuilding)
	var observeRebuild func(rimeadapter.RebuildProgress)
	if progress != nil {
		observeRebuild = func(p rimeadapter.RebuildProgress) {
			progress.processed.Store(p.ProcessedItems)
			progress.materializerProgress(p.CurrentTable, p.RowsInserted, p.RowsSkipped)
		}
	}
	if rebuildErr := db.recordAdapter.Rebuild(ctx, 1024, observeRebuild); rebuildErr != nil {
		db.applyMu.Unlock()
		db.setState(StateFailed)
		db.closeRecordDB()
		db.closeStore()
		db.cancel()
		return nil, fmt.Errorf("murmur: rebuild: %w", rebuildErr)
	}
	progress.phase(OpenFinalizing)
	if gen, err := store.StateGeneration(); err != nil {
		db.applyMu.Unlock()
		db.setState(StateFailed)
		db.closeRecordDB()
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
			db.log.Warn("murmur: remove restore intent failed", "err", err.Error())
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

	db.subMgr = newSubscriptionManager(db.ctx, cfg.Subscription)

	if cfg.Files.Enabled {
		files, err := openFileStore(db)
		if err != nil {
			db.setState(StateFailed)
			db.closeRecordDB()
			db.closeStore()
			db.cancel()
			return nil, err
		}
		db.files = files
	}

	if err := ctx.Err(); err != nil {
		_ = db.Close()
		return nil, err
	}
	db.setState(StateReady)
	if db.repl != nil {
		db.startReplication(db.repl)
	}
	if cfg.Durability.SyncInterval > 0 || cfg.Durability.MaxUnsyncedBytes > 0 {
		db.wg.Add(1)
		go db.periodicSyncLoop()
	}
	db.wg.Add(1)
	go db.gcLoop()
	db.wg.Add(1)
	go db.valueGCloop()
	if db.files != nil && (len(db.files.sources()) > 0 || db.fetchDiscoveryActive()) && cfg.Files.FetchInterval >= 0 {
		db.wg.Add(1)
		go db.fetchLoop()
	}

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
			db.log.Warn("murmur: start backup worker failed", "err", err.Error())
		}
	}

	return db, nil
}

func (db *DB) closeStore() {
	if db.store != nil {
		_ = db.store.Close()
		db.store = nil
	}
}

func (db *DB) closeRecordDB() {
	if db.recordDB != nil {
		db.recordDB.Close()
		db.recordDB = nil
		db.recordAdapter = nil
		db.recordTables = nil
	}
}

// groupCommitEnabled reports whether synchronous group commit is active.
func (db *DB) groupCommitEnabled() bool { return db.grouper != nil }

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

// crashHooks injects failures at data and schema durability boundaries. It is nil in production;
// white-box crash tests set it to prove every boundary converges to durable state
// authoritative state.
type crashHooks struct {
	beforeDurable     func() error
	afterDurable      func() error
	beforeSchemaStore func() error
	afterSchemaStore  func() error
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

// --- replication applier ---

// ApplyRemote durably merges a received batch and publishes its winning rows
// through the managed RIME adapter.
// It satisfies replication.Applier. Acknowledgements are sent by the
// replication manager only after this returns nil.
func (db *DB) ApplyRemote(ctx context.Context, batch *codec.MutationBatch) error {
	if st := db.getState(); st == StateClosed || st == StateClosing || st == StateFailed {
		return fmt.Errorf("murmur: apply in state %s", st)
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
		return fmt.Errorf("murmur: writer admission: %w", err)
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
		var rerr error
		rerr = db.rebuildRecordMaterializer(ctx)
		if rerr != nil {
			db.log.Error("rebuild failed", "err", rerr.Error())
			db.setState(StateFailed)
			return rerr
		}
		failed = false
		return nil
	}
	err = db.applyRemoteRecords(ctx, res)
	if err != nil {
		db.log.Error("typed remote apply failed; database failed closed", "err", err.Error())
		db.setState(StateFailed)
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
// writer admission and one Spool sync. Transaction IDs, receipts, and origin
// sequence positions remain independent. RIME publication completes before
// acknowledgement, after the durable Spool commit.
func (db *DB) ApplyRemoteGroup(ctx context.Context, batches []*codec.MutationBatch) error {
	if len(batches) == 0 {
		return nil
	}
	if len(batches) == 1 {
		return db.ApplyRemote(ctx, batches[0])
	}
	if st := db.getState(); st == StateClosed || st == StateClosing || st == StateFailed {
		return fmt.Errorf("murmur: apply in state %s", st)
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
		return fmt.Errorf("murmur: writer admission: %w", err)
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
		rerr := db.rebuildRecordMaterializer(ctx)
		if rerr != nil {
			db.log.Error("rebuild failed", "err", rerr.Error())
			db.setState(StateFailed)
			return rerr
		}
		failed = false
		return nil
	}
	err = db.applyRemoteRecords(ctx, res)
	if err != nil {
		db.setState(StateFailed)
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
		return false, fmt.Errorf("murmur: snapshot apply in state %s", st)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if last {
		release, err := db.quiesceRecordPreparation(ctx)
		if err != nil {
			return false, err
		}
		defer release()
	}
	db.metrics.applyInflight.Add(1)
	defer db.metrics.applyInflight.Add(-1)
	ticket, err := db.sched.Admit(ctx, WriterRemote)
	if err != nil {
		return false, fmt.Errorf("murmur: writer admission: %w", err)
	}
	defer ticket.Release()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.drainGroupCommitsLocked()
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
		if err := db.rebuildRecordMaterializer(ctx); err != nil {
			db.metrics.snapshotApplyFailures.Add(1)
			db.setState(StateFailed)
			return false, fmt.Errorf("murmur: rebuild typed records after snapshot: %w", err)
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
		return fmt.Errorf("murmur: peer NodeID cannot be zero")
	}
	if peer.NodeID == db.cfg.NodeID {
		return fmt.Errorf("murmur: cannot add self as peer")
	}
	repl := db.replManager()
	if repl == nil {
		return fmt.Errorf("murmur: replication not configured")
	}
	repl.AddPeer(peer.NodeID, peer.Addrs)
	// Clear any persisted retirement so the next successful
	// authentication starts a fresh admission obligation. Exclusion is
	// cleared by the manager above.
	if db.store != nil {
		if err := db.store.ReadmitMember(peer.NodeID); err != nil {
			db.log.Warn("murmur: readmit member failed", "err", err.Error())
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
		return fmt.Errorf("murmur: peer NodeID cannot be zero")
	}
	repl := db.replManager()
	if repl == nil {
		return fmt.Errorf("murmur: replication not configured")
	}
	repl.RemovePeer(nodeID)
	// Persist retirement alongside the exclusion the manager records:
	// the member holds no retention obligation and handshake traffic
	// cannot re-admit it.
	if db.store != nil {
		if err := db.store.RetireMember(nodeID); err != nil {
			db.log.Warn("murmur: retire member failed", "err", err.Error())
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
		return fmt.Errorf("murmur: peer NodeID cannot be zero")
	}
	if nodeID == db.cfg.NodeID {
		return fmt.Errorf("murmur: cannot force sync with self")
	}
	repl := db.replManager()
	if repl == nil {
		return fmt.Errorf("murmur: replication not configured")
	}
	if err := repl.ForceSync(ctx, nodeID); err != nil {
		if errors.Is(err, replication.ErrPeerExcluded) {
			return fmt.Errorf("murmur: peer %s is locally excluded: %w", nodeID, ErrPeerExcluded)
		}
		if errors.Is(err, replication.ErrPeerNotFound) {
			return fmt.Errorf("murmur: unknown peer %s: %w", nodeID, ErrPeerNotFound)
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
		cached.OpenProgress = db.openProgress.snapshot()
		return cached
	}
	return db.statusLive()
}

// statusLive reads the open store. Callers must ensure the store is usable.
func (db *DB) statusLive() Status {
	st := Status{
		State:        db.getState(),
		NodeID:       db.cfg.NodeID,
		Uptime:       time.Since(db.openedAt),
		OpenProgress: db.openProgress.snapshot(),
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
	st.SpoolDiskBytes = pm.DiskBytes
	st.SpoolKeys = pm.Keys
	st.SpoolPendingBytes = pm.PendingBytes
	st.SpoolPendingRecords = pm.PendingRecords
	st.SpoolBlocksWritten = pm.BlocksWritten
	st.SpoolBytesWritten = pm.BytesWritten
	st.SpoolCompactions = pm.Compactions
	st.SpoolStorageFailure = pm.StorageFailure
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
				NodeID:                 p.NodeID,
				Addrs:                  p.Addrs,
				Connected:              p.Connected,
				Dynamic:                p.Dynamic,
				SchemaAgreed:           p.SchemaAgreed,
				SnapshotRequired:       p.SnapshotRequired,
				AwaitingSnapshot:       p.AwaitingSnapshot,
				SnapshotChunksReceived: p.SnapshotChunksReceived,
				SnapshotChunksTotal:    p.SnapshotChunksTotal,
				Retired:                retired,
				Excluded:               excluded,
				Selected:               p.Selected,
				MembershipState:        memState,
				RetirementDeadline:     deadline,
				RTT:                    p.RTT,
				LastSeen:               p.LastSeen,
				LastHandshake:          p.LastHandshake,
				LastSend:               p.LastSend,
				LastRecv:               p.LastRecv,
				LastAntiEntropy:        p.LastAntiEntropy,
				RemoteSchemaEpoch:      p.RemoteSchemaEpoch,
				RemoteSchemaHash:       p.RemoteSchemaHash,
				BytesSent:              p.BytesSent,
				BytesReceived:          p.BytesReceived,
				QueuedNeed:             p.QueuedNeed,
				QueuedCtrl:             p.QueuedCtrl,
				QueuedSchema:           p.QueuedSchema,
				Have:                   p.Have,
				Sent:                   p.Sent,
				LagByOrigin:            make(map[NodeID]uint64, len(applied)),
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
	// Either trigger may be unset; a nil channel blocks forever in the
	// select below, leaving the other trigger (at least one is set, see
	// Open) to drive the loop.
	var intervalC <-chan time.Time
	if d := db.cfg.Durability.SyncInterval; d > 0 {
		ticker := time.NewTicker(d)
		defer ticker.Stop()
		intervalC = ticker.C
	}
	var sizeC <-chan time.Time
	if db.cfg.Durability.MaxUnsyncedBytes > 0 {
		sizeTicker := time.NewTicker(10 * time.Millisecond)
		defer sizeTicker.Stop()
		sizeC = sizeTicker.C
	}
	threshold := uint64(db.cfg.Durability.MaxUnsyncedBytes)
	for {
		select {
		case <-db.ctx.Done():
			return
		case <-intervalC:
			if !db.periodicSyncOnce() {
				return
			}
		case <-sizeC:
			if db.store.UnsyncedBytes() < threshold {
				continue
			}
			if !db.periodicSyncOnce() {
				return
			}
		}
	}
}

// periodicSyncOnce runs one scheduled durability sync. It reports false
// when the sync failed and the node failed closed.
func (db *DB) periodicSyncOnce() bool {
	if err := db.store.Sync(); err != nil {
		db.metrics.periodicSyncFailures.Add(1)
		db.log.Error("murmur: periodic durability sync failed", "err", err.Error())
		db.setState(StateFailed)
		db.cancel()
		return false
	}
	db.metrics.periodicSyncs.Add(1)
	return true
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
	_ = db.gcContext(db.ctx, withReceipts)
}

// GC runs one operator-triggered log and receipt collection pass. It removes
// only history permitted by persisted peer acknowledgements and retention
// limits; callers should use a deadline for large backlogs.
func (db *DB) GC(ctx context.Context) error {
	return db.gcContext(ctx, true)
}

func (db *DB) gcContext(ctx context.Context, withReceipts bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := db.requireWrite(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	db.gcRunMu.Lock()
	defer db.gcRunMu.Unlock()
	if db.getState() != StateReady {
		return ErrNotReady
	}
	origins, err := db.store.KnownOrigins()
	if err != nil {
		db.metrics.gcFailures.Add(1)
		return err
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
	if ticket, err := db.sched.Admit(ctx, WriterMaintenance); err != nil {
		return err
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
		// unit re-admits so interactive writers interleave. Units repeat
		// until a short return proves the origin drained: one unit per
		// 30s pass caps collection at ~137 batches/s, which any
		// sustained workload outruns (unbounded retained growth).
		for unit := 0; ; unit++ {
			ticket, err := db.sched.Admit(ctx, WriterMaintenance)
			if err != nil {
				return err
			}
			n, cerr := db.store.CollectLog(origin, floor, cutoff, db.cfg.Replication.MinRetainedBatches)
			ticket.Release()
			if cerr != nil {
				db.metrics.gcFailures.Add(1)
				db.log.Debug("log GC failed", "origin", origin.String(), "err", cerr.Error())
				return cerr
			}
			db.metrics.gcLogCollected.Add(uint64(n))
			if n < state.CollectUnitCap {
				break
			}
		}
		floors[origin] = floor
	}
	if withReceipts {
		for {
			ticket, err := db.sched.Admit(ctx, WriterMaintenance)
			if err != nil {
				return err
			}
			n, cerr := db.store.CollectReceipts(floors)
			ticket.Release()
			if cerr != nil {
				db.metrics.gcFailures.Add(1)
				db.log.Debug("receipt GC failed", "err", cerr.Error())
				return cerr
			}
			db.metrics.gcReceiptsCollected.Add(uint64(n))
			if n < state.CollectUnitCap {
				break
			}
		}
	}
	return ctx.Err()
}

func (db *DB) valueGCloop() {
	defer db.wg.Done()
	// Spool manages segments in background; valueGCloop is kept for loop draining.
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
		return nil, fmt.Errorf("murmur: replication requires TLS credentials")
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
	mgr, err := replication.NewManager(replication.ManagerConfig{
		Codec:                       db.cfg.Spool.Codec,
		Codecs:                      db.cfg.Codecs,
		TrustedSnapshotSources:      append([]ids.NodeID(nil), db.cfg.Replication.TrustedSnapshotSources...),
		Store:                       db.store,
		Applier:                     db,
		Creds:                       db.replCreds,
		Local:                       db.cfg.NodeID,
		DBID:                        db.store.DBID(),
		SchemaEpoch:                 schemaId.Epoch,
		SchemaHash:                  schemaId.Hash,
		SchemaAuthor:                schemaId.Author,
		SchemaTime:                  schemaId.TimeCreated,
		AcceptRemoteSchema:          db.cfg.Schema.acceptRemoteSchema(),
		SchemaSync:                  db,
		ListenAddr:                  db.cfg.Replication.ListenAddr,
		Peers:                       peers,
		MaxBatchBytes:               db.cfg.Replication.MaxBatchBytes,
		MaxBatchMutations:           db.cfg.Replication.MaxBatchMutations,
		SendInterval:                db.cfg.Replication.SendInterval,
		DialInterval:                db.cfg.Replication.DialInterval,
		AckInterval:                 db.cfg.Replication.AckInterval,
		AckRetention:                db.cfg.Replication.MaxOfflineLogRetention,
		SnapshotChunkCells:          db.cfg.Replication.SnapshotChunkCells,
		MaxSnapshotBytes:            uint64(db.cfg.Replication.MaxSnapshotBytes),
		SnapshotTransferTimeout:     db.cfg.Replication.SnapshotTransferTimeout,
		SnapshotRequestTimeout:      db.cfg.Replication.SnapshotRequestTimeout,
		Fanout:                      db.cfg.Replication.Fanout,
		PeerRotationInterval:        db.cfg.Replication.PeerRotationInterval,
		AntiEntropyInterval:         db.cfg.Replication.AntiEntropyInterval,
		MaxConcurrentRepairs:        db.cfg.Replication.MaxConcurrentRepairs,
		MaxReplicationSessions:      db.cfg.Replication.MaxReplicationSessions,
		MaxQUICConnections:          db.cfg.Replication.MaxQUICConnections,
		EnablePlumtree:              db.cfg.Replication.Dissemination == DisseminationPlumtree,
		AdvertiseProtocolVersion:    db.cfg.Replication.ProtocolVersionOverride,
		AdvertiseMinProtocolVersion: db.cfg.Replication.MinProtocolVersionOverride,
		MaxTransactionBytes:         db.cfg.MaxTransactionBytes,
		Limits:                      codec.Limits{MaxValueBytes: db.cfg.MaxReplicatedValueBytes, MaxMutations: db.cfg.MaxBatchMutations, MaxTransactionBytes: db.cfg.MaxTransactionBytes},
		Logger:                      db.cfg.Logger,
	})
	if err != nil {
		return nil, err
	}

	memCfg := db.cfg.Replication.Membership
	bootstrap := append([]string(nil), memCfg.Bootstrap...)
	if len(bootstrap) == 0 && len(db.cfg.Replication.Bootstrap) > 0 {
		bootstrap = append(bootstrap, db.cfg.Replication.Bootstrap...)
	}
	if memCfg.Enabled || len(bootstrap) > 0 {
		advAddr := memCfg.AdvertiseAddr
		if advAddr == "" {
			advAddr = db.cfg.Replication.ListenAddr
		}
		tr, err := transport.NewMemberlistTransport(transport.MemberlistTransportConfig{
			LocalNodeID:   db.cfg.NodeID,
			DBID:          db.store.DBID(),
			Creds:         db.replCreds,
			Pool:          mgr.Pool(),
			BindAddr:      "127.0.0.1:0",
			AdvertiseAddr: advAddr,
		})
		if err == nil {
			var slogLogger *slog.Logger
			if db.cfg.Logger != nil {
				slogLogger = slog.New(slog.NewTextHandler(io.Discard, nil))
			}
			svc, err := replication.NewMembershipService(
				db.cfg.NodeID,
				db.store.DBID(),
				replication.MembershipConfig{
					Bootstrap:      bootstrap,
					AdvertiseAddr:  advAddr,
					ProbeInterval:  memCfg.ProbeInterval,
					ProbeTimeout:   memCfg.ProbeTimeout,
					GossipInterval: memCfg.GossipInterval,
					GossipNodes:    memCfg.GossipNodes,
					IndirectChecks: memCfg.IndirectChecks,
				},
				tr,
				mgr,
				slogLogger,
			)
			if err == nil {
				mgr.SetMembership(svc)
			} else {
				_ = tr.Shutdown()
			}
		}
	}
	return mgr, nil
}

// startReplication runs a manager to completion.
func (db *DB) startReplication(mgr *replication.Manager) {
	done := make(chan struct{})
	db.replDone = done
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		defer close(done)
		if err := mgr.Run(db.ctx); err != nil {
			db.mu.Lock()
			db.replExitErr = err
			db.mu.Unlock()
		}
	}()
}

// ReplExitError reports the replication manager's terminal error, or nil
// when it is still running or exited cleanly.
func (db *DB) ReplExitError() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.replExitErr
}

// --- backup & restore ---

// HoldCommits implements backup.SourceDB.
func (db *DB) HoldCommits() func() {
	if db.store == nil {
		return func() {}
	}
	return db.store.HoldCommits()
}

// Checkpoint implements backup.SourceDB, taking an online snapshot via hard links.
func (db *DB) Checkpoint(ctx context.Context, stagingDataDir string) (func() error, error) {
	if db.store == nil {
		return nil, errors.New("murmur: store is closed")
	}
	cp, err := db.store.SpoolCheckpoint(ctx, stagingDataDir)
	if err != nil {
		return nil, err
	}
	return cp.Release, nil
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

// SchemaTables returns the current replicated schema table definitions.
func (db *DB) SchemaTables() ([]schema.TableSchema, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.store == nil {
		return nil, nil
	}
	cur, err := db.store.LoadSchemaManifest()
	if err != nil {
		return nil, err
	}
	if cur == nil {
		return nil, nil
	}
	return cur.Tables, nil
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
	db.recordPrepMu.Lock()
	for db.recordPreparers > 0 {
		db.recordPrepCond.Wait()
	}
	db.recordPrepMu.Unlock()
	// Drain in-flight work (new work is rejected by state checks). The
	// drains also synchronize with an in-progress key rotation or rewrite
	// so the manager read below cannot race a maintenance restart.
	db.writeMu.Lock()
	db.writeMu.Unlock()
	// Grouped commits release writeMu before their shared fsync; drain
	// them before the applyMu drain so no group is in flight past Close.
	if db.grouper != nil {
		db.grouper.flush()
	}
	db.groupWG.Wait()
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
	if (db.cfg.Durability.SyncInterval > 0 || db.cfg.Durability.MaxUnsyncedBytes > 0) && db.store != nil {
		if err := db.store.Sync(); err != nil {
			first = fmt.Errorf("murmur: final durability sync: %w", err)
		}
	}
	if db.subMgr != nil {
		db.subMgr.close()
		db.subMgr = nil
	}
	if db.recordDB != nil {
		db.recordMu.Lock()
		db.recordDB.Close()
		for _, retired := range db.retiredRecordDBs {
			retired.Close()
		}
		db.recordMu.Unlock()
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
	db.setState(StateClosed)
	return first
}

var _ replication.Applier = (*DB)(nil)
