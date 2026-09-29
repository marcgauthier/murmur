package replicateddb

import "sync/atomic"

// dbMetrics holds monotonically increasing node-local counters: writer
// scheduling, remote apply, snapshots, log GC, schema convergence, and
// membership operations. All fields are updated with atomic operations;
// DB is always used by pointer so the struct is never copied after Open.
//
// Together with replication.StatsSnapshot (exposed as Status.Replication)
// this covers the diagnostics surface in
// architecture/runtime-and-diagnostics.md section 52 for the subsystems that
// exist today. Unimplemented subsystems (SWIM membership, bounded peer
// selection, Plumtree, overload budgets, High/Low bridge, file objects,
// subscriptions) have no counters; their zero values must not be mistaken
// for measured zeros.
type dbMetrics struct {
	// Local writer (serialized write coordinator + durable commits).
	localCommits            atomic.Uint64
	localCommitMutations    atomic.Uint64
	localCommitLatencyNanos atomic.Uint64
	periodicSyncs           atomic.Uint64
	periodicSyncFailures    atomic.Uint64
	writeAcquisitions       atomic.Uint64
	writeQueueWaitNanos     atomic.Uint64

	// Remote apply and deferred SQLite materialization.
	remoteApplies           atomic.Uint64
	remoteApplyMutations    atomic.Uint64
	remoteApplyWinners      atomic.Uint64
	remoteApplyLatencyNanos atomic.Uint64
	remoteApplyFailures     atomic.Uint64
	applyInflight           atomic.Int64

	// Snapshot receive (via ApplySnapshotChunk).
	snapshotChunksApplied   atomic.Uint64
	snapshotAppliesComplete atomic.Uint64
	snapshotApplyFailures   atomic.Uint64

	// Materializer maintenance.
	rebuilds     atomic.Uint64
	rebuildNanos atomic.Uint64
	repairs      atomic.Uint64

	// Log and receipt GC.
	gcRuns              atomic.Uint64
	gcNanos             atomic.Uint64
	gcLogCollected      atomic.Uint64
	gcReceiptsCollected atomic.Uint64
	gcFailures          atomic.Uint64

	// Schema convergence (SyncSchemas decisions and Migrate).
	schemaConflicts  atomic.Uint64
	schemaAgreements atomic.Uint64
	schemaSyncNeeds  atomic.Uint64
	schemaAdoptions  atomic.Uint64
	schemaMerges     atomic.Uint64
	schemaMergeReuse atomic.Uint64
	schemaMigrations atomic.Uint64

	// Membership operations (static mesh; SWIM pending).
	peersAdded   atomic.Uint64
	peersRemoved atomic.Uint64
	forceSyncs   atomic.Uint64
}

// MetricsSnapshot is a point-in-time copy of node-local counters. Latency
// fields are totals in nanoseconds; divide by the matching count for the
// mean. It never contains secrets.
type MetricsSnapshot struct {
	LocalCommits            uint64
	LocalCommitMutations    uint64
	LocalCommitLatencyNanos uint64
	PeriodicSyncs           uint64
	PeriodicSyncFailures    uint64
	WriteAcquisitions       uint64
	WriteQueueWaitNanos     uint64

	RemoteApplies           uint64
	RemoteApplyMutations    uint64
	RemoteApplyWinners      uint64
	RemoteApplyLatencyNanos uint64
	RemoteApplyFailures     uint64
	ApplyInflight           int64

	SnapshotChunksApplied    uint64
	SnapshotAppliesCompleted uint64
	SnapshotApplyFailures    uint64

	Rebuilds     uint64
	RebuildNanos uint64
	Repairs      uint64

	GCRuns              uint64
	GCNanos             uint64
	GCLogCollected      uint64
	GCReceiptsCollected uint64
	GCFailures          uint64

	SchemaConflicts  uint64
	SchemaAgreements uint64
	SchemaSyncNeeds  uint64

	// StmtCacheHits/Misses count prepared-statement cache lookups
	// summed over the engine's read and write caches.
	StmtCacheHits    uint64
	StmtCacheMisses  uint64
	SchemaAdoptions  uint64
	SchemaMerges     uint64
	SchemaMergeReuse uint64
	SchemaMigrations uint64

	PeersAdded   uint64
	PeersRemoved uint64
	ForceSyncs   uint64

	// Scheduler carries writer-share admission diagnostics.
	Scheduler SchedulerSnapshot
}

// snapshot copies the current counters.
func (m *dbMetrics) snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		LocalCommits:            m.localCommits.Load(),
		LocalCommitMutations:    m.localCommitMutations.Load(),
		LocalCommitLatencyNanos: m.localCommitLatencyNanos.Load(),
		PeriodicSyncs:           m.periodicSyncs.Load(),
		PeriodicSyncFailures:    m.periodicSyncFailures.Load(),
		WriteAcquisitions:       m.writeAcquisitions.Load(),
		WriteQueueWaitNanos:     m.writeQueueWaitNanos.Load(),

		RemoteApplies:           m.remoteApplies.Load(),
		RemoteApplyMutations:    m.remoteApplyMutations.Load(),
		RemoteApplyWinners:      m.remoteApplyWinners.Load(),
		RemoteApplyLatencyNanos: m.remoteApplyLatencyNanos.Load(),
		RemoteApplyFailures:     m.remoteApplyFailures.Load(),
		ApplyInflight:           m.applyInflight.Load(),

		SnapshotChunksApplied:    m.snapshotChunksApplied.Load(),
		SnapshotAppliesCompleted: m.snapshotAppliesComplete.Load(),
		SnapshotApplyFailures:    m.snapshotApplyFailures.Load(),

		Rebuilds:     m.rebuilds.Load(),
		RebuildNanos: m.rebuildNanos.Load(),
		Repairs:      m.repairs.Load(),

		GCRuns:              m.gcRuns.Load(),
		GCNanos:             m.gcNanos.Load(),
		GCLogCollected:      m.gcLogCollected.Load(),
		GCReceiptsCollected: m.gcReceiptsCollected.Load(),
		GCFailures:          m.gcFailures.Load(),

		SchemaConflicts:  m.schemaConflicts.Load(),
		SchemaAgreements: m.schemaAgreements.Load(),
		SchemaSyncNeeds:  m.schemaSyncNeeds.Load(),
		SchemaAdoptions:  m.schemaAdoptions.Load(),
		SchemaMerges:     m.schemaMerges.Load(),
		SchemaMergeReuse: m.schemaMergeReuse.Load(),
		SchemaMigrations: m.schemaMigrations.Load(),

		PeersAdded:   m.peersAdded.Load(),
		PeersRemoved: m.peersRemoved.Load(),
		ForceSyncs:   m.forceSyncs.Load(),
	}
}

// Metrics returns a point-in-time copy of node-local counters. See Status
// for the combined diagnostic snapshot including replication and peers.
func (db *DB) Metrics() MetricsSnapshot {
	m := db.metrics.snapshot()
	m.Scheduler = db.sched.Snapshot()
	if db.engine != nil {
		m.StmtCacheHits, m.StmtCacheMisses = db.engine.StmtCacheStats()
	}
	return m
}
