# API and configuration

Public Go API, configuration example, and internal interfaces.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [5. Public Package API](#5-public-package-api)
- [66. Package Configuration Example](#66-package-configuration-example)
- [80. Suggested Internal Interfaces](#80-suggested-internal-interfaces)

---

## 5. Public Package API

`Config.OriginSigning` is mandatory, including offline writers. It supplies a separate Ed25519 private key and explicit origin key registry. `Replication.TrustedSnapshotSources` controls merged-state recovery and `ScanReplicationLog` returns complete signed transactions. Legacy SQL stores must be exported with the previous release and reopened from a fresh directory. See [origin signatures](origin-signatures.md) for the exact format and trust boundaries.

Keep the public API small.

`SchemaConfig` accepts the replicated version, table descriptors and
remote-adoption policy. It no longer accepts raw SQL schema or local-object
DDL; define native tables and indexes through `Config.Models`, `Config.Tables` and typed RIME
options.

Initial concept:

```go
type Config struct {
    Path        string
    NodeID      NodeID
    DBID        DBID
    Encryption EncryptionConfig
    Replication ReplicationConfig
    Schema      SchemaConfig
    Spool       SpoolConfig
    Durability  DurabilityConfig
    Backup      BackupScheduleConfig
    Files       FilesConfig
    Scheduling  WriterSchedulingConfig
    Subscription SubscriptionConfig
    MaxReplicatedValueBytes int
    MaxBatchMutations       int
    MaxTransactionBytes     int64
    Logger      Logger
    OnOpenProgress func(OpenProgress)
}

func Open(ctx context.Context, cfg Config) (*DB, error)

type DB struct {
    // unexported internals
}

func (db *DB) AddPeer(ctx context.Context, peer Peer) error
func (db *DB) RemovePeer(ctx context.Context, nodeID NodeID) error
func (db *DB) ForceSync(ctx context.Context, nodeID NodeID) error

func (db *DB) RotateStorageKey(
    ctx context.Context,
    material KeyMaterial,
) error

func (db *DB) RotateDataKey(ctx context.Context) error
func (db *DB) SetEncryptionAlgorithm(ctx context.Context, algorithm EncryptionAlgorithm) error
func (db *DB) RewriteEncryptedFiles(ctx context.Context) error
func (db *DB) EncryptionStatus() EncryptionStatus

func (db *DB) Status() Status
func (db *DB) Metrics() MetricsSnapshot
func (db *DB) DurabilityMode() DurabilityMode
func (db *DB) Sync(ctx context.Context) error
func (db *DB) GC(ctx context.Context) error
func (db *DB) Close() error
```

`Peers` and `AddPeer` supply bootstrap candidates, not permanent replication links. Discovery and the bounded scheduler select actual connections. `RemovePeer` explicitly retires and persistently excludes a NodeID locally, including from subsequent discovery; `AddPeer` clears that exclusion. This is local administrative policy, not a cluster-wide revocation. `ForceSync` schedules immediate synchronization subject to the same session and connection limits as background work. See [Sections 26](membership-and-transport.md#26-replication-transport)–[28](replication-and-dissemination.md#28-quic-connection-model) for membership configuration and connection budgets.

`GC` runs an operator-triggered log and receipt collection pass. It obeys
persisted peer acknowledgements and configured retention limits, so it never
collects history still required by an admitted peer. Use a context deadline
when collecting a large backlog.

The `database/sql/driver` wrapper, SQL statement/query/transaction methods,
SQL subscription worker, SQLite materializer, and SQLite package have been
removed. Reusable parameterized queries use the `ItemQuery.Compile`
API. `DB.Subscribe` is the managed subscription surface.

`Config.OnOpenProgress func(OpenProgress)` optionally reports startup while
`Open` is blocked. `OpenProgress` contains `Phase`, `StartedAt`, `Elapsed`,
`PhaseElapsed`, `CountedItems`, `TotalItems`, `ProcessedItems`, `TotalItemsKnown`,
`PercentComplete`, `EstimatedRemaining`, `EstimateKnown`, `CurrentTable`,
`RowsInserted`, `RowsSkipped`, and terminal `Error`. `OpenPhase` values are
`OpenOpening`, `OpenRebuilding`, `OpenIndexing`, `OpenFinalizing`,
`OpenReady`, `OpenFailed`, and `OpenCancelled`. Only `OpenReady` signals success.
`Status.OpenProgress *OpenProgress` returns a copy of the retained startup
snapshot, or nil when reporting was disabled. Callbacks are excluded from JSON
configuration serialization. See [startup progress](runtime-and-diagnostics.md#startup-progress-reporting)
for processed-cell, timing, callback, and cancellation semantics. Totals and
estimates remain unknown; their numeric fields stay zero. `OpenCounting` remains
defined for compatibility but is never emitted. No counting scan is performed.

`Open` requires `cfg.Spool` and managed definitions in `Config.Models` or `Config.Tables`. Only
replicated definitions enter the cluster manifest, so a local-only database
may have an empty replicated table set. `Schema.Tables` is manifest metadata
produced from managed definitions; it is not an alternate SQL configuration.
Do not infer replicated table or column declarations from existing data.
Validate definitions against [Section 6](schema.md#6-schema-rules-for-version-1)
and persisted metadata before rebuilding data or starting replication. Errors
must identify the offending table, field, or index.

### Automatic model API

`Config.Models: []any{Device{}, User{}}` discovers stable schemas automatically.
Use `ID ids.RowID` tagged `rime:"ID"`, or legacy `ID rime:"primary"`.
A separate `primary` field derives unset IDs with UUIDv5; without it, generation
uses UUIDv4. Explicit IDs are retained. `Model[T]` provides
identity/configuration overrides and is the only table-definition API.
`Transaction`, item CRUD, partial updates, and insert/update/save/delete batches
use the existing durable coordinator. `MigrateModels` is explicit and additive.
See [automatic model architecture](model-api.md) for exact identity and
registration rules and [application usage](../USAGE.md) for examples.

### Managed record API status

`RecordOptions.Scope` defaults to `TableScopeReplicated`. Set it to
`TableScopeNodeLocal` for a persistent table whose cells stay in the
node-local Spool namespace and are absent from replication logs and snapshots.
Set it to `TableScopeEphemeral` for an in-memory table that resets on
reopen. Both local scopes currently support LWW fields only. Persistent
node-local writes can share one atomic Spool commit with replicated
writes; ephemeral writes cannot share a transaction with durable tables.
Node-local tables are not eligible for bridge import or export.


The generic item API is the only supported database API. `Open` requires
at least one model or compiled `Config.Tables` definition and rejects SQL-only
`Schema.Tables` configurations before opening Spool:

```go
type Contact struct {
    ID   ids.RowID `rime:"primary"`
    Name string
}

definition, err := murmur.Model[Contact](murmur.ModelOptions{
    Name: "contacts", TableID: 17,
    RecordOptions: murmur.RecordOptions{
        PrimaryField: "ID",
        FieldIDs: map[string]uint32{"ID": 1, "Name": 2},
    },
})
if err != nil { return err }

refuseSchemaDrift := false
db, err := murmur.Open(ctx, murmur.Config{
    // Path, NodeID, encryption, Spool and origin signing are required.
    Schema: murmur.SchemaConfig{Version: 1, AcceptRemoteSchema: &refuseSchemaDrift},
    Tables: []murmur.TableDefinition{definition},
})
```

`Model[T]` with explicit `ModelOptions` requires a stable nonzero table ID,
a 16-byte primary field marked `rime:"primary"`, and explicit stable IDs for
all exported persisted fields. `InsertItem`, `GetItem`, `SaveItem`,
`UpdateFields`, `DeleteItem`, and their batch forms stage through
`DB.WriteTxContext` and `Tx`; `GetItem` populates the supplied pointer with a
mutable deep clone. `WriteTxContext` runs its callback before
writer admission, so independent callbacks can stage concurrently. Commits are
still serialized; stale transactions may return `rime.ErrConflict`, while
disjoint writes can proceed. The database tracks these in-flight callbacks so
`Close` drains them before closing RIME or Spool. `DB.Query` returns
a read-only query builder with `q` matchers, ordering, pagination,
`FindInto`, `FirstInto`, `Count`, `Exists`, `Explain`, read-only `Aggregate`,
and `GroupBy` with grouped aggregate methods; returned records are independent
deep copies.
`DB.InnerJoin` and `DB.LeftJoin` join two models on equal key fields over one
pinned read snapshot, and return detached `ItemJoinRow` pairs.
`ItemQuery.Compile` creates a reusable parameterized read query with `q.Param()`
placeholders; `In` and `WithContext` bind copies to a managed write overlay or
cancellation. Compiled terminals return detached rows and retain RIME's
parameter count/type validation. `DB.Subscribe` pins a RIME read snapshot while
capturing its observer cursor, then evaluates without holding the writer lock.
Events include the full current `Rows` snapshot and, for updates, deterministic
primary-key `Changes` (`ItemAdded`, `ItemUpdated`, or `ItemRemoved`).
Equality uses built-in canonical field semantics and registered custom codec
equality hooks. Keep `ItemSubscription.ResumeCursor()` and provide that
`ItemSubscriptionCursor` through `ItemSubscriptionOptions.ResumeFrom` to
resume after the retained sequence during the same database open. Tokens bind
the database ID, observer epoch, and RIME materializer generation, so tokens
from another database, another open, or a rebuild fail with
`ErrSubscriptionExpired`. The cursor is an in-memory observer position, not a
durable replication or application checkpoint; synchronous durability does not
make it survive reopening. A full bounded buffer or a rebuild emits
`EventReset` with `ErrSubscriptionReset`.
Use `q.StartsWith`/`EndsWith`/`Contains`/`Like` for RIME's indexed prefix,
suffix, substring, and LIKE predicates; these operations do not provide
tokenized full-text search or ranking.
If storage failure makes a commit outcome uncertain,
`WriteTxContext` returns `*CommitOutcomeUncertainError` with its `TxID`.
Reopen the database and call `HasTransactionReceipt(TxID)` to determine whether
that transaction committed.

Host-managed lifecycles can use `DB.BeginTx(ctx)`, then stage operations
through the returned `*Tx` and call `Commit`, `CommitContext`, or
`Rollback`. Commit follows the same writer scheduler and Spool-before-RIME
publication path as `WriteTxContext`. A transaction opened before a
schema/materializer generation change is rejected at commit. The former SQL
transaction methods are gone from Murmur's public API. Production has no SQLite
materializer or SQL application interface.
Use `q.Count()` for row counts and `q.Sum`/`q.Avg`/`q.Min`/`q.Max` with
`ItemQuery.Aggregate` or `GroupBy` to build aggregates. The current
durable facade accepts LWW fields, numeric MIN/MAX fields, top-level int64
PN_COUNTER fields, and top-level `[]string` OR_SET fields. Extrema, counters,
and sets change through `Min`, `Max`, `CounterAdd`,
`SetAdd`, and `SetRemove`; existing extrema fields cannot be
replaced directly. A new record's initial numeric value seeds its MIN/MAX field.
The removed SQL methods cannot bypass Spool. Opened databases rebuild directly
into RIME. A configuration whose
descriptor is an additive subset of the stored schema can reopen as an older
writer; unknown top-level and supported nested fields remain durable through
its writes. Custom field types can register stable codec IDs and versions with
encode, decode, clone, and equality hooks in `RecordOptions.Codecs`. Read
snapshots stay inside the engine: joins and subscriptions pin a local MVCC
snapshot and release it promptly so old versions can be reclaimed. Snapshot
IDs are local to one materializer generation, not cluster-wide timestamps.
`MigrateModels` and `MigrateRecords(ctx, completeDefinitions)` publish an
additive schema revision, rebuild the private RIME materializer from
Spool, and rebind adapter handles. Existing table and field IDs,
types, and merge policies must remain unchanged; drops and incompatible edits
fail before the manifest changes. Peers with compatible older bindings
can adopt the new manifest and retain fields they do not know. Operational
feature ports remain incomplete. See [the migration plan](migration-plan.md)
for the cutover gates.

A generic Storm-style item API serves every registered struct through one
function per operation, without per-table handles. `Model[T]` and runtime
exemplars capture type-erased operation bindings per table; `Open` and
migration helpers build a struct-type registry from them. `DB.InsertItem`,
`DB.GetItem` (populating the supplied pointer), `DB.SaveItem` (full-record
upsert), `DB.UpdateFields` (partial update, including zero values), and
`DB.DeleteItem` resolve the table from the item's Go type and commit through
the same Spool-before-RIME path as the engine. `DB.CounterAdd`, `DB.SetAdd`,
`DB.SetRemove`, `DB.Max`, and `DB.Min` mutate merge-policy fields the same
way. `Tx` offers the same ten operations in a managed transaction, and
`DB.Query` builds read-only
queries from `q` matchers (`Eq`, `Ne`, `Gt`, `Gte`, `Lt`, `Lte`, `In`, `And`,
`Or`, `Not`) with `Where`, `OrderBy`/`OrderByDescending`, `Limit`, `Offset`,
`In`/`WithContext` bindings, and `FindInto`/`FirstInto`/`Count`/
`Exists`, `Explain`, and `Aggregate` terminals, plus `GroupBy` grouped
aggregation over `q` descriptors. `DB.InnerJoin`/`DB.LeftJoin` join
two models on equal key fields over one pinned snapshot. `DB.Subscribe`
streams an initial snapshot plus coalesced primary-key diffs for a filtered
item query with observer cursors and reset semantics. `ItemQuery.Compile`
prepares a reusable
query whose `q.Param()` placeholders bind positionally per execution through
the same terminals; binds coerce through the same conversions as one-shot
queries, ordering/limit/offset are fixed at compile time, and
`In`/`WithContext` rebind copies for transactions and cancellation. Matchers
translate into typed RIME predicates so
equality and range filters keep using indexes; queryable field types are
exactly `string`, `bool`, integer/float widths, `ids.RowID`/`[16]byte`, and
`rime.UUID`. A struct registered for more than one table is ambiguous for
the item API and must use distinct model names. Arbitrary `map[string]any`
records without a struct definition remain unsupported.

`SpoolConfig` is required; use `DefaultSpoolConfig()` for standard settings. `Config.Path` is the database directory. Expose block/segment sizing, worker concurrency, pending-memory bounds, compaction, and compression. Validate settings before opening Spool. Encryption keys come from `Encryption.Key` or `Encryption.Provider`; never weaken the replication acknowledgement or durability contract through storage options.

`DurabilityConfig{Mode: DurabilityAsync, SyncInterval: time.Second}` enables
opt-in scheduled Spool syncs for applications accepting approximately one
second of unsynced acknowledged writes. `SyncInterval` must be non-negative
and is valid only with `DurabilityAsync`; zero keeps manual `DB.Sync` behavior.
`Metrics().PeriodicSyncs` and `PeriodicSyncFailures` report scheduled sync
results. See [Section 17](transactions.md#17-durability-modes)
for the durability contract.

`DurabilityConfig.MaxUnsyncedBytes` adds a size trigger beside the time
trigger: once approximately that many bytes have been written without a sync,
a durability sync runs. It is only valid with `DurabilityAsync`; zero
disables it. When both triggers are set, whichever is reached first fires,
bounding the loss window under bursty load. With both zero, asynchronous
commits sync only on explicit `DB.Sync` and graceful close.

`DurabilityConfig.GroupCommit` configures synchronous group commit for managed
typed writes. It is enabled by default in `DurabilitySynchronous` mode and
ignored in `DurabilityAsync` mode.
`MaxDelay` (default one millisecond; negative disables grouping) bounds the
leader's wait for concurrent transactions, `MaxTransactions` (default 64,
maximum 512) caps transactions per group, and `MaxBytes` (default 4 MiB,
maximum 64 MiB) caps the group's total encoded size. Grouped transactions
share one synced Spool commit and are each acknowledged after that shared
fsync, so the synchronous durability contract is unchanged. See
[Section 16](transactions.md#16-durable-first-typed-commit-ordering) for the commit
path and failure semantics.

```go
type SpoolConfig struct {
    WriteBufferSize   int
    SegmentSize       int64
    BlockSize         int
    Workers           int
    PendingMemoryCap  int64
    CompactionRatio   float64
    DisableCompaction bool
    Compression       spool.Compression
    Faults            *spool.FaultHooks
}
```

The default constructor sets 4 MiB write buffers, 64 MiB segments, 64 KiB blocks, background workers, 64 MiB pending-memory bounds, 0.5 compaction ratio, and `spool.CompressionDeflate`. Explicit `CompressionNone` disables compression. A missing/invalid storage budget is an error rather than silently allocating an unbounded cache.

Spool serves as the sole persistence backend under `Config.Path/data`. Detect and reject existing legacy Pebble or unsupported storage directories with `ErrUnsupportedStorageFormat` before creating or modifying storage files. No importer, automatic conversion, or Pebble runtime fallback is supported.

`Open` also requires `cfg.Encryption`: AES-256-GCM storage encryption with a 32-byte application wrapping key (directly or via a provider), and positive internal data-key rotation duration. Validate wrapping-material key length and algorithm before opening Spool; see [Sections 39](encryption.md#39-at-rest-encryption-via-spool-aes-256-gcm)–[43](encryption.md#43-application-key-rotation-and-compaction-rewriting) for rotation semantics.

Key-provider interfaces are application-facing because callers implement them; storage and transport interfaces remain internal. Rotation methods operate on the Spool storage layer described in [Sections 39](encryption.md#39-at-rest-encryption-via-spool-aes-256-gcm)–[43](encryption.md#43-application-key-rotation-and-compaction-rewriting). `RotateStorageKey` rewraps the keyring; data key rotation and compaction rewriting are separate operations.

### 5.1 Do not expose unrestricted raw write access

Do not expose a raw `*sql.DB` that can perform writes behind the package.

That would allow:

```go
raw.Exec("UPDATE ...")
```

without the package controlling durability and replication acknowledgement.

If a raw SQL handle is exposed, make it read-only or clearly unsafe/debug-only.

---

## 66. Package Configuration Example

Target developer experience:

`Schema`, `Spool`, and `Encryption` are mandatory. `Open` validates them before materialization or replication starts. `storageKey` below is an application-supplied 32-byte secret.

```go
db, err := murmur.Open(ctx, murmur.Config{
    Path:   "./node-data",
    NodeID: murmur.MustNodeID("..."),
    DBID:   clusterDBID, // shared configured/persisted ID for joining this cluster

    Spool: murmur.DefaultSpoolConfig(),

    Schema: murmur.SchemaConfig{
        Version: 7,
        Tables:  schema,
        // AcceptRemoteSchema nil (default) auto-adopts higher schema versions
        // from peers; set an explicit false pointer to refuse remote schemas.
    },

    MaxTransactionBytes: 64 << 20, // default pre-commit cap (top-level Config field)

    Encryption: murmur.EncryptionConfig{
        Algorithm:       murmur.AES256GCM,
        Key:             storageKey,
        KeyID:           "storage-key-v1",
        DataKeyRotation: 24 * time.Hour,
    },

    Replication: murmur.ReplicationConfig{
        ListenAddr: ":7443",
        TLS: tlsConfig,
        Membership: murmur.MembershipConfig{
            AdvertiseAddr: "192.0.2.10:7443", // this node's reachable endpoint
            Bootstrap: []string{"seed-a.example:7443", "seed-b.example:7443"},
        },
        Fanout:                 3,
        PeerRotationInterval:   30 * time.Second,
        AntiEntropyInterval:    10 * time.Second,
        MaxReplicationSessions: 8,
        MaxQUICConnections:     32,
        Dissemination:          murmur.DisseminationGossip, // optional: DisseminationPlumtree
        MaxSnapshotBytes:       512 << 20, // default staging/publication bound
        SnapshotTransferTimeout: 10 * time.Minute, // source read-cut lease
        // Overload's zero fields select the bounded defaults in Section 32.
    },
})
if err != nil {
    return err
}
defer db.Close()
```

Usage:

```go
_, err = db.ExecContext(
    ctx,
    `UPDATE contacts SET phone=? WHERE id=?`,
    "613-555-0100",
    id,
)
```

No external database service is required.

---

## 80. Suggested Internal Interfaces

### Query engine

```go
type QueryEngine interface {
    Begin(ctx context.Context) (QueryTx, error)
    Query(ctx context.Context, q string, args ...any) (Rows, error)
    Rebuild(ctx context.Context, src state.Reader) error
    Apply(ctx context.Context, changes []WinningChange) error
    Generation() uint64
    Close() error
}
```

### Durable state

```go
type StateStore interface {
    CommitLocal(ctx context.Context, batch MutationBatch) (MergeResult, error)
    CommitRemote(ctx context.Context, batch MutationBatch) (MergeResult, error)
    CommitRemoteGroup(ctx context.Context, batches []*MutationBatch) (MergeResult, error)

    GetCell(...)
    IterateTable(...)
    GetWatermarks(...)
    SetPeerAck(...)

    Snapshot(...)
    Close() error
}
```

`DB.ApplyRemoteGroup` is the replication manager's optional grouped-applier
entry point. It takes ordered, contiguous transactions and commits their
independent receipts and sequence positions atomically. It publishes winning
records through RIME before acknowledging the apply.

The query view is RIME. SQL-only configurations and statements are not part of
the public API.

### Transport

```go
type Transport interface {
    Listen(ctx context.Context) error
    Dial(ctx context.Context, peer Peer) (Session, error)
    Close() error
}
```

### Key provider

```go
type KeyProvider interface {
    Current(ctx context.Context) (KeyMaterial, error)
    Lookup(ctx context.Context, id string) (KeyMaterial, error)
}
```

Keep storage and transport interfaces internal until real alternate implementations exist. KeyProvider is public because applications implement key sourcing. Do not expose abstraction for abstraction's sake.

The internal membership adapter implements `memberlist.NodeAwareTransport` on the shared QUIC pool and publishes a membership view/events to the scheduler. The scheduler owns target selection and work admission; the replication manager retains durable batch/acknowledgement processing. Keep memberlist and QUIC implementation types internal rather than exposing them through configuration or status.

---

Current schema-level counter, set and extrema behavior, causal storage, signed wire formats, bridge ownership and upgrade requirements are specified in [merge policies](merge-policies.md). LWW remains the default.

### Item API conveniences

`DB.Find(ctx, dest, filters...)` and `FindOne` infer the registered collection
from slice and struct pointers. `Count` and `Exists` take a model exemplar.
`Tx` and `RecordReadTx` expose the same reads without context arguments;
pinned reads retain their model bindings across migration. `DB.Update(ctx,
item, Set(...))` and `Tx.Update` are explicit partial assignments that share
`UpdateFields` validation and atomicity. See [model API](model-api.md) and the
[application guide](../USAGE.md) for matcher and timestamp semantics.
