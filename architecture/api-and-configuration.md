# API and configuration

Public Go API, configuration example, and internal interfaces.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [5. Public Package API](#5-public-package-api)
- [66. Package Configuration Example](#66-package-configuration-example)
- [80. Suggested Internal Interfaces](#80-suggested-internal-interfaces)

---

## 5. Public Package API

Keep the public API small.

Initial concept:

```go
type Config struct {
    Path        string
    NodeID      NodeID
    QueryStore  QueryStoreConfig
    Encryption EncryptionConfig
    Replication ReplicationConfig
    Cache       CacheConfig
    Schema      SchemaConfig
    Pebble      PebbleConfig
    Durability  DurabilityConfig
    Scheduling  WriterSchedulingConfig
}

func Open(ctx context.Context, cfg Config) (*DB, error)

type DB struct {
    // unexported internals
}

func (db *DB) ExecContext(
    ctx context.Context,
    query string,
    args ...any,
) (Result, error)

func (db *DB) QueryContext(
    ctx context.Context,
    query string,
    args ...any,
) (*Rows, error)

func (db *DB) QueryRowContext(
    ctx context.Context,
    query string,
    args ...any,
) *Row

func (db *DB) BeginTx(
    ctx context.Context,
    opts *TxOptions,
) (*Tx, error)

func (db *DB) PrepareContext(
    ctx context.Context,
    query string,
) (*Stmt, error)

func (db *DB) Subscribe(ctx context.Context, query string, args ...any) (*Subscription, error)
func (db *DB) SubscribeWithOptions(ctx context.Context, query string, opts SubscriptionOptions, args ...any) (*Subscription, error)

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
func (db *DB) Close() error
```

`Peers` and `AddPeer` supply bootstrap candidates, not permanent replication links. Discovery and the bounded scheduler select actual connections. `RemovePeer` explicitly retires and persistently excludes a NodeID locally, including from subsequent discovery; `AddPeer` clears that exclusion. This is local administrative policy, not a cluster-wide revocation. `ForceSync` schedules immediate synchronization subject to the same session and connection limits as background work. See [Sections 26](membership-and-transport.md#26-replication-transport)–[28](replication-and-dissemination.md#28-quic-connection-model) for membership configuration and connection budgets.

A `database/sql/driver.Driver` is implemented (`driver.go`, registered as
`replicateddb`) so applications can do:

```go
replicateddb.RegisterDriverDB("primary", db)
sqldb, err := sql.Open("replicateddb", "primary")
// or: sqldb := sql.OpenDB(replicateddb.NewConnector(db))
```

Exec runs through implicit transactions, Query through reads, and Begin maps
to `DB.BeginTx`; write statements issued as Query outside an explicit
transaction are rejected so change capture cannot be bypassed. Closing sql
handles never closes the underlying `*DB`. The explicit `DB`/`Tx` API remains
the primary interface: transaction interception and connection-specific hooks
are easier to prove there first.

`Open` requires both `cfg.Schema` and `cfg.Pebble`. The schema must include its version and complete table, column, constraint, and index declarations; do not infer it from existing data. Validate it against [Section 6](schema.md#6-schema-rules-for-version-1) and persisted schema metadata before creating tables, rebuilding data, or starting replication. Errors must identify the offending table, column, or index.

`PebbleConfig` is required; use `DefaultPebbleConfig()` for standard settings. `Config.Path` is the database directory. Expose cache size, memtable size/count, maximum open files, compaction concurrency, and compression; keep storage caches out of `CacheConfig`. Validate settings before opening Pebble. Encryption keys come from `Encryption.Key` or `Encryption.Provider`; never weaken the replication acknowledgement or durability contract through storage options.

`DurabilityConfig{Mode: DurabilityAsync, SyncInterval: time.Second}` enables
opt-in scheduled Pebble syncs for applications accepting approximately one
second of unsynced acknowledged writes. `SyncInterval` must be non-negative
and is valid only with `DurabilityAsync`; zero keeps manual `DB.Sync` behavior.
`Metrics().PeriodicSyncs` and `PeriodicSyncFailures` report scheduled sync
results. See [Section 17](transactions.md#17-alternative-write-optimization)
for the durability contract.

```go
type PebbleConfig struct {
    CacheBytes              int64
    MemTableBytes           uint64
    MemTableCount           int
    MaxOpenFiles            int
    MaxConcurrentCompactions int
    Compression             CompressionConfig
}

type CompressionConfig struct {
    Mode      CompressionMode // default, none, snappy, or zstd
    ZstdLevel int             // default 3; v1 supports only level 3
}

type CompressionMode string

const (
    CompressionDefault CompressionMode = ""
    CompressionNone    CompressionMode = "none"
    CompressionSnappy  CompressionMode = "snappy"
    CompressionZstd    CompressionMode = "zstd"
)
```

The default constructor sets a 16 MiB block cache, 4 MiB memtable, two memtables, 1,000 open files, one concurrent compaction, and `CompressionZstd` at level 3. Explicit `CompressionNone` disables compression; zero compression configuration resolves to enabled Zstd level 3. A missing/invalid storage budget is an error rather than silently allocating an unbounded cache.

Target `github.com/cockroachdb/pebble/v2` v2.1.6 and its versioned APIs. This is a new-database architecture change: detect and reject existing Badger directories with `ErrUnsupportedStorageFormat` before creating or modifying storage files. No importer, automatic conversion, or Badger runtime fallback is planned. The current Go implementation uses Pebble; the roadmap describes target capabilities and is not a record of completed phases. See the [implementation inventory](capability-gaps.md).

`Open` also requires `cfg.Encryption`: a write algorithm (default AES-256-GCM), application wrapping key (directly or via a provider), and positive internal data-key rotation duration. Validate wrapping-material key length against its own algorithm and validate the selected write algorithm before opening Pebble; see [Sections 39](encryption.md#39-encrypted-pebble-vfs)–[43](encryption.md#43-application-key-rotation-and-file-rewriting) for rotation semantics.

Key-provider interfaces are application-facing because callers implement them; storage and transport interfaces remain internal. Rotation methods operate on the encryption manager described in [Sections 39](encryption.md#39-encrypted-pebble-vfs)–[43](encryption.md#43-application-key-rotation-and-file-rewriting). `RotateStorageKey` rewraps the key registry; cipher changes and existing-file rewrites are separate operations.

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

`Schema`, `Pebble`, and `Encryption` are mandatory. `Open` validates them before materialization or replication starts. `storageKey` below is an application-supplied 32-byte secret.

```go
db, err := replicateddb.Open(ctx, replicateddb.Config{
    Path:   "./node-data",
    NodeID: replicateddb.MustNodeID("..."),
    DBID:   clusterDBID, // shared configured/persisted ID for joining this cluster

    Pebble: replicateddb.DefaultPebbleConfig(), // includes Zstd level 3

    QueryStore: replicateddb.QueryStoreConfig{
        Mode: replicateddb.QueryStoreMMap, // disposable; LumoSQL LMDB with lumosql build tag
        TempDir: "./tmp",                // optional parent directory
        MMapBytes: 256 << 20,             // optional; default 256 MiB
    },

    Schema: replicateddb.SchemaConfig{
        Version:            7,
        Tables:             schema,
        AcceptRemoteSchema: true, // auto-adopt higher schema versions from peers
    },

    Encryption: replicateddb.EncryptionConfig{
        Algorithm:       replicateddb.AES256GCM,
        Key:             storageKey,
        KeyID:           "storage-key-v1",
        DataKeyRotation: 24 * time.Hour,
    },

    Cache: replicateddb.CacheConfig{
        StatementCacheEntries: 256,
    },

    Replication: replicateddb.ReplicationConfig{
        ListenAddr: ":7443",
        TLS: tlsConfig,
        Membership: replicateddb.MembershipConfig{
            AdvertiseAddr: "192.0.2.10:7443", // this node's reachable endpoint
            Bootstrap: []string{"seed-a.example:7443", "seed-b.example:7443"},
        },
        Fanout:                 3,
        PeerRotationInterval:   30 * time.Second,
        AntiEntropyInterval:    10 * time.Second,
        MaxReplicationSessions: 8,
        MaxQUICConnections:     32,
        Dissemination:          replicateddb.DisseminationGossip, // optional: DisseminationPlumtree
        MaxTransactionBytes:    64 << 20,
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
    Rebuild(ctx context.Context, src StateReader) error
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
independent receipts and sequence positions atomically; acknowledgements may
advance after durable Pebble receipt, before SQLite query visibility. The query
store batches affected rows until `QueryStore.RemoteApplyInterval` (default 1s)
or `QueryStore.RemoteApplyMaxTransactions` (default 1,000) is reached. Zero
selects the default for each; negative values are invalid. A local SQL write
flushes pending remote rows first.

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
