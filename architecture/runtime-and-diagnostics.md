# Runtime and diagnostics

Lifecycle, workers, metrics, logging/errors, resource budgeting, and concurrency.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [45. Node Lifecycle](#45-node-lifecycle)
- [46. Background Workers](#46-background-workers)
- [52. Metrics and Diagnostics](#52-metrics-and-diagnostics)
- [53. Logging](#53-logging)
- [54. Error Model](#54-error-model)
- [62. Memory Budgeting](#62-memory-budgeting)
- [63. Concurrency Model](#63-concurrency-model)
- [64. LumoSQL Connection & MVCC Concurrency Model](#64-lumosql-connection--mvcc-concurrency-model)
- [Optional service and administration adapters](#optional-service-and-administration-adapters)

---

## 45. Node Lifecycle

Package states:

```go
type DBState uint8

const (
    StateOpening DBState = iota
    StateRebuilding
    StateReady
    StateMaterializerDirty
    StateRotatingKey
    StateClosing
    StateClosed
    StateFailed
)
```

Expose state through:

```go
db.Status()
```

Writes should be rejected in:

```text
Rebuilding
MaterializerDirty
RotatingKey
Closing
Closed
Failed
```

unless a specific state safely permits them.

Reads may be allowed during key rotation because LumoSQL is in memory, but this should be explicit.

Storage failure (disk full, unrecoverable I/O error, MANIFEST or background
flush/compaction failure) fails the node closed: the store records the first
Pebble fatal signal and every subsequent store operation returns
`state.ErrStorageFailed` (wrapping the underlying cause, so
`errors.Is(err, syscall.ENOSPC)` identifies a full disk). The DB layer
rejects reads and writes with that error and transitions to `StateFailed`;
only a process restart — after the operator resolves the underlying problem —
recovers, replaying the last durable WAL prefix with failed writes absent.

---

## 46. Background Workers

When `Open` succeeds, package owns internal goroutines:

```text
QUIC listener
SWIM membership probes/gossip/push-pull
bootstrap resolver and retry
bounded connection pool and dial workers
peer selection/rotation and anti-entropy scheduler
replication sender(s)
replication receiver/apply queue
ack manager
replication log GC
Pebble compaction/key-reference reconciliation
snapshot coordinator
status/metrics collector
data-key expiry/registry maintenance
```

Every goroutine must be bound to the DB context.

`Close()` must:

```text
stop accepting new writes
stop scheduling new replication work
attempt memberlist Leave with a five-second deadline while QUIC remains available
stop memberlist and membership delivery
cancel peer workers
finish/abort active transactions
flush required state
close QUIC
close LumoSQL
close Pebble
zero cached key material where practical
return
```

No leaked goroutines.

Run `go test -race` continuously.

---

## 52. Metrics and Diagnostics

No standalone HTTP server is required.

Expose a Go metrics/status API.

Example:

```go
type Status struct {
    State DBState

    NodeID NodeID

    StateGeneration       uint64
    MaterializedGeneration uint64

    PebbleSizeBytes uint64
    LogBytes        uint64

    PeerCount       int
    ConnectedPeers  int
    MembershipCount int
    SelectedPeers   int
    QUICConnections int
    PendingDials    int

    PendingApply int
    PendingSend  int

    HLC uint64
}
```

Per peer:

```text
connected
last handshake
RTT
last receive time
last send time
origin watermarks
lag by origin
snapshot required
protocol/schema compatibility
membership state (alive/suspect/dead/left)
selected replication target
durable acknowledgement retention deadline
last successful anti-entropy sync
local retirement/exclusion
```

Add counters/gauges for probe failures and refutations, bootstrap retries, membership queue drops, datagram-size errors, connection/session budget usage and deferrals, pool evictions, and anti-entropy attempts/failures/completions. Keep membership reachability separate from replication progress. Metrics must make a growing membership view with a bounded connection pool observable without exposing secrets.

Expose queued bytes/entries/oldest age, bandwidth-token wait time, pending gap ranges and staged transaction bytes, staging evictions, Plumtree eager/lazy counts and payload duplicates/cache hits/misses, schema conflicts/merge reuse, snapshot publication state, and restore identity mode. Report applied watermarks separately from observed heads/chunk progress. Adaptive apply metrics include group transactions/bytes, synchronized-commit latency, and acknowledgement delay.

Optionally provide Prometheus collectors as a separate subpackage instead of forcing an HTTP endpoint into the core.

Implementation status: `DB.Status` carries the node snapshot above plus
per-peer records (`PeerDiagnostics`: session state, schema agreement,
watermarks, per-origin lag, traffic totals, queue depths, SWIM membership state,
selection state, and retention deadlines), `DB.Metrics` carries node-local writer/apply/GC/schema
counters plus scheduled durability sync successes and failures, `Status.Pool` and `Status.Membership` expose shared connection pool and SWIM
diagnostics, and the `metrics` subpackage provides Prometheus collectors over a caller-owned
registry with no HTTP listener. Counters cover implemented subsystems (SWIM membership and datagram transport,
shared connection pool and admission/eviction, QUIC sessions, origin-log repair, snapshots, schema sync).
Metrics for Plumtree and overload integration land with those subsystems; remaining observability work is tracked
in [TASKS_PENDING.md](../TASKS_PENDING.md#pending-tasks).

---

## 53. Logging

Accept a logging interface rather than selecting a global logger.

Example:

```go
type Logger interface {
    Debug(msg string, attrs ...Attr)
    Info(msg string, attrs ...Attr)
    Warn(msg string, attrs ...Attr)
    Error(msg string, attrs ...Attr)
}
```

Never log:

```text
encryption keys
TLS private keys
unredacted sensitive row values
complete replication payloads by default
```

Debug payload logging must be explicit and clearly unsafe.

---

## 54. Error Model

Export typed errors.

Examples:

```go
ErrClosed
ErrNotReady
ErrMaterializerDirty
ErrSchemaMismatch
ErrProtocolMismatch
ErrPeerNotAllowed
ErrReplicationGap
ErrSnapshotRequired
ErrEncryptionKey
ErrEncryptionCorrupt
ErrUnsupportedStorageFormat
ErrValueTooLarge
ErrUnsupportedSchema
ErrAmbiguousCommit
```

Storage failures use `state.ErrStorageFailed` (with `state.IsStorageFailure`
for detection). The first data-path I/O error is preserved as the wrapped
cause, so operators can distinguish a full disk via
`errors.Is(err, syscall.ENOSPC)`.

Wrap underlying errors with context while preserving `errors.Is` / `errors.As`.

---

## 62. Memory Budgeting

Total memory approximately includes:

```text
LumoSQL table data
LumoSQL indexes
FTS indexes
Pebble block cache
encrypted-VFS index and buffer memory
Pebble memtables
replication queues
SWIM membership metadata (O(N))
bounded QUIC connection/handshake and membership queues
bounded Plumtree seen/payload caches
one bounded transaction reassembly/apply group (separate from queue byte budgets)
snapshot buffers
prepared statements
Go heap
CGO/LumoSQL allocations
```

Add config-level memory controls.

Account for encrypted-VFS index checkpoints, plaintext chunk buffers, and retained key material without loading every file into RAM.

Partial transaction staging and snapshot candidate generations consume bounded encrypted disk capacity, not an unbounded RAM queue. Include the active plus staged/previous generations in disk budgeting; reject/defer transfers before exceeding receiver capacity. Network queues, transient caches, and reassembly each have distinct accounting; configuring one does not silently expand the others.

Do not set a 1 GiB Pebble cache by default when the same process already holds the complete dataset in LumoSQL memory.

Expose memory-related metrics wherever practical.

---

## 63. Concurrency Model

Start conservative.

Recommended v1:

```text
one serialized local write coordinator
one remote apply coordinator into LumoSQL
one state-store writer coordinator shared by all local/remote/GC/metadata merges
concurrent Pebble snapshot readers
concurrent SQL readers where validated
parallel network senders
```

SQLite-family engines fundamentally serialize writes, so forcing many simultaneous SQL writers is unlikely to help.

The write coordinator also simplifies:

```text
HLC allocation
local sequence allocation
capture state
materialization generation
statement connection ownership
```

Increase concurrency only after profiling.

---

## 64. LumoSQL Connection & MVCC Concurrency Model

Regular SQLite in `:memory:` mode is connection-scoped and uses a rollback journal (`PRAGMA journal_mode = MEMORY`). Under shared-cache mode (`cache=shared`), regular SQLite introduces coarse table-level locks: active readers block writes (`SQLITE_LOCKED`), and active writes freeze all readers. Furthermore, regular SQLite lacks MVCC, and SQLite WAL mode is unsupported in pure `:memory:` and prone to checkpoint starvation when readers are active.

LumoSQL with its **LMDB backend** provides the planned MVCC behavior. The default `QueryStoreMMap` uses a disposable file-backed `modernc.org/sqlite` database with SQLite mmap enabled; readers and writers still follow SQLite locking. A tagged LumoSQL implementation now opens pooled read connections so readers retain LMDB snapshots while writes proceed. Actual-LMDB builds and acceptance runs remain pending.

Remote Pebble commits can temporarily lead SQLite materialization. Compare
`StateGeneration` with `MaterializedGeneration` to see this lag; the one-second
or 1,000-transaction bulk flush closes it. A receive watermark certifies durable
Pebble state and is not a query-visibility marker.

The LMDB target is expected to provide:
1. **Copy-on-Write MVCC:** Every write transaction creates a new root in an immutable B+ tree. Readers hold an immutable root pointer and read virtual memory pages directly with **zero locks** on database tables or pages.
2. **Readers Never Block Writers:** Long-running read queries do not block write transactions.
3. **Writers Never Block Readers:** Writing new transactions does not block existing or new read queries.
4. **No Checkpointing Contention:** Unlike SQLite WAL, LMDB has no separate checkpointing step that can stall or lock. Old pages are tracked in an internal free-list and safely reused only after old reader transactions finish.
5. **No RAM Disk Required on Windows or Linux:** LMDB operates as a memory-mapped file (`mmap`) on normal disk storage (e.g. `%LOCALAPPDATA%\NOMADSQL\query_lmdb\` or `./node-data/query_lmdb/`).
6. **RAM-Speed Commits:** Configured with `PRAGMA synchronous = OFF;` (`MDB_NOSYNC | MDB_NOMETASYNC`), write commits return in microseconds at memory speed. Physical disk durability is handled exclusively by Pebble via `batch.Commit(pebble.Sync)`.

For lightweight environments without a C compiler, `QueryStoreMemory` provides a pure-Go fallback using `modernc.org/sqlite`, with writes serialized via the write coordinator.

---

---

## Optional service and administration adapters

GALVANIZE includes HTTP APIs, a remote client SDK, administration/unlock endpoints,
and Prometheus instrumentation. NOMADSQL remains an embedded library; these are
optional deployment extensions rather than a mandatory daemon or sidecar.

Expose new High/Low status, provenance/replay, file, subscription, and writer
scheduling diagnostics through package-owned APIs first. Extend planned metrics
with backlog age/bytes, held imports, stream gaps, file availability/fetch failures,
and per-class writer service time/wait. Optional Prometheus collectors are
provided without requiring a globally registered exporter or HTTP listener
(implemented: `metrics` package over `DB.Status`).

An optional service adapter may expose authenticated SQL, streaming subscriptions,
file operations, and narrowly authorized administrative controls. A corresponding
SDK consumes those interfaces; it does not replace the embedded Go API. Version
and authenticate service interfaces separately from the inter-node QUIC protocol.

Implemented: the `service` package serves `GET /v1/status` (redacted snapshot),
`POST /v1/query` (read-only statements only; writes are rejected so change
capture cannot be bypassed), `POST /v1/exec` (implicit-transaction path), and
`GET /v1/subscribe` (SSE stream of initial/update/reset subscription events
with resume cursors). Every endpoint requires TLS plus a 32-byte-or-longer
Bearer [REDACTED] compared in constant time, mirroring the `admin` package; the
core library starts no listener and the handler never closes the caller's DB.
Results are bounded (10,000 rows and 1 MiB request bodies by default) with
explicit too-large failures, and values use a typed JSON codec that preserves
int64 precision and base64 blobs. `service.Client` is the remote SDK (status,
query, exec, streaming subscribe over TLS-only https URLs). File operations
are absent until encrypted file replication exists; unlock stays in `admin`.

Remote unlock is a separate lifecycle extension. A locked service may expose only
its authenticated control surface; it must not initialize/open encrypted state or
start replication/bridge workers before key verification. Do not reuse mesh access
as administrative permission or expose keys in status/logging. Existing direct or
provider-based key loading at Open remains the default.

The optional `admin` package provides a TLS-only handler with a separate
32-byte-or-longer bearer token. Its `POST /v1/unlock` accepts a key ID, cipher
name, and 32-byte key, then invokes the application's open callback; failures
return a generic response and do not publish an unlocked runtime. `GET
/v1/status` reports only locked/unlocked state, and `POST /v1/lock` closes the
active DB. Applications mount the handler on their own TLS listener; the core
library starts no HTTP listener and existing `db.Open` behavior is unchanged.

Acceptance for an adapter includes authorization separation, failed unlock,
worker startup only after successful unlock, streaming cancellation, shutdown,
redacted diagnostics, and an embedded application running with no service listener.
