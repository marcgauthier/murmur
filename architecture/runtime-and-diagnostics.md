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
- [64. SQLite Connection and Locking Model](#64-sqlite-connection-and-locking-model)
- [Optional service adapter](#optional-service-adapter)

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

Reads use the in-memory SQLite materialization during key rotation; durable
writes and remote apply remain gated by maintenance.

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
close the SQLite materialization
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
in [TODO.md](../TODO.md).

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
SQLite table data
SQLite indexes
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
CGO allocations (default mattn build)
```

Add config-level memory controls.

Account for encrypted-VFS index checkpoints, plaintext chunk buffers, and retained key material without loading every file into RAM.

Partial transaction staging and snapshot candidate generations consume bounded encrypted disk capacity, not an unbounded RAM queue. Include the active plus staged/previous generations in disk budgeting; reject/defer transfers before exceeding receiver capacity. Network queues, transient caches, and reassembly each have distinct accounting; configuring one does not silently expand the others.

Do not set a 1 GiB Pebble cache by default when the same process already holds the complete dataset in SQLite memory.

Expose memory-related metrics wherever practical.

---

## 63. Concurrency Model

Start conservative.

Recommended v1:

```text
one serialized local write coordinator
one remote apply coordinator into SQLite
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

## 64. SQLite Connection and Locking Model

Both supported drivers use one named shared-cache in-memory SQLite
database with a reserved write connection and a pool of read connections. The engine
read/write lock permits overlapping queries while a materialization is stable.
A write takes the exclusive lock, waits for active result sets to close or
exhaust, and prevents new reads until commit or rollback. Callers should close
rows promptly.

Remote Pebble commits can temporarily lead SQLite materialization. Compare
`StateGeneration` with `MaterializedGeneration` to see this lag; the one-second
or 1,000-transaction bulk flush closes it. The materialized generation is held
in memory and initialized after startup rebuild; reporting it does not write
to Pebble. A receive watermark certifies durable Pebble state, not query
visibility.

The default mattn build enables the SQLite pre-update hook and FTS5 with
`sqlite_preupdate_hook sqlite_fts5`. The optional `modernc` build uses the
pure-Go driver and `CGO_ENABLED=0`. SQLite durability pragmas apply only to the
rebuildable query view; Pebble controls the durable acknowledgement contract.

---

---

## Optional service adapter

No HTTP service adapter ships with the library. The embedded Go API is the
interface; a hosting process that needs HTTP defines, authenticates, and
serves its own endpoints, separate from the inter-node QUIC protocol.

Expose new High/Low status, provenance/replay, file, subscription, and writer
scheduling diagnostics through package-owned APIs first. Extend planned metrics
with backlog age/bytes, held imports, stream gaps, file availability/fetch failures,
and per-class writer service time/wait. Optional Prometheus collectors are
provided without requiring a globally registered exporter or HTTP listener
(implemented: `metrics` package over `DB.Status`).

The internal `tests-live` harness serves test-only HTTPS routes for live
scenarios. Those routes are test tooling, not a product API, and no client
SDK is provided for them.

Key delivery is the hosting process's responsibility. Murmur provides no HTTP
unlock endpoint: the process obtains the encryption key by whatever means it
chooses (local config, KMS, or its own HTTP listener) and passes key material
to `Open` directly or through a `KeyProvider`. The database opens and its
workers start only after the key is verified; there is no locked-but-listening
library state. Do not reuse mesh access as administrative permission or expose
keys in status/logging.

Acceptance is an embedded application running with no HTTP listener, which
is the default: the core library starts no listener.
