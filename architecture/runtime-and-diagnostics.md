# Runtime and diagnostics

Lifecycle, workers, metrics, logging, resource budgeting, and concurrency.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [45. Node lifecycle](#45-node-lifecycle)
- [46. Background workers](#46-background-workers)
- [52. Metrics and diagnostics](#52-metrics-and-diagnostics)
- [53. Logging](#53-logging)
- [54. Error model](#54-error-model)
- [62. Memory budgeting](#62-memory-budgeting)
- [63. Concurrency model](#63-concurrency-model)
- [Optional service adapter](#optional-service-adapter)

---

## 45. Node lifecycle

`DB.Status()` reports the database state. Writes are rejected while opening,
rebuilding, rotating keys, closing, closed, or failed. A storage failure such as
disk full or unrecoverable I/O error fails the node closed. Spool preserves the
first fatal cause, and the DB returns errors wrapping it so
`errors.Is(err, syscall.ENOSPC)` identifies a full disk. Restart after resolving
the underlying problem replays the last durable state; uncommitted writes are
absent.

---

## 46. Background workers

`Open` owns internal workers for QUIC, SWIM membership, bootstrap resolution,
peer selection and anti-entropy, replication send/apply, acknowledgements,
retention GC, Spool compaction, snapshots, status, key maintenance, and typed
subscriptions. Every worker is bound to the database context.

Typed subscription shutdown and cancellation-before-writer-drain ordering are
described in [query subscriptions](query-and-search.md#resource-bounds-and-concurrency).

`Close` rejects new writes, cancels subscriptions, stops replication and
membership, drains or aborts active transactions, flushes required state, closes
QUIC, RIME and Spool, and clears cached key material where practical. It returns
only after owned workers have stopped.

---

## 52. Metrics and diagnostics

Origin verification counters expose unsigned inputs, unknown origins, invalid
signatures, digest mismatches, and denied snapshot sources without exposing key
material. See [origin signatures](origin-signatures.md).

### Startup progress reporting

Applications may opt into startup reporting with `Config.OnOpenProgress`.
`Open` remains synchronous while a dedicated reporter delivers immutable
snapshots. Routine updates coalesce every 250 ms; phase transitions and one
terminal event are preserved. Callbacks are serialized and run outside store and
engine locks. They must return promptly and must not wait for `Open` to return.

Typed RIME opens report opening, rebuilding, finalizing, and ready. Failures or
cancellation report a terminal failed or cancelled phase. `Elapsed` covers the
whole startup; `PhaseElapsed` covers the current phase. The terminal snapshot is
retained in `DB.Status().OpenProgress` with frozen clocks. Disabled reporting
returns nil.

Progress uses the reconstruction scan without a preliminary scan or durable
counters. `ProcessedItems` counts cells visited in registered tables, including
cells in deleted or incomplete rows, but excluding logs, metadata, separate
tombstone keys, and unregistered tables. RIME reports row-level rebuild counts
which are coalesced into heartbeats. Exact registered-cell totals are not
available: `CountedItems`, `TotalItems`, `PercentComplete`, and
`EstimatedRemaining` remain zero and their known flags remain false. Applications
should display completed work and elapsed time without a percentage estimate.

`DB.Status` and `DB.Metrics` expose Spool, replication, writer scheduling,
materialized generation, and maintenance diagnostics. The optional JSON metrics
handler renders these values; metric names are documented in
[metrics and diagnostics](#52-metrics-and-diagnostics).

---

## 53. Logging

Accept a logging interface rather than selecting a global logger. The embedded
package does not configure application logging. Log state transitions,
replication failures, maintenance outcomes, and storage errors without logging
secrets, encryption keys, or plaintext payloads.

---

## 54. Error model

Use sentinel errors for conditions applications may branch on, including closed
database, unsupported schema, value/transaction limits, optimistic write
conflicts, and uncertain commit outcomes. Preserve wrapped causes with `%w` so
`errors.Is` and `errors.As` continue to work. A durable commit whose publication
outcome is uncertain returns its transaction ID; callers can resolve it using
the durable receipt API after reopen.

Storage failures use `state.ErrStorageFailed` and `state.IsStorageFailure`.
The first data-path I/O error is preserved as the wrapped cause.

---

## 62. Memory budgeting

Total memory includes the authoritative Spool state index and causal history,
RIME records and indexes, Spool write buffers, replication queues, SWIM
membership metadata, bounded QUIC and membership queues, Plumtree caches,
transaction reassembly/apply groups, snapshot buffers, subscriptions, and the Go
heap. RIME and the state index are separate in-memory structures; measure both.

Partial transaction staging and snapshot transfers use bounded encrypted disk
capacity rather than an unbounded RAM queue. Budget active and staged snapshot
generations. Network queues, transient caches, and reassembly have separate
limits; configuring one does not expand another. Use `DB.Status` for Spool
memory and disk metrics where available.

---

## 63. Concurrency model

Typed callbacks stage optimistically and commit through the managed writer
scheduler. Synchronous group commit orders Spool durability and RIME publication;
remote apply and maintenance use the apply coordinator. RIME provides concurrent
snapshot readers and optimistic conflict detection. Replication senders and
peer sessions operate concurrently within configured resource bounds.

Measure before increasing parallelism. Preserve commit order, durable
acknowledgements, subscription visibility, and snapshot pins when tuning
concurrency.

---

## Optional service adapter

No HTTP service adapter ships with the library. The embedded Go API is the
interface; a hosting process that needs HTTP defines, authenticates, and serves
its own endpoints, separate from the inter-node QUIC protocol.
