# Query and search

RIME indexes, query plans, derived search objects, and reactive subscriptions.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [23. Search Architecture](#23-search-architecture)
- [24. Retired SQL Prepared-Statement Cache](#24-retired-sql-prepared-statement-cache)
- [65. FTS Maintenance](#65-fts-maintenance)
- [Reactive query subscriptions](#reactive-query-subscriptions)

---

## 23. Search Architecture

Keeping tables in RAM improves latency, but indexing determines search complexity.

Support three classes of local index:

### Managed indexes

Declare indexes with the RIME table schema for equality, range, sort, and join
workloads. RIME compiles those declarations into its local index structures.
The simple item API exposes comparisons, string predicates, inclusive ranges,
and membership while preserving these plans. Named scalars and `time.Time`
queries use checked runtime fields; timestamp index and predicate keys share
UTC, monotonic-free instant semantics. See [model API](model-api.md) for the
adapter behavior and [application usage](../USAGE.md) for syntax.

### FTS

The former SQLite FTS5 integration was removed with the SQL materializer. Native
full-text indexing is not currently part of the managed query API.

FTS structures are local derived data and are never replicated directly.

Replicated base-table changes update FTS locally.

### Application-specific derived indexes

Allow application migrations to create local-only indexes/views.

Never put local index definitions into mutation data.

The durable replicated state should remain query-engine agnostic.

---

## 24. Retired SQL Prepared-Statement Cache

This section describes the removed SQLite implementation and is retained only
as historical context. The production code has no SQL materializer or prepared
statement cache. RIME maintains its own query-plan cache, invalidated when the
managed schema changes.

---

## 65. FTS Maintenance

FTS is derived local state.

Options:

- Application-defined FTS triggers.
- Package-managed updates whenever winning base cells change.
- Full rebuild during startup.

Prefer package-managed FTS maintenance for tables declared in schema metadata.

Do not replicate FTS internal mutations.

---

---

## Reactive query subscriptions

### RIME record subscriptions

In native `Config.Tables` mode, `DB.Subscribe` evaluates queries on
pinned RIME read snapshots. Snapshot acquisition and observer-cursor capture
share the local apply lock, but query evaluation releases that lock so writers
can continue. Each update carries the full detached `Rows` snapshot plus a
deterministically ordered primary-key diff in `Changes`: additions and updates
carry detached rows, while removals carry only the key. Equality uses each
field's canonical built-in semantics or its registered custom codec equality
hook. Keep `ItemSubscription.ResumeCursor()` and pass it back as
`ItemSubscriptionOptions.ResumeFrom` to reconnect within the same database
open epoch. Tokens bind database identity, open epoch, and materializer
generation. Resume validates the retained sequence and sends the latest
snapshot as an update; stale, future, cross-database, or expired tokens return
`ErrSubscriptionExpired`. Closing and reopening the database invalidates every
token. The cursor is an in-memory observer position, not a durable replication
or application checkpoint; configuring synchronous durability does not change
that contract. Slow consumers receive a bounded-buffer `EventReset`; rebuilds
invalidate tokens and reset active subscriptions.

The legacy SQL query subscription API has been removed. Applications use
`DB.Subscribe` for RIME-backed snapshots and row diffs; it is available
for databases with `Config.Tables` and does not parse SQL.

### Cursor and Resumption Model

Subscription events carry a monotonic local cursor sequence (`Cursor
uint64`), distinct from mesh origin sequences. The database retains a
configurable history buffer (`MaxRetainedEvents`) allowing reconnecting
subscribers to resume from `ItemSubscriptionCursor` via
`ItemSubscriptionOptions.ResumeFrom`. If a requested resume token has expired
from the history buffer or is invalid, `DB.Subscribe` returns
`ErrSubscriptionExpired`.

### Slow Consumers and Resets

Each typed subscription has a bounded event channel (`EventBufferSize`). When a
slow consumer fills its buffer, the stream receives an explicit `EventReset`
event containing `ErrSubscriptionReset` and closes. Materializer generation
changes similarly reset active subscriptions, signaling that clients must
resnapshot.

### Resource Bounds and Concurrency

- `MaxSubscribers`: Limits concurrent typed subscriptions and their workers (default 1024; excess requests return `ErrMaxSubscribersReached`). Each worker serializes its own query evaluations and event delivery; a slow worker does not hold up other subscribers.
- `EventBufferSize`: Per-subscriber event channel buffer (default 64).
- `MaxRetainedEvents`: Size of the retained history ring buffer for cursor resumption (default 256).
- **Worker lifecycle**: A subscription-manager context is linked to the database context. Initial typed snapshot evaluation and worker updates honor the caller's context; manager shutdown cancels both phases. Subscription close/reset cancels its worker, and database shutdown waits for subscription workers before closing RIME.
