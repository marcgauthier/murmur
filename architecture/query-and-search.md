# Query and search

Indexes, FTS, derived search objects, prepared statements, and FTS maintenance.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [23. Search Architecture](#23-search-architecture)
- [24. SQL Prepared-Statement Cache](#24-sql-prepared-statement-cache)
- [65. FTS Maintenance](#65-fts-maintenance)
- [Reactive query subscriptions](#reactive-query-subscriptions)

---

## 23. Search Architecture

Keeping tables in RAM improves latency, but indexing determines search complexity.

Support three classes of local index:

### Standard SQL indexes

```sql
CREATE INDEX ...
```

For equality, range, sort, and join workloads.

### FTS

Enable FTS5 when available.

FTS structures are local derived data and are never replicated directly.

Replicated base-table changes update FTS locally.

### Application-specific derived indexes

Allow application migrations to create local-only indexes/views.

Never put local index definitions into mutation data.

The durable replicated state should remain query-engine agnostic.

---

## 24. SQL Prepared-Statement Cache

Add an LRU prepared-statement cache.

Key:

```text
SQL string + connection identity/schema epoch
```

Config:

```go
type StatementCacheConfig struct {
    MaxStatements int
}
```

Default candidate:

```text
128-512 prepared statements per active connection
```

Invalidate cache on schema migration.

Do not cache statements across incompatible schema epochs.

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

SPeD-SQL provides an embedded reactive query subscription interface (`DB.Subscribe` and `DB.SubscribeWithOptions`) returning an initial query result and subsequent notifications whenever committed, SQL-visible state changes. Local writes, remote apply, accepted High/Low imports, and snapshot publication notify through the materialization boundary. Uncommitted, staged, or partially materialized states never emit query updates.

### Cursor and Resumption Model

Subscription events carry a monotonic local cursor sequence (`Cursor uint64`), distinct from mesh origin sequences. The database retains a configurable history buffer (`MaxRetainedEvents`) allowing reconnecting subscribers to resume from their last seen cursor via `SubscriptionOptions{ResumeFromCursor: cursor}` without replaying from scratch. If a requested resume cursor has expired from the history buffer or is invalid, `SubscribeWithOptions` returns `ErrSubscriptionExpired`.

### Slow Consumers and Resets

Each subscription has a bounded event channel (`EventBufferSize`). When a slow consumer fills its buffer, the subscription is not silently truncated: it receives an explicit `EventReset` event containing `ErrSubscriptionReset`, and is unregistered. Schema migrations (`DB.Migrate`) and materializer rebuilds similarly deliver `EventReset` to all active subscriptions, signaling that subscribers must resnapshot.

### Resource Bounds and Concurrency

- `MaxSubscribers`: Limits concurrent active subscriptions (default 1024; excess requests return `ErrMaxSubscribersReached`).
- `EventBufferSize`: Per-subscriber event channel buffer (default 64).
- `MaxRetainedEvents`: Size of the retained history ring buffer for cursor resumption (default 256).
- **Non-blocking Write Isolation**: Subscription query re-evaluation and event dispatch execute asynchronously in a background worker, ensuring subscriber processing never blocks durable transactions or replication apply loops.

