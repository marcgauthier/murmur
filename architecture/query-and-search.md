# Query and search

Indexes, FTS, derived search objects, prepared statements, and FTS maintenance.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [23. Search Architecture](#23-search-architecture)
- [24. SQL Prepared-Statement Cache](#24-sql-prepared-statement-cache)
- [65. FTS Maintenance](#65-fts-maintenance)

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

