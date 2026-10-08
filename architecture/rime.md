# RIME — Rapid In-Memory Engine

RIME is Murmur's separate native-Go in-memory relational engine. It exposes
relational operations through typed Go functions and executes no SQL text.
The core uses only the standard library and has no SQLite dependency. Murmur's
typed-record integration projects authoritative Spool state into RIME through
the storage-owned `state.Reader` contract. RIME is the only managed query
materializer in production.

This page is the repository overview. The package's
[USAGE reference](../rime/USAGE.md) documents public syntax; its
[README](../rime/README.md) is a short introduction and
[ARCHITECTURE](../rime/ARCHITECTURE.md) is the detailed implementation reference.

## 1. Registering tables

Records are Go structs with one primary key and optional tags for indexes,
constraints, defaults and foreign-key references. Registration builds metadata
and caches field accessors; exact-type field reads compile to allocation-free
unsafe offset loads, with reflection kept only for converted types. Table options
configure names, compound indexes, shards and cloning.

See [schema and configuration](../rime/USAGE.md#schema-and-registration) for
supported tags/types, and
[registration internals](../rime/ARCHITECTURE.md#3-schema-registration-and-field-access)
for accessors and generated IDs.

## 2. Reads and writes

Reads return immutable record pointers. Writers operate on private copies and
commit atomically; records with nested mutable data require a deep cloner.
Inside a write transaction, both point reads and queries see staged writes
(read-your-writes, still mutable until commit); other transactions keep their
pinned snapshot. Historical snapshots must remain registered to retain history.

See [CRUD usage](../rime/USAGE.md#table-handles-transactions-and-contexts),
and the architecture's
[read path](../rime/ARCHITECTURE.md#5-point-reads-and-snapshot-visibility),
[commit path](../rime/ARCHITECTURE.md#6-write-staging-and-commit-publication) and
[reclamation/lifecycle](../rime/ARCHITECTURE.md#13-history-reclamation-and-lifecycle).

## 3. Queries

Typed field handles build predicates, ordering, pagination and compiled queries.
Latest-state indexes accelerate fresh snapshots; older snapshots can require
historical scans. `Explain` reports the selected strategy.

See [query usage](../rime/USAGE.md#queries-and-fields),
[compiled queries](../rime/USAGE.md#compiled-queries) and
[planning/execution](../rime/ARCHITECTURE.md#9-query-planning-and-execution).

Maintained query views cache typed Go results and publish updates atomically
with source commits. Unordered, unpaginated filters update changed membership;
complex queries reevaluate once per relevant commit. Refresh failures preserve
source writes and make the affected view unavailable until recovery. Definitions
and results are in-memory and recreated by the application at startup.

See [view syntax and lifecycle](../rime/USAGE.md#maintained-query-views) and
[maintenance internals](../rime/ARCHITECTURE.md#17-maintained-query-views).

## 4. Relational features

RIME implements aggregation/grouping, inner/left/self joins, projection, bulk
mutations, record constraints, hooks, bounded async events, contexts, limits
and statistics. Foreign keys currently validate committed target existence on
new/updated records; they do not implement full SQL referential actions.

See [relational usage](../rime/USAGE.md#aggregates-groups-joins-and-projection),
[constraints and hooks](../rime/USAGE.md#constraints-hooks-events-and-lifecycle) and
[relational internals](../rime/ARCHITECTURE.md#10-aggregates-joins-and-projection).

## 5. Concurrency model

Multiple goroutines may read and prepare writes concurrently using separate
transactions. Published records and version chains are immutable, and storage
is sharded. All write commits still serialize database-wide; shard count does
not enable parallel commit publication. GC also coordinates through global
commit/publication locks. Isolation is snapshot isolation with optimistic
write checks, not serializable predicate validation.

The detailed [lock model](../rime/ARCHITECTURE.md#7-locks-and-concurrency-limits)
and [index design](../rime/ARCHITECTURE.md#8-index-structures-and-maintenance)
explain concurrency, freshness and current costs. The
[performance plan](rime-performance-plan.md) separates implemented allocation
optimizations from remaining history and concurrent-publication work.

## 6. Status and verification

Use the [qualification runbook](rime-testing.md) for correctness, race/model
checks, retention tests and live reader/writer workloads. The
[benchmark document](rime-benchmarks.md) records measured results, frozen
acceptance baselines and profiling methods. Consult the architecture's
[source map](../rime/ARCHITECTURE.md#16-source-map-and-verification) to locate code.

RIME has no persistence, WAL, crash recovery, encryption, replication, server or
SQL parser. RIGHT/FULL joins, parallel commits and persistent chunked version
history are not implemented. Stats report counts rather than memory bytes.
