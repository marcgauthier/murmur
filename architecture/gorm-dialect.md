# GORM dialect

Embedded GORM dialect for Murmur-SQL: schema mapping, migration
contract, and transaction semantics.

[Architecture index](README.md) · [Project README](../README.md) ·
[Dialect README](../gormmurmur/README.md)

## Contract

`gormmurmur` (package `murmur`) is the GORM dialect for the embedded
engine. It generates the engine's SQLite flavor through the
`database/sql` driver, so ordinary GORM code — queries, preloading,
associations, hooks, soft delete, transactions — works unchanged.
Schema management differs from `gorm.io/driver/sqlite` in one load-
bearing way: table structure is published through the engine's
additive `Migrate` API, never through SQL DDL, because DDL through
SQL would land on the ephemeral materialization and never replicate.

## Schema mapping

- One GORM model maps to one replicated table. Column order follows
  model declaration order.
- Go types map to Murmur's four column types: integer kinds and
  `bool` to `integer`, floats to `real`, strings (including
  serializer fields) to `text`, byte slices to `blob`.
- Every table needs exactly one primary key: the `id` blob column.
  Key columns use `murmur.ID` (binds as a blob, renders as UUID
  text in JSON) or `[]byte`; raw `[16]byte` cannot bind through
  `database/sql` and is rejected. Zero `murmur.ID`/`[]byte` keys
  auto-fill from `murmur.NewID()` ahead of create; explicit IDs pass
  through.
- Timestamps use `murmur.Time` (stored as fixed-width UTC datetime
  `TEXT`, preserving chronological string ordering across nodes and
  SQLite backends) and `murmur.DeletedAt` (same
  soft-delete query/update/delete clauses as `gorm.DeletedAt`).
  Existing legacy timestamp rows remain readable but need an
  application rewrite through GORM before relying on text ordering.
  Plain `time.Time`, `sql.NullTime`, and `gorm.DeletedAt` struct
  fields are rejected: the engine returns `TEXT`, which
  `database/sql` cannot scan into them. Integer fields with
  `autoCreateTime`/`autoUpdateTime` work natively.
- `size:` and `comment:` tags are accepted and ignored (SQLite
  ignores lengths too). Explicit `type:` tags must name a
  Murmur-storable type.

## Migration contract

- `AutoMigrate` validates all requested models first, then creates
  missing tables and adds missing columns in one additive engine
  migration. It is idempotent for genesis tables and is rejected inside
  a GORM transaction.
  Applications derive engine genesis from their models with
  `GenesisTables` (the engine requires at least one table at open);
  custom `NamingStrategy` users must use `GenesisTablesWithNamer`
  with the same strategy GORM parses with at runtime.
- Type or nullability drift is a loud error: migrations are
  additive-only, per [Schema and migrations](schema.md#6-schema-rules-for-version-1).
  `NOT NULL` columns can only be added to empty tables.
- The following are rejected loudly at migrate time, never silently
  dropped: `unique`/`uniqueIndex` tags (uniqueness cannot converge
  across offline writers), `check:` tags, `constraint:` tags on
  relations, `default:` tags, autoincrement, many-to-many relations
  (model the join table explicitly with two one-to-many
  relations). Plain associations without `constraint:` tags work
  normally; foreign keys stay unenforced.
- Plain `index` tags are rejected by the migrator for a different
  reason: secondary indexes are node-local objects, not replicated
  schema, so they cannot come from `AutoMigrate`. Declare them in the
  engine's `LocalDDL` (`CREATE INDEX IF NOT EXISTS ...`) instead of
  the model.
- Destructive operations (`DropTable`, `DropColumn`, `AlterColumn`,
  renames), views, and index/constraint management return explicit
  not-supported errors.

## Transactions and unsupported operations

- `db.Transaction(...)` maps to engine transactions with true
  atomicity and rollback. Nested `Transaction` blocks fail with
  `gorm.ErrUnsupportedDriver` (no savepoints).
- `clause.OnConflict` upserts work. Explicit `clause.Returning`
  works inside explicit transactions and fails loudly on autocommit
  creates. Locking reads (`clause.Locking`) are rejected by the
  engine: there is no `SELECT FOR UPDATE`.
- The dialect is embedded-only. The `murmurd` wire servers are a
  separate deployment story with autocommit semantics.

## Evidence

- Implementation: `../gormmurmur/` (`murmur.go`, `migrator.go`,
  `model.go`).
- Live tests against real engines on both SQLite backends:
  `go test -tags "sqlite_preupdate_hook sqlite_fts5" ./gormmurmur/`
  and `go test -tags modernc ./gormmurmur/`.
- Live acceptance suites (GORM as the only access path, real QUIC
  replication): `tests-live/gorm-sync` (concurrent writes converge),
  `tests-live/gorm-migrate` (AutoMigrate on a live cluster plus
  rolling restart), `tests-live/gorm-tx` (transaction atomicity);
  shared bring-up in `tests-live/gormharness`.
