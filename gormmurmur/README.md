# gormmurmur — the GORM dialect for Murmur-SQL

`gormmurmur` lets a [GORM](https://gorm.io) application use Murmur as
its database: same GORM API for queries, preloading, associations,
hooks, and transactions, with Murmur's replicated engine underneath.
It is modeled on `gorm.io/driver/sqlite` (same SQL flavor), but
schema management goes through the engine's additive `Migrate` API
instead of SQL DDL, and everything Murmur cannot replicate is
rejected loudly at migrate time.

Import path: `github.com/marcgauthier/murmur/gormmurmur`
(package name `murmur`).

## Quick start

```go
import (
    "github.com/marcgauthier/murmur"
    gormmurmur "github.com/marcgauthier/murmur/gormmurmur"
    "gorm.io/gorm"
)

type User struct {
    gormmurmur.Model
    Name string
}

// The engine needs at least one table at Open: derive genesis from
// the models themselves.
tables, err := gormmurmur.GenesisTables(&User{})
if err != nil { /* model violates the contract (see below) */ }

// NodeID/DBID are this node's stable identity: generate once with
// murmur.NewNodeID()/murmur.NewDBID() and persist them — a fresh identity on every
// restart breaks cluster membership. (Key management likewise belongs
// to the application; see Opening below.)
db, err := murmur.Open(ctx, murmur.Config{
    Path:       "data/pebble",
    NodeID:     nodeID, // loaded after first run, not regenerated
    DBID:       dbID,   // loaded after first run, not regenerated
    Encryption: murmur.EncryptionConfig{Key: key, KeyID: "k1"},
    Schema:     murmur.SchemaConfig{Version: 1, Tables: tables},
})
gdb, err := gorm.Open(gormmurmur.Open(db))
gdb.AutoMigrate(&User{}) // idempotent for genesis tables

u := User{Name: "ada"} // ID auto-fills; no autoincrement exists
gdb.Create(&u)
```

Custom `NamingStrategy` users must derive genesis with
`GenesisTablesWithNamer` using the same strategy GORM parses with,
or table/column names diverge.

## Opening: engine first, then GORM

Unlike normal GORM dialects, `gormmurmur` does not open the database:
`gormmurmur.Open(db)` wraps an already-open `*murmur.DB`. This is
deliberate. Opening a Murmur node is distributed-system bootstrap,
not a DSN connect: node identity, encryption keys, storage path,
genesis schema, replication (listen address, peers, TLS), and bridge
settings. Identity and keys must be stable across restarts, and the
genesis schema must be derived from your models *before* GORM exists
(the engine requires at least one table at open) — none of that fits
inside a dialector.

The split, in practice:

- **Preopen** (`GenesisTables` → `murmur.Open`): everything
  GORM has no vocabulary for. Replication, encryption, and
  node-local `LocalDDL` indexes all live in the engine `Config`.
- **After that, everything is GORM**: queries, creates, preloading,
  transactions, `AutoMigrate`. GORM never sees replication or
  encryption.

Keep the `*murmur.DB` handle: you need it for `Close`, for
reopens, and for cluster operations (add/remove peers, backups),
which are engine APIs, not GORM.

**Restarts** reopen from the `LiveSchema` export, never by
re-deriving genesis: after `AutoMigrate` (or a peer adoption) the
stored schema epoch has advanced, and reopening must match the
stored manifest exactly.

```go
epoch, live, err := db.LiveSchema()
if err != nil { /* ... */ }
db.Close()

cfg.Schema = murmur.SchemaConfig{Version: epoch, Tables: live}
db2, err := murmur.Open(ctx, cfg) // same Path, IDs, key
gdb2, err := gorm.Open(murmur.Open(db2))
gdb2.AutoMigrate(&User{}) // still idempotent
```

See `TestGormMurmurReopen` for the full pattern.

## Model rules

Every replicated table needs a single application-generated `id`
`BLOB(16)` primary key. Embed `murmur.Model` (or declare
`ID murmur.ID \`gorm:"primaryKey;column:id"\`` yourself):

| Instead of (gorm/sqlite) | Use with Murmur | Why |
|---|---|---|
| `gorm.Model` (`uint` ID) | `murmur.Model` | No integer keys, no autoincrement |
| Zero `ID` on create | Nothing (auto-fills via `murmur.NewID()`) | Explicit IDs pass through untouched |
| `time.Time` fields | `murmur.Time` | Engine returns `TEXT`, unscannable into `time.Time` |
| `gorm.DeletedAt` | `murmur.DeletedAt` | Same soft-delete semantics, `TEXT` storage |
| `sql.NullTime` | `murmur.Time` / `murmur.DeletedAt` | Same storage reason |
| `int64` + `autoCreateTime` | Works unchanged | Native GORM unix-time support |
| `sql.NullString/Int64/...` | Work unchanged | Scanner/Valuer round-trips |

Timestamps store as fixed-width UTC datetime `TEXT`, so string ordering
matches chronological ordering across time zones and SQLite backends.
`murmur.Time` also parses legacy SQLite datetime spellings when reading;
applications with existing timestamp rows should rewrite them through
GORM before relying on text ordering, since reads do not rewrite stored data.

Query by primary key with `murmur.ID` (or the raw `[]byte` form), not
with the UUID string: the stored key is a blob, so a UUID-text
comparison matches nothing and reads come back empty.

Rejected loudly at migrate time (never silently dropped):

- `unique` and `uniqueIndex` tags — Murmur replicates only the
  primary-key index. Uniqueness cannot converge across offline
  writers (two disconnected nodes can accept the same value, with no
  valid merge), so it is rejected everywhere, including `LocalDDL`.
- `index` tags and `Migrator().CreateIndex` — plain secondary
  indexes are legal, but they are node-local objects, not replicated
  schema, so they cannot come from `AutoMigrate`. Declare them in the
  engine's `LocalDDL` instead of the model:
  `LocalDDL: []string{"CREATE INDEX IF NOT EXISTS idx_users_name ON
  users(name)"}`. (`IF NOT EXISTS` keeps the declaration idempotent
  across restarts and rebuilds.)
- `check:` tags and `CreateConstraint` — no check constraints.
- `constraint:` tags on relations (e.g. `OnDelete:CASCADE`) —
  foreign keys are unenforced; plain associations (no `constraint:`
  tag) work normally, without database cascades.
- `default:` tags — Murmur columns have no database defaults.
- `AUTO_INCREMENT` (explicit or GORM-implied on integer PKs).
- Missing, composite, non-`id`, or non-blob primary keys.
- Raw `[16]byte` columns — they cannot bind through `database/sql`;
  use `murmur.ID` (binds, compares, JSON-renders as UUID) or `[]byte`.
- Many-to-many relations — join tables cannot carry the required
  single-blob key; model the join explicitly with two one-to-many
  relations.
- `type:` tags naming non-Murmur types (`jsonb`, `decimal`, …).
  `size:`/`comment:` tags are accepted and ignored (SQLite ignores
  lengths too).

## Migrations

- `AutoMigrate` validates all requested models first, then creates
  missing tables and adds missing columns in one additive engine
  migration. It is idempotent for genesis tables and is rejected inside
  a GORM transaction. Pass model structs, never table-name strings.
- Never issue DDL through `Exec`/`Raw` (`CREATE INDEX`, `CREATE
  TABLE`, ...): it lands on the ephemeral SQLite materialization,
  never replicates, and vanishes on the next restart. All replicated
  schema goes through `AutoMigrate`/`CreateTable`/`AddColumn`;
  local-only objects go through the engine's `LocalDDL`.
- Type or nullability drift is a loud error — migrations are
  additive-only, matching the engine.
- `DropTable`, `DropColumn`, `AlterColumn`, `Rename*`, views, and
  index/constraint operations all return explicit not-supported
  errors.
- Add `NOT NULL` columns only to empty tables (SQLite cannot add a
  `NOT NULL` column to populated tables); prefer nullable columns.
- Restarts reopen from the `LiveSchema` export; `AutoMigrate` stays
  idempotent afterwards. See Opening above.

## Transactions and caveats

- Transactions are real: `db.Transaction(...)` maps to engine
  transactions with true atomicity and rollback.
- Nested `Transaction` blocks fail with `gorm.ErrUnsupportedDriver`
  (no savepoints); set `DisableNestedTransaction` to run nested
  blocks in the outer transaction, or flatten them.
- `clause.OnConflict` upserts work (real SQLite underneath).
- Explicit `clause.Returning` works on creates, both on autocommit
  and inside explicit transactions.
- Locking reads (`clause.Locking`) are rejected by the engine —
  Murmur has no `SELECT FOR UPDATE`.
- This dialect is embedded-only (in-process engine). The `murmurd`
  wire servers are a separate deployment story with autocommit
  semantics and create-only migrations.

## Layout

- [`murmur.go`](murmur.go) — the `Dialector`, connection setup,
  clause builders, ID auto-fill processor.
- [`migrator.go`](migrator.go) — the `Migrator` over the engine's
  `Migrate`/`LiveSchema` APIs, plus `GenesisTables`.
- [`model.go`](model.go) — `Model`, `ID`, `Time`, `DeletedAt`.
- [`murmur_test.go`](murmur_test.go) — live tests against real
  engines on both SQLite backends.
