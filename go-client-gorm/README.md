# go-client-gorm

GORM-compatible SQL layer over Murmur's [`go-client`](../go-client) (module
`github.com/marcgauthier/murmur/go-client-gorm`, package `murmurgorm`).

Murmur's SQLite is a private in-memory instance inside the Rust core: stock
GORM+SQLite drivers cannot reach it (no file, no DSN, no shared cache across
the separately-linked SQLite copies). This module bridges `go-client`'s
callback API to `database/sql/driver`, then serves GORM through a vendored
SQLite dialect. The upstream `gorm.io/driver/sqlite` package itself cannot be
imported: its `mattn/go-sqlite3` amalgamation exports the same `sqlite3_*`
symbols as Murmur's staticlib and fails at link time.

## Build

Requires Go 1.24+, cgo, and the prebuilt native library that ships with
`go-client` (`go-client/native/lib/<os-arch>/libmurmur_go.a`; rebuild with
`go-client/build.sh` if missing). The `go-client` dependency is wired with a
`replace` directive to `../go-client` (it is not published on the module
proxy).

```sh
cd go-client-gorm
go build ./...
go test -count=1 ./...
```

No extra `CGO_LDFLAGS` are needed: the link flags in `go-client`'s cgo
preamble suffice, and this module compiles no SQLite amalgamation of its own.

## Use

Open and own the native database yourself (key, `Options`, lifecycle), then
hand it to GORM. Pool connections are Murmur clones; closing the pool or the
GORM handle releases clones and never closes your database.

```go
import (
    murmur "github.com/marcgauthier/murmur/go-client"
    murmurgorm "github.com/marcgauthier/murmur/go-client-gorm"
    "gorm.io/gorm"
)

opts := murmur.DefaultOptions()
opts.Mode = murmur.CreateNew
db, err := murmur.Open("/data/murmur", key32, opts)
if err != nil { ... }
defer db.Close()

gdb, err := gorm.Open(murmurgorm.Open(db), &gorm.Config{
    TranslateError: true, // opt into ErrDuplicatedKey / ErrForeignKeyViolated
})
if err != nil { ... }

type User struct {
    ID        uint `gorm:"primaryKey"`
    Name      string
    CreatedAt time.Time
    UpdatedAt time.Time
}
gdb.AutoMigrate(&User{})
gdb.Create(&User{Name: "ada"})

// Admin stays native, on the same database, while GORM is active:
native, _ := murmurgorm.Native(gdb)
native.Flush()
```

`murmurgorm.SQLDB(gdb)` exposes the `*sql.DB` for pool tuning. There is no
DSN form: use `sql.OpenDB(murmurgorm.NewConnector(db))` for plain
`database/sql` access.

## GORM notes

- Error translation (`ErrDuplicatedKey`, `ErrForeignKeyViolated`) requires
  `Config.TranslateError`, same as stock drivers.
- `DisableForeignKeyConstraintWhenMigrating` is forced on: Murmur's schema
  validation rejects the `REFERENCES` keyword, so database-level FK
  constraints can never migrate. Application-level associations
  (`Preload`, joins) work normally.
- `AUTOINCREMENT` is not emitted (`integer PRIMARY KEY` keeps the rowid alias
  and automatic IDs; only the monotonic never-reuse guarantee is lost).
- Every model needs a single-column primary key; GORM's default `ID uint`
  satisfies this.

## Limitations (all enforced by Murmur, not this package)

- **No nested transactions.** `SAVEPOINT` is denied by Murmur's authorizer in
  every mode; nested `Transaction` calls fail with
  `ErrSavepointsUnsupported`. Single-level transactions (including GORM's
  default per-write transaction) work.
- **DDL runs through `Migrate`, never inside transactions.** `CREATE` / `DROP` /
  `ALTER` / `REINDEX` statements are detected by first keyword and routed to
  `DB.Migrate`. DDL inside a transaction fails with `ErrDDLInTransaction`;
  DDL with bind arguments is rejected (inline values instead). Table-recreate
  migrations (`DropColumn`, `AlterColumn`) run step-by-step in autocommit
  rather than atomically; re-running converges.
- **Schema subset.** Rejected by Murmur: non-PK `UNIQUE` columns and unique
  indexes (`uniqueIndex` tags), `CHECK` constraints, foreign key constraints,
  views, triggers, generated columns, `AUTOINCREMENT`. PK identity is
  immutable across migrations.
- **Writes denied in reads; PK/policy columns frozen.** Ordinary reads reject
  writes; `UPDATE` of primary-key columns requires the migration path.
  `PRAGMA` is limited to `table_info`/`table_xinfo`/`index_list`/`index_info`
  (foreign-key pragmas are no-ops) and raw `BEGIN`/`COMMIT`/`ROLLBACK` SQL is
  rejected (use the `Tx` API).
- **Reads are buffered.** Murmur cursors live only inside their callback, so
  each query materializes its rows; there is no streaming cursor. Prepared
  statements are re-prepared per call (correct, one prepare per call).
- **Timestamps are format-driven.** `time.Time` binds as canonical SQLite
  TEXT; TEXT values exactly matching SQLite datetime layouts read back as
  `time.Time` (Murmur rows carry no declared column types, so a plain string
  that looks exactly like a timestamp parses as one).
- `database/sql` transaction options (isolation, read-only) are accepted and
  ignored; Murmur serializes writers internally.

## Layout

| File | Contents |
|---|---|
| `driver.go`, `conn.go`, `stmt.go`, `rows.go` | `database/sql/driver` shim |
| `tx.go` | transaction pump over callback-scoped `Transaction` |
| `ddl.go` | DDL / tx-control statement routing |
| `values.go` | value mapping, timestamp formats |
| `gorm.go` | `Dialector`, `Open`, `Translate`, `Native`, `SQLDB` |
| `dialect.go`, `migrator.go`, `ddlmod*.go` | vendored SQLite dialect (see below) |
| `driver_test.go`, `gorm_test.go` | maintained suite (30 tests) |

## Vendored code

`dialect.go`, `migrator.go`, `ddlmod.go`, `ddlmod_parse_all_columns.go`
(plus one error value in `errors.go`) are vendored from
`gorm.io/driver/sqlite` v1.6.0 (MIT, Copyright Jinzhu; full text in
`LICENSE.UPSTREAM`) with Murmur adaptations noted at each site: package
rename, `mattn/go-sqlite3` import removed (link conflict, see above), DSN
surface removed, `Migrator` on a pointer receiver, `SavePoint`/`RollbackTo`
report unsupported, table-recreate runs without a transaction, and
`DataTypeOf` omits `AUTOINCREMENT`.
