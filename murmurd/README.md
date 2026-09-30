# murmurd — the assembled Murmur-SQL database

`murmurd` is Murmur-SQL as a runnable database server: the same
embedded engine (encrypted Pebble storage, CRDT replication over
QUIC+mTLS, SQLite materialization) with **MySQL-protocol** and
**PostgreSQL-protocol** frontends instead of the Go API. Configure it
with a TOML file, point any MySQL or Postgres client at it, and run.

If you want the building block (embed the engine in your own Go
program), that is the [parent package](../README.md). If you want a
database you can download, configure, and connect to, this is it.

## One engine, two protocols, one dialect

Both frontends funnel into the **one SQLite engine**. The wire
protocols are access paths, not dialects: only SQLite-expressible
statements succeed, and anything else is rejected with an explicit
reason (`murmurd: only SQLite-expressible statements are supported:
...`). There is no query translation and no emulation of server-side
features SQLite cannot express.

This makes behavior predictable: whatever the engine accepts through
one frontend behaves identically through the other (prove it to
yourself with `TestPGMySQLCrossTalk`).

## Quick start

```sh
# Build the daemon (CGO backend; see below for pure-Go).
go build -tags "sqlite_preupdate_hook sqlite_fts5" ./murmurd/cmd/murmurd

# Write an example config, then edit data_dir + key.
./murmurd --generate-config > config.toml
$EDITOR config.toml

# Provide the schema file next to it (required, see below).
cp murmurd/schema.example.sql schema.sql
$EDITOR schema.sql

# Run it (SIGINT/SIGTERM shuts down cleanly).
./murmurd --config config.toml
```

Then connect (defaults: MySQL on `127.0.0.1:3306`, Postgres on
`127.0.0.1:5432`, database `murmur`):

```sh
mysql -h 127.0.0.1 -P 3306 -u root murmur
psql -h 127.0.0.1 -p 5432 -U murmur -d murmur
```

```sql
-- Either client, same result.
CREATE TABLE notes (id BLOB PRIMARY KEY, title TEXT);
-- MySQL clients must use a fixed-length key instead:
-- CREATE TABLE notes (id VARBINARY(16) PRIMARY KEY, title TEXT);
INSERT INTO notes (id, title) VALUES (randomblob(16), 'hello');
SELECT title FROM notes;
```

Pure-Go build (no C compiler):

```sh
CGO_ENABLED=0 go build -tags modernc ./murmurd/cmd/murmurd
```

## Configuration (`config.toml`)

See [config.example.toml](config.example.toml) (also printed by
`--generate-config`). Sections:

- `[murmur]` — `data_dir` (pebble store + `node.json` identity),
  optional fixed `node_id`/`db_id`. Empty IDs load from (or are
  generated into) `node.json` on first boot, so restarts keep a
  stable identity; a configured ID that disagrees with the persisted
  one refuses to start.
- `[murmur.encryption]` — `key_id` plus exactly one of `key_hex`
  (16/24/32 bytes hex) or `key_file` (path to the hex, `0600`
  recommended). There is no unlock endpoint: whoever can read the
  config (or key file) can open the store, so protect it like any
  database credential. Never commit a real key.
- `[mysql]` / `[postgres]` — `enable`, `addr`, `database`.
- `[replication]` — mesh settings. Replication stays off while
  `addr` is empty and no peers/bootstrap entries exist; meshing
  requires the TLS files (`ca_cert_file`, `node_cert_file`,
  `node_key_file`). Peers are static `node_id` + `addrs` entries.
- `[schema]` — `file`, the user-provided `schema.sql` (required).
  It holds `CREATE TABLE` statements in the strict subset (see
  [schema.example.sql](schema.example.sql)): at least one table,
  every table with an `id` blob primary key, column types any
  SQLite/PostgreSQL/MySQL spelling of integer, real, text, blob.
  Relative paths resolve from the config file's directory.
  (`[[schema.tables]]` is embedded-only and rejected here.)

## What works (and what is filtered out)

Supported on **both** frontends:

| Statement | Notes |
|---|---|
| `SELECT` (incl. `WHERE`/`ORDER BY`/`LIMIT`/`JOIN`/`GROUP BY`, `WITH`, `VALUES`) | MySQL: executed by the go-mysql-server engine over table scans. Postgres: text runs on SQLite directly. |
| `INSERT` / `UPDATE` / `DELETE` | Single statements, autocommit. PG placeholders are `$1..$N` (sequential); `$n` inside the target column list binds by position. |
| `REPLACE` (MySQL) | Rewritten to delete-by-key + insert. |
| `SHOW TABLES` / `DESCRIBE` (MySQL) | Engine-provided over the provider. (PG `SHOW` is rejected; there is no `pg_catalog`.) |
| `CREATE TABLE` | Additive only, mapped to a murmur migration: exactly one `PRIMARY KEY` on `id`, blob-typed, stored `NOT NULL` (the `NOT NULL` keyword itself is optional — both wires normalize a bare `PRIMARY KEY` to stored `PRIMARY KEY NOT NULL`); columns limited to integer/real/text/blob. MySQL note: `BLOB PRIMARY KEY` is rejected by MySQL itself — use `VARBINARY(16)`. Missing/composite/non-`id`/non-blob keys, types with no murmur equivalent (`JSON`, `SERIAL`, dates, numerics, …), `UNIQUE`, secondary indexes, defaults, `AUTO_INCREMENT`, generated columns, checks, and foreign keys are rejected with Murmur reasons, leaving no table, epoch advance, or sidecar change behind. |
| `TRUNCATE [TABLE] t` | Rewritten to `DELETE FROM t` (SQLite has no `TRUNCATE`). |
| Transaction framing (`BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`…) | Absorbed per session: the engine autocommits every statement, so these succeed and do nothing. Never rely on atomicity or rollback. |
| Session statements (`SET`, `RESET`, `DISCARD`) | Accepted and ignored (drivers send these on connect). |
| `SELECT version()` / `SELECT current_database()` (PG) | Emulated by the frontend (`PostgreSQL 15.0 (murmurd …)`). |

Rejected on **both** frontends (explicit errors, never silent):

- `DROP` / `ALTER` / `RENAME` — murmur schema changes are
  additive-only.
- Secondary `UNIQUE` (column attribute, table constraint,
  `UNIQUE KEY`, `CREATE UNIQUE INDEX`, `ALTER … ADD UNIQUE`) —
  murmur replicates only the primary-key index. (MySQL note: a
  `UNIQUE` keyword merged into the `PRIMARY KEY` column itself is
  redundant and normalized away; no secondary index exists
  afterwards.)
- Plain secondary `CREATE INDEX` / `ALTER … ADD KEY` — local-only
  in murmur, managed through embedded `LocalDDL`, never over the
  wire.
- `DEFAULT`, `AUTO_INCREMENT` (column or table option),
  `SERIAL`, generated columns, `CHECK`, `FOREIGN KEY` /
  `REFERENCES` — murmur models none of these; they are rejected
  rather than silently dropped.
- `CREATE VIEW`/`TRIGGER`/`FUNCTION`/`SEQUENCE`/…
- `INSERT`/`UPDATE`/`DELETE … RETURNING` (writes return no rows).
- `GRANT`/`REVOKE`, users, roles, `COPY`, `LISTEN`/`NOTIFY`,
  `VACUUM`/`ANALYZE`, `PREPARE` (SQL-level), procedures.
- `SHOW` (Postgres side only; MySQL `SHOW TABLES`/`DESCRIBE` work).
- MySQL specifics: anything outside reads, row-routed writes,
  `REPLACE`, `TRUNCATE`, `SHOW`/`DESCRIBE`, and `CREATE TABLE` (no
  `ALTER`/`DROP`/`RENAME`, no locking clauses SQLite rejects).
- Postgres specifics: `::casts`, `LIMIT ALL`, `DISTINCT ON`,
  `ILIKE`, data-modifying CTEs, and anything else SQLite cannot
  parse — the engine error is wrapped and returned. Multi-statement
  strings work in the simple protocol only (split quote-aware,
  including `$tag$` bodies).

Type mapping (both directions): murmur `integer` ↔ `INT8`,
`real` ↔ `FLOAT8`, `text` ↔ `TEXT`, `blob` ↔ `BYTEA`. PG result
columns infer OIDs from values (unanimous kinds keep their type,
mixed kinds degrade to text); a parameter bound to a blob column
decodes PG-escaped bytea to raw bytes. Values the decided type
cannot render fail loudly instead of corrupting.

## Schema evolution and restarts

- `schema.sql` is **genesis plus desired additive state**: it
  bootstraps a fresh `data_dir`, and on every boot any
  file-declared table/column missing from the live schema migrates
  in. `CREATE TABLE` over either frontend works the same way at
  runtime.
- The daemon owns `data_dir/schema.json`, a sidecar holding the last
  live declaration (epoch + resolved tables). The engine requires a
  reopen to match the stored schema exactly, and the stored schema
  advances on every `CREATE TABLE` and every replicated adoption —
  so the daemon reopens from the sidecar and refreshes it after
  each local migration, every 10 s while running, and on shutdown.
- **Never hand-edit `schema.json`.** To evolve the schema, edit
  `schema.sql` and restart, or `CREATE TABLE` over SQL. Removing a
  table from the file does not drop it (murmur schemas are
  additive-only); changing a stored column's type in the file
  fails the boot with a conflict error.
- If the sidecar is lost but the data dir is intact, the boot error
  tells you to re-declare the missing tables in the file once; the
  daemon then tracks automatically again. Stop-the-world file
  backup of `data_dir` while stopped captures both.
- Pre-sidecar data dirs (murmurd before v0.1.0 schema tracking)
  need the same one-time treatment: declare every SQL-created
  table in the file, boot once, and the sidecar takes over.

## Transactions, sessions, and consistency notes

- **Autocommit everywhere.** Every statement is one murmur implicit
  transaction. `BEGIN`/`COMMIT` succeed but change nothing; a
  multi-statement "transaction" is neither atomic nor isolated, and
  `ROLLBACK` cannot undo committed statements. Design accordingly.
- **Replication is murmur replication.** Writes through either
  frontend enter the same CRDT log and converge on peers identically
  (`TestReplicateOverMySQL` proves wire-to-wire replication).
- **Reads are materialization reads.** Both frontends read the same
  SQLite materialization the Go API reads.

## Security notes

- **No client authentication in v1.** Both frontends trust all
  connections (any MySQL user, any PG user). Defaults bind
  loopback; do not expose the ports to untrusted networks — firewall
  them or tunnel over SSH.
- **No wire encryption on the SQL ports in v1.** Replication traffic
  is QUIC+mTLS; the MySQL/PG listeners are plaintext. Same advice:
  loopback or a trusted tunnel.
- **The config holds the storage key.** `key_hex`/`key_file` opens
  the encrypted store. Prefer `key_file` with `0600`, and never
  commit either.
- Node identity (`node.json`) and TLS key files are read at startup
  from operator-owned paths.

## Operating

- Logs go to stderr (slog text). Per-query logging is the
  frontends' default (go-mysql-server logs errors; psql-wire logs at
  Warning+ by daemon default).
- Backups: use the engine's backup story against `data_dir`
  (stop-the-world file copy while stopped is always safe).
- Schema evolution: see "Schema evolution and restarts" above;
  anything beyond additive tables/columns follows the engine's
  migration path (embedded API).
- Metrics/health endpoints are engine-level (embedded API); the
  daemon currently exposes SQL ports only.

## Layout

- [`config.go`](config.go) — TOML config, validation, identity, keys.
- [`filter.go`](filter.go) — SQLite-subset classifier, `$n`→`?`
  translation, strict `CREATE TABLE` parser. Shared by both
  frontends.
- [`provider.go`](provider.go), [`table.go`](table.go) —
  go-mysql-server backend over murmur.
- [`mysqlguard.go`](mysqlguard.go) — pre-analyze guard rejecting
  non-murmur DDL (secondary/unique indexes, checks, foreign keys,
  defaults, autoincrement) before anything commits.
- [`pgwire.go`](pgwire.go) — psql-wire frontend.
- [`server.go`](server.go) — daemon lifecycle (`New`/`Run`/`Close`).
- [`schema_state.go`](schema_state.go) — live-schema sidecar
  (`data_dir/schema.json`), boot reconcile, shutdown persist.
- [`schemafile.go`](schemafile.go) — `schema.sql` loader (strict
  `CREATE TABLE` subset + comments, shared key rules).
- [`schema.example.sql`](schema.example.sql) — documented schema
  file example.
- [`cmd/murmurd`](cmd/murmurd/main.go) — the binary
  (`--config`, `--generate-config`, `--version`).
- [`config.example.toml`](config.example.toml) — documented example.
- [`murmurd_test.go`](murmurd_test.go),
  [`filter_config_test.go`](filter_config_test.go),
  [`wire_rules_test.go`](wire_rules_test.go) — live
  over-the-wire tests plus filter/config units. `wire_rules_test.go`
  is the schema-rule matrix: every rejection asserted live on both
  frontends, with ghost-table, epoch, and sidecar residue checks.
