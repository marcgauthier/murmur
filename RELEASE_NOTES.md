# Release notes

## Unreleased — RIME storage and API cutover

This release replaces Murmur's SQLite-backed SQL runtime with managed Go record
tables materialized by RIME and persisted by encrypted Spool. It is a breaking
API and on-disk format change.

### Breaking changes

- SQL execution, query, and schema APIs have been removed. Applications define
  records with `Define[T]`, provide the definitions through `Config.Tables`, and
  access them through Murmur's typed table and query APIs. See the [RIME usage
  reference](rime/USAGE.md) and [project README](README.md) for examples.
- SQLite, its driver, SQL CLI commands, and SQLite-specific build tags are no
  longer part of the production module. The normal storage build works with
  `CGO_ENABLED=0`.
- The Spool store format is 6 and the replication protocol is 6. Mutation codec
  version 4, schema manifest encoding 3, and snapshot manifest format 3 are in
  use. Protocol-5 peers cannot join this release.
- Format-5 SQL-era stores and legacy Pebble directories fail closed. This
  release does not convert them in place, and the new restore engine rejects
  legacy backups. Use the previous release to export legacy data, define the
  corresponding typed tables, and import into a fresh data directory.

There is no dual-engine mode or SQL fallback. Historical SQLite comparisons are
isolated under `tests-benchmark/` and excluded from production builds.

### Qualification status

The sequential live release gate and the Murmur root test suite passed on the
current worktree. The all-package suite, scheduled long-duration soaks, and
fixed-host rich-record performance acceptance remain pending; this note does
not represent a final release approval. See the [migration plan](architecture/migration-plan.md)
and [release status](architecture/release-status.md) for current evidence and
remaining gates.
