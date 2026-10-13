# schema-evolution (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/schema-evolution/`
(`schema_evolution_test.go`, `TestRollingTypedSchemaMigration`).

## What it proves

Three nodes share five typed rows with counters. Only node 1 registers the
additive Note field; its peers adopt the replicated schema. The new field is
written from node 1 while an older binding on node 2 renames the same row
after adoption (must not erase Note), node 3 inserts a late row, and all
nodes converge on names, the surviving note, and intact counters. Node 3
restarts and names plus counters are verified again.

## Pass criteria (same as Go)

- Names converge to the five seeded rows within 30 s.
- Migrating node adopts the column within 10 s; each peer within 30 s, with
  the `schema_adoptions` metric advancing (column presence + metric stand in
  for the Go epoch-2 assertions).
- Names converge to the six-row renamed set within 30 s; Note reads
  `added-field-survives` under the renamed key; counters stay `100+i`.
- After the node 3 restart + re-mesh, names and counter `typed-row-4 == 104`
  verify within 30 s.

## Deviations from the Go original

- In-process multi-`Database` cluster instead of daemon processes; restart is
  `close()` + reopen of the same directory with the same key, followed by an
  explicit re-mesh (fresh ephemeral port).
- Plain SQL replaces the typed-records HTTP API: table
  `live_typed_records(name TEXT PRIMARY KEY, counter INTEGER NOT NULL)` plus
  `ALTER TABLE ... ADD COLUMN note TEXT`; counter adds are
  `UPDATE ... SET counter = counter + ?`.
- Epoch waits are expressed as `PRAGMA table_info` column-presence waits (the
  established `typed-records` port pattern).

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/schema-evolution/Cargo.toml
```
