# migration-concurrency (Rust port)

Go source:
`/media/marc/2TB/Archives/MURMUR/tests-live/migration-concurrency/`
(`migration_concurrency_test.go`, `TestSimultaneousMigrationConverges`).

## What it proves

The same additive Note migration fires simultaneously on two of three meshed
nodes while all keep writing. Both migrating nodes must land well-defined
(old or new, never mixed), mixed-version replication must flow in both
directions during the overlap, and after the straggler migrates the whole
mesh converges fully migrated with identical counts, digests, and
materialized new-column reads on every node.

## Pass criteria (same as Go)

- `MURMUR_MIGRATION_CONCURRENCY_SEED` rows (default 60) converge everywhere
  within 60 s; all nodes old.
- Node 1 is peer-isolated; background writers flow on all nodes; the
  simultaneous migrate fires on nodes 2-3; at least one overlap write is
  acknowledged (else the overlap proved nothing).
- Both racers classify well-defined (column exposed iff new); node 1 still
  old; at least one racer new; laggards migrated sequentially.
- Heal re-adds the isolated edges; all overlap writes converge by name
  within 90 s in both directions.
- New-column writes from a new node plus an old-schema `post-race` insert
  from node 1 converge within 90 s.
- After migrating the third node, exact counts plus ordered-digest equality
  within 90 s, new schema everywhere, and `seed-0 == 100` / `seed-4 == 104`
  read identically on all nodes.

## Deviations from the Go original

- In-process multi-`Database` cluster instead of daemon processes; peer
  isolation/heal via `remove_peer`/`add_peer` (no explicit re-mesh needed:
  no restarts, no port rebinds).
- Plain SQL replaces the typed-records HTTP API: table
  `live_typed_records(name TEXT NOT NULL PRIMARY KEY, counter INTEGER NOT NULL)` plus
  `ALTER TABLE ... ADD COLUMN note TEXT`.
- Epoch assertions are `PRAGMA table_info` column-presence checks (the
  established `typed-records` port pattern); the Note exposed/hidden probe
  is a raw `SELECT note` succeeding vs failing with no-such-column.
- Go `MigrateTypedRecords` is an idempotent manifest bind; Rust DDL errors
  on a duplicate column, so every migrate is guarded by a column-presence
  check (`migrate_unless_adopted`).
- Background writers are scoped threads on cloned `Database` handles with a
  25 ms tick and an `AtomicBool` stop flag.

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/migration-concurrency/Cargo.toml
```
