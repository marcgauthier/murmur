# rejoin-storm (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/rejoin-storm/`
(`rejoin_storm_test.go`, `TestDualKillSimultaneousRejoin`).

## What it proves

Two of three meshed nodes stop at once while the survivor keeps taking
tracked outage writes (`MURMUR_REJOIN_STORM_OUTAGE_ROWS`, default 40), then
both restart simultaneously behind a start barrier. Both must rejoin
un-wedged, every outage write must land everywhere, counts plus ordered
digests must match exactly, each node's rejoin path is recorded, and
post-rejoin writes on both revived nodes replicate everywhere.

## Pass criteria (same as Go)

- Positive control: baseline + full mesh + convergence within 30 s.
- The outage is real: both stopped nodes answer no queries.
- Survivor holds exactly `3 + outage` rows.
- Both nodes reach ready in under 60 s each after the simultaneous restart.
- All outage writes present on all three with exact counts plus equal
  digests within 90 s; rejoin path recorded per node (never mandated).
- Post-rejoin writes on both revived nodes converge within 60 s.

## Deviations from the Go original

- In-process multi-`Database` cluster instead of daemon processes; the
  "kill" is `close()` (**clean close, not SIGKILL**: killing one in-process
  node would kill the test driver); restart reopens the same directory with
  the same key.
- Plain SQL replaces the typed-records HTTP API: table
  `live_typed_records(name TEXT PRIMARY KEY)`.
- Peer counts come from the replication status (`peers`), not `/v1/status`;
  ordered-vec equality replaces the joined-name digest.
- The outage-real probe checks that no open handle remains
  (`node_info.db.is_none()`) instead of an HTTP failure.
- `start_node` is `&mut`, so the simultaneously-released restart threads
  serialize briefly on a mutex; the async catch-up after both are up still
  overlaps. One explicit re-mesh follows (fresh ephemeral ports).
- The rejoin path is inferred from the fresh post-restart log counters: a
  flat `(received, applied)` pair means snapshot, an advanced pair means log
  range repair (established `snapshot-resync` port pattern; Go reads
  `spedsql_repl_snapshots_received_total`, which has no Rust equivalent).

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/rejoin-storm/Cargo.toml
```
