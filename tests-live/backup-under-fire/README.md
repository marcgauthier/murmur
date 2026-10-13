# backup-under-fire (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/backup-under-fire/`
(`README.md`, `backup_under_fire_test.go` `TestBackupUnderFire`,
`backupagent/main.go`).

## What it proves

A 2-node mesh absorbs sustained writes (25 ms ticker per node) for the
configured load window with **zero failed writes**, while a third member
takes repeated **online backups** (live checkpoint pipeline, node never
stops). A truncated backup remnant must fail closed. After quiescence a
fresh agent takes the final backup. Every completed backup restores;
intermediate snapshots are non-empty, non-decreasing subsets, and the
final backup matches the survivors exactly.

## Env knobs

- `MURMUR_BACKUP_UNDER_FIRE_WRITE_SECONDS` (default 30): sustained-write
  window, same name and default as Go.
- `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`:
  standard harness knobs.

## Pass criteria

1. Both writers run the full window with zero failed writes.
2. Agent A (cluster node 3, unmeshed at boot) joins mid-load and converges
   on `max(node0 count, 100)` rows within 120 s (Go `WAIT`).
3. Online backups b1, b2 from agent A's live handle verify cleanly.
4. A truncated remnant (copy of b1 with the manifest cut, i.e. stream
   ended before completion) is rejected by both `verify_backup` and
   `restore_with_options`, and the failed restore publishes no target dir.
5. Survivors converge on every written row within 90 s of quiescence.
6. Agent B (fresh data dir, fresh node identity, fresh TLS leaf) joins,
   converges on the quiesced total within 120 s, takes b4, closes cleanly.
7. Restored b1/b2 are non-empty, non-decreasing, and subset-free of phantom
   rows; restored b4 equals the survivors exactly (count + ordered names).

## Deviations from the Go original

- **SQL instead of the typed-records mesh API**: the Rust product has no
  HTTP server, so writes/reads use `Database::transaction`/`read` on a
  plain `live_typed_records(name TEXT NOT NULL PRIMARY KEY)` table. Every Go write
  inserts a fresh unique row, so the typed merge policies never trigger —
  behaviorally identical for this scenario.
- **In-process members instead of the backupagent subprocess**: agent A is
  cluster node 3 (opened unmeshed via `manual_peers`, meshed mid-load);
  agent B is a fresh out-of-band `Database` open (new dir + identity +
  `issue_cert` leaf, survivors as bootstrap/trusted snapshot sources).
  The `testnode` fixture has no replication/backup mode (harness TODO), so
  a subprocess agent that joins the QUIC mesh is not currently possible.
- **No literal SIGKILL-mid-backup**: killing the test process would kill
  the driver, and the product exposes no backup-stream throttle. The leg's
  two assertions are ported deterministically instead: a truncated remnant
  must fail closed on verify + restore, and the failed restore must leave
  no target dir (the Rust analog of Go's "no intent file" check). Atomic
  publish (backup destination must be a new dir; removed on error) is by
  construction in `src/backup.rs`.
- **Canonical ordered-names comparison** instead of the SHA-256 name
  digest (same equality semantics; follows the `backup-restore` port).
- Node stop (`close()`) replaces the agent process kill between runs A
  and B; agent A never originates rows, so it holds no unique data.
