# graceful-shutdown (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/graceful-shutdown/`
(`graceful_shutdown_test.go`,
`TestGracefulShutdownMidWriteAndMidSnapshot`).

## What it proves

Phase A stops node 1 mid-write under load; phase B restarts node 3 stale
(log ranges GC'd, forcing snapshot-resync) and stops it again mid-transfer.
Both restarts must be clean and fast, lose no acknowledged rows, and the
mesh must reconverge with identical contents; the snapshot path is proven
via the receiver's counters, not merely by convergence.

## Pass criteria (same as Go)

- 5-row baseline converges within 30 s; 4 s load pre-roll.
- Phase A stop lands with a write provably in flight (5 s gate); restart
  under 60 s; load converges within 90 s with every acked row present.
- 100 survivor rows + 8 fat (~256 KB) rows converge on both survivors
  within 30 s each.
- Member-deadline sleep (`MURMUR_GRACEFUL_MEMBER_DEADLINE_SECONDS`,
  default 25) plus a GC pass that observably collects log batches (a
  post-expiry pass that collects nothing fails loudly: the snapshot path
  would not be forced).
- The rejoin transfer is caught in flight (row count strictly between the
  stale count and the target); a completed-before-interrupt transfer fails
  loudly ("widen the payload").
- Post-interrupt restart under 60 s; convergence within 120 s; the bounded
  post-restart log pair proves the snapshot path (log catch-up would hide
  a snapshot-path regression; the retained tail legitimately arrives via
  logs, so the proof bounds it instead of requiring zero); all acked rows
  present.
- A `post-recovery` write converges within 60 s.

## Deviations from the Go original

- In-process multi-`Database` cluster instead of daemon processes; the
  "SIGTERM" is `close()` + reopen of the same directory with the same key,
  followed by an explicit re-mesh (fresh ephemeral port).
- Plain SQL replaces the typed-contention HTTP API: table
  `contention(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL, phone TEXT NOT NULL
  DEFAULT '')`; ordered-name vecs replace the joined-name digest.
- Clean shutdown is proven by completed handle release (`db.is_none()`
  after stop; `stop_node` logs close errors) instead of daemon log
  markers, which have no Rust equivalent; the no-loss assertions carry the
  real proof.
- GC passes run on demand (`collect_retention` in the poll loop) instead
  of waiting for the background tick (established `delete-pruning` port
  pattern); run/collection counters are the same proof.
- Transfer-in-flight detection uses row counts (strictly between stale
  and target) instead of manifest/chunk counters, which have no Rust
  equivalent; the snapshot-path proof bounds the log-counter pair by the
  retained tail instead of requiring zero (the retained tail legitimately
  arrives via logs), unlike the flat-pair `snapshot-resync` pattern.
- GC passes run on every survivor (one survivor's pass is not enough: the
  rejoining node would pull the other's intact log). Fixed node ids with
  the stale node trusting both survivors as snapshot donors (the harness
  trusts nobody by default; Go fixture policy trusts all nodes).
- Writers resolve the current-generation handle per write from a shared
  slot refreshed after every restart; a drop guard stops them on panic.

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/graceful-shutdown/Cargo.toml
```
