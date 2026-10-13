# open-progress (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/open-progress/`
(`open_progress_test.go` only; that dir has no `README.md`, `attacker.go`,
or helpers). Ports `TestEmbeddedOpenProgressLive` (roles `populate`,
`reload`, `cancel`, `reload`, `open-failure`, `reload`) and
`TestEmbeddedOpenProgressLiveExtra` (roles `table-tracking`, `heartbeat`,
`empty-db`, `phase-elapsed`, `status-snapshot`).

## What it proves

A real encrypted store holding 6000 rows x 2 tables (~30 MB) is closed and
reopened while `on_open_progress` callbacks record every event. Each
reopen must report a sane terminal snapshot, a monotonic non-overlapping
event stream with exactly one terminal event, and the full phase sequence;
failed opens must report `Failed` without a handle; and the store must
recover afterwards with identical contents. Extra legs cover per-table
rebuild tracking, rebuild heartbeating, empty-database opens, per-phase
timing, and status-snapshot isolation.

## Pass criteria

- `populate`: fresh encrypted DB takes 6000 `requests` + 6000 `events`
  rows (1000/batch), flushes, closes.
- `reload-1` / `reload-2-recovery` (under the unlock watchdog): terminal
  `OpenProgress` is `Ready` with `rows_inserted == 12000`,
  `rows_skipped == 0`, `processed_items >= rows_inserted`, cleared
  `current_table`, no error; both tables verify by count + ordinal sum;
  callbacks never overlap; stream is non-empty, monotonic
  (`processed_items`, `elapsed`), has exactly one terminal event ending in
  `Ready`, and collapses to
  `Opening, Rebuilding, Indexing, Finalizing, Ready`.
- `open-failure-short-key` (3-byte key) / `open-failure-wrong-key`
  (wrong 32-byte key): open fails with no handle; stream starts at
  `Opening` and ends in `Failed` with exactly one terminal event.
- `table-tracking`: terminal shows `rows_inserted == 12000` and a cleared
  `current_table`; both tables countable at 6000.
- `heartbeat`: at least one `Rebuilding`-phase event.
- `empty-db`: fresh create fires the full phase sequence with zero
  `processed_items`/`rows_inserted`/`rows_skipped` and ends `Ready`.
- `phase-elapsed`: every event has `phase_elapsed <= elapsed`.
- `status-snapshot`: 10 consecutive `status().open_progress` snapshots are
  all `Ready`, mutually independent (mutating one never affects the
  others), and stable in `processed_items`, with
  `rows_inserted == 12000`.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS` (reload opens run under
`unlock::open_with_watchdog`).

## Deviations from the Go original

- In-process `close()` + reopen per leg (isolated subdirs under
  `runtime/open-progress/`) instead of re-execed child processes per role
  (`MURMUR_PROGRESS_CHILD`), per the tests-live design.
- No `cancel` leg: Rust `Database::open` takes no context and the product
  never emits `OpenPhase::Cancelled` (`progress.rs: finish()` only emits
  `Ready`/`Failed`).
- Rust phase sequence `Opening, Rebuilding, Indexing, Finalizing, Ready`
  instead of Go's `Opening, Replaying, Rebuilding, Finalizing, Ready`:
  the Rust product has no `Replaying` phase, and index creation is a
  separate `Indexing` phase.
- Rust processed-items semantics: `processed_items` counts spool records
  loaded plus rebuilt rows, not Go's cells (rows x fields), so the exact
  `6000*2*3` assertion becomes `rows_inserted == 12000` plus
  `processed_items >= rows_inserted` and monotonicity.
- Go's counting/estimate fields (`TotalItemsKnown`, `EstimateKnown`,
  `PercentComplete`, `EstimatedRemaining`, `CountedItems`, `OpenCounting`)
  and `StartedAt` do not exist on Rust `OpenProgress`; those assertions
  have no counterpart and are omitted.
- Plain SQL schema (`requests`/`events` with `id/message/ordinal`) instead
  of Go typed models with field IDs; count + `SUM(ordinal)` replaces
  `Find` + ordinal summation.
- `status().open_progress` is an owned `Option<OpenProgress>` clone, so
  snapshot independence holds by construction; a terminal snapshot exists
  only when a callback was set (the status-snapshot leg uses a no-op).
- Single embedded node without replication: the harness
  `artifacts`/`forensics`/`unlock` modules are used directly instead of
  `Cluster`, which always builds a replicated QUIC mesh. The currently
  open handle is tracked for failure forensics; artifacts follow the
  standard `runtime/` -> `failures/` + `RESULT` convention.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/open-progress/Cargo.toml
```
