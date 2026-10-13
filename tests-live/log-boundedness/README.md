# log-boundedness (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/log-boundedness/`
(`README.md` + `log_boundedness_test.go`:
`TestLogBoundednessUnderSustainedLoad`).

## What it proves

Two encrypted, mutually authenticated nodes absorb sustained alternating
inserts (zero-based `%032x` keys, ~200-byte `payload-<seq>-...` values) for
`MURMUR_LOG_BOUNDEDNESS_SECONDS` (default 75s) while log-GC runs. Per-node
retained origin-log batches must stay within a floor-plus-slope ceiling
(the in-process analog of the Go `128 KiB + 16 bytes/row` log-file
ceiling), no replication-engine error may be recorded during the load
window, per-node on-disk growth must stay under 8 KiB/row with no
second-half acceleration past `2 * first_half + 1 MiB`, and the mesh must
still converge on exactly the acked rows with identical contents.

## Pass criteria

- `Cluster::new` opens 2 meshed nodes under
  `tests-live/runtime/log-boundedness/` with aggressive retention overlays.
- Fixed-window sustained inserts alternate nodes (`seq % 2`); at least 100
  rows are acked (else the proof is vacuous and the test fails).
- Start/mid/end samples per node: retained batches
  (`committed - collected_transactions`) and data-dir bytes.
- Per-node retained-batch growth `<= 512 + produced / 4` (floor absorbs the
  minimum-retained batches and in-flight unacknowledged batches; the slope
  is far below the dead-GC slope of ~1 retained batch per produced row).
- Per-node replication `last_error` is unchanged across the load window.
- Per-node data-dir growth `<= acked * 8 KiB`, and second-half growth `<=
  2 * first-half growth + 1 MiB`.
- Both nodes converge on exactly the acked rows with identical ordered
  `(id, name, phone, score)` contents within 90s.

## Env knobs

- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.
- `MURMUR_LOG_BOUNDEDNESS_SECONDS` (default 75): sustained-load window
  length, same name and default as Go.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`contention(id TEXT PRIMARY KEY, name TEXT, phone TEXT,
  score INTEGER)`) via `Database::transaction`/`Database::read` instead of
  the managed typed-contention HTTP API (`TypedContentionInsert/Rows`);
  keys keep the Go zero-based `%032x` format and values keep the Go
  `payload-<seq>-` + 200-character body shape; phone/score keep their zero
  values. Per-op `flush()` publishes each write to replication.
- No daemon text log exists in-process, so the Go log-file ceiling maps
  onto retained origin-log batches sampled from product APIs
  (`Status.committed` and
  `retention_status().collected_transactions`, the same mapping the
  `gc-balance` port uses): each sustained insert appends one origin-log
  batch, so dead GC grows the retained set ~1 batch/row while working GC
  holds it ~flat. The ceiling keeps Go's floor-plus-slope shape
  (`512 + produced / 4` batches per node).
- No log file exists to grep for panic/fatal, so the no-errors check
  asserts the replication `last_error` is unchanged across the window
  (collection and engine errors land there); a repeated identical error
  string would be missed, unlike Go's log grep.
- Disk growth measures the node `data/` dir (database storage) via a
  recursive `std::fs` walk instead of the daemon dir; caps and the
  acceleration check are unchanged from Go.
- Aggressive retention overlays (`retention: 1s`,
  `retention_interval: 1s`, `minimum_retained: 10`, same as the
  `gc-balance` port) instead of daemon defaults: the Rust product
  defaults (1h retention, 60s cadence) would collect nothing inside the
  75s window, so the scenario could not observe GC.
- Digest agreement compares full ordered row vectors directly instead of
  SHA-256 hex digests (equivalent, strictly stronger); the expected vector
  is rebuilt deterministically from the acked key/value functions.
- If a trivially short window never crosses the halfway sample, the
  midpoint pins to the end values (Go would index a nil sample); the
  acceleration check then degrades to a pass.

## Shared-harness addition

None. The scenario uses only the existing harness API (`Cluster`,
`ClusterOptions`, `ReplicationOptions`, `env_seconds`, `wait_until`, and
the `sqlite_session` re-export).

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/log-boundedness/Cargo.toml
```
