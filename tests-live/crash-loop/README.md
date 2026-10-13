# crash-loop (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/crash-loop/`
(`crash_loop_test.go`, `TestCrashLoopNodeConvergesWithoutLoss`).

## What it proves

Node 3 is restarted N times (`MURMUR_CRASH_LOOP_ROUNDS`, default 5) under
continuous writes from every node, with short uptimes between restarts
(`MURMUR_CRASH_LOOP_UPTIME_MS`, default 1000). Every write acknowledged
before each restart must survive; the looped node must converge to
identical contents; it must reach ready within 60 s after the final
restart (no wedge); and a post-loop write on it must replicate everywhere.

## Pass criteria (same as Go)

- Positive control: baseline + full mesh + convergence within 30 s.
- Every non-final restart lands with a write provably in flight on the
  victim (10 s gate); the final round bursts 30 rows on a survivor and
  restarts within 500 ms (mid-replication by construction).
- Final restart reaches ready in under 60 s; mesh reforms (`[2, 2, 2]`
  peers) within 90 s; convergence within 90 s; every acknowledged write
  present everywhere.
- A `post-loop-from-victim` write then converges within 60 s.

## Deviations from the Go original

- In-process multi-`Database` cluster instead of daemon processes; restart is
  `close()` + reopen of the same directory with the same key (**clean close,
  not SIGKILL**: killing one in-process node would kill the test driver).
- Plain SQL replaces the typed-records HTTP API: table
  `live_typed_records(name TEXT NOT NULL PRIMARY KEY)`; writers tick every 25 ms.
- Peer counts come from the replication status (`peers`), not `/v1/status`.
- Ordered-vec equality replaces the joined-name digest.
- Ready time covers `start_node` (open under the unlock watchdog) plus the
  explicit re-mesh (fresh ephemeral port).
- Writers resolve the current-generation handle per write from a shared
  slot the driver refreshes after every restart; refused requests during
  victim downtime are skipped like Go.

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/crash-loop/Cargo.toml
```
