# crash-recovery (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/crash-recovery/`
(`crash_recovery_test.go`, `TestAbruptProcessDeathRecoversCommittedRows`).

## What it proves

Two nodes are abruptly restarted at different times while background
writers (2 workers x 3 nodes, 50 ms tick) hammer all nodes. Every
acknowledged write must survive on every node with equal contents, plus a
post-recovery write that replicates everywhere.

## Pass criteria (same as Go)

- One baseline write per node; full mesh (`[2, 2, 2]` peers) and baseline
  convergence within 20 s.
- Each restart lands only with a write provably in flight on the victim
  (5 s gate; a vacuous restart fails loudly).
- After each restart + re-mesh, full peering returns within 90 s under
  continuing load.
- After writers stop: convergence within `MURMUR_CRASH_SETTLE_SECONDS`
  (default 60), every acknowledged value present on every node, equal
  contents on all nodes.
- A `post-recovery-write` then converges (settle default 20) with the same
  ack + equality assertions.
- Write windows honor `MURMUR_CRASH_WRITE_SECONDS` (default 12).

## Deviations from the Go original

- In-process multi-`Database` cluster instead of daemon processes; restart is
  `close()` + reopen of the same directory with the same key (**clean close,
  not SIGKILL**: killing one in-process node would kill the test driver).
  Abrupt mid-write death is therefore not exercised; durability of
  acknowledged writes across restart is.
- Plain SQL replaces the typed-records HTTP API: table
  `live_typed_records(name TEXT PRIMARY KEY)`.
- Peer counts come from the replication status (`peers`), not `/v1/status`.
- Ordered-vec equality replaces the SHA-256 name digest (identical strength
  for an equality check; no new dependency).
- Writers resolve the current-generation handle per write from a shared
  slot the driver refreshes after every restart (mirrors Go writers
  re-resolving the node endpoint; a pre-restart clone only sees the old
  generation). Refused requests during restart windows are skipped like Go.
- Explicit re-mesh after each restart (fresh ephemeral port).

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/crash-recovery/Cargo.toml
```
