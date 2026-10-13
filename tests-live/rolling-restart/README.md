# rolling-restart (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/rolling-restart/`
(`rolling_restart_test.go` + `churn_test.go`).

## What it proves

- `rolling_restart_loses_no_writes`: each node stops and rejoins in turn
  while every node keeps accepting writes. Any failed write outside a
  restart window aborts (zero-downtime restart); after each restart the
  writers quiesce and digests must agree exactly; the mesh converges with
  identical digests at the end.
- `rolling_restart_churn`: six back-to-back restarts (nodes 1,2,3,1,2,3)
  with no quiesce between, maximizing handshake/attach churn while writes
  flow; regression test for the heal stall (restart N+1 while restart N
  still healing wedged one link forever).

## Pass criteria (same as Go)

- 5-row baseline converges within 60 s.
- Zero failed writes outside restart windows (main test; the restarting
  node's writer pauses while it is down).
- After each restart: 3 s catch-up, then quiesce proves exact digest
  agreement within 150 s.
- Final: converged counts within 150 s (with settle re-sample), identical
  ordered digests on all nodes (divergence diagnostics on mismatch).

## Deviations from the Go original

- In-process multi-`Database` cluster instead of daemon processes; restart
  is `close()` + reopen of the same directory with the same key, followed
  by an explicit re-mesh (fresh ephemeral port).
- Plain SQL replaces the typed-contention HTTP API: table
  `contention(id TEXT PRIMARY KEY, name TEXT NOT NULL)`; ordered
  `(id, name)` vecs replace the SHA-256 digest.
- Writers resolve the current-generation handle per write from a shared
  slot the driver refreshes after every restart.
- A drop guard stops background writers on driver panic so the scope join
  cannot wedge (Rust-only robustness; no Go equivalent needed).

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/rolling-restart/Cargo.toml
```
