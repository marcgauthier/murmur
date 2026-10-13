# resource-exhaustion (Rust port, 3 of 5 legs)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/resource-exhaustion/`
(`oom_test.go`, `fd_test.go`, `compaction_test.go`, `slow_peer_test.go`,
`proxy_test.go`, `huge_txn_test.go`, `helpers_test.go`).

## What it proves

Each ported leg squeezes real nodes and demands the same contract: fail
closed under pressure, stay exactly converged after.

- `stalled_compaction_stays_correct`: a 32 MiB-segment victim piles sealed
  `.spool` debt under 64 KiB update churn while the 256 MiB control does
  not (stall proof: victim >= 3 segments and ahead); exact convergence
  holds under the stall; after dropping the override + restart + retention
  collection, dead segments drain and the digest is stable.
- `slow_peer_converges_without_wedge`: replication crosses userspace UDP
  throttle proxies both directions (default 256 KiB/s, no privileges);
  the slow node must make steady progress (90 s without progress = wedge
  = FAIL) until converged within the bound, with byte-count proof the
  data crossed the proxies; direct re-peer heals.
- `huge_transactions_at_default_budgets`: an 8 MiB value replicates
  byte-exact; an oversize transaction is rejected naming the budget with
  no partial row and a healthy writer; 20 x 512 KiB converge exactly.

## Not ported (blocked, not skipped lightly)

- Near-OOM leg: Go pins `GOMEMLIMIT` + a small block cache on the victim
  *process* and caps RSS via a cgroup on its PID. In-process nodes share
  the driver's heap and cgroup, so a victims-only squeeze is impossible;
  port when the harness fixture gains a replicating mode runnable under
  its own limits.
- FD-exhaustion leg: Go squeezes the victim PID's `RLIMIT_NOFILE` via
  `prlimit` and holds idle TLS connections against its HTTPS API. Both
  halves are missing in Rust (a whole-process squeeze would break the
  driver too, and no HTTPS API exists); same fixture requirement as OOM.

## Pass criteria (same as Go, per ported leg)

- Compaction: `MURMUR_RX_COMPACT_ROWS` updates (default 2000) over 200
  keys; stall proven within 2 min; convergence within 3 min; debt drains
  within 4 min of retention collection; digest stable.
- Slow peer: knobs `MURMUR_RX_SLOW_RATE` (262144),
  `MURMUR_RX_SLOW_ROWS` (600), `MURMUR_RX_SLOW_VALBYTES` (1024),
  `MURMUR_RX_SLOW_BOUND_SECONDS` (300); baseline crosses the throttle;
  throttled bytes >= half the churn payload (bypass guard); heal + 10
  rows converge within 1 min.
- Huge transactions: 8 MiB byte-exact both nodes; rejection names
  `budget exceeded` with count still 1 on both nodes; probe write works;
  22 rows converge within 3 min.

## Deviations from the Go original

- In-process multi-`Database` clusters instead of daemon processes; plain
  SQL replaces the typed-contention HTTP API (`contention(id, name)`).
- Ordered-vec equality replaces SHA-256 digests (identical strength), and
  in-process reads have no client timeout, so huge tables need no
  digest segmenting.
- The Database layer runs no background spool reclaim
  (`reclaim_interval = None`), so the stall is purely segment-size debt
  and "re-enable" maps to explicit retention collection (which drives
  the internal reclaim) after dropping the override + restart.
- Rejection trips the default 64 MiB *transaction* budget via one 9 x 8
  MiB transaction (Rust has no per-value limit; each value stays under
  the 16 MiB spool value cap so only the transaction budget trips).
- The UDP proxy sender polls its two lane channels (std has no `select`;
  1 ms park when empty; priority preference is otherwise exact) and uses
  kernel-default socket buffers (std has no read-buffer API).
- Deterministic xorshift64* replaces `math/rand` for incompressible
  values (no new dependencies).
- This scenario needed two small harness additions (same as Go's
  `SpoolByNode`): `SpoolOptions`/`spool_by_node` + `set_spool_options`,
  and `add_peer_addr` for proxied dialing.

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/resource-exhaustion/Cargo.toml
```
