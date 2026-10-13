# snapshot-resync (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/snapshot-resync/`
(`README.md` + `snapshot_resync_test.go` + `snapshot_multisource_test.go`:
`TestStaleNodeRejoinsViaSnapshot`, `TestDualStaleNodesRejoinViaSnapshot`,
`TestStaleNodeRejoinsViaSingleSnapshotSource`).

## What it proves

Three nodes mesh with aggressive log retention (1 s retention, 20 s offline
pin, 10 retained batches). One node stops while the survivors write past
retention and GC expiry, then restarts with the same durable directory: its
needed log ranges are unrecoverable, so it must rejoin by merging a snapshot
without discarding its acknowledged pre-stop state. A second test stops two
nodes and restarts both back-to-back so their snapshot requests overlap on
the survivor. A multi-source case gives the stale node three snapshot
sources concurrently over a 6,000-row snapshot.

## Pass criteria

- 3 nodes mesh; node3 writes 5 `stale-*` rows; all converge (60 s).
- Node3 stops; node1 writes 60 `fresh-*` rows; survivors converge on 65
  rows (30 s each).
- After the member-deadline wait, each survivor prunes >= 40 transactions
  and no longer pins node3 (proves the rejoin must use the snapshot path,
  since only the newest 10 per origin survive).
- Node3 restarts with the same durable directory; all converge on 65 rows
  with equal digests (90 s) while node3's log-path counters stay flat
  (proves the rows arrived via snapshot, which bypasses those counters);
  the 5 pre-stop `stale-*` rows survive the merge.
- Dual leg: same shape with two stale nodes and one survivor; both rejoin
  via snapshots (120 s), both keep their 5 pre-stop rows, and each
  rejoined node reports live replication sessions.
- Multi-source leg: 4 nodes; the victim stops while a survivor writes 6,000
  rows in 100-row batches; all three survivors prune past retention; the
  victim rejoins under three concurrent sources and converges on 6,005
  rows (120 s) via the snapshot path with its 5 pre-stop rows intact.

## Env knobs

- `MURMUR_SNAPSHOT_RESYNC_GC_WAIT_SECONDS` (default 30): member-deadline
  wait before the collection passes. Lower than the Go default (55) because
  this port drives retention with explicit `collect_retention` passes
  instead of waiting out the engine's 60 s collection interval.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records HTTP API; digests are newline-joined names.
- Close/reopen replaces process stop/start (no SIGKILL leg in this
  scenario); `mesh()` re-dials after each restart because the node
  rebinds a fresh ephemeral port.
- Snapshot trust is configured up front (every node trusts every node)
  instead of via the Go fixture policy; the empty-trust default is
  unchanged, so without this overlay no leg could request a snapshot.
- No snapshot metrics exist (`ReplicationMetrics` carries no snapshot
  counters), so the Go counter legs map to behavior: `snapshots_received
  >= 1` becomes "rows converged while the receiver's
  `received/applied_transactions` stayed flat"; the dual test's
  progress/deferral-series presence legs become a connected-peer
  diagnostics assertion; the multi-source suppressed-frames leg becomes
  behavioral contention coverage (three live sources, one completed
  transfer, convergence to 6,005 rows).
- The collection proof is explicit (`collect_retention` passes plus a
  retention-pin age assertion per survivor/victim pair) rather than a
  fixed 55 s sleep, but the member-deadline wait is kept.
