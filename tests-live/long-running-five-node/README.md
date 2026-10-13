# long-running-five-node (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/long-running-five-node/`
(`five_node_soak_test.go`: `TestFiveNodePersistentCapacityAndMeshSoak`).

## What it proves

Five encrypted nodes run a continuous per-node write soak across a full mesh
for `MURMUR_FIVE_NODE_DURATION_SECONDS` (default 30 s smoke; one hour for
capacity acceptance). Node2 and node4 restart in place mid-run (at 1/3 and
2/3 of the soak) while writes continue; their persisted rows must survive
each reopen. Afterwards all five nodes must converge to the same row set,
every node must hold fully materialized state within a 2 GiB spool budget,
and one final application write must converge everywhere.

## Env knobs

- `MURMUR_FIVE_NODE_DURATION_SECONDS` (default 30): soak length.
- `MURMUR_FIVE_NODE_INTERVAL_MS` (default 50): per-node write cadence.
- `MURMUR_FIVE_NODE_SETTLE_SECONDS` (default 120): convergence deadline.
- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

One-hour capacity acceptance form (Go passes
`CGO_ENABLED=0`; run via `tests-live/run.sh`, never directly):

```sh
MURMUR_FIVE_NODE_INTERVAL_MS=200 \
MURMUR_FIVE_NODE_DURATION_SECONDS=3600 \
MURMUR_FIVE_NODE_SETTLE_SECONDS=300 \
  bash tests-live/run.sh long-running-five-node
```

## Pass criteria

- Every node holds 4 live peer sessions before, between, and after restarts.
- Each node acknowledges >= 10 rows; >= 100 rows acknowledged mesh-wide.
- Each restarted node holds at least its pre-restart row count after rejoin.
- All five nodes converge to identical sorted row sets with at least the
  acknowledged row count.
- Every node is fully materialized and reports `spool disk_bytes <= 2 GiB`.
- The post-soak write converges to `final + 1` identical rows on all nodes.

## Deviations from the Go original

- Plain SQL (`records(id TEXT PRIMARY KEY, name TEXT NOT NULL)`) replaces the
  typed-records HTTP API; row names keep the
  `node-<n>-capacity-<seq>` shape with dense per-node sequences.
- In-process `close()` + reopen replaces the daemon restart; the Go
  `UnlockNode`/`WaitNodeReady` steps fold into the harness reopen watchdog,
  and readiness is the live session mesh plus row availability.
- The reopened node rebinds a fresh ephemeral port, so the test re-meshes
  after each restart (Go: `StartNode` re-dials static addresses).
- Soak writers are in-process threads sharing a handle table that the main
  thread swaps on restart; failed transactions/flushes during the restart
  windows count as transient errors, like Go's transient HTTP write errors.
- Digest equality is byte-identical sorted vectors (which imply identical
  digests); the `digest=` log field is a dependency-free FNV-1a hex string
  rather than sha256.
- `spedsql_materialized_generation == spedsql_state_generation` has no Rust
  product metric; the port asserts quiesced status instead (no storage
  error, `pending_bytes == 0`, `committed == durable` after a flush, with a
  30 s settle), following the `high_low.rs` quiescence-gate pattern.
- `spedsql_spool_disk_bytes` maps directly to `spool_metrics().disk_bytes`.
- Connected peers are directed-pair session probes over
  `peers_diagnostics` instead of `/v1/status` scrapes.
