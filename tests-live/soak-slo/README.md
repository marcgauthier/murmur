# soak-slo (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/soak-slo/`
(`soak_slo_test.go`: `TestThreeNodeEncryptedSustainedWriteSLO`).

## What it proves

Three encrypted nodes sustain independent managed-record writes for
`MURMUR_SLO_DURATION_SECONDS` (default 5 s smoke; two hours for SLO
acceptance). Each node owns a bounded 1,000-row range: it inserts until the
range fills, then updates those rows so the working set stays bounded.
Afterwards all three nodes must converge to an identical table digest within
`MURMUR_SLO_SETTLE_SECONDS`, with write p95 under 5 s, maximum write latency
under 20 s, replication queues under 5,000 staged transactions, and spool
disk usage under 512 MiB per node.

## Env knobs

- `MURMUR_SLO_DURATION_SECONDS` (default 5): soak length.
- `MURMUR_SLO_SETTLE_SECONDS` (default 30): convergence deadline.
- Standard harness knobs: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

Two-hour SLO acceptance form (run via `tests-live/run.sh`, never directly):

```sh
MURMUR_SLO_DURATION_SECONDS=7200 \
MURMUR_SLO_SETTLE_SECONDS=300 \
  bash tests-live/run.sh soak-slo
```

## Pass criteria

- Every node holds 2 live peer sessions before the soak starts.
- No writer thread reports a transaction/flush failure.
- At least 60 application writes committed mesh-wide.
- All three nodes converge to exactly the owned row count with identical
  sorted row contents.
- Every node has replication running, is fully materialized, reports at
  most 5,000 staged transactions, and reports `spool disk_bytes <= 512 MiB`.
- Write p95 <= 5 s and maximum write latency <= 20 s (same nearest-rank
  p95 position as the Go original).

## Deviations from the Go original

- Plain SQL (`contention(id TEXT PRIMARY KEY, name TEXT NOT NULL,
  phone TEXT NOT NULL)`) replaces the typed-contention HTTP API; row ids
  keep the Go `%032x`-of-`(node*1e12+slot+1)` shape, names keep
  `node-<n>`, and values keep `node-<n>-write-<seq>`.
- Soak writers are in-process threads on cloned `Database` handles; there
  are no restarts, so handles stay valid for the whole run.
- Measured write latency covers transaction commit plus `flush` (the Go
  HTTP write path likewise persists before acknowledging).
- Service state `ready` maps to an open handle plus a running replication
  engine; there is no HTTP `/v1/status` to scrape.
- `spedsql_materialized_generation == spedsql_state_generation` has no Rust
  product metric; the port asserts quiesced status instead (no storage
  error, `pending_bytes == 0`, `committed == durable` after a flush, with a
  30 s settle), following the `long-running-five-node` port.
- `spedsql_queued_need` / `spedsql_queued_ctrl` map to the closest Rust
  queue-depth metric, `replication status().staged_transactions <= 5000`.
- `spedsql_spool_disk_bytes` maps directly to `spool_metrics().disk_bytes`.
- Digest equality is byte-identical sorted vectors (which imply identical
  digests); the `digest=` log field is a dependency-free FNV-1a hex string
  rather than sha256.
- Connected peers are directed-pair session probes over
  `peers_diagnostics` instead of `/v1/status` scrapes.
