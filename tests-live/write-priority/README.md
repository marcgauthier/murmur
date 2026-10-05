# Local-write priority live scenario

Two Murmur-SQL daemons take continuous writes on both nodes (sixteen writers
per node, disjoint 16-byte row-ID ranges) while serving reads, under
full-mesh replication. The test gates on:

1. Local progress: at least 80 rows written and 4 reads served.
2. Priority: the remote (replication) share of dual-contention
   writer service time (`spedsql_sched_dual_service_seconds_total`,
   granted while the other interactive class had waiters, so the
   scheduler actually chose between backlogged classes) is at most
   25%, with at least 1s of dual service measured. The scheduler
   targets 90/10 local/remote with a 1s debt bound; the gate
   leaves headroom above the 10% target. Dual-contention
   attribution is load-bearing: whenever either queue drains, the
   other legitimately borrows the idle share, and counting
   borrowed service would measure driver stalls instead of the
   scheduling policy.
3. Convergence: identical row counts and matching SHA-256
   application-data hashes after writes stop.

## Run

```sh
MURMUR_LIVE_WRITE_PRIORITY_SECONDS=12 \
MURMUR_LIVE_WRITE_PRIORITY_SYNC_TIMEOUT_SECONDS=120 \
go test ./tests-live/write-priority/ -run TestLocalWritePriority -v -count=1
```

Sample report lines:

```text
dual-contention writer service: local=20.305s replication=2.284s share=0.101
converged: 9622 rows, 420 reads, identical SHA-256 6fd8cc0e…
```

## Notes

- Service time is scraped from each daemon's `/metrics`
  (`spedsql_sched_service_seconds_total{class="local|remote"}`) around
  the write phase. Values render as float64 text, so the parser uses
  `ParseFloat` — large or tiny values arrive in scientific notation,
  which `ParseUint` rejects (silent zero, then unsigned underflow).
- Ported from the GALVANIZE `tests-live/write-priority` runner, adapted
  to the daemon's single-statement HTTP service surface (one transaction
  per insert) and the `spedsql_sched_*` metric family.
