# Local-write priority live scenario

Two `spedsql` daemons take continuous writes on both nodes (two writers
per node, disjoint 16-byte row-ID ranges) while serving reads, under
full-mesh replication. The test gates on:

1. Local progress: at least 80 rows written and 4 reads served.
2. Priority: both scheduler classes contended, with the remote
   (replication) share of admitted writer service time at most 25%.
   The scheduler targets 90/10 local/remote with a 1s debt bound; the
   gate leaves headroom above the 10% target.
3. Convergence: identical row counts and matching SHA-256
   application-data hashes after writes stop.

## Run

```sh
SPEDSQL_LIVE_WRITE_PRIORITY_SECONDS=12 \
SPEDSQL_LIVE_WRITE_PRIORITY_SYNC_TIMEOUT_SECONDS=120 \
go test ./tests-live/write-priority/ -run TestLocalWritePriority -v -count=1
```

Sample report lines:

```text
contended writer service: local=17.998s replication=2.725s share=0.131
converged: 3415 rows, 384 reads, identical SHA-256 1a859088…
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
