# write-priority (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/write-priority/`
(`write_priority_test.go`, `TestLocalWritePriority`).

## What it proves

Two nodes take continuous writes on both nodes (dense writers with
disjoint 16-byte row-ID ranges) while serving reads, under full-mesh
replication. The test gates on local progress, the replication share of
dual-contention writer service, and convergence.

## Env knobs

- `MURMUR_LIVE_WRITE_PRIORITY_SECONDS` (default 12): write-phase duration.
- `MURMUR_LIVE_WRITE_PRIORITY_SYNC_TIMEOUT_SECONDS` (default 120):
  convergence deadline after writes stop.

## Pass criteria

1. Local progress: at least 80 rows written and 4 reads served.
2. Priority: the replication share of dual-contention writer service
   (granted while the other interactive class had waiters) is at most
   25%, with at least 1s of dual service measured. The scheduler
   targets 90/10 local/remote with a 1s debt bound; the gate leaves
   headroom above the 10% target.
3. Convergence: identical row counts and identical row sets on both
   nodes after writes stop.

## Deviations from the Go original

- Plain SQL (`records(id TEXT PRIMARY KEY, name TEXT)` via
  `Database::transaction`/`read` + `flush`) replaces the typed-contention
  HTTP API; one transaction per insert, as in Go. Same key shapes
  (16-byte node/worker/seq row IDs, `v-n<node>-w<worker>-<seq>` names).
- Scheduler assertions read the in-process
  `Database::scheduler().snapshot()` dual-service counters
  (`local`/`remote.dual_service_nanos`) instead of scraping Prometheus
  `/metrics`. The port follows the Go README's dual-*service* gate
  (share <= 25%, >= 1s measured): the Rust product exposes
  `dual_service_nanos` but no dual-grants counter, so the
  `write_priority_test.go` dual-grants variant (share of
  `spedsql_sched_dual_grants_total`, >= 100 grants) has no in-process
  equivalent.
- 64 OS-thread writers per node instead of 256 goroutines per node;
  still enough looping writers to keep both scheduler classes
  backlogged, where the 90/10 policy binds.
- Convergence compares the full sorted row sets for equality (strictly
  stronger than the Go SHA-256-over-rows hash) and requires both nodes
  to hold the full set; no new hash dependency.
- In-process `Cluster` nodes instead of `testnode` daemons; forensics via
  `dump_forensics` on failure.
