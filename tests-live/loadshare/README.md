# loadshare (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/loadshare/`
(`loadshare_test.go`, `TestWriterSharesUnderLoad`).

## What it proves

Two nodes form an encrypted mesh. Node1 absorbs 240 local writes across
four concurrent writers while node2 replicates 20 rows. The test requires
full convergence (no remote starvation on the saturated node), acquisitions
in both scheduler classes, and service debt within the 1s bound, and logs
the local-commit latency distribution (p50/p95/max).

## Env knobs

None. Deadlines and bounds are the Go constants: 60s convergence deadline
(50ms poll), 10ms pacing between peer writes, 1s service-debt bound, 30s
absurd-latency tripwire.

## Pass criteria

1. Both nodes converge on all 260 rows (node1 applies the 20 remote rows
   despite local saturation; node2 absorbs the 240 local rows).
2. On node1, local-class scheduler acquisitions >= 240 and remote-class
   acquisitions > 0 (the remote class is never starved).
3. Node1 service debt <= 1s.
4. Max local-commit latency <= 30s; p50/p95/max logged.

## Deviations from the Go original

- Plain SQL (`records` table via `Database::transaction`/`read` + `flush`)
  replaces the managed typed-RIME HTTP API; same row counts and key shapes
  (`local-<w>-<i>`, `remote-<i>`).
- Scheduler assertions read the in-process
  `Database::scheduler().snapshot()` (`local`/`remote` acquisitions,
  `debt_nanos`) instead of scraping
  `spedsql_sched_acquisitions_total{class}` and
  `spedsql_sched_debt_seconds` from Prometheus `/metrics`.
- The Go suite waits on node1's count only; the port waits for both nodes
  to hold 260 rows (same full-convergence requirement, explicit on both
  sides). `SELECT COUNT(*)` replaces `TypedNames` length.
- In-process `Cluster` nodes instead of `testnode` daemons; forensics via
  `dump_forensics` on failure.
