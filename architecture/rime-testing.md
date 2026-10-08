# RIME production qualification

RIME is an in-memory engine. These tests exercise the real engine directly;
Murmur's disk recovery, encryption, and QUIC suites cover separate components.
See [the engine contract](rime.md) and [benchmark methods](rime-benchmarks.md).

## 1. Correctness gates

```sh
go test -race ./rime -timeout=5m
(cd tests-benchmark/rime-sqlite && RIME_BENCH_ROWS=1000 go test -race -run '^TestCompareRimeVsSQLiteConcurrent$' -count=1 -timeout=3m .)
go test -race ./rime -run '^TestQualification(CommitPublication|SnapshotRegistrationDuringGC|StaleWriter|CommitFailures|UniqueReuse|GeneratedIDs|Lifecycle|CacheBound)$' -cpu=1,2,8 -count=100 -timeout=10m
go vet ./rime ./internal/rimequal ./tests-live/rime
```

The isolated SQLite comparison module holds its separate CGO benchmark gate.
The concurrent comparison runs a small correctness smoke with race detection:
one and four writers, single-row and 256-row transactions, exact row-state
verification after every phase, and hotspot updates to detect lost increments.
See [benchmark methods](rime-benchmarks.md) for the full-size throughput run.
The opt-in `TestRimeWriteProfile` records isolated CPU/allocation and contention
captures; see [write profiling](rime-benchmarks.md#write-profiles-2026-10-07).
It skips unless `RIME_PROFILE_DIR` is set, and validates final state after
capturing each workload.

The qualification suite adds:

- A commit paused between publications: the completed commit counter must
  stay unchanged until all records and indexes publish.
- Concurrent two-table balance transfers, indexed reads, full scans, snapshot
  creation, delete/reinsert transactions, background GC, and async events.
  Subprocesses exercise shard counts of 1 and 64 and bound fatal failures.
- Historical snapshot/model comparisons through successive updates, deletion,
  reinsertion, GC, and snapshot release; reclaimed history and expired
  transactions must fail explicitly.
- Commit rejection by conflict, unique, CHECK, or BeforeCommit hook; aborted
  records, indexes, versions, commit IDs, and post-commit callbacks stay absent.
  Unique-value swaps and reuse of intermediate values check final-state claims.
- Model comparisons for hash, compound, ordered and prefix index paths,
  cached plans, alternating compiled parameters, pagination with a secondary
  sort key, joins, grouped keys containing separators, and historical MIN/MAX.
- Deterministic seeded differentials (`TestModelEquivalence`,
  `TestCompiledQueryEquivalence`, `TestBulkOpEquivalence`): a randomized
  two-table workload checked against a reference model inside every
  transaction and after commit, rollback, and GC. Point/indexed/full reads,
  exact ordering with pagination, joins, aggregates, group-by, hook streams,
  bulk writes, and cancellation must match the model exactly.
- Ordered-index AVL balance, key-directory and duplicate-bucket consistency,
  randomized ranges/deletions against an independent model, and comparator
  counts at 8,192 rows to detect quadratic mutation regressions.
- Deep-copy isolation for slices, maps and nested pointers, including caller
  mutation and rollback; transaction ownership, closure, and read-only writes.
- Cancellation before execution, during scanning and projection, and before
  commit; exact scan/result/mutation boundaries and atomic failed bulk writes.
- Bounded event queues with a blocked subscriber, both backpressure policies,
  and commits racing shutdown. Shutdown waits for callbacks to return; a
  callback that never returns prevents graceful draining.
- A separate memory worker with fixed live cardinality, changing prefix paths
  and cache fingerprints, repeated reclamation, final deletion, and shutdown.
  After warmup, retained heap must stay within baseline + 4 MiB + 25%; version
  and index counts must remain bounded and background workers must terminate.
  This envelope detects retention trends, not a universal memory capacity limit.

Reference-model fuzzing runs without an SQL engine or external dependencies:

```sh
go test ./rime -run '^$' -fuzz '^FuzzQueryModel$' -fuzztime=10m -parallel=2 -timeout=15m
go test ./rime -run '^$' -fuzz '^FuzzTransactionModel$' -fuzztime=10m -parallel=2 -timeout=15m
go test ./rime -run '^$' -fuzz '^FuzzEquivalence$' -fuzztime=10m -parallel=2 -timeout=15m
```

The transaction model covers staged point reads, queries with pending-write overlays,
rollback, multiple mutations per key, missing keys, concurrent retained
snapshots, and GC. The equivalence model replays the deterministic seeded
differentials (reads, joins, aggregates, ordering, hooks, bulk writes) with a
fuzzer-chosen workload seed. Save any failing Go fuzz corpus in `rime/testdata/fuzz/`.

## 2. Standalone live workload

```sh
go run -race ./tests-live/rime -duration=30m -rows=1000 -shards=64 -readers=4 -writers=4 -seed=127 > rime-live.jsonl
# Equivalent scenario entrypoint:
MURMUR_RACE=1 RIME_DURATION=30m bash tests-live/run.sh rime
```

This starts one standalone process; it does not launch Murmur's SQLite
fixture or require a network, disk database, certificates, or CGO.
`RIME_ROWS`, `RIME_SHARDS`, `RIME_READERS`, `RIME_WRITERS`, and `RIME_SEED`
configure the scenario runner. Seeding runs in batches before the duration timer and background GC start.
The command has an additional ten-minute allowance for seeding and cleanup.

Readers verify that paired balances sum to 100 and generations match at
one pinned snapshot. Writers contend on a shared hot set plus uniformly
chosen keys, and periodically delete/reinsert both sides atomically.
Readers also verify index predicates and compare complete table scans with
paired records while GC runs. Unexpected errors, partial commits, lost
snapshots, missing rows, and cleanup residue exit with a failure.
`ErrConflict` is expected and counted. SIGINT/SIGTERM stop the workload and
perform the same final cleanup checks.

JSON lines report successful read/write operations, cumulative throughput,
conflicts, delivered events, rolling mixed-operation p50/p95/p99 latency,
heap bytes, Linux RSS (omitted elsewhere), goroutines, cumulative Go GC
count/pause time, versions, index entries, and active transactions. Latencies
cover completed operations including correctness checks and use a bounded
8192-sample rolling window; they are not isolated engine call timings.
Final statistics follow worker shutdown, event draining, engine GC, and Go GC.

Each run records its seed. Failure messages identify worker/key/iteration;
the final failure sample includes recent write attempts. Seeds reproduce
operation choices, not the operating system's exact goroutine schedule.

## 3. Scheduling and release acceptance

[The RIME workflow](../.github/workflows/rime-qualification.yml) runs the
race suite and 100 repetitions of deterministic concurrency regressions on
relevant PRs/pushes. Nightly and manual runs fuzz each model for ten minutes
and run a race-instrumented thirty-minute workload. JSON reports, environment
information, and generated failure corpora are uploaded even after failure.
The existing root CI still runs repository-wide tests.

Before release, run on fixed hardware with enough memory for the dataset:

```sh
# Twenty-four-hour endurance run, then larger-cardinality qualification.
go run ./tests-live/rime -duration=24h -rows=1000 -seed=127 > rime-24h.jsonl
go run ./tests-live/rime -duration=30m -rows=100000 -seed=127 > rime-100k.jsonl
go run ./tests-live/rime -duration=30m -rows=1000000 -seed=127 > rime-1m.jsonl
go test ./rime -run '^$' -bench '^BenchmarkQualification$' -benchmem -benchtime=1s -count=5
```

Repeat measurements under comparable CPU/memory conditions. Investigate
throughput regressions above 10% or p99 regressions above 20% across repeated
runs against the same workload baseline. These are investigation thresholds,
not shared-runner CI timing gates. Check fixed-cardinality heap/RSS and
versions for sustained growth, and verify zero active transactions and one
version per live row after cleanup. No 24-hour release run is implied by
passing the short subprocess tests or by adding the workflow.

## 4. Maintained view qualification

```sh
go test -race ./rime -run 'TestView|TestComputedView|TestLiveMaintainedViews' -count=1
go run -race ./tests-live/rime -views -duration=30m -rows=1000 -shards=4 -readers=4 -writers=4 -seed=127 > rime-views-live.jsonl
```

View tests compare filters, ordering/pagination, joins, groups, extrema, point
reads, streaming iteration and projections with source-query results. They
exercise repeated-key and cross-table writes, intervening commits, publication
barriers, failures/panics, dependency violations, read-only callbacks, limits,
cancellation, registration races, refresh recovery, close and reclamation.
`TestLiveMaintainedViews` runs a short real-engine concurrent workload by default.

The standalone `-views` flag adds an incremental filter and computed join to the
existing workload. Readers pin history before taking each view snapshot, then
compare against a fresh query at its reported commit ID. Writers, delete/reinsert
transactions and GC keep running during these comparisons. JSON `view_checks`
counts successful comparisons; zero view checks fail the view-enabled run.
General heap/RSS, latency, progress and final cleanup checks remain enabled.
Longer qualification durations are still required for release acceptance.
