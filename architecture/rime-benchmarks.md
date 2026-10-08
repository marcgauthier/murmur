# RIME Benchmarks

Measured numbers for the [`rime/`](../README.md) in-memory engine. No claims
are made beyond what is recorded here. Machine, method, and full output are
recorded so anyone can reproduce.

## Method

```bash
go test ./rime/ -run XXX -bench . -benchtime=1s -count=1
```

- Package: `github.com/marcgauthier/murmur/rime`, single process, no server.
- `benchDB` seeds tables with `SaveMany` before the timer starts; timed
  sections exclude seeding unless noted.
- Thread sweeps set `GOMAXPROCS` per sub-benchmark and use `RunParallel`.
- Point-lookup keys cycle uniformly (`fmt.Sprintf` included in the timed
  section, so pure engine cost is lower than shown).
- `BenchmarkQualification/rows=.../full-scan` materializes and visits every
  record at 1K, 100K and 1M rows, with seeding excluded. It validates row count
  and an order-independent ID/bucket checksum, and reports allocations,
  `rows/op` and `rows/s`. This new case has not been run or measured yet.

```sh
go test ./rime -run '^$' -bench '^BenchmarkQualification$/^rows=100000$/^full-scan$' -benchmem -benchtime=1s
```

## Environment

- CPU: TBD
- Go: TBD (`go version`)
- Date / revision: TBD

## Results

### Point lookups by table size (ns/op, single goroutine)

| rows | primary `Get` | indexed `Eq` + Limit(100) |
| --- | --- | --- |
| 1K | TBD | TBD |
| 10K | TBD | TBD |
| 100K | TBD | TBD |
| 1M | TBD | TBD |

### Point lookups by goroutine count (ns/op, 100K rows)

| GOMAXPROCS | primary `Get` | mixed read/write |
| --- | --- | --- |
| 1 | TBD | TBD |
| 2 | TBD | TBD |
| 4 | TBD | TBD |
| 8 | TBD | TBD |
| 16 | TBD | TBD |
| 32 | TBD | TBD |
| 64 | TBD | TBD |

### Operation benchmarks (100K rows unless noted, ns/op)

| benchmark | ns/op | notes |
| --- | --- | --- |
| TBD | TBD | TBD |

## Notes

- RIME performs no record serialization; reads return shared immutable
  pointers, so lookup cost is dominated by shard hashing, map access, and
  version-chain binary search.
- Indexed queries stop scanning unordered candidates once `Limit` is
  satisfied (without `ORDER BY`).
- The engine uses only the standard library. Optional SQLite comparisons are
  isolated in the nested `tests-benchmark/rime-sqlite` module and are not part
  of production or the root module; see the mutation measurements below for
  their methodology and limits.

## Qualification smoke measurements (2026-10-06)

These are development smoke measurements from a shared host, not the fixed
hardware release baseline. The earlier legacy benchmark matrix remains
unmeasured. Host: Intel Core i5-6500, four logical CPUs, Linux amd64,
Go 1.26.8; base revision `62a6902` plus uncommitted qualification changes.
Other workloads were active. A temporary Go build overlay removed an unused
`context` import from an independently added SQLite comparison test; its
source file was left unchanged.

```sh
go test ./rime -run '^$' -bench '^BenchmarkQualification$/^rows=1000$/' -benchmem -benchtime=300ms -count=3
```

The isolated qualification benchmark pre-boxes keys and excludes seeding and
formatting. The point benchmark reads 1K integer-key rows. The indexed query
matches one row via an integer bucket index and exercises cached execution.
Raw output (the actual command also supplied the temporary `-overlay` file):

```text
BenchmarkQualification/rows=1000/get-4        4828610     72.61 ns/op       0 B/op     0 allocs/op
BenchmarkQualification/rows=1000/get-4        4981338     66.89 ns/op       0 B/op     0 allocs/op
BenchmarkQualification/rows=1000/get-4        5440687     66.19 ns/op       0 B/op     0 allocs/op
BenchmarkQualification/rows=1000/indexed-4     152842      3792 ns/op     776 B/op    13 allocs/op
BenchmarkQualification/rows=1000/indexed-4      98452      3540 ns/op     776 B/op    13 allocs/op
BenchmarkQualification/rows=1000/indexed-4     102543      4117 ns/op     776 B/op    13 allocs/op
```

Standalone workload checks used seed 127 with four readers and four writers.
The 1K race-instrumented run lasted 30 seconds; the 100K ordinary run lasted
10 measured seconds and the 1M run lasted 30 measured seconds. All three
completed invariant and cleanup checks. Each size denotes rows **per table**
(two tables). Final observations:

| Rows per table | Successful reads | Successful writes | Conflicts | Final versions | Final index entries | Open transactions |
| --- | --- | --- | --- | --- | --- | --- |
| 1K (`-race`) | 456,902 | 15,868 | 476 | 2,000 | 6,000 | 0 |
| 100K | 5,568 | 95,503 | 2,560 | 200,000 | 600,000 | 0 |
| 1M | 1,363 | 1,795 | 21 | 2,000,000 | 6,000,000 | 0 |

These workload operation counts include index validation and repeated complete
snapshot scans; they are not primary-read throughput measurements and the
sizes/race modes are not comparable. At 1M rows, mixed-operation p99 exceeded
one second on this shared host: full scans and global GC publication pauses
must be measured against application latency requirements before release.
The shorter 1M attempt was rejected for insufficient reader progress, prompting
removal of unnecessary view-lock acquisition on pinned point reads and a
longer measured interval. The smoke runs are not 24-hour release evidence.

See [RIME qualification](rime-testing.md) for the nightly suite, JSON
measurement definitions, size sweeps, and release acceptance thresholds.

## Mutation index redesign measurements (2026-10-07)

Host: Intel Core i5-6500, four logical CPUs, Linux amd64, Go 1.26.8.
These are single development runs on a shared host, not a release baseline.
The same 100K-row workload was measured before and after replacing the sorted
ordered-index slice with an AVL tree, leaving unchanged indexes alone during
updates, removing per-row field maps, and avoiding index rescans in tombstone GC.

```sh
(cd tests-benchmark/rime-sqlite && go test -run '^TestCompareRimeVsSQLiteMemory$' -count=1 -v -timeout=5m .)
```

| RIME phase | Before | After | Throughput before / after |
| --- | --- | --- | --- |
| Insert 100K users | 6.867 s | 0.830 s | 14,562 / 120,494 rows/s |
| Insert 100K orders | 6.587 s | 0.593 s | 15,182 / 168,625 rows/s |
| Update 100K users | 87.239 s | 0.608 s | 1,146 / 164,420 rows/s |
| Delete 10K users | 6.327 s | 0.045 s | 1,580 / 220,564 rows/s |

The earlier sorted-slice removal used a linear search and shifted entries on
every mutation. A 10K-row CPU profile attributed about 58% of sampled CPU
cumulatively to ordered-index removal. The new model test verifies AVL balance,
key-directory consistency, duplicates, ranges, limits and extremes against an
independent model. A comparator-count test covers insertion/deletion at 8,192
rows with both 60 and 8,192 distinct values to catch quadratic regressions
without using timing assertions.

The original comparison gives RIME extra secondary indexes. Set
`RIME_BENCH_MATCH_INDEXES=1` to also create SQLite indexes on user name, email,
age and order amount, alongside its existing order-user index:

```sh
(cd tests-benchmark/rime-sqlite && RIME_BENCH_MATCH_INDEXES=1 go test -run '^TestCompareRimeVsSQLiteMemory$' -count=1 -v -timeout=5m .)
```

| Matched secondary indexes, 100K rows | SQLite rows/s | RIME rows/s |
| --- | --- | --- |
| Insert users | 215,265 | 116,137 |
| Insert orders | 258,586 | 163,658 |
| Update users age+1 | 324,291 | 158,245 |
| Point delete, 10K rows | 183,297 | 210,779 |

This removes the worst mutation scaling problem; it does **not** establish
faster inserts or updates than SQLite. Storage is sharded, but commits still
serialize database-wide. These bulk phases use one writer, so they do not
measure concurrent writer scaling. MVCC record versions, constraint checks,
transaction bookkeeping and event handling remain costs to profile.
The comparison also has different full-join work (SQL aggregates versus
materialized RIME pairs), and bulk deletion compares SQL whole-table deletion
with individual MVCC tombstones. Those phases are not equivalent performance
comparisons. SQLite runs in memory with synchronous writes disabled; these
numbers say nothing about encrypted disk or replication throughput.

## Concurrent writer comparison (2026-10-07)

The earlier bulk comparison is a single-writer workload. The concurrent
comparison measures one and four writer goroutines sharing **one database per
engine**, rather than independent per-worker databases:

```sh
(cd tests-benchmark/rime-sqlite && go test -run '^TestCompareRimeVsSQLiteConcurrent$' -count=1 -v -timeout=5m .)
```

- Default: 100K rows; override with `RIME_BENCH_ROWS`. Both engines have
  primary keys plus user name/email/age and order-user/amount secondary indexes.
- Each writer has a disjoint key partition for insert, update and delete.
  Both engines use the same transaction boundaries: one row or up to 256 rows
  per commit. Data generation, connection warmup, statement preparation and
  validation are outside the timers. Worker goroutines start behind a barrier.
- SQLite uses a named shared in-memory database, shared cache and up to one
  connection per writer. All connections are opened before timing and checked
  for the shared schema. `BEGIN IMMEDIATE` acquires its writer lock first;
  SQLite still serializes writers. Synchronous writes and foreign keys are
  disabled. RIME uses the default 64 shards and serialized commit publication.
- Each failed transaction rolls back and retries in full. Lock errors and
  RIME conflicts use the same 50-microsecond retry delay and a two-minute phase
  timeout. Successful committed rows determine throughput; retries are included
  in elapsed time and reported alongside commit counts and SQLite pool waits.
- A separate single-row phase distributes 100K increments over 64 hotspot
  keys. Exact final counts detect lost updates. Every phase validates every
  row's key, name, email and age against an independent expected result for
  both engines. Timed phases exclude explicit GC; RIME GC runs between phases.

This is an application-level Go API comparison. SQLite's `database/sql`
transaction and driver overhead, shared-cache locking and retry policy are part
of its measured cost; results do not isolate either engine's internal execution
cost or establish an optimal SQLite deployment configuration. The 256-row
case uses point statements for both engines; it differs from the earlier
whole-table SQL `UPDATE`. Compare one versus four writers within each engine
to assess writer scaling, separately from the ratio between engines.

Both the original and concurrent comparisons now include a separate full-table
scan that retrieves all user columns and visits every row. This is distinct
from COUNT/AVG aggregation. SQLite uses `SELECT id,name,email,age FROM users`
and consumes its cursor; RIME uses unfiltered `Where().Find(nil)` and traverses
the returned immutable pointers. Each traversal checks row count and a checksum
of IDs, ages and string lengths against the seeded data plus committed updates.
Preparation is outside timing; retrieval, consumption and checks are timed.
Rates count rows visited, rather than scans. `RIME_BENCH_SCAN_ITERS` controls
the repeat count (default 5). In the concurrent comparison this read phase
runs after disjoint updates and all writers have completed; it does not measure
scans overlapping active writers. The new full-table scan phase has not been
run, so it has no measured results in the tables below.

Measured on the same i5-6500 / Go 1.26.8 host as above, with 100K rows and
connections warmed before timing. These are one development run, not a release
baseline; the small race-instrumented comparison is a correctness check only.

| Transaction rows | Writers | Phase | SQLite rows/s | RIME rows/s |
| --- | --- | --- | --- | --- |
| 1 | 1 | Insert | 42,687 | 128,106 |
| 1 | 4 | Insert | 30,141 | 97,349 |
| 1 | 1 | Update disjoint keys | 52,261 | 178,735 |
| 1 | 4 | Update disjoint keys | 39,855 | 134,316 |
| 1 | 1 | Update 64 hotspot keys | 59,642 | 50,537 |
| 1 | 4 | Update 64 hotspot keys | 47,410 | 39,933 |
| 1 | 4 | Delete | 35,426 | 135,052 |
| 256 | 1 | Insert | 126,312 | 181,874 |
| 256 | 4 | Insert | 97,323 | 181,384 |
| 256 | 1 | Update disjoint keys | 197,714 | 231,937 |
| 256 | 4 | Update disjoint keys | 156,688 | 261,334 |
| 256 | 4 | Delete | 144,475 | 225,414 |

The four-writer single-row insert/update phases had zero RIME conflicts;
their slowdown is not attributable to transaction retries. The four-writer
hotspot phase had 1,600 RIME conflicts. SQLite retried 61,395 insert,
33,978 disjoint-update and 26,116 hotspot transactions, with zero connection
pool waits in each case. Its shared-cache writer contention is part of these
numbers. Exact row checks and matching commit counts passed throughout.
RIME outperforms this SQLite configuration on disjoint mutations, but does
not scale linearly with writers. The hotspot schedule also builds long version
chains without timed GC, exposing a separate MVCC cost. Neither a broad
SQLite speed claim nor parallel commit support follows from these results.

## Write profiles (2026-10-07)

The opt-in `TestRimeWriteProfile` isolates RIME from SQLite, data generation,
seeding and final assertions. It records CPU, allocation, mutex and blocking
profiles using standard-library instrumentation. CPU/allocation captures and
contention captures run in separate processes; sampling every mutex/block
event can perturb timings. Heap allocation reports subtract a profile taken
after seeding, rather than attributing seed allocations to the workload.
Profiling overhead remains in the captures, so these rates are diagnostic
measurements, not replacements for the uninstrumented comparisons above.

```sh
go test -c -o /tmp/rime-profile.test ./rime
RIME_PROFILE_DIR=/tmp/rime-pprof RIME_PROFILE_WRITERS=4 RIME_PROFILE_KIND=cpu /tmp/rime-profile.test -test.run '^TestRimeWriteProfile$' -test.v
RIME_PROFILE_DIR=/tmp/rime-pprof RIME_PROFILE_WRITERS=4 RIME_PROFILE_KIND=contention /tmp/rime-profile.test -test.run '^TestRimeWriteProfile$' -test.v
go tool pprof -top /tmp/rime-profile.test /tmp/rime-pprof/update-w4-b1-cpu-cpu.pprof
go tool pprof -top -sample_index=alloc_space -base /tmp/rime-pprof/update-w4-b1-cpu-alloc-base.pprof /tmp/rime-profile.test /tmp/rime-pprof/update-w4-b1-cpu-heap.pprof
go tool pprof -list 'Tx.*commit' /tmp/rime-profile.test /tmp/rime-pprof/update-w4-b1-contention-mutex.pprof
go tool pprof -top /tmp/rime-profile.test /tmp/rime-pprof/update-w4-b1-contention-block.pprof
```

Controls: `RIME_PROFILE_OP=insert|update|delete|hotspot` (default update),
`RIME_PROFILE_WRITERS` (default 4), `RIME_PROFILE_BATCH` (default 1),
`RIME_BENCH_ROWS` (default 100K, divisible by writer count), and
`RIME_PROFILE_ROUNDS` (default 5, used only for disjoint updates). Insert,
delete and hotspot each execute one row-count's worth of operations. Disjoint
update writers repeatedly cycle only their own key partitions; hotspot writers
share 64 keys. Explicit database GC is disabled during capture. Files include
JSON measurements and exact final record-state checks run after sampling.
With no profile directory set, this test skips during ordinary suites.

Measured on the same four-CPU i5-6500 / Go 1.26.8 host:

| CPU/allocation capture | Operations | Rows/s | Allocated bytes/row | Allocations/row |
| --- | --- | --- | --- | --- |
| Disjoint update, 1 writer, batch 1 | 500K over 100K keys | 184,990 | 2,578 | 36.01 |
| Disjoint update, 4 writers, batch 1 | 500K over 100K keys | 146,401 | 2,578 | 36.01 |
| Disjoint update, 4 writers, batch 256 | 500K over 100K keys | 265,868 | 1,420 | 23.31 |
| Hotspot update, 4 writers, batch 1 | 100K over 64 keys | 55,480 | 22,810 | 35.33 |
| Insert, 4 writers, batch 1 | 200K keys | 97,444 | 3,670 | 36.05 |
| Delete, 4 writers, batch 1 | 200K keys | 147,989 | 2,367 | 27.01 |

Findings and implementation priorities:

1. **Commit serialization is measured.** In the four-writer single-row update
   contention capture, 8.55 of 8.60 seconds of recorded mutex contention
   (99.4%) is attributed to `commitMu.Unlock` in `Tx.commit`. The separate
   blocking profile attributes 6.50 seconds to waiting for that lock. These
   are aggregate goroutine delays across a 3.36-second run, not percentages of
   wall-clock time, and must not be added together. WaitGroup blocking is the
   harness waiting for completion. Zero transaction conflicts occurred.
2. **Reduce transaction bookkeeping before a larger commit rewrite.** 500K
   updates allocate approximately 1.29 GB in 18 million allocations. In the
   seed-subtracted allocation profile, about 76% of flat allocation volume
   is in pending-write construction/buffering, commit maps, and unique-claim
   release/map construction. The four-writer CPU capture spends 26.8% of
   sampled CPU in background GC and 14.6% directly in runtime spin yielding.
   Small-transaction fast paths and avoiding unnecessary claim/map allocation
   should reduce GC and work performed under the commit lock. This is a
   proposal supported by profiles, not an implemented optimization.
3. **Fix history copying for hotspots.** `applyPublish` accounts for 88.4% of
   flat allocation volume in the hotspot profile; it allocates a new version
   slice and copies all retained versions on each update. Background GC takes
   51.3% of sampled CPU. A persistent/chunked history or safely coordinated
   incremental reclamation should be evaluated. This capture intentionally
   retains history without timed GC; production GC settings can change the
   result, but parallel commits alone cannot eliminate the copying cost.
4. **Then shorten or parallelize commit work.** The global lock is the observed
   contention point. Table-index locks currently operate inside that serialized
   path; these profiles do not establish that index lock partitioning is the
   first required change. Reprofile after reducing commit work before selecting
   a larger lock/index redesign. Preserve atomic snapshots, unique claims,
   cross-table publication and the existing qualification invariants.

On inserts, `onCommit` contributes 26.2% of flat allocation volume, mostly
hash-index maintenance; unique validation contributes another 14.3%. On
deletes, transaction construction and claims dominate allocations again.
All captured workloads passed final state validation. Shared-host samples are
too short to predict an exact speedup from any proposed change.

See the [RIME performance implementation plan](rime-performance-plan.md) for
the proposed sequence, engineering targets and correctness/live acceptance gates.

## Frozen acceptance baselines (2026-10-07)

Host: Intel Core i5-6500, four logical CPUs, Linux amd64, Go 1.26.8,
GOMAXPROCS=4. Harness: `rime/freeze_bench_test.go` (stdlib + rime only, no
CGO, no SQLite). Each cell seeds outside the timer (except insert), runs
writers behind a barrier with 50µs conflict backoff, and verifies final
state. B/row and allocs/row come from `runtime.MemStats` deltas across the
timed phase (includes per-commit `time.Now` sampling and, for the readers
cell, reader allocations); identical methodology before/after makes cells
comparable. Commit p50/p95/p99 are per-commit latencies over all commits.

Acceptance cells (median of five unprofiled runs):

```sh
GOMAXPROCS=4 go test ./rime -run XXX -bench 'BenchmarkFreeze/update/disjoint/matched/w[14]/b1/n100000$' -benchtime=1x -count=5
```

| Cell (update/disjoint/matched/b1/100K) | rows/s | B/row | allocs/row | commit p50 | p95 | p99 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 writer | 151,557 | 2,888 | 40.0 | 5.01µs | 13.68µs | 22.27µs |
| 4 writers | 119,836 | 2,888 | 40.0 | 7.82µs | 76.92µs | 463.69µs |

Four writers are slower than one on single-row transactions with zero
conflicts: pure commit-lock serialization, matching the write profiles.
Single-row updates cost ~2.9KB / 40 allocs per row; these are the baseline
for the 50% allocation-reduction target.

Single-run reference matrix (same host/method, one run per cell; reference
only, not acceptance):

```sh
GOMAXPROCS=4 go test ./rime/ -run XXX -bench 'BenchmarkFreeze' -benchtime=1x -count=1 -v
```

| Cell | rows/s | B/row | allocs/row | conflicts | commit p50/p95/p99 |
| --- | --- | --- | --- | --- | --- |
| update/disjoint/w1/b1/100K | 133,939 | 2,888 | 40.0 | 0 | 5.22/13.94/22.75µs |
| update/disjoint/w1/b16/100K | 191,650 | 1,502 | 27.9 | 0 | 72.95/140.73/177.61µs |
| update/disjoint/w1/b256/100K | 143,457 | 1,540 | 25.4 | 0 | 1.27/2.60/14.26ms |
| update/disjoint/w2/b1/100K | 77,455 | 2,888 | 40.0 | 0 | 5.86/19.60/92.84µs |
| update/disjoint/w2/b16/100K | 87,018 | 1,502 | 27.9 | 0 | 87.00/703.76µs/6.24ms |
| update/disjoint/w2/b256/100K | 179,227 | 1,540 | 25.4 | 0 | 2.35/5.14/14.75ms |
| update/disjoint/w4/b1/100K | 72,085 | 2,888 | 40.0 | 0 | 6.20/24.86/482.22µs |
| update/disjoint/w4/b16/100K | 153,768 | 1,502 | 27.9 | 0 | 92.26µs/1.62/3.01ms |
| update/disjoint/w4/b256/100K | 198,214 | 1,540 | 25.4 | 0 | 4.20/9.62/21.13ms |
| update/disjoint/w8/b1/100K | 107,384 | 2,888 | 40.0 | 0 | 6.09/26.14µs/1.37ms |
| update/disjoint/w8/b16/100K | 164,327 | 1,502 | 27.9 | 0 | 105.47µs/3.26/5.37ms |
| update/disjoint/w8/b256/100K | 195,437 | 1,536 | 25.4 | 0 | 9.08/14.07/43.16ms |
| update/hot1/w1/4Kops | 24,940 | 55,108 | 40.0 | 0 | 16.31/185.59/237.05µs |
| update/hot64/w1/4Kops | 199,003 | 3,688 | 39.0 | 0 | 4.37/8.26/18.44µs |
| update/uniform/w1/4Kops | 159,332 | 2,890 | 40.0 | 0 | 5.45/10.24/20.44µs |
| insert/disjoint/w1/b1/100K | 117,268 | 3,856 | 37.2 | 0 | 5.37/17.76/38.79µs |
| delete/disjoint/w1/b1/100K | 157,751 | 2,704 | 30.0 | 0 | 4.92/12.52/21.98µs |
| update/disjoint/noindex/w1 | 238,632 | 2,360 | 29.0 | 0 | 3.22/7.85/18.45µs |
| update/disjoint/full/w1 | 107,484 | 3,055 | 48.1 | 0 | 7.21/19.46/28.36µs |
| update/disjoint/w1/b1/1K | 126,899 | 2,892 | 39.7 | 0 | 4.98/21.06/35.76µs |
| update/disjoint/w1/b1/1M | 137,214 | 2,888 | 40.0 | 0 | 5.31/15.89/24.09µs |
| update/hot1/w4/4Kops | 37,001 | 55,145 | 40.3 | 106 | 11.33/104.22/385.49µs |
| update/hot64/w4/4Kops | 139,531 | 3,707 | 39.2 | 54 | 5.50/23.66/397.10µs |
| update/uniform/w4/4Kops | 119,234 | 2,894 | 40.0 | 1 | 7.20/220.46/429.41µs |
| insert/disjoint/w4/b1/100K | 101,360 | 3,856 | 37.2 | 0 | 7.20/180.34/530.45µs |
| delete/disjoint/w4/b1/100K | 125,701 | 2,704 | 30.0 | 0 | 6.38/110.09/454.34µs |
| update/disjoint/noindex/w4 | 214,420 | 2,360 | 29.0 | 0 | 5.34/20.04/413.54µs |
| update/disjoint/full/w4 | 83,251 | 3,055 | 48.1 | 0 | 8.66/307.08/511.04µs |
| update/disjoint/w4/b1/1K | 116,785 | 2,908 | 39.8 | 0 | 8.81/32.06/792.86µs |
| update/disjoint/w4/b1/1M | 110,242 | 2,888 | 40.0 | 0 | 8.16/178.42/465.15µs |
| update/w4/b1/100K/hooks | 117,173 | 2,896 | 41.0 | 0 | 6.22/161.29/269.36µs |
| update/w4/b1/100K/readers | 53,122 | 3,430 | 60.3 | 0 | 14.65/356.76µs/1.12ms |
| transfer/w1/10K | 128,963 | — | — | 0 | — |
| transfer/w4/10K | 98,955 | — | — | 0 | — |

Notes: single-run cells vary run to run on this shared host (compare the
w1/b1 and w4/b1 rows above against the 5-run medians); only the two
acceptance medians gate the throughput/allocation targets. The hot1 cell's
55KB/row confirms version-history copying dominates contended updates. The
readers cell includes background-reader allocations by construction.

## Stage 3.1 results (2026-10-07)

Same host, harness, and acceptance command as the frozen baselines above
(median of five runs, `GOMAXPROCS=4`). Changes: commit dispatch methods,
single-write fast path, pooled effects, lazy staged map, typed `equals`
fast paths, no per-row closures. All gates green (`go test ./rime/`,
`-race`, `go vet`).

| Cell (update/disjoint/matched/b1/100K) | rows/s | B/row | allocs/row | commit p50 | p95 | p99 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 writer | 373,986 | 536 | 12.0 | 2.17µs | 4.84µs | 14.24µs |
| 4 writers | 281,456 | 536 | 12.0 | 2.45µs | 18.23µs | 338.74µs |

Deltas vs the frozen acceptance medians:

- allocs/row 40.0 → 12.0 (**−70%**; target was −50%): exceeded.
- B/row 2,888 → 536 (**−81%**).
- 1-writer throughput 151,557 → 373,986 (**2.47×**).
- 4-writer throughput 119,836 → 281,456 (**2.35×**; the Stage 3
  provisional 2× four-writer target is already met, but four writers are
  still slower than one — commit serialization remains, for Stage 3.4+).
- Batch-16 cells (same runs): 16.81 allocs/row, down from 27.9 (−40%).

Raw per-run output: `/tmp/rime-stage31-accept.txt` (run on this host, not
committed). Provisional targets from the performance plan are validation
targets, not promised results; methodology is unchanged so cells are
directly comparable.

## Stage 3.2 results (2026-10-07)

Same host, harness, and methodology. Change: version chains are now a
persistent newest-first list of 32-version chunks (`rime/mvcc.go`); publish
copies at most 31 retained versions and shares older chunks, so per-write
work is constant regardless of history length. GC unlinks whole chunks and
copies only the boundary chunk. All gates green (`go test ./rime/`,
`-race`, `go vet`).

Hot-single-key cells (the Stage 3.2 target; median of three runs,
`update/hot1/matched/b1/100K`, 4K ops):

| Cell | rows/s | B/row | allocs/row | commit p50 | conflicts |
| --- | --- | --- | --- | --- | --- |
| 1 writer | 429,609 | 984 | 12.0 | 2.05µs | 0 |
| 4 writers | 314,697 | ~1,000 | 12.0 | 2.14µs | 36–66 |

Deltas vs the frozen single-run reference (24,940 / 37,001 rows/s,
~55.1KB/row, ~40 allocs/row):

- B/row 55,108 → 984 (**−98%**): full-history copying eliminated.
- 1-writer hot1 throughput 24,940 → 429,609 (**17.2×**).
- 4-writer hot1 throughput 37,001 → 314,697 (**8.5×**).

Disjoint acceptance cells show no significant change: a 10-round
interleaved A/B of the pre/post-3.2 trees measured means of 333,059 vs
328,515 rows/s (−1.4%, within host noise; per-round wins split 6–4). An
apparent −7% vs the Stage 3.1 medians was host drift between sessions, not
the change — session-separated medians on this shared desktop host are not
directly comparable at single-digit percentages. B/row on disjoint is 560
(+24 from chunk headers), allocs/row steady at 12.0.

## Stage 3.3 results (2026-10-07)

Same host, harness, and methodology. Profile-driven (`TestRimeWriteProfile`
alloc_space): `orderedIdx.add` was 9.1% of write-path allocation (one item
alloc plus directory delete+insert per changed ordered value). Changes:
`orderedIdx.replace` reuses the directory item across remove+add, and
unique/primary fields no longer maintain a redundant parallel hash bucket
(unique lookups resolve through `ix.unique` alone). All gates green
(`go test ./rime/`, `-race`, `go vet`).

Deterministic allocation deltas (malloc counts, no host drift):

- Disjoint update/b1: 560 → **512 B/row** (−9%), 12.0 → **11.0
  allocs/row** (−8%). `orderedIdx.add` is gone from the profile top.
- Hot1 update: 984 → **936 B/row**, 11.98 → **10.98 allocs/row**.
- Observed medians this session (disjoint/b1/100K): w1 410,951 rows/s, w4
  313,904 rows/s — reported, not claimed as a code effect, per the
  session-drift caveat above.

Intended contract change: `IndexEntries` no longer double-counts unique
values (one entry per unique value instead of two), and per-index stats now
report unique fields as Kind `unique` instead of `hash`. Memory footprint
drops by one hash bucket per unique value. The two qualification
assertions encoding the old double count were updated to the new counts
(768 → 512, keys×10 → keys×8); their intents (observations present,
constancy across churn, zero residue after delete) are unchanged and pass.

Remaining write-path allocs per profile: `Tx` struct 36%, version-chunk
publish 23%, `pendingWrite` 21%, reflect field boxing 7.5%, record clone
6%. Reflect boxing needs kind-specialized index storage to eliminate; that
rewrite is disproportionate to its 7.5% and is deferred to the backlog
(`rime/IMPROVEMENTS.md`). Transaction/pending pooling is the candidate for
the next stage.

## Stage 3.4 results (2026-10-07)

Same host, harness, and methodology. Two parts. All gates green
(`go test ./rime/`, `-race`, `go vet`).

**Commit-section shortening.** Analysis first: the single-write critical
section was already near-minimal in structure — validation, conflict
check, and record+index publication must stay atomic (moving index
maintenance out would break index/record consistency for concurrent
readers; moving the chain copy out saves 1–4% of hold time at the cost of
an extra allocation, so it was rejected with reasoning). The certain wins
were lock operations, not structure: hook dispatch now skips the table
`RLock` via append-only atomic flags when no hooks are registered (three
lock pairs per write), and single-write unique maintenance folds into
`onCommit`'s index critical section (`applyPendingSingle`, one `idx.mu`
acquisition instead of two) with byte-identical logic. Observed medians
(disjoint/b1/100K): w1 420,320 rows/s, p50 1.92µs; w4 322,233 rows/s —
within the host drift band, reported not claimed.

**Pending-write pooling.** The profile's next item (21% of write-path
allocation): `pendingWrite` structs now recycle through a `sync.Pool`.
Release clears every field (no record retention; the memory-worker leak
test guards this) and acquisition overwrites every field; release runs in
`finish`/`Close` before the slice is cleared, idempotent via nil-ing, so
commit-then-close releases exactly once. A white-box hygiene test
(`TestPendingWritePoolHygiene`, verified non-vacuous by breaking both
clear paths) pins the end-to-end invariant. `Tx` itself stays unpooled: it
is user-visible and stashable, so reuse would be observable.

Deterministic allocation deltas from pooling:

- Disjoint update/b1: 512 → **384 B/row** (−25%), 11.0 → **10.0
  allocs/row**.
- Observed medians this session: w1 422,869 rows/s, w4 328,036 rows/s.

Cumulative from the frozen baselines: 40.0 → 10.0 allocs/row (**−75%**),
2,888 → 384 B/row (**−87%**). Remaining per profile: `Tx` struct,
version-chunk publish, record clone, reflect boxing — all fundamental or
deferred items. Four writers remain slower than one; only parallel commits
(Stage 3.5) change that curve.

## Batch path and typed index keys (2026-10-07)

Same host and methodology. Between Stage 3.4 and the reads work, three
batch/commit items landed: write-path map pre-sizing plus lazy staging,
hash-bucket single/set promotion, and a shard-resolution cache. Then the
item `rime/IMPROVEMENTS.md` §2 had deferred as disproportionate was
revisited with fresh profiles and **implemented**: per-field kind-specialized
index storage (`map[string]…`, `map[int64]…`, `map[uint64]…`,
`map[float64]…`, `map[bool]…`, with `map[any]…` fallback), killing
per-value boxing allocations and interface-map CPU on both maintenance
and lookup. A bulk-delete gap triage and the write-side `uniClean` /
`checked` / `final` map kills plus `Tx.Grow` followed. Observed
write-side scorecard peaks this round: bulk insert users 1.18×,
batch update 1.01× vs SQLite `:memory:` (matched indexes); the closing
run below shows 1.14×/0.98× on the same code — session drift band,
reported not claimed. All gates green at every step.

## Stage 3.6 results: reads, scans, joins (2026-10-07)

Profile-driven, one lever at a time with interleaved A/B where wall time
was close. All gates green (`go test ./rime/` 43.4s, `-race` 193.9s,
`go vet`, plus targeted fuzz/equivalence tests).

**Unsafe offset getters.** Every field read on every read path
(predicates, aggregates, ordering, grouping, join keys, FK checks) went
through `reflect.ValueOf(rec).Elem().FieldByIndex(idx)` plus
`.Interface()` — ~1 alloc and ~100ns+ per access. Registration already
recorded top-level field offsets, so exact-type matches now compile to
one unsafe offset load (~2ns, zero alloc; stdlib `unsafe` only, no CGO,
no deps). Assignable-but-different types (interface fields requested
concrete) keep the reflective path with identical panic behavior.
Locked by `TestTypedGetterParity` (14 field kinds vs the reflective
getter), `TestTypedGetterZeroAlloc` (`AllocsPerRun` == 0), and
`TestTypedGetterPanics`.

**Direct SUM/AVG extraction (`numAt`).** `Agg` construction records the
field offset plus a width enum derived from kind+size (covers named
types and platform `int`/`uint`); per-row extraction is one direct load
instead of the `num`→`get` closure chain. `Agg.run` and grouped
aggregation inherit it. `TestNumAtWidths` covers all 10 widths, a named
type, and the closure fallback.

**Shared scan walk (`scanEach`).** The three scan branches moved
verbatim from `collect` into one helper shared by Find/Count/Aggregate
paths. Verified behavior-identical (same 98 allocs / 5.36MB on the
100K-row aggregate probe before and after).

**Streaming aggregates: built, measured, reverted.** A single-pass fold
cut per-query garbage 6.2× (5.36MB → 0.87MB) but ran ~1.5× SLOWER in
wall time (~15ms vs ~10ms, 5-round interleaved A/B, no GC pressure in
either arm). Profiles attribute it to serialized record-cache misses in
the interleaved fold vs overlapped misses (memory-level parallelism) in
the tight second pass over the dense result slice; de-virtualizing
extraction did not change the outcome. Reverted; the lesson is recorded
in `rime/IMPROVEMENTS.md` §1.1 so the deferred dense-scan work does not
repeat it. `TestAggregateFindEquivalence` (5 filters × 7 modifiers
against an independent manual oracle; count-only for nondeterministic
unordered Limit/Offset) stays as Aggregate regression coverage.

**Indexed joins.** Two algorithmic wins, both measured on
`BenchmarkJoin` (20K rows): a same-key match cache (matches depend only
on key+snapshot; repeats skip probe, locks, and allocs) and a direct
typed probe (`resolveEqOp` picks 1 of 10 typed shapes once per join;
per-row probes skip `any` boxing and the scratch map; exotic/named keys
decline to the exact previous path). Join outputs presized to the left
side in all four builders. 9.7ms → 5.3ms (**1.83×**). The per-row
`viewMu` RLock was proven load-bearing (publication holds it
exclusively; bump-before-index-write ordering makes optimistic
validation unsound; a writer-side seqlock was costed at ~5% and
rejected). Locked by `TestIndexedJoinRepeatKeys` (alternating keys,
pair-dependent filters, outer joins), `TestIndexedJoinProbeShapes`
(all 5 kinds × unique/hash × fan-out buckets × named-type fallback),
and `TestResolveEqOp` (selection table).

Scorecard deltas from the reads work: full-scan COUNT+AVG 0.22× →
**0.40×**, full InnerJoinOn 0.11× → **0.14×**, join point lookup 1.2× →
**1.27×** (re-verified post-refactor values; the pre-refactor run read
0.47×/0.14×/1.44× — same band). The remaining full-scan/full-join gaps are the scattered-layout
wall (per-row chain/record pointer chases vs SQLite's dense pages),
owned by the deferred dense-scan design in `rime/IMPROVEMENTS.md` §1 —
not by per-row instruction overhead, which is now at the software floor
(verified by the streaming reversion experiment).

## Closing SQLite scorecard (2026-10-07)

`TestCompareRimeVsSQLiteMemory`, rows=100000, GOMAXPROCS=4, matched
secondary indexes (`RIME_BENCH_MATCH_INDEXES=1`). Re-verified after the
BoundTable API refactor on the same host; write-side figures move ±5%
across sessions on this shared host (the pre-refactor run showed
1.14×/1.28×/0.98× on the three mutation phases).

| Phase | SQLite | RIME | Speedup |
|---|---|---|---|
| bulk insert users (100K rows) | 0.377s | 0.325s | +1.16× |
| bulk insert orders (100K rows) | 0.325s | 0.235s | +1.39× |
| update users age+1 (100K rows) | 0.246s | 0.227s | +1.08× |
| point select by PK (20K lookups) | 0.107s | 0.008s | +14.03× |
| full scan COUNT+AVG (5×) | 0.020s (245/s) | 0.051s (98/s) | 0.40× |
| full table scan (all columns) | 0.679s | 0.043s | +15.70× |
| join point lookup (10K lookups) | 0.054s | 0.043s | +1.27× |
| join full InnerJoinOn (3×) | 0.032s (94/s) | 0.227s (13/s) | 0.14× |
| point delete (10K rows) | 0.050s | 0.018s | +2.77× |
| bulk delete remaining tables | 0.006s | 0.461s | 0.01× |

RIME leads 7 phases outright and trails three. Of the three: full-scan
aggregate and full join are the documented layout-wall losses (§1 above);
**bulk delete is a methodology artifact, not an engine gap** — SQLite
executes two bare `DELETE FROM t` statements, which it folds into
O(1)-ish truncate work (~6ms, ~32ns/row), while RIME writes 190,000
snapshot-preserving MVCC tombstones plus per-row index removal. RIME's
delete rate there is 2.6µs/row — exactly its bulk-insert rate
(2.5µs/row) — and point deletes lead 2.82×. Per-row deletes are healthy;
only a truncate exists on one side. A generation-based mass-delete fast
path was considered and rejected: it would tax every read's visibility
check for a rare operation.

## Maintained view development sample (2026-10-07)

```sh
go test ./rime -run '^$' -bench '^BenchmarkView(Read|Maintenance)$' -benchmem -benchtime=300ms -count=1
```

Linux/amd64, Intel Core i5-6500 @ 3.20 GHz, Go 1.26.8, GOMAXPROCS=4.
This is a single short development sample taken while race qualification also
ran on the machine; it is not an isolated production baseline. Seeding is
excluded. Read cases return half of the source records through an ordered-field
predicate, without requested result ordering. Cached reads copy map membership
into a result slice; the source query uses RIME's existing planner/indexes.

| Source rows | Cached read ns/op | Fresh query ns/op | Cached allocs/op | Query allocs/op |
| --- | --- | --- | --- | --- |
| 100 | 2,269 | 8,396 | 1 | 10 |
| 10,000 | 171,918 | 714,243 | 1 | 19 |

The maintenance cases use 1,000 source records and a filter matching half the
records. Every timed operation upserts one existing record. The computed case
uses the same filter through a callback, showing full-query reevaluation cost;
it is not a measurement of incremental joins or aggregate maintenance.

| Maintenance | Write ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| No view | 3,958 | 766 | 8 |
| Incremental filter | 4,682 | 1,576 | 16 |
| Computed callback | 151,769 | 36,883 | 539 |

The [view qualification workload](rime-testing.md#4-maintained-view-qualification)
measures live heap/RSS and compares cached results against source queries while
writers and GC run. Cached reads remain O(result count); synchronous computed
views move source query execution cost onto relevant writes.
