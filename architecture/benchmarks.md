# Benchmarks

Search, write/replication, and startup performance scenarios.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [59. Search Benchmarks](#59-search-benchmarks)
- [60. Write/Replication Benchmarks](#60-writereplication-benchmarks)
- [61. Startup Benchmarks](#61-startup-benchmarks)
- [64. Origin signature measurements](#64-origin-signature-measurements)
- [65. Characterization matrix](#65-characterization-matrix)

---

## 59. Search Benchmarks

Build a repeatable benchmark suite.

Datasets:

```text
100K rows
1M rows
10M rows
```

Workloads:

```text
PK lookup
indexed equality
indexed range
ORDER BY + LIMIT
JOIN
GROUP BY
FTS term query
FTS prefix query
substring/trigram if enabled
mixed read/write
```

Compare:

```text
Murmur-SQL in-memory SQLite materialization
ordinary SQLite baseline
```

Measure:

```text
p50
p95
p99
throughput
CPU
peak RSS
startup/rebuild time
```

Do not assume "in RAM" automatically means every query is fast.

---

## 60. Write/Replication Benchmarks

Measure:

```text
single-cell updates/sec
multi-column updates/sec
10-row transactions/sec
1,000-row transactions/sec
10,000-row transactions
Pebble commit latency
QUIC batches/sec
replication MB/sec
remote apply mutations/sec
```

Scenarios:

```text
1 node
2 nodes LAN
5 nodes LAN
high latency
1% packet loss
node reconnect after large backlog
snapshot seed
```

Test with encryption enabled because it is part of the intended production configuration.

Measure each cipher with Zstd level 3 enabled and with compression disabled. Include encrypted-container read/write amplification, index memory, random-read latency, WAL sync throughput, compaction, checkpoint creation, and maintenance rewrite throughput. Compare compressible and incompressible datasets; these benchmarks are acceptance evidence, not a reason to silently change security or compression defaults.

---

## 61. Startup Benchmarks

The explicit [live reload benchmark](../tests-live/reload-benchmark/README.md)
populates ten realistic log tables using seeded gofakeit, targeting
10,000,000,000 SQLite page bytes including secondary indexes. It uses real
encrypted Pebble under `/media/marc/2TB/TEST` and separate processes for
population, production `DB.Open`, and direct `Engine.Rebuild` measurement.
Run `bash tests-live/run.sh reload-benchmark`; a 32 MiB development run uses
`MURMUR_RELOAD_TARGET_BYTES=33554432`. It is excluded from routine live suites.

Reports distinguish full open, registry/Pebble/engine open, direct rebuild,
first query, and validation. They include logical payload and SQLite/disk
sizes, throughput, and Linux peak RSS before validation. All table counts,
full typed content hashes, secondary indexes, and the absence of a SQLite
backing file are checked. FTS and replication traffic are excluded. OS cache
is uncontrolled; these are fresh-process measurements, not cold-disk results.
Completed datasets and measurement artifacts remain available for reuse.

The 2026-10-02 UTC baseline on Linux amd64 (Go 1.26.0, SQLite 3.50.4) loaded
1,371,480 rows across ten tables, initially occupying 10,000,498,688 SQLite
page bytes and 5,664,469,882 encrypted Pebble file bytes. Production open
took 362.369 seconds; direct rebuild took 153.671 seconds, including indexes.
Both paths passed all full content hashes, counts, and index checks. The
direct path ran second with different cache state, so the duration difference
does not isolate startup overhead. See the scenario's
[recorded baseline](../tests-live/reload-benchmark/README.md#recorded-10-gb-baseline)
for peak RSS, validation times, and retained artifact locations.

Measure separately:

```text
Pebble open
schema initialization
state scan
row assembly
SQLite materialization inserts
index creation
FTS build
time until first query
total time until replication ready
registry open and authenticated VFS index loading
```

If startup becomes the bottleneck, optimize rebuild before adding a more complicated persistence layer.

Potential later optimizations:

```text
parallel state decoding
table-level parallel rebuild
persistent disposable materialization
incremental checkpoint
encrypted Pebble SSTable ingestion
```

---

## 62. Running the Matrix

All benchmarks run with encryption enabled (explicit AES-256-GCM storage
key unless the benchmark varies the cipher). Datasets are reproducible:
`benchmark/populate` seeds its generator with 42, and single-node
benchmarks share one lazily built template store per size (copied per
benchmark, so setup cost is paid once per size per run).

All commands below need the build tags (`-tags "sqlite_preupdate_hook
sqlite_fts5"`, or `-tags modernc` for the pure-Go backend); the CGO
concurrent-reader commands already include their full tag sets.

```bash
# Fast pass: 10K datasets (short mode)
go test ./tests-benchmark/benchmark/ -bench . -short -benchtime 1s

# Standard pass: 10K + 100K datasets
go test ./tests-benchmark/benchmark/ -bench . -benchtime 1s

# Large datasets (slow one-time template build: minutes for 1M)
MURMUR_BENCH_ROWS=1000000 go test ./tests-benchmark/benchmark/ -bench . -benchtime 1s

# White-box snapshot-seed benchmark (root package)
go test . -bench 'BenchmarkSnapshotSeed' -benchtime 1x

# Cipher x compression matrix only
go test ./tests-benchmark/benchmark/ -bench 'BenchmarkCipherMatrix' -short

# Five-node / backlog / throughput replication scenarios only
go test ./tests-benchmark/benchmark/ -bench 'BenchmarkReplicationThroughput|BenchmarkFiveNodeSync|BenchmarkReconnectBacklog' -short

# Direct Go API, one encrypted database, one and four concurrent writers
MURMUR_LOCAL_WRITE_BENCH_SECONDS=10 go test ./tests-benchmark/benchmark/ -run '^TestLocalWriterThroughput$' -v -count=1 -timeout=90s

# Same local workload, async commits with a Pebble sync about once per second
MURMUR_LOCAL_WRITE_BENCH_SECONDS=10 go test ./tests-benchmark/benchmark/ -run '^TestLocalPeriodicSyncThroughput$' -v -count=1 -timeout=90s

# Local transaction-size matrix: 1/10/100/1000 rows, 1/4 writers, both durability modes
MURMUR_LOCAL_BATCH_BENCH_SECONDS=5 go test ./tests-benchmark/benchmark/ -run '^TestLocalTransactionBatchThroughput$' -v -count=1 -timeout=300s

# Durability matrix: sync group commit vs async (10s + 10MB), 1/4/8 writers (10s each)
MURMUR_GROUP_BENCH_SECONDS=10 go test -tags "sqlite_preupdate_hook sqlite_fts5" ./tests-benchmark/benchmark/ -run '^TestGroupCommitDurabilityMatrix$' -v -count=1 -timeout=600s

# Live multi-process cluster, one and four concurrent SQL writers (10s each)
MURMUR_LIVE_WRITER_BENCH_SECONDS=10 go test ./tests-benchmark/replication/ -run '^TestWriterThroughput$' -v -count=1 -timeout=90s

# Pebble folder-size matrix across compression modes and traffic shapes
go test ./tests-live/compression/ -run '^TestPebbleCompressionSizes$' -v -count=1 -timeout=15m

# Concurrent-reader query benchmarks (CGO tags required for default backend)
go test -tags 'sqlite_preupdate_hook sqlite_fts5' ./tests-benchmark/benchmark/ -bench 'BenchmarkConcurrent' -short -benchtime 1s

# Same suite, restricted reader levels and 100K datasets
MURMUR_BENCH_READERS=1,8 go test -tags 'sqlite_preupdate_hook sqlite_fts5' ./tests-benchmark/benchmark/ -bench 'BenchmarkConcurrentMixedSQLite' -benchtime 1s

# Fixed-window sustained multi-reader throughput (10s per reader level, 10K rows)
go test -tags 'sqlite_preupdate_hook sqlite_fts5' ./tests-benchmark/benchmark/ -run '^TestSQLiteConcurrentReaderThroughput$' -v -count=1 -timeout=300s
```

The direct local writer benchmark opens one encrypted database with no peers
and sends individual SQL `INSERT` statements from one or four goroutines
through the Go API. It measures acknowledged inserts per wall-clock second,
then closes and reopens the database and checks the durable row count. It has
no daemon, HTTP, or QUIC traffic. Each writer-count run uses a fresh store.

On 2026-09-28, a 10-second run on the four-core Intel i5-6500 measured
3,205 inserts (320.4/sec) with one writer and 3,276 inserts (327.3/sec)
with four writers. Both reopened row-count checks passed.

The default synchronous durability mode waits for a Pebble WAL sync on each
single-row transaction, and local writes pass through one serialized write
coordinator. In-memory SQLite avoids a second durable SQL write but does not
remove the Pebble sync cost. Batch transactions amortize that cost across rows.

The periodic-sync variant uses the same one-row transactions, checks that
scheduled syncs occurred, and verifies all acknowledged rows after a graceful
close and reopen. Its higher rate trades away per-write crash durability.

On 2026-09-28, a 10-second run on the same four-core Intel i5-6500 measured
52,454 inserts (5,245.3/sec) with one writer and 51,908 inserts (5,186.6/sec)
with four writers. The runs recorded 9 and 10 scheduled syncs respectively,
and both reopened row-count checks passed. The result measures one WAL fsync
about every second; individual commits still append to the WAL.

The transaction-size matrix uses separate `INSERT` statements within one
`BeginTx`/`Commit` for each batch. Each SQL transaction produces one atomic
Pebble mutation batch. It reports completed transactions/sec and inserted
rows/sec for 1, 10, 100, and 1,000 rows per transaction, with one and four
application writers under both durability modes. The timed window includes
completion of transactions started before its deadline; setup and reopen are
excluded. Each case opens a fresh encrypted single-node store and verifies
the acknowledged row count after graceful close and reopen.

Results from a five-second-per-case run on 2026-09-28 (four-core Intel i5-6500;
rates rounded to whole operations/sec):

| Rows/transaction | Synchronous, 1 writer tx/s | Synchronous, 1 writer rows/s | Synchronous, 4 writers tx/s | Synchronous, 4 writers rows/s |
|---:|---:|---:|---:|---:|
| 1 | 340 | 340 | 329 | 329 |
| 10 | 271 | 2,705 | 269 | 2,689 |
| 100 | 107 | 10,673 | 106 | 10,563 |
| 1,000 | 14 | 13,557 | 11 | 10,888 |

| Rows/transaction | One-second sync, 1 writer tx/s | One-second sync, 1 writer rows/s | One-second sync, 4 writers tx/s | One-second sync, 4 writers rows/s |
|---:|---:|---:|---:|---:|
| 1 | 3,950 | 3,950 | 4,045 | 4,045 |
| 10 | 783 | 7,830 | 709 | 7,085 |
| 100 | 110 | 10,976 | 131 | 13,079 |
| 1,000 | 14 | 13,816 | 13 | 12,759 |

All 16 cases passed the durable row-count check. Four writers share one SQL
write coordinator, so they add queueing rather than parallel local SQL work.
At 100–1,000 inserts per transaction, SQL insert/change-capture work dominates
the cost of the transaction's Pebble sync. These are inserts into a growing
table; the earlier `BenchmarkTxn1000Rows` updates existing rows in a template
with a different schema and is a separate workload.

The synchronous columns above predate synchronous group commit (2026-10-04),
which lets concurrent writers share one fsync per group. The durability
matrix (`TestGroupCommitDurabilityMatrix`, same Intel i5-6500 class,
ten-second cases) measured, in acknowledged single-row sync inserts/sec:
330 at 1 writer (mean group 1.0), 1,183 at 4 writers (mean group 3.8), and
1,833 at 8 writers (mean group 5.4) — about 3.6x and 5.6x over the
single-writer rate, which is unchanged (no batching partner). The same
matrix measured async mode (ten-second interval plus ten-megabyte size
trigger) at roughly 4,500–5,500 tx/s regardless of writer count, identical
with group settings present or absent (group commit is inactive in async
mode). Every case passed the reopen row-count check.

The live writer benchmark starts a fresh encrypted single-node process per
writer count and measures acknowledged single-row `INSERT` statements per
wall-clock second. It checks the final SQL row count against the number of
acknowledged inserts. It is separate from the in-process write microbenchmarks and QUIC replication
measurements. See [the live benchmark scenario](../tests-benchmark/replication/README.md).

The 2026-09-28 10-second run on the four-core Intel i5-6500 measured
294.3 inserts/sec with one writer and 328.2 inserts/sec with four writers;
both count checks passed.

The concurrent-reader benchmarks measure query speed with 1-32 simultaneous
readers against the in-memory SQLite materialization. Each standalone query
runs through the pooled read connections and a prepared-statement cache. Each case
reopens the shared dataset template, starts N reader goroutines
behind a gate, and reports per-query p50/p95/p99 plus wall-clock throughput
(summing per-op durations would overcount under concurrency). The four
workloads are point lookups, single-column indexed equality probes,
100-row-capped indexed ranges, and an alternating point/range mix. Reader
levels default to 1, 2, 4, 8, 16, 32 and accept a comma-separated
`MURMUR_BENCH_READERS` override; dataset sizes follow the standard
`MURMUR_BENCH_ROWS` / `-short` selection. The benchmarks run under
both the default mattn driver and the optional `modernc` driver. The
fixed-window test runs the mixed workload for
`MURMUR_READ_BENCH_SECONDS` (default 10) per reader level on a 10K-row
store and checks that every point lookup returns exactly one row.

A 2026-09-28 short run on the four-core Intel i5-6500 (`-benchtime 1s`,
10K rows) measured this wall-clock throughput in queries/sec:

| Readers | Point lookup | Indexed equality | Indexed range | Mixed |
|---:|---:|---:|---:|---:|
| 1 | 96,810 | 135,325 | 10,157 | 18,147 |
| 2 | 142,473 | 131,970 | 19,004 | 19,712 |
| 4 | 100,426 | 363,348 | 31,284 | 51,329 |
| 8 | 318,971 | 359,241 | 27,260 | 35,899 |
| 16 | 330,045 | 376,874 | 32,063 | 38,480 |
| 32 | 244,014 | 352,804 | 21,928 | 57,064 |

Throughput scales to about the core count and then plateaus, while tail
latency grows past 4 readers from oversubscription and pool queueing (the
pool has a 32-connection cap, including the reserved write connection). The box
was under background desktop load during this run, so single cells vary
between runs: the 4-reader point-lookup cell above dipped to 100K while a
focused rerun measured 289K there. The same-day 10-second fixed-window run
measured 17,882 mixed queries/sec with one reader, 61,780 with four,
41,061 with eight, and 55,661 with 32.

The recorded backend comparison results below were collected before the
backend refactors. They are historical data, not measurements of the latest
revision. The current code is in-memory only; rerun the
matrix against an identified revision before making performance claims.

Benchmark names map to matrix sections: `BenchmarkPKLookup`,
`BenchmarkIndexedEquality`, `BenchmarkIndexedRange`, `BenchmarkOrderLimit`,
`BenchmarkJoin`, `BenchmarkGroupBy`, `BenchmarkFTSTerm`,
`BenchmarkFTSPrefix`, `BenchmarkConcurrentPKLookupSQLite`,
`BenchmarkConcurrentIndexedEqualitySQLite`,
`BenchmarkConcurrentRangeLookupSQLite`, `BenchmarkConcurrentMixedSQLite`
(§59 search); `BenchmarkSingleCellUpdate`,
`BenchmarkMultiColumnUpdate`, `BenchmarkTxn10Rows`,
`BenchmarkTxn1000Rows`, `BenchmarkTxn10000Rows`, `BenchmarkMixedReadWrite`,
`BenchmarkPebbleCommitLatency`, `BenchmarkRemoteApplyRate`,
`BenchmarkReplicationThroughput` (batches/sec, mutations/sec, payload and
wire MB/sec via `Status().Replication` counters), `BenchmarkFiveNodeSync`,
`BenchmarkReconnectBacklog`, `BenchmarkSnapshotSeed` (root package),
`BenchmarkCipherMatrix` (cipher x block-compression x compressibility,
with on-disk bytes), `BenchmarkCheckpoint`,
`BenchmarkMaintenanceRewrite` (§60 write/replication);
`BenchmarkStartupComponents` (registry-open, store-open, full-open,
first query; compare full against store across runs to size the
rebuild), `BenchmarkReplicationReady`,
`BenchmarkRebuild` (§61 startup);
`BenchmarkQueryMaterialization` and `BenchmarkSQLiteBaseline` (stock SQLite
reference) cover query materialization comparisons.

Coverage notes and manual procedures:

- Impaired networks (high latency, 1% loss) need OS-level impairment;
  rerun any replication benchmark under e.g.
  `tc qdisc add dev lo root netem delay 50ms loss 1%` (Linux, root) and
  remove it afterwards with `tc qdisc del dev lo root`.
- 10M datasets are supported (`MURMUR_BENCH_ROWS=10000000`) but the
  template build is populate-bound (SQL transactions); prefer a restored
  snapshot fixture when 10M runs become routine.
- Engine-internal rebuild phases (row assembly vs inserts vs index/FTS
  build) have no phase hooks; the suite reports rebuild as one derived
  component. Compaction is Pebble-automatic with no public trigger, so it
  is not benchmarked separately. Substring/trigram search is not enabled.
- Query materialization comparisons use the in-memory query view; historical
  results below require a fresh run before making performance claims.

## 63. Recorded Results (10K, 2026-09-27)

Machine: linux/amd64, Intel i5-6500 @ 3.20GHz (4 cores).
Command: `go test ./tests-benchmark/benchmark/ -bench . -short -benchtime 1s`
(plus `go test . -bench 'BenchmarkSnapshotSeed' -benchtime 1x`).
Encryption enabled throughout (AES-256-GCM unless varied).

Large transaction chunk encoding (`go test ./codec -run '^$' -bench
'^BenchmarkTransactionChunkEncoding$' -benchtime=3x -benchmem`) on the same
machine with a 16 MiB transaction: buffered encoding used 35,749,882 B/op and
273 allocs/op; streaming visit encoding used 16,859,506 B/op and 7 allocs/op.
The three-iteration timing was 68.7 ms/op buffered versus 52.3 ms/op visited;
this microbenchmark measures codec memory and CPU, not end-to-end network
throughput.

Encrypted large-snapshot chunk merge (`go test ./state -run '^$' -bench
'^BenchmarkSnapshotChunkMergeEncrypted$' -benchtime=3x -benchmem`) with 12,000
900-byte cells: Pebble batch merge measured 165.1 ms/op and 260,919,005 B/op;
encrypted-VFS SSTable ingestion measured 100.6 ms/op and 171,546,490 B/op.
This is an in-process storage microbenchmark on one machine, not a full
replication/convergence result.

Search, 10K rows (ops/sec / p50 / p95):

| benchmark | ops/sec | p50 | p95 |
|---|---|---|---|
| PKLookup | 122,520 | 5.9µs | 19µs |
| IndexedEquality | 212,142 | 3.6µs | 7.1µs |
| IndexedRange (100-row cap) | 6,661 | 109µs | 217µs |
| OrderLimit | 35,303 | 22µs | 47µs |
| Join | 2,910 | 315µs | 482µs |
| GroupBy (full scan) | 281 | 3.5ms | 4.1ms |
| FTSTerm | 42,751 | 20µs | 40µs |
| FTSPrefix | 33,173 | 25µs | 52µs |

Backend comparison, mixed point+range workload: memory 15,486 ops/sec,
stock SQLite baseline 13,527 ops/sec.

Writes, 10K rows:

| benchmark | rate | p50/op |
|---|---|---|
| SingleCellUpdate | 335 ops/sec | 2.86ms |
| MultiColumnUpdate | 262 ops/sec | 2.87ms |
| Txn10Rows | 2,752 rows/sec | 3.36ms/txn |
| Txn1000Rows | 50,572 rows/sec | 19ms/txn |
| Txn10000Rows | 56,731 rows/sec | 171ms/txn |
| MixedReadWrite (9:1) | 3,270 ops/sec | 7.1µs read |
| PebbleCommitLatency | 700 commits/sec | 1.37ms |
| RemoteApplyRate | 6,673 mutations/sec | 1.45ms/batch |

Replication (loopback): single-row throughput 179 batches/sec,
716 mutations/sec, 0.044 payload MB/sec; five-node star 1,056 rows/sec
to last convergence (9.5s for 10K); reconnect backlog 2,276 rows/sec
(4.4s catch-up); snapshot seed 9,777 rows/sec (0.23MB transfer).

Cipher x compression matrix, 2K rows, single-cell updates: all ciphers
≈300 ops/sec (commit fsync dominates; cipher cost is not visible at
single-row granularity). Populate footprint: compressible text 0.83MB
under both zstd and none (WAL-dominated at this size); random values
1.31MB under zstd vs 1.94MB uncompressed.

Startup, 10K rows: registry open ≈0.2ms, store open ≈0.6-1.0s (cold),
full open ≈0.6-1.2s, first query ≈0.1ms, replication ready ≈2.0s
(includes dial plus handshake). Checkpoint 0.45s / 2.9MB. Full
maintenance rewrite 3.2s at 2.6MB/sec (10 files).

At 100K rows (same machine, `go test ./tests-benchmark/benchmark/ -bench .`): point
lookups hold steady (PK 121K ops/sec); full-scan GroupBy drops to
20 ops/sec; single-cell writes hold at 313 ops/sec; 10K-row
transactions drop to 11K rows/sec (index maintenance grows with data);
full open takes 4.0s (25K rows/sec); reconnect backlog catches up at
1,162 rows/sec. Order is unchanged: memory 12.8K,
SQLite baseline 9.9K ops/sec on the mixed workload.

---

Embedded startup progress uses the existing rebuild scan without a preliminary
count. A paired 10 GB SSD observation took 67.542 seconds with reporting disabled
and 70.322 seconds with reporting enabled. OS cache was uncontrolled; see the
[live benchmark results](../tests-live/reload-benchmark/README.md#startup-progress-without-a-counting-pass)
for artifacts and validation details.

## 64. Origin signature measurements

The 2026-10-04 uncommitted origin-signature implementation was measured on
Linux amd64, Intel i5-6500, Go 1.26.0 with:

```bash
go test -tags modernc -run '^$' -bench '^BenchmarkOrigin$' -benchmem -benchtime 300ms ./codec
```

| Mutation blob size | Sign (digest included) | Verify (digest included) | Forward serialization | Sign/verify allocation |
|---|---:|---:|---:|---:|
| 32 bytes | 36.0 µs | 77.1 µs | 1.03 µs | 480 B / 4 allocations |
| 64 KiB | 227 µs | 322 µs | 89.6 µs | 480 B / 4 allocations |
| 1 MiB | 3.32 ms | 3.78 ms | 866 µs | 480 B / 4 allocations |

Digest computation streams blob bytes without allocating a payload-sized copy.
Forwarding serializes the existing proof without re-signing; receive-side
verification remains required. These are microbenchmarks with concurrent host
activity, not end-to-end replication throughput guarantees. They do not include
fsync, merge, compression, network transport, or text-value allocation behavior.
The local artifact is `/tmp/murmur-origin-bench-final.log`; this is working-tree
evidence, not verification of a release commit.

### CRDT join cost

Run `go test -tags modernc -run '^$' -bench '^BenchmarkMergePolicy$' -benchtime=1s ./state`.
This kernel benchmark measures Pebble reads, causal join and projection at fixed
cardinality; it excludes origin-signature verification, SQL capture and fsync.
It compares LWW, PN_COUNTER, OR_SET and MAX/MIN at 1 record, and counter/set
histories at 100 and 1,000 records. Cardinality includes retained removals;
work scales with retained history, not only visible set membership.

A 100 ms development run on an Intel i5-6500 (modernc, Linux/amd64) measured
approximately 1.05 µs for LWW, 1.23–1.30 µs for extrema, 4.95 µs for one
counter component and 755 µs for 1,000 components. OR_SET measured 11.9 µs
at one addition and 2.95 ms at 1,000 additions. These are local kernel
measurements under concurrent validation load, not throughput or latency SLOs.
See [merge policies](merge-policies.md) for memory/storage growth and limits.

---

## 65. Characterization matrix

The perf matrix (`TestPerfMatrix` in `tests-benchmark/benchmark/`,
run with `bash tests-benchmark/run.sh perf-matrix`) is the repeatable
characterization harness: one command produces machine-stamped numbers
across dataset sizes, transaction batches, ciphers, mesh sizes,
network shapes, and reconnect backlogs, plus a JSON report
(`tests-benchmark/benchmark/perf-report-<ts>.json`, git-ignored) and a
Markdown table in the test log ready for publication below. Tiers via
`MURMUR_PERF_TIER`: `smoke` (10K rows, 2-node mesh; minutes),
`standard` (10K+100K rows, 1/2/5-node meshes, 3 ciphers; tens of
minutes), `full` (adds 1M rows, 10-node mesh, 50K backlog; ~1–2h).
`MURMUR_PERF_MESH_ROWS` overrides the mesh blast size (default 2000).

Method, shared by every cell:

- Encryption is always on (AES-256-GCM unless the cell varies the
  cipher); datasets are seeded (`populate` seed 42) and single-node
  cells share one lazily built template store per size.
- Every cell records wall time, throughput, p50/p95 where sampled,
  on-disk bytes, and peak RSS (process `VmHWM` in-process, max daemon
  `VmHWM` for live cells). Cells never constrain memory; RAM
  characterization is a footprint curve (RSS vs rows/nodes), and the
  runbook below covers enforced-limit runs.
- Live cells use real multi-process daemons via the shared
  `tests-live` harness with isolated dirs, certs, and ports; the
  verdict inside a cell is exact convergence (counts plus digests),
  so a published throughput number always implies a converged mesh.
- Impairment shapes only replication ports (prio qdisc 77: with
  netem on band 77:3, u32 filters on repl ports); API/metrics stay
  clean so visibility latency measures replication, not API
  slowness. Cells skip loudly without `CAP_NET_ADMIN`.
- Numbers are single-run, loopback, and box-specific (stamped with
  CPU/RAM/Go/tags); treat them as characterization, not SLOs.

Cell definitions:

| cell | dimensions | measures |
|---|---|---|
| `store_open` | rows 10K/100K/1M | template-copy + full production open wall, disk, RSS |
| `pk_lookup` | rows | 10K point lookups: qps, p50/p95 |
| `range_100` | rows | 1K capped indexed ranges: qps, p50/p95 |
| `single_update` | rows | 300 sync single-cell txns: tx/s, p50/p95 |
| `tx_batch` | rows × batch 1/10/100/1000 | tx/s and rows/s, sync durability |
| `cipher_bulk` | cipher × rows | fresh-store populate rows/s + disk + full-reopen wall |
| `mesh_blast` | nodes 1/2/5/10 | M-row parallel blast to convergence: rows/s; 100 visibility samples p50/p95 |
| `mesh_impair` | delay50ms / loss1pct | 500-row blast rows/s + 20 visibility samples p50/p95 |
| `reconnect` | backlog 1K/10K/50K | catch-up rows/s + whether the snapshot path fired |

### Recorded results (perf matrix, 2026-10-04)

Machine: linux/amd64, Intel i5-6500 @ 3.20GHz (4 cores), 31 GB RAM,
Go 1.26.8, default CGO tags, loopback, quiet box (desktop SSD at 96%
full — see the stall note). Encryption on throughout. `mesh_blast`
uses 2000-row blasts; visibility is 100 sequential samples (20 under
impairment). Tables show the full-tier run; the standard tier and
focused reruns reproduced every read/cipher/mesh cell within ~30%
unless noted. Open wall includes the template copy (per-file sync),
which itself caught the loaded-filesystem stalls: the 100K copy
measured 0.3s vs 4.9s across runs, so compare open walls accordingly.

Single-node store by rows (template contacts/orders schema with
name/score indexes plus FTS):

| rows | open wall | disk | RSS | pk qps (p50/p95) | range qps (p50/p95) | 1-row update tx/s (p50/p95) |
|---:|---|---|---|---|---|---|
| 10K | 0.6s | 4.9 MB | 183 MB | 118,537 (0.01/0.02 ms) | 9,915 (0.09/0.16 ms) | 34* (3.3/149 ms) |
| 100K | 9.1s | 52.7 MB | 471 MB | 121,807 (0.01/0.02 ms) | 7,645 (0.12/0.20 ms) | 281 (3.5/4.5 ms) |
| 1M | 29.6s | 566 MB | 1,785 MB | 112,359 (0.01/0.02 ms) | 7,751 (0.12/0.20 ms) | 32* (3.9/78 ms) |

Reads are flat from 10K to 1M (point ~115K qps, capped ranges ~8K
qps). Open+rebuild runs ~11–34K rows/s with ~590 encrypted bytes per
contact row (plus its orders/indexes/FTS) on disk; RSS grows
183 → 471 → 1,785 MB. Starred (*) throughputs hit the sync-commit
stalls below (p50 stays ~3–4 ms in every row).

Sync-commit stall note (read before quoting a write throughput):
every sync-write cell shows a stable p50 (~3–6 ms/commit) but
throughput varies up to 15x run to run from episodic multi-second
commit stalls — seen in all write cells across runs, including
in-process phases with zero daemons up, on a 96%-full desktop SSD
with concurrent desktop load. A spot fsync probe measured p50 2 ms /
max 6 ms, so the stalls are episodic system I/O, not Murmur's steady
state or a data-size cliff (the 100K store measured both 16/s and
274/s for the same workload minutes apart). Quote p50 for sync
commits; treat single-run sync throughput as a stall-affected sample.
The §62 historical sync numbers (~340/s single-row) sit at the top of
the observed range, i.e. stall-free runs.

Transaction batches on the 100K store (sync durability; full-tier run):

| batch | tx/s | rows/s | p50/txn |
|---:|---:|---:|---:|
| 1 | 274 | 274 | 3.5 ms |
| 10 | 131 | 1,310 | 7.0 ms |
| 100 | 28.8 | 2,884 | 31.6 ms |
| 1,000 | 2.7 | 2,713 | 336 ms |

Repeat runs measured b=1 at 16–18 tx/s under stalls (p50 6–60 ms);
batching past 100 rows/txn stops gaining rows/s on this schema.

Cipher bulk on the 100K store (fresh populate + full reopen):

| cipher | populate rows/s | disk | reopen |
|---|---|---|---|
| aes256gcm | 2,056–2,441 | 59 MB | 3.3–3.7s |
| chacha20 | 1,848–2,067 | 59 MB | 5.6–5.9s |
| aegis256 | 1,740–1,909 | 59 MB | 4.7–5.8s |

Ranges span the standard and full runs. Disk footprint is
cipher-independent; AES (hardware) runs at or above the software
ciphers within run variance. (At single-row granularity fsync hides
all cipher cost; see `BenchmarkCipherMatrix`.)

Live mesh blast (2000 rows, exact-convergence verdict):

| nodes | rows/s | vis p50 | vis p95 | max daemon RSS |
|---:|---:|---:|---:|---:|
| 1 | 199 | 5 ms | 6 ms | 38 MB |
| 2 | 199–223 | 704–905 ms | 933–1,074 ms | 40–41 MB |
| 5 | 128–183 | 998 ms | 1,006–1,110 ms | 45–46 MB |
| 10 | 90 | 1,046 ms | 2,116 ms | 52 MB |

Ranges span up to three runs. Blast throughput falls with mesh size
(fanout-limited gossip: 223 → 90 rows/s from 2 to 10 nodes) while
daemon RSS stays ~40–52 MB. Visibility latency is dominated by the
~1s remote-materialization timer (single-node local writes stay at
5 ms); the 10-node tail (p95 2.1s) is last-follower convergence.

Reconnect catch-up (2 nodes, log tail — no snapshot fired in any
run; default retention covers these backlogs):

| backlog | catch-up rows/s | max daemon RSS |
|---:|---:|---:|
| 1K | 536–901 | 38–39 MB |
| 10K | 417–1,725 | 62 MB |
| 50K | 262 | 109 MB |

Ranges span two runs (50K single sample); catch-up writes hit the
same sync-commit stalls. For snapshot-path catch-up numbers, force
expiry with tight retention as in `tests-live/snapshot-resync/`.

Impairment cells (`delay50ms`, `loss1pct`) skipped on this box: no
`CAP_NET_ADMIN`. They run in privileged CI / big-box sessions.

### Scaling runbook (10M/100M rows, 200 nodes, RAM limits, WAN)

Cells above the `full` tier do not run on a 31 GB dev box; this
runbook specifies how to produce them so numbers stay comparable when
a bigger machine is available.

- 10M/100M rows: do not extend `populate` (SQL-transaction-bound).
  Restore a snapshot fixture into a template dir once, then point new
  cells at it; keep the contacts/orders schema and seed. Budget
  roughly 1 KB of encrypted Pebble per contact row plus indexes (read
  the actual ratio off the `store_open` disk column first), and size
  tmpfs/disk plus RSS headroom from the footprint curve before
  launching. Publish the fixture's row count, content digest, and
  build command alongside the numbers.
- 200 nodes: the `abuse` live suite already proves 200-daemon
  correctness; for perf, run `mesh_blast` with 200 nodes and
  `MURMUR_PERF_MESH_ROWS` scaled down (blast time grows with
  fanout-limited gossip, not just rows). Needs tens of GB of RAM;
  record max daemon RSS and time-to-last-convergence, and expect the
  visibility tail to dominate the blast rate.
- RAM limits: wrap daemon invocations with
  `systemd-run --user --scope -p MemoryMax=<N>G` (or a cgroupfs
  equivalent) and rerun `store_open`/`mesh_blast`; report the
  smallest limit that still converges plus the OOM-kill boundary.
  In-process cells can use `RLIMIT_AS`, but it constrains the test
  binary as a whole — prefer daemon-scoped limits.
- WAN: the `mesh_impair` cells are the loopback proxy (50 ms / 1%).
  For true multi-host runs, place daemons on separate hosts with the
  harness port claims intact, shape the inter-host link with the same
  netem specs, and publish RTT/loss traces next to the numbers; clock
  sync (chrony/PTP) matters for visibility percentiles across hosts.
