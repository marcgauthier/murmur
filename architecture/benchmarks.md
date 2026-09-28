# Benchmarks

Search, write/replication, and startup performance scenarios.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [59. Search Benchmarks](#59-search-benchmarks)
- [60. Write/Replication Benchmarks](#60-writereplication-benchmarks)
- [61. Startup Benchmarks](#61-startup-benchmarks)

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
LumoSQL memory
LumoSQL mmap/LMDB mode if implemented
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

Measure separately:

```text
Pebble open
schema initialization
state scan
row assembly
LumoSQL inserts
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

```bash
# Fast pass: 10K datasets (short mode)
go test ./benchmark/ -bench . -short -benchtime 1s

# Standard pass: 10K + 100K datasets
go test ./benchmark/ -bench . -benchtime 1s

# Large datasets (slow one-time template build: minutes for 1M)
REPLICATEDDB_BENCH_ROWS=1000000 go test ./benchmark/ -bench . -benchtime 1s

# White-box snapshot-seed benchmark (root package)
go test . -bench 'BenchmarkSnapshotSeed' -benchtime 1x

# Cipher x compression matrix only
go test ./benchmark/ -bench 'BenchmarkCipherMatrix' -short

# Five-node / backlog / throughput replication scenarios only
go test ./benchmark/ -bench 'BenchmarkReplicationThroughput|BenchmarkFiveNodeSync|BenchmarkReconnectBacklog' -short

# Direct Go API, one encrypted database, one and four concurrent writers
SPEDSQL_LOCAL_WRITE_BENCH_SECONDS=10 go test ./benchmark/ -run '^TestLocalWriterThroughput$' -v -count=1 -timeout=90s

# Same local workload, async commits with a Pebble sync about once per second
SPEDSQL_LOCAL_WRITE_BENCH_SECONDS=10 go test ./benchmark/ -run '^TestLocalPeriodicSyncThroughput$' -v -count=1 -timeout=90s

# Live daemon, one and four concurrent HTTP SQL writers (10s each)
SPEDSQL_LIVE_WRITER_BENCH_SECONDS=10 go test ./tests-live/benchmark/ -run '^TestWriterThroughput$' -v -count=1 -timeout=90s
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

The live writer benchmark starts a fresh encrypted single-node daemon per
writer count and measures acknowledged single-row `INSERT` statements per
wall-clock second. It checks the final SQL row count against the number of
acknowledged inserts. Its rate includes the application HTTP and SQL paths;
it is separate from the in-process write microbenchmarks and QUIC replication
measurements. See [the live benchmark scenario](../tests-live/benchmark/README.md).

The 2026-09-28 10-second run on the four-core Intel i5-6500 measured
294.3 inserts/sec with one writer and 328.2 inserts/sec with four writers;
both count checks passed.

Benchmark names map to matrix sections: `BenchmarkPKLookup`,
`BenchmarkIndexedEquality`, `BenchmarkIndexedRange`, `BenchmarkOrderLimit`,
`BenchmarkJoin`, `BenchmarkGroupBy`, `BenchmarkFTSTerm`,
`BenchmarkFTSPrefix` (§59 search); `BenchmarkSingleCellUpdate`,
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
`BenchmarkBackendCompare` (memory vs mmap query store) and
`BenchmarkSQLiteBaseline` (stock SQLite reference) cover backend
comparisons.

Coverage notes and manual procedures:

- Impaired networks (high latency, 1% loss) need OS-level impairment;
  rerun any replication benchmark under e.g.
  `tc qdisc add dev lo root netem delay 50ms loss 1%` (Linux, root) and
  remove it afterwards with `tc qdisc del dev lo root`.
- 10M datasets are supported (`REPLICATEDDB_BENCH_ROWS=10000000`) but the
  template build is populate-bound (SQL transactions); prefer a restored
  snapshot fixture when 10M runs become routine.
- Engine-internal rebuild phases (row assembly vs inserts vs index/FTS
  build) have no phase hooks; the suite reports rebuild as one derived
  component. Compaction is Pebble-automatic with no public trigger, so it
  is not benchmarked separately. Substring/trigram search is not enabled.
- LMDB backend comparisons depend on external-backend validation and are
  out of scope; the mmap file-backed backend is the current alternative.

## 63. Recorded Results (10K, 2026-09-27)

Machine: linux/amd64, Intel i5-6500 @ 3.20GHz (4 cores).
Command: `go test ./benchmark/ -bench . -short -benchtime 1s`
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
mmap 14,751 ops/sec, stock SQLite baseline 13,527 ops/sec.

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

At 100K rows (same machine, `go test ./benchmark/ -bench .`): point
lookups hold steady (PK 121K ops/sec); full-scan GroupBy drops to
20 ops/sec; single-cell writes hold at 313 ops/sec; 10K-row
transactions drop to 11K rows/sec (index maintenance grows with data);
full open takes 4.0s (25K rows/sec); reconnect backlog catches up at
1,162 rows/sec. Backends stay ordered: memory 12.8K, mmap 12.3K,
SQLite baseline 9.9K ops/sec on the mixed workload.

---
