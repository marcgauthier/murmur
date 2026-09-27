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

