# Benchmark live scenario

Multi-process two-node replication throughput benchmark. One Murmur-SQL
daemon writes continuously (one insert plus one hot-row update per round)
while its peer converges; the report covers statement/mutation rates,
convergence time, and logical protocol byte rates.

## Gate

Identical row counts plus matching SHA-256 application-data hashes on
both peers after the write phase.

## Run

```sh
MURMUR_LIVE_BENCHMARK_WRITE_SECONDS=10 \
MURMUR_LIVE_BENCHMARK_SYNC_TIMEOUT_SECONDS=120 \
go test ./tests-benchmark/replication/ -run TestTwoNodeReplicationBenchmark -v -count=1
```

Sample report line:

```text
benchmark writes=10s statements=1244 (inserts=622 updates=622) rate=124.3/s convergence=682ms rows=722 sha256=df6a… batch_bytes_sent=3564864 batch_bytes_recv=3235337
```

## Notes

- `batch_bytes_sent/recv` are scraped from each daemon's `/metrics`
  (`spedsql_repl_batch_bytes_{sent,received}_total`) around the write
  phase. Counters render as float64 text, so the parser uses
  `ParseFloat` — large values arrive in scientific notation, which
  `ParseUint` rejects (silent zero, then unsigned underflow).
- Statement rate is bounded by the daemon's single-statement HTTP
  service surface (one transaction per statement), not by the
  replication path; convergence after a 10s write phase is typically
  under one second.

## One versus four writers

`TestWriterThroughput` starts a separate encrypted single-node daemon for
each writer count. One or four concurrent application clients send unique
single-row `INSERT` statements through `/v1/exec` for the same duration.
Each successful response counts as one acknowledged write; the elapsed
wall time includes the completion of in-flight requests. Afterward the test
queries `COUNT(*)` through `/v1/query` and requires it to equal the number
of acknowledged inserts. The two runs use fresh databases, so rates are
comparable without rows from the first run affecting the second.

```sh
MURMUR_LIVE_WRITER_BENCH_SECONDS=10 \
go test ./tests-benchmark/replication/ -run '^TestWriterThroughput$' -v -count=1 -timeout=90s
```

The log reports `writers`, `acknowledged_inserts`, `elapsed`, and
`writes_per_second` for each subtest. This measures durable application SQL
write throughput including HTTP request overhead on one local daemon; it
does not include QUIC replication.

On 2026-09-28, a 10-second run on a four-core Intel i5-6500 measured
2,943 inserts (294.3/sec) with one writer and 3,285 inserts (328.2/sec)
with four writers. Both row-count checks passed. These are measurements of
this machine and workload, not throughput guarantees.
