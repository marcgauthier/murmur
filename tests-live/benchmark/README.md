# Benchmark live scenario

Multi-process two-node replication throughput benchmark. One `spedsql`
daemon writes continuously (one insert plus one hot-row update per round)
while its peer converges; the report covers statement/mutation rates,
convergence time, and logical protocol byte rates.

## Gate

Identical row counts plus matching SHA-256 application-data hashes on
both peers after the write phase.

## Run

```sh
SPEDSQL_LIVE_BENCHMARK_WRITE_SECONDS=10 \
SPEDSQL_LIVE_BENCHMARK_SYNC_TIMEOUT_SECONDS=120 \
go test ./tests-live/benchmark/ -run TestTwoNodeReplicationBenchmark -v -count=1
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
