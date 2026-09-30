# MURMUR-SQL Benchmark Suites

Performance-only suites: they measure throughput/latency and are not
correctness gates. Each test folder has its own runner — this one runs
only the suites below:

- `benchmark/` — in-process Go benchmarks and throughput tests
  (reads, writes, FTS, replication, maintenance, cipher matrix).
- `replication/` — live two-node replication throughput benchmark
  (multi-process daemons via the shared `tests-live` harness).
- `sqlite-bench/` — `:memory:` SQLite driver comparison
  (mattn vs modernc) over 100k-row tables.

Run one suite or everything:

```sh
bash tests-benchmark/run.sh replication
bash tests-benchmark/run.sh sqlite-bench
bash tests-benchmark/run.sh benchmark
bash tests-benchmark/run.sh all
```

`benchmark` runs the throughput `Test*` first, then the `-short`
micro-benchmark fast pass (`SPEDSQL_BENCH_BENCHTIME`, default `1s`).

The runner builds its own daemon binary at
`tests-benchmark/bin/testnode` and isolates runtime artifacts under
`tests-benchmark/runtime/` (failures under
`tests-benchmark/failures/`); overrides: `SPEDSQL_BIN`,
`SPEDSQL_LIVE_RUNTIME`, `SPEDSQL_LIVE_FAILURES`, `SPEDSQL_TAGS`
(default `"sqlite_preupdate_hook sqlite_fts5"`).
