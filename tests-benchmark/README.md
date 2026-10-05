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
bash tests-benchmark/run.sh perf-matrix   # characterization matrix (tiers below)
bash tests-benchmark/run.sh all
```

`benchmark` runs the throughput `Test*` first, then the `-short`
micro-benchmark fast pass (`MURMUR_BENCH_BENCHTIME`, default `1s`).

`perf-matrix` runs only `TestPerfMatrix`: the tiered characterization
matrix (dataset sizes, tx batches, ciphers, live mesh scaling,
impairment, reconnect). Tiers via `MURMUR_PERF_TIER`:

- `smoke` — 10K rows, 1 cipher, 2-node mesh, 1K backlog; minutes.
- `standard` (default) — 10K+100K rows, 3 ciphers, 1/2/5-node meshes,
  delay/loss cells when privileged, 1K/10K backlogs; tens of minutes.
- `full` — adds 1M rows (template build takes minutes), 10-node mesh,
  50K backlog; ~1–2h. `MURMUR_PERF_MESH_ROWS` overrides the mesh blast
  size (default 2000). `MURMUR_PERF_ONLY=<substr>` restricts the run to
  matching cell groups (`store`, `cipher`, `tx`, `mesh`, `impair`,
  `reconnect`) for focused reruns.

Each run writes `tests-benchmark/benchmark/perf-report-<ts>.json`
(machine stamp + cells) and logs the same cells as a Markdown table
for publication. Published numbers live in
[architecture/benchmarks.md](../architecture/benchmarks.md#65-characterization-matrix).

The runner builds its own daemon binary at
`tests-benchmark/bin/testnode` and isolates runtime artifacts under
`tests-benchmark/runtime/` (failures under
`tests-benchmark/failures/`); overrides: `MURMUR_BIN`,
`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`, `MURMUR_TAGS`
(default `"sqlite_preupdate_hook sqlite_fts5"`).
