# SQLite live benchmark

Measures the CGO `mattn/go-sqlite3` driver on fresh `:memory:` databases
with the same schema and pragmas the engine uses.

`TestSQLiteMattnMemory` seeds two tables (`users`, `orders`) with
100k rows each, then times bulk insert, row-by-row update,
simple point/range queries, and join queries, and logs a timing
table. Row counts and spot values gate correctness.

## Run

```sh
cd tests-benchmark/sqlite-bench
go test . -run TestSQLiteMattnMemory -v -count=1
```

Micro-benchmarks (`-bench`) cover single-row insert/update latency and
query throughput with `b.N` loops over seeded 100k-row tables:

```sh
cd tests-benchmark/sqlite-bench
go test . -bench . -benchtime 1s -run '^$'
```

Or via the benchmark runner: `bash tests-benchmark/run.sh sqlite-bench`.

## Knobs

- `MURMUR_SQLITE_BENCH_ROWS` (default 100000): rows per table.
- `MURMUR_SQLITE_BENCH_POINT_ITERS` (default 20000): point lookups.
- `MURMUR_SQLITE_BENCH_JOIN_ITERS` (default 10000): join lookups.
- `MURMUR_SQLITE_BENCH_SCAN_ITERS` (default 10): full-scan repeats.
- `MURMUR_SQLITE_BENCH_JOIN_SCAN_ITERS` (default 5): join-scan repeats.

## Notes

- This standalone benchmark module needs CGO (mattn driver). It does not use
  `testnode` or the harness; the benchmark runner still builds the daemon for
  the sibling `replication` suite.
