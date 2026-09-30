# SQLite driver live benchmark

Compares the two SQLite drivers vendored by this repo — CGO
`mattn/go-sqlite3` vs pure-Go `modernc.org/sqlite` — on fresh
`:memory:` databases with identical schema, pragmas, and data.

`TestSQLiteDriverComparison` seeds two tables (`users`, `orders`) with
100k rows each per driver, then times bulk insert, row-by-row update,
simple point/range queries, and join queries, and logs a comparison
table. Row counts and spot values gate correctness.

## Run

```sh
go test ./tests-benchmark/sqlite-bench/ -run TestSQLiteDriverComparison -v -count=1
```

Micro-benchmarks (`-bench`) cover single-row insert/update latency and
query throughput with `b.N` loops over seeded 100k-row tables:

```sh
go test ./tests-benchmark/sqlite-bench/ -bench . -benchtime 1s -run '^$'
```

Or via the benchmark runner: `bash tests-benchmark/run.sh sqlite-bench`.

## Knobs

- `SPEDSQL_SQLITE_BENCH_ROWS` (default 100000): rows per table.
- `SPEDSQL_SQLITE_BENCH_POINT_ITERS` (default 20000): point lookups.
- `SPEDSQL_SQLITE_BENCH_JOIN_ITERS` (default 10000): join lookups.
- `SPEDSQL_SQLITE_BENCH_SCAN_ITERS` (default 10): full-scan repeats.
- `SPEDSQL_SQLITE_BENCH_JOIN_SCAN_ITERS` (default 5): join-scan repeats.

## Notes

- The mattn leg needs CGO; under `CGO_ENABLED=0` it skips and the
  modernc leg still runs, so the pure-Go CI matrix stays green.
- This suite does not use the `testnode` daemon or the harness; the
  runner still rebuilds the daemon binary for the sibling `replication`
  suite.
