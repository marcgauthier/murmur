# RIME and SQLite comparison fixtures

This isolated module contains historical in-memory SQLite comparisons for
RIME. It is excluded from the root module's `go test ./...` and requires CGO
plus a C compiler. It does not represent Murmur's durable write path.

Run the concurrency smoke test from this directory:

```sh
RIME_BENCH_ROWS=1000 go test -race -run '^TestCompareRimeVsSQLiteConcurrent$' -count=1 -timeout=3m .
```

Run the broader in-memory comparison with a smaller dataset when desired:

```sh
RIME_BENCH_ROWS=1000 go test -run '^TestCompareRimeVsSQLiteMemory$' -count=1 -timeout=10m .
```
