# Commit budgets and pressure convergence (multi-process)

Run with `go test -count=1 ./tests-live/overload-budgets`. Two Murmur-SQL
daemons run with small commit budgets (4 KiB values, 64 KiB transactions;
see the harness `Limits` options and the daemon `limits` config section).
A 2 MiB POST must fail at the 1 MiB HTTP body cap, a 5 KiB value must be
rejected as too large with no partial row on either node, and the writer
must stay healthy. Then 8 concurrent writers × 50 inserts must fully
converge with p99 insert latency under 10s while both nodes stay
responsive.
