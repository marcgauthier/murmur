# Concurrent deletes, resurrections, and updates

Run with `go test -count=1 ./tests-live/delete-pruning`. Three daemons
share 300 rows (`SPEDSQL_DELETE_PRUNING_KEYS`, multiple of 3), then
concurrently delete, update, and resurrect disjoint thirds under
aggressive log retention. A log-GC pass is forced mid-flight (observed
via `spedsql_gc_runs_total` / `spedsql_gc_log_collected_total`) and all
nodes must converge on exact counts plus equal PK-ordered digests.
Scope: delete/resurrect convergence plus log-GC collection of the
delete batches; stored tombstone markers themselves are out of scope.
