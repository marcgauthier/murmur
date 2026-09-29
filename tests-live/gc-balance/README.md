# Log-GC balance (multi-process)

Run with `go test -count=1 ./tests-live/gc-balance` (tags required; or
`bash tests-live/run.sh gc-balance`). Three `spedsql` daemons mesh with
aggressive log retention (1s log, 20s offline pin, 10 batches) while one
node updates a fixed 200-key set with eight parallel updaters over
disjoint ranges.

GC contract: after a 20s warmup, a 40s window must show GC collecting
at least 90% of the origin-log batches the window produced (commits
vs `spedsql_gc_log_collected_total`), i.e. the retained set does not
grow; GC runs with zero failures. The balance is measured from daemon
counters rather than directory bytes: at this scale nothing ever
flushes to SSTs, so filesystem size is flat MANIFEST/WAL noise
regardless of GC behavior. The suite ends with exact row counts and
identical digests on all nodes (no loss). Deletes are out of scope:
delete pruning is unimplemented, so only version churn is asserted.
