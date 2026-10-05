# Multi-process graceful shutdown under load

Run with `go test -count=1 ./tests-live/graceful-shutdown`. Three Murmur-SQL
daemons mesh with aggressive log retention. Node1 takes SIGTERM with a write
in flight; node3 restarts stale (its log ranges GC'd, forcing snapshot
resync) and takes SIGTERM mid-snapshot-apply. Both restarts must be clean
(shutdown markers), fast (under 60s), and lossless, with the mesh
reconverging to identical digests. The snapshot path is proven via the
receiver's `spedsql_repl_snapshots_received_total` counter, and GC-phase
forcing is proven by a polled `spedsql_gc_runs_total` pass plus
`spedsql_gc_log_collected_total` advancement (a slipped tick fails loudly
instead of silently taking the log path). Set
`MURMUR_GRACEFUL_MEMBER_DEADLINE_SECONDS` (default 25) to tune the
member-deadline wait. Failure artifacts preserve configs and logs under
`tests-live/failures/`.
