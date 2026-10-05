# Multi-process abrupt crash recovery

Run with `go test -count=1 ./tests-live/crash-recovery`. Three Murmur-SQL
processes use separate encrypted node directories and continuously accept
application SQL writes. The scenario sends `SIGKILL` to node2 and later node3
while requests are in flight, restarts each process with the same durable
directory and key, and waits for the mesh to restore all acknowledged rows.
It verifies equal ordered-state digests and performs another SQL write after
recovery. Each write window defaults to 12 seconds; set
`MURMUR_CRASH_WRITE_SECONDS` to shorten or extend each window and
`MURMUR_CRASH_SETTLE_SECONDS` to tune the final convergence wait. Failure
artifacts preserve configs and logs under `tests-live/failures/`.
