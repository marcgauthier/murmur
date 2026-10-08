# Multi-process abrupt crash recovery

Run with `CGO_ENABLED=0 bash tests-live/run.sh crash-recovery`. Three native
typed-record processes use separate encrypted node directories and continuously
accept managed writes. The scenario sends `SIGKILL` to node2 and later node3
while requests are in flight, restarts each process with the same durable
directory and key, and waits for the mesh to restore all acknowledged rows.
It verifies equal ordered-state digests and performs another typed write after
recovery. Each write window defaults to 12 seconds; set
`MURMUR_CRASH_WRITE_SECONDS` to shorten or extend each window and
`MURMUR_CRASH_SETTLE_SECONDS` to tune the final convergence wait. Failure
artifacts preserve configs and logs under `tests-live/failures/`.
