# Rolling restart (multi-process)

Run with `go test -count=1 ./tests-live/rolling-restart` (tags required;
or `bash tests-live/run.sh rolling-restart`). Three Murmur-SQL daemons mesh
while a writer hammers every node at 40 inserts/s. Each node stops and
rejoins in turn (same durable directory, unlock, readiness), reconverging
before the next restart.

Zero-downtime contract: no write to a live node may fail at any point
(the restarting node's own writer pauses while its daemon is down). The
suite ends with identical digests on all three nodes.
