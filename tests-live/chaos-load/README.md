# Multi-process writes through partition and healing

Run with `go test -count=1 ./tests-live/chaos-load`. Three encrypted Murmur-SQL
processes run in separate node directories. All processes write concurrently
while node3 is cut off from the connected node1/node2 pair, and the test checks
that the groups have divergent row counts and ordered SHA-256 digests. It then
re-admits peers while writes continue, waits for complete count/digest
convergence, and commits a final write after healing.

The partition and post-heal write windows each run 12 seconds by default. Set
`SPEDSQL_CHAOS_PARTITION_SECONDS` and `SPEDSQL_CHAOS_HEAL_SECONDS` to tune them.
The harness preserves node configs and logs in `tests-live/failures/` when a
run fails.
