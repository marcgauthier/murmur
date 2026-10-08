# Five-node persistent capacity soak

Run the 30-second smoke form with
`go test -count=1 ./tests-live/long-running-five-node`. Five encrypted Murmur
daemon processes run in isolated node directories and accept continuous
managed typed-record writes across the full mesh. The scenario restarts
node2 and node4 in place during active writes, checks that their persisted rows
remain available after reopening, and requires all five nodes to converge to
the same logical-state digest. It also checks materialized generations and
per-node Spool capacity, then commits one final application write.

For the one-hour capacity acceptance run:

```sh
CGO_ENABLED=0 MURMUR_FIVE_NODE_INTERVAL_MS=200 \
MURMUR_FIVE_NODE_DURATION_SECONDS=3600 \
MURMUR_FIVE_NODE_SETTLE_SECONDS=300 \
  bash tests-live/run.sh long-running-five-node
```

Tune write cadence with `MURMUR_FIVE_NODE_INTERVAL_MS` (default 50 ms). The
unlock helper allows up to 60 seconds for reopening and rebuilding an existing
store after a restart.
Failures retain all five node directories, configurations, and logs under
`tests-live/failures/`.

On 2026-10-08, the one-hour CGO-disabled run passed with five nodes and a
200 ms write interval. It acknowledged 72,925 writes, survived both orderly
restarts, converged all five logical-state digests, and cleaned up its runtime.
The run reported 35 transient HTTP write errors; the fixture checks that the
acknowledged records remain durable and converge.
