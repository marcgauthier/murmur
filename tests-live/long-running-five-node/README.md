# Five-node persistent capacity soak

Run the 30-second smoke form with
`go test -count=1 ./tests-live/long-running-five-node`. Five encrypted
`spedsql` daemon processes run in isolated node directories and accept
continuous application writes across the full mesh. The scenario restarts
node2 and node4 in place during active writes, checks that their persisted rows
remain available after reopening, and requires all five nodes to converge to
the same logical-state digest. It also checks materialized generations and
per-node Pebble capacity, then commits one final application write.

For the one-hour capacity acceptance run:

```sh
SPEDSQL_FIVE_NODE_DURATION_SECONDS=3600 \
SPEDSQL_FIVE_NODE_SETTLE_SECONDS=300 \
go test -count=1 -timeout=75m ./tests-live/long-running-five-node
```

Tune write cadence with `SPEDSQL_FIVE_NODE_INTERVAL_MS` (default 50 ms).
Failures retain all five node directories, configurations, and logs under
`tests-live/failures/`.
