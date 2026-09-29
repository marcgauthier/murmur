# Three-node sustained-write SLO soak

Run the five-second smoke form with `go test -count=1 ./tests-live/soak-slo`.
Three encrypted Murmur-SQL daemon processes run in separate node directories
and accept continuous independent SQL writes through the HTTP service. They
must converge to an identical logical-state SHA-256 digest. The test reports
write p95 and maximum latency, and checks service readiness, materialized
generation, replication queue depth, and Pebble disk usage from Prometheus.

Configure a longer acceptance run with:

```sh
SPEDSQL_SLO_DURATION_SECONDS=7200 \
SPEDSQL_SLO_SETTLE_SECONDS=300 \
go test -count=1 -timeout=3h ./tests-live/soak-slo
```

`SPEDSQL_SLO_DURATION_SECONDS` and `SPEDSQL_SLO_SETTLE_SECONDS` are independently
configurable. The two-hour command is the long-running acceptance profile; the
short smoke run is not evidence that it has completed. These are broad
stability gates for the current test environment, not a cross-hardware
performance claim.
