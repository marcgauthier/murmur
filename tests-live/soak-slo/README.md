# Three-node sustained-write SLO soak

Run a short live smoke with:

```sh
CGO_ENABLED=0 MURMUR_SLO_DURATION_SECONDS=20 \
  bash tests-live/run.sh soak-slo
```
Three encrypted Murmur-SQL daemon processes run in separate node directories
and accept continuous independent managed-record writes through the HTTP
service. Each node inserts into a 1,000-row owned range, then updates those
rows so the working set stays bounded during the two-hour run. They must
converge to an identical logical-state SHA-256 digest. The test reports
write p95 and maximum latency, and checks service readiness, materialized
generation, replication queue depth, and Spool disk usage from Prometheus.

Configure a longer acceptance run with:

```sh
MURMUR_SLO_DURATION_SECONDS=7200 \
MURMUR_SLO_SETTLE_SECONDS=300 \
go test -count=1 -timeout=3h ./tests-live/soak-slo
```

`MURMUR_SLO_DURATION_SECONDS` and `MURMUR_SLO_SETTLE_SECONDS` are independently
configurable. The two-hour command is the long-running acceptance profile; the
short smoke run is not evidence that it has completed. These are broad
stability gates for the current test environment, not a cross-hardware
performance claim.

## Latest acceptance run

On 2026-10-08, the CGO-disabled two-hour profile passed with three Murmur
nodes in 7,216.7 seconds. The bounded workload performed 597,169 operations
against 3,000 rows; write p95 was 107.4 ms and maximum write latency was 2.47 s.
All nodes converged to the same logical-state digest, and the runner cleaned up
the runtime. An earlier unbounded unique-insert attempt exceeded the configured
512 MiB Spool guard and is excluded from acceptance evidence.
