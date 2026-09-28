# Writer-share load test

Run with `go test -count=1 ./tests-live/loadshare`. Two `spedsql` daemon
processes in discrete `node1`/`node2` directories form an encrypted mesh.
One node absorbs 240 local writes across four concurrent HTTP writers while
its peer replicates 20 rows. The test requires full convergence (no remote
starvation), acquisitions in both scheduler classes and service debt within
the 1s bound (scraped from Prometheus `/metrics`), and logs the
local-commit latency distribution.
