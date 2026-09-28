# Three-node sustained-sync smoke test

Run with `go test -count=1 ./tests-live/three-node-sync`. Three encrypted,
mutually authenticated nodes each commit a 20-row transaction concurrently.
The test waits for all 60 rows on every node and compares a canonical ordered
SHA-256 digest. This ports the convergence and digest checks from GALVANIZE's
three-node sustained-sync benchmark at integration-test scale; it does not
claim benchmark throughput or duration results.
