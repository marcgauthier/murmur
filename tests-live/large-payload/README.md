# Large payload and bulk replication Go integration test

Run from the repository root with:

```sh
go test -count=1 ./tests-live/large-payload
```

Two `spedsql` daemon processes in discrete `node1`/`node2` directories form
an encrypted mutual-TLS mesh. The test issues 250 row inserts plus one
1.5 MiB text value insert over HTTP (the daemon's single-statement service
surface), then checks the replicated row count and the payload's exact
length and SHA-256 digest. This ports the direct High-mesh portion of
GALVANIZE's large-payload scenario. Cross-domain bundle transfer is covered
separately by `tests-live/highlow`.
