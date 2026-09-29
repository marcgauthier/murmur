# Node identity allow-list multi-process scenario

Run from the repository root with:

```sh
go test -count=1 ./tests-live/allow-nodes
```

Three standalone Murmur-SQL processes use separate node directories, encrypted
Pebble stores, and certificates issued by one cluster CA. Node1 and node3 accept
all identities; node2 allows only node1. All nodes produce application writes
concurrently for three minutes by default. The test requires the unauthorized
node3-to-node2 session to remain absent while traffic is active, then checks
all rows and ordered state digests converge through node1. Set
`SPEDSQL_ALLOW_NODES_WRITE_SECONDS` to shorten or extend the workload. Failure
artifacts preserve all node configurations and logs under `tests-live/failures/`.
