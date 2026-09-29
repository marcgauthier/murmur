# Subscription continuity live test

Run with `go test -count=1 ./tests-live/subscribe`. Two Murmur-SQL daemon
processes in discrete `node1`/`node2` directories form an encrypted mesh.
A server-sent-events subscriber on one node observes a far-side write,
survives a split (no leak across the partition), then observes the
partitioned write after healing with no reset in between.
