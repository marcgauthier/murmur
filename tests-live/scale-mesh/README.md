# Ten-node mesh (multi-process)

Run with `go test -count=1 ./tests-live/scale-mesh` (tags required; or
`bash tests-live/run.sh scale-mesh`). Ten `spedsql` daemons mesh with the
default fanout of 4 while two origins write 400 rows.

Smaller suites never exceed the fanout, so this is the only live proof
that peer selection and rotation hold at scale: every node must converge
to identical digests, and a 2s sampler asserts no node ever selects more
than 4 replication targets for the whole run.
