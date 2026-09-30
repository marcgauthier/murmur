# Corrupt snapshots are discarded, then honest resync converges

Run with `go test -count=1 ./tests-live/corrupt-snapshot`. A stale
node is fed corrupt snapshot chunks from a malicious fixture peer
(`attacker.go`); every corrupt chunk must be discarded (rejection
observed, GC wait bounded by
`SPEDSQL_CORRUPT_SNAPSHOT_GC_WAIT_SECONDS`), and the node must then
converge via the honest resync path with digests equal to the mesh.
