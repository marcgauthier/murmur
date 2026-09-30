# GORM sustained-sync live test

Run with `go test -count=1 ./tests-live/gorm-sync`. Three encrypted,
mutually authenticated nodes commit associated rows concurrently
through the GORM dialect (nested has-many creates). The test waits
for full row counts on every node, compares canonical ordered
SHA-256 digests read through GORM, and proves associations resolve
for rows the reading node never wrote.

Nodes run in-process, because GORM requires an embedded engine
handle, and replicate over real QUIC with on-disk encrypted state.
