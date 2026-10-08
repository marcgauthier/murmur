# Simultaneous migrations converge

Run with `CGO_ENABLED=0 bash tests-live/run.sh migration-concurrency`.
Managed typed records without SQLite or CGO undergo the same additive
`Note` migration concurrently on two nodes while all three keep writing.
Each node must reach a well-defined epoch, preserve acknowledged rows, and
converge on matching names and migrated field values after the partition heals.
