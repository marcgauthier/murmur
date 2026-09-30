# Simultaneous migrations converge

Run with `go test -count=1 ./tests-live/migration-concurrency`.
Disjoint additive migrations are published concurrently from
different nodes (`SPEDSQL_MIGRATION_CONCURRENCY_SEED` selects the
variant); the mesh must converge on the union schema at the same
epoch with all pre- and post-migration rows intact and equal digests.
