# Partitioned tail repair from peer logs

Run with `go test -count=1 ./tests-live/tail-repair`. One node is
partitioned while peers take a write burst
(`SPEDSQL_TAIL_REPAIR_BURST_ROWS`); isolation is proven (frozen count
plus zero `snapshots_sent` mid-partition), then the node heals and must
repair its tail from peer logs with zero resends of already-held data
and equal digests everywhere.
