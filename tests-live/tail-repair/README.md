# Partitioned tail repair from peer logs

Run with `CGO_ENABLED=0 bash tests-live/run.sh tail-repair`. The scenario uses
managed typed records without SQLite or CGO. One node is
partitioned while peers take a write burst
(`MURMUR_TAIL_REPAIR_BURST_ROWS`); isolation is proven (frozen count
plus zero `snapshots_sent` mid-partition), then the node heals and must
repair its tail from peer logs with zero resends of already-held data
and equal digests everywhere.
