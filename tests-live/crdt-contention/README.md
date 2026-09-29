# Multi-process CRDT contention scenario

Run with `go test -count=1 ./tests-live/crdt-contention`. Three encrypted
Murmur-SQL processes run in isolated node directories. The test starts from a
replicated application row, performs concurrent disjoint-column updates from
all nodes, and then applies 40 competing updates to one shared cell from each
process. It checks that the independent cells survive, the shared-cell LWW
winner is identical everywhere, and ordered logical-state digests match.
Failures retain per-node configs and logs under `tests-live/failures/`.
