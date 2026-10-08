# Multi-process CRDT contention scenario

Run with `bash tests-live/run.sh crdt-contention`. Three encrypted Murmur
processes run in isolated node directories using typed RIME records. The test
starts from a replicated application row, performs concurrent disjoint-column updates from
all nodes, and then applies 40 competing updates to one shared cell from each
process. It checks that the independent cells survive, the shared-cell LWW
winner is identical everywhere, and full typed records match.
Failures retain per-node configs and logs under `tests-live/failures/`.
