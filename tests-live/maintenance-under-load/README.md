# Log-GC maintenance under sustained load

Run with `CGO_ENABLED=0 bash tests-live/run.sh maintenance-under-load`. The
scenario uses managed typed records without SQLite or CGO.
Three nodes take sustained concurrent writes
(`MURMUR_MAINT_LOAD_SEED`, `MURMUR_MAINT_LOAD_MAX_INSERTS`) with
zero failed writes allowed while tight retention forces origin-log
GC. GC passes and batch collection must advance on every node (with
writers provably still progressing), GC failures must stay flat, and
all nodes must converge on exact counts plus equal id-ordered
digests, followed by a post-maintenance write that replicates
everywhere. Only counter-observed maintenance is claimed; the suite
header inventories what is deliberately out of scope.
