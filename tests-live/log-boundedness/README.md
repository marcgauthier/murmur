# Replication log stays bounded under sustained load

Run with `go test -count=1 ./tests-live/log-boundedness`. Nodes absorb
sustained writes for `SPEDSQL_LOG_BOUNDEDNESS_SECONDS` while log-GC
runs; retained-batch counts and on-disk log size must stay within
bounds (no unbounded growth), and the mesh must still converge
exactly at the end.
