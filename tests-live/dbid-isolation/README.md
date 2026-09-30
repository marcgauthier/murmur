# Cross-database peers never connect

Run with `go test -count=1 ./tests-live/dbid-isolation`. Two
clusters with different DBIDs, byte-identical schemas, and shared CA
trust (so TLS cannot be the refusal cause) are cross-peered in both
directions: every cross edge must stay disconnected for the
`SPEDSQL_DBID_ISO_WINDOW_S` window (minimum 10), markers must never
cross, identity refusals must advance on all nodes while schema
refusals stay flat, and both clusters must stay internally healthy.
