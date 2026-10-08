# Cross-database peers never connect

Run with `bash tests-live/run.sh dbid-isolation`. Two typed-record
clusters with different DBIDs and shared CA
trust (so TLS cannot be the refusal cause) are cross-peered in both
directions: every cross edge must stay disconnected for the
`MURMUR_DBID_ISO_WINDOW_S` window (minimum 10), markers must never
cross, identity refusals must advance on all nodes while schema
refusals stay flat, and both clusters must stay internally healthy.
