# Convergence under impaired networks

Run with `go test -count=1 ./tests-live/impaired-network`. Uses
tc/netem on loopback (needs `CAP_NET_ADMIN`): 150ms latency, 3% loss,
and a bandwidth-capped snapshot resync, each with a p95-visibility
bound over `SPEDSQL_IMPAIRED_NETWORK_ROWS` rows. Skips with a message
when tc is unusable; `SPEDSQL_IMPAIRED_NETWORK_FORCE=1` runs the same
workload unimpaired as a smoke path (each subtest banners FORCE mode
so green logs cannot be misread as impairment proof).
