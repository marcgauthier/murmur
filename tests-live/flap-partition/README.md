# Flapping partition loses no writes

Run with `go test -count=1 ./tests-live/flap-partition`. All three
nodes take continuous writes while node3's links flap through
`SPEDSQL_FLAP_CYCLES` split/heal cycles (`SPEDSQL_FLAP_SPLIT_SECONDS`,
`SPEDSQL_FLAP_HEAL_SECONDS`). The first split carries an isolation
proof (marker reaches node1, never node2); no write to a live node may
fail, and the final heal must converge every written row with equal
digests.
