# Simultaneous rejoin after dual kill

Run with `go test -count=1 ./tests-live/rejoin-storm`. Two nodes are
killed at once while the survivor takes outage writes
(`MURMUR_REJOIN_STORM_OUTAGE_ROWS`); both killed nodes restart
simultaneously and the full mesh must reconverge on every row with
equal digests (no lost writes, no double-apply).
