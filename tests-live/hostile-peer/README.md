# Hostile peer attacks are rejected

Run with `go test -count=1 ./tests-live/hostile-peer`. A malicious
fixture peer (`attacker.go`) throws protocol attacks at an honest
mesh: forged/oversized/malformed frames and handshake abuse. Every
attack must be rejected (observed via rejection metrics/counters,
never assumed), honest writes must keep converging, and final digests
must be equal.
