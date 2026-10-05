# Client flood does not deny honest service

Run with `go test -count=1 ./tests-live/dos-client`. Layered
connection abuse for `MURMUR_DOS_CLIENT_ATTACK_SECONDS` (TCP
half-open/partial, TLS idle, QUIC half-open, slow-loris trickle; each
leg toggled by its `MURMUR_DOS_CLIENT_*` knob): honest API writes
and replication must keep succeeding within bounds and the mesh must
converge exactly afterwards. Slow by design; runs via `all`, not the
gate.
