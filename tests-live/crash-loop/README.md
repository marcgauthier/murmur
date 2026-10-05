# Crash-looping node converges without loss

Run with `go test -count=1 ./tests-live/crash-loop`. One node is
SIGKILLed and restarted in a tight loop (`MURMUR_CRASH_LOOP_ROUNDS`,
`MURMUR_CRASH_LOOP_UPTIME_MS`) while peers keep writing; once the
loop ends the node must rejoin and converge on every row with
PK-ordered digests equal to the mesh.
