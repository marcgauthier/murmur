# Protocol version skew (multi-process)

Run with `go test -count=1 ./tests-live/version-skew` (tags required; or
`bash tests-live/run.sh version-skew`). Three Murmur-SQL daemons mesh, but
the third advertises handshake protocol version 99 via the
`protocol_version_override` testing knob.

Fail-closed contract: the skewed node never connects in either direction
(`connected_peers` stays 0 while the compatible pair holds 1), exchanges
no data either way, but stays alive and keeps serving local reads/writes.
The compatible pair converges normally throughout.
