# Remote unlock resists abuse and misleads no one

Run with `go test -count=1 ./tests-live/unlock-abuse`. Certless admin
access is refused (401) while `/healthz` stays open; wrong-key,
unknown-key-id, and right-key-unknown-id attempts return byte-identical
generic 401s with no key material (the endpoint is not a key/key-ID
oracle; detail is logged server-side). An honest unlock then succeeds
(`"unlocked":true`, audit log line) with data intact and the mesh
reconverged.
