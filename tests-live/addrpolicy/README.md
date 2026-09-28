# Address-policy multi-process scenario

Run with `go test -count=1 ./tests-live/addrpolicy`. The scenario launches
three `spedsql` daemon processes under `node1`, `node2`, and `node3`, each with
its own encrypted Pebble directory and TLS identity. `node1` admits loopback
addresses and forms a session with unrestricted `node3`; `node2` is configured
with a TEST-NET CIDR and admits no loopback path. SQL writes prove that the
admitted pair replicates while the denied node keeps its write local. Failure
artifacts retain each node's config and logs under `tests-live/failures/`.
