# Certificate expiry rejects, rotation heals

Run with `bash tests-live/run.sh cert-lifecycle`. The fixture uses typed
records and runs with CGO disabled. A node's
certificate is swapped for a short-expiry cert
(`MURMUR_CERT_LIFECYCLE_TTL_S`, default 25) minted for the same
NodeID; the mesh must stay connected pre-expiry. After NotAfter
passes, fresh handshakes must reject the node (Connected=false,
connected_peers 1->0, markers never cross during the
`MURMUR_CERT_LIFECYCLE_WINDOW_S` isolation window), with the served
cert parsed at assert time to prove it is genuinely expired. Rotating
to a fresh cert must fully reconverge the mesh.
