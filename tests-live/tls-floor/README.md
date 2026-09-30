# TLS version floors are enforced

Run with `go test -count=1 ./tests-live/tls-floor`. The HTTPS API must
refuse TLS 1.1 and 1.0 handshakes with a server-side rejection (the
client genuinely emits the old ClientHello, verified by the failure
shape) while TLS 1.2 succeeds and runs a live query with the
negotiated version asserted. QUIC replication pins TLS 1.3 by
construction (a sub-1.3 QUIC probe cannot be emitted, documented in
the suite header); the suite guards the product's QUIC TLS
constructors instead, with a converged live mesh as the positive
control.
