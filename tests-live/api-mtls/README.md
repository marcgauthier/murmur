# api-mtls (Rust port, test-side equivalent)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/api-mtls/`
(`api_mtls_test.go`, `TestDaemonAPIRequiresMTLSExceptHealth`).

## What it proves

Go requires mutual TLS on the HTTPS daemon API. The Rust product has no
HTTPS API server, so this port proves the same TLS policy on the
product's only TLS server — the QUIC replication endpoint, which
verifies client certificates against the cluster CA at handshake:
trusted client certs complete handshakes and open streams, missing or
foreign-CA certs are rejected at handshake, plaintext is refused, and
rejected handshakes attach no sessions.

## Leg mapping (Go -> Rust)

- Trusted cert reads API -> 200 becomes: trusted node cert completes a
  QUIC handshake and opens a stream.
- Missing cert -> 401 on `/v1/status` + `/metrics` becomes: handshake
  without a client certificate fails. QUIC has no HTTP layer for a
  401, so the same policy enforces one step earlier — and the
  `/healthz` exemption has no target: every connection to this endpoint
  is authenticated (uniformly stricter, documented here).
- Foreign-CA cert rejected becomes: handshake with a rogue-CA
  certificate fails (rogue CA minted in-test via `harness::certs`).
- Plaintext HTTP refused becomes: TCP connect refused (nothing
  listens) plus UDP garbage attaching no session.
- Expired client cert: NOT portable (minting one needs the cluster CA
  key, which the harness never exposes); port with the HTTPS suite.
- CLI status legs: NOT portable (`murmur-admin` is an offline CLI with
  no API client); port with the HTTPS suite.

## Pass criteria

- Trusted handshake + stream open within 10 s / 5 s.
- Certless and foreign-CA handshakes fail within 10 s each.
- Zero sessions attach from any rejected handshake or garbage burst.
- Plaintext TCP refused.

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/api-mtls/Cargo.toml
```
