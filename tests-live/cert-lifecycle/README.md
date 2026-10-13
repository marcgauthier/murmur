# cert-lifecycle (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/cert-lifecycle/`
(`README.md` + `cert_lifecycle_test.go`:
`TestCertExpiryRejectsAndRotationHeals`).

## What it proves

A node whose certificate expires is rejected on fresh replication
handshakes, and rotation restores the mesh: converge a marker (positive
control), swap B to a short-expiry cert for the same NodeID and show the
mesh stays connected pre-expiry, wait past NotAfter and restart B so both
sides must re-handshake (survivor sees no session, `peers` 1->0, an
A-side marker never reaches B during the isolation window), then rotate
to a fresh cert and show full reconvergence plus a bidirectional write.

## Pass criteria

- Phase 0: `marker-init` converges on both nodes; A holds a session to B
  and `peers(A) == 1`.
- Phase 1: after rotating B to a TTL cert, the served leaf's remaining
  lifetime fits the minted TTL (stale-cert guard), A connects to B, and
  `marker-pre-expiry` converges before NotAfter minus 5 s.
- Phase 2: past NotAfter + 2 s, with B restarted on the same expired
  cert: A reports B not connected, `peers(A) == 0`, B's own view agrees,
  the served leaf parses with NotAfter in the past, and during the
  isolation window `marker-post-expiry` never reaches B while A keeps
  serving all 3 markers with `peers(A) == 0` throughout.
- Phase 3: after rotating B to a fresh 24 h cert, the mesh reconnects,
  all 3 markers converge, a B-side `marker-rotated` converges back, and
  both nodes hold equal ordered names.

## Env knobs

- `MURMUR_CERT_LIFECYCLE_TTL_S` (default 25): short-cert lifetime. Values
  below 15 s abort: too little pre-expiry budget (same guard as Go).
- `MURMUR_CERT_LIFECYCLE_WINDOW_S` (default 10): post-expiry isolation
  window.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`records(name TEXT PRIMARY KEY)`) instead of the managed
  typed-records HTTP API (`/v1/typed/insert`, `/v1/typed/names`).
- Session state comes from `Handle::status()` (`peers`,
  `peers_diagnostics`) instead of `debug/peers` (`Connected`) and
  `/v1/status` (`connected_peers`). Peers whose handshakes fail never
  enter the session table, so absence reads as not connected (same
  helper shape as the `churn-retirement` port).
- No HTTPS server exists, so there is no dial-the-API served-cert fetch
  and no insecure client: the served leaf is read from the node's
  `tls/node.der`, which the harness rewrites from the same material
  passed to `Database::open`, so file NotAfter always equals served
  NotAfter. Expiry is enforced by the standard rustls/webpki verifiers
  on both handshake directions (`NodeVerifier` client-side,
  `WebPkiClientVerifier` server-side); opening with an expired cert
  succeeds (open checks the SAN identity, not validity), matching Go.
- Rotation goes through the harness `Cluster::rotate_node_cert` helper
  (stop, issue a fresh leaf for the same NodeID, rewrite `tls/`,
  reopen) instead of rewriting `tls/node.crt` + `tls/node.key` on disk;
  credentials are in-memory (`ReplConfig::credentials`), so a file swap
  alone would not take effect.
- The isolation-window argument is unchanged: `mesh()` redials
  immediately and the engine's 5 s bootstrap retick keeps dialing, so a
  steady zero across the window proves rejection rather than idleness.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/cert-lifecycle/Cargo.toml
```
