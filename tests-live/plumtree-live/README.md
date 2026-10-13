# plumtree-live (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/plumtree-live/`
(`README.md` + `plumtree_live_test.go`: `TestPlumtreeMeshConverges`,
`TestMixedModeRefusesGossipPeer`).

## What it proves

- `plumtree_mesh_converges`: three nodes in Plumtree dissemination mode
  converge end to end (10 rows from node 0 on every node within 60 s).
- `mixed_mode_refuses_gossip_peer`: two Plumtree nodes meshed with one
  gossip-mode node — the Plumtree pair converges on its 10 rows while the
  gossip node stays isolated in both directions (10/10/1 row counts after a
  5 s leak window), with handshake refusals recorded.

## Pass criteria

- All-Plumtree mesh: `SELECT name FROM records ORDER BY name` on every node
  equals the 10 `plum-{0..9}` names within 60 s (200 ms poll).
- Mixed mesh: nodes 0/1 reach 10 `mixed-plum-{0..9}` names within 30 s; after
  the leak window the counts are exactly 10/10/1.
- At least one node records a handshake refusal (`status().last_error`
  contains `incompatible`); the gossip node holds 0 replication sessions
  while each Plumtree node holds exactly 1.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records HTTP API (`TypedInsert`/`TypedNames`); canonical
  order comes from `ORDER BY name` rather than the API response.
- Per-node dissemination uses the harness `ClusterOptions::dissemination_by_node`
  overlay (added for this scenario; mirrors Go `DisseminationByNode`).
- Refusal evidence is `replication().status().last_error` containing
  `incompatible` (the Rust engine fails closed with `incompatible hello` on
  accept and `incompatible welcome` on dial) plus session counts, instead of
  the Go `spedsql_repl_handshake_capability_refusals_total` Prometheus metric,
  which has no Rust equivalent.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/plumtree-live/Cargo.toml
```
