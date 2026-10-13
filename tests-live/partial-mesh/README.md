# partial-mesh (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/partial-mesh/`
(`README.md` + `partial_mesh_test.go`:
`TestChainPeersConvergeViaForwarding`,
`TestDynamicBootstrapDiscovery`).

## What it proves

- Chain forwarding: three nodes peered only as a chain
  (node1-node2, node2-node3) still converge, with writes on node1
  reaching node3 via forwarding by node2.
- Bootstrap discovery: three nodes using node 0 as a bootstrap seed
  discover one another with no explicit `AddPeer` calls, form a full
  mesh, and converge exactly on writes from all three origins.

## Pass criteria

- Chain leg (`partial-mesh-chain` runtime dir): 10 inserts on node 0
  converge (identical ordered names) on all 3 nodes within 60 s; no
  direct node1-node3 session exists in either direction; live peer
  counts are exactly {1, 2, 1}.
- Bootstrap leg (`partial-mesh-bootstrap` runtime dir): all 3 nodes
  discover 2 members each and reach 2 live peers each within 30 s with
  zero explicit `AddPeer` calls; 10 + 5 + 5 inserts (one batch per
  node) then converge exactly (identical ordered 20 names) on all 3
  nodes within 150 s.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records HTTP API (`TypedInsert` / `TypedNames`); row ids
  are node-disjoint small integers instead of typed-record keys. The Go
  `AwaitUnlock` / `TypedRecords` cluster options have no Rust equivalents
  (no unlock endpoint; SQL schema instead of typed fixtures).
- `spedsql_peer_count` maps to `Handle::status().peers` (live QUIC
  sessions by node id); `spedsql_peer_connected{peer=...}` maps to a
  `connected` entry for that node id in
  `Handle::status().peers_diagnostics`.
- `spedsql_membership_count` includes self, while Rust
  `Status.members` counts only discovered peers (`View.members`
  excludes the local node), so the discovery gate is `members >= 2`
  (plus `peers == 2` to prove the full mesh formed) instead of `>= 3`.
- The chain leg disables gossip membership
  (`ReplicationOptions.membership = Some(false)`, a minimal harness
  addition): the Rust engine auto-dials discovered member addresses,
  which would self-heal the chain into a full mesh and defeat the
  "forwarding only, no direct session" proof. Transaction push and
  anti-entropy run over the peer sessions, so forwarding is unaffected.
  The bootstrap leg keeps membership enabled (the default).
- The two legs use distinct runtime dirs (`partial-mesh-chain`,
  `partial-mesh-bootstrap`) so the two `#[test]` fns never share or
  wipe one another's directory under parallel execution.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/partial-mesh/Cargo.toml
```
