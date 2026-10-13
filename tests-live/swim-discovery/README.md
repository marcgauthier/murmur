# swim-discovery (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/swim-discovery/`
(`README.md` + `swim_discovery_test.go`:
`TestSeedOnlyDiscoveryFormsFullMesh`,
`TestSeedKillAndRediscovery`).

## What it proves

- Seed-only discovery: four nodes start with a single SWIM seed address
  and no static peers; a full mesh must form purely via discovery
  (membership view plus a live replication session to every peer), and
  writes from every node must converge with identical contents.
- Seed kill and rediscovery: after baseline convergence the seed is
  stopped, survivors must observe the death and keep converging writes
  while it is down, then the restarted seed rejoins through one live
  member and the full mesh reconverges with identical contents.

## Pass criteria

- Discovery leg (`swim-discovery` runtime dir): all 4 nodes see 3
  discovered members each and hold a live session to every other node
  (all 12 directed pairs) within `MURMUR_SWIM_DISCOVERY_SECONDS`
  (default 60 s); 10 inserts per node (40 rows) then converge exactly
  (identical ordered names) on all 4 nodes within 150 s.
- Rediscovery leg (`swim-rediscovery` runtime dir): same discovery
  gates; 8 baseline rows on node 3 converge on all 4 nodes; after the
  seed stops, all 3 survivors drop below 3 live sessions each within
  the discovery bound; 12 survivor rows (round-robin over nodes 2-4)
  converge on the 3 survivors; after the seed restarts and rejoins via
  node 2, membership, full session mesh, and all 20 rows reconverge on
  all 4 nodes.

## Env knobs

- `MURMUR_SWIM_DISCOVERY_SECONDS` (default 60): bound for the
  membership, session-mesh, and death-observed gates (same name as Go).
  Convergence waits keep the Go 150 s loaded-box bound (fixed).
  Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records HTTP API (`TypedInsert` / `TypedNames`); row ids
  are node-disjoint small integers instead of typed-record keys. The Go
  `AwaitUnlock` / `TypedRecords` cluster options have no Rust equivalents
  (no unlock endpoint; SQL schema instead of typed fixtures).
- `spedsql_membership_count` includes self, while Rust
  `Status.members` counts only discovered peers (`View.members`
  excludes the local node), so the discovery gate is `members == 3`
  instead of `== 4`.
- `spedsql_peer_connected{peer=...}` maps to a `connected` entry for
  that node id in `Handle::status().peers_diagnostics`; the session-mesh
  gate checks all 12 directed pairs exactly like the Go original.
- `assertNoStaticPeers` holds by construction: `manual_peers: true`
  means the harness issues zero `add_peer` calls, and the discovery leg
  issues none either (Rust `PeerDiagnostics` carries no static/dynamic
  flag, so there is no config file to re-audit at runtime).
- Seed "kill" is `stop_node` (`close()`) rather than SIGKILL: a true
  async kill needs the replication-enabled fixture mode that the
  harness does not provide yet. `close()` gossips a `Left` state and
  tears down the QUIC sessions; survivors observe the death via live
  session teardown (`status().peers` drops 3 -> 2 as sessions exit and
  the `peers` map is pruned) rather than via SWIM suspect timers, and
  there is no alive/suspect/dead breakdown in `Status` to assert on.
  Note `Status.members` retains `Left` entries, so the death gate uses
  session counts, not the member count.
- `patchSeedBootstrap` maps to one `add_peer(0, 1)` after `start_node`:
  the restarted seed (empty bootstrap list, fresh ephemeral port)
  dials a single live member and gossip plus discovery dialing rebuild
  the rest of the mesh from there; no full `mesh()` call is made, so
  the re-formed mesh is still discovered, not configured.
- Stall/timeout forensics (`dumpStallForensics`, goroutine stacks,
  per-peer byte flow) map to the harness `dump_forensics` (truncated
  log + per-node `forensics-<reason>-*` files); the periodic
  converge-progress logging is omitted.
- The two legs use distinct runtime dirs (`swim-discovery`,
  `swim-rediscovery`) so the two `#[test]` fns never share or wipe one
  another's directory under parallel execution.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/swim-discovery/Cargo.toml
```
