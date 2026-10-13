# allow-nodes (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/allow-nodes/`
(`README.md` + `allow_nodes_test.go`:
`TestAllowedNodeIDsAcrossProcesses`).

## What it proves

Certificate validity alone does not authorize a peer: node2's identity
allow-list accepts only node1, while node1 and node3 accept every
identity. All three nodes write concurrently for
`MURMUR_ALLOW_NODES_WRITE_SECONDS` (default 180 s); the unauthorized
node3-to-node2 session must stay absent throughout, and afterwards every
node must hold all rows with identical ordered digests (node3's rows
reach node2 through node1).

## Pass criteria

- `Cluster::new` opens 3 nodes under `tests-live/runtime/allow-nodes/`,
  with node2's `allowed_peers` set to `{node1}`.
- Initial topology `[2, 1, 1]` forms within 20 s (50 ms poll).
- Each node inserts `node{n}-write-{seq}` rows (one transaction per row)
  on a 100 ms cadence for the write window; at least 3 rows mesh-wide.
- Within 45 s (100 ms poll) every node holds all rows and the
  NUL-joined ordered digests are identical.
- No direct node2-node3 session exists in either direction, and the
  final topology is still `[2, 1, 1]` (2 s bound).

## Env knobs

- `MURMUR_ALLOW_NODES_WRITE_SECONDS` (default 180, Go parity): length of
  the concurrent-write window.
- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`, row ids
  node-disjoint via `(node+1)<<32 | seq`) instead of the managed
  typed-records HTTP API (`TypedInsert`, `TypedNames`); canonical order
  comes from `ORDER BY name`.
- Live session counts come from the replication `Handle::status().peers`
  and `peers_diagnostics[].connected` instead of `/v1/status`
  `connected_peers` and `/v1/debug/peers`.
- Writers are `std::thread::scope` workers sharing an `AtomicBool` stop
  flag (plus per-node `AtomicI64` counters and a first-error slot)
  instead of goroutines + channel + `WaitGroup`; a failed transaction
  or flush records the Go-shaped `node{n} write {seq}: {err}` message
  and stops that writer.
- Fixed distinct node ids (`[0xA1; 16]`, `[0xB2; 16]`, `[0xC3; 16]`)
  instead of random `NewNodeID()` values so the allow-list overlay can
  be built before the cluster opens (corrupt-snapshot precedent); the
  property under test is unchanged.
- The final leg adds explicit both-directions `peer_connected` asserts
  for the unauthorized pair; they follow from the `[2, 1, 1]` counts
  but name the forbidden session directly.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/allow-nodes/Cargo.toml
```
