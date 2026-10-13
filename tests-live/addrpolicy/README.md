# addrpolicy (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/addrpolicy/`
(`README.md` + `addrpolicy_test.go`:
`TestAddressPolicyAcrossProcesses`).

## What it proves

Address admission is enforced on the replication mesh: node1 admits
loopback peers (`127.0.0.0/8`), node2's TEST-NET policy
(`198.51.100.0/24`) excludes every actual loopback socket address, and
node3 is unrestricted. The admitted node1-node3 pair must form a session
and replicate, while the denied node2 keeps its write local.

## Pass criteria

- `Cluster::new` opens 3 nodes under `tests-live/runtime/addrpolicy/`
  with the `allowed_networks` overlays above.
- Topology `[1, 0, 1]` forms within 20 s (50 ms poll): the allowed pair
  has one session each, the denied node has none.
- One row per node (`allowed-loopback`, `denied-testnet`,
  `unrestricted-loopback`); nodes 1 and 3 converge to 2 rows each
  within 20 s (50 ms poll).
- After a 750 ms settle window (several dial/accept retries) node2
  still holds only `denied-testnet`, and node1 holds exactly 2 rows.
- Every node has an isolated directory.

## Env knobs

- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`, row ids
  node-disjoint via `(node+1)<<32 | 1`) instead of the managed
  typed-records HTTP API (`TypedInsert`, `TypedNames`); canonical order
  comes from `ORDER BY name`. The row *names* keep the Go values.
- Live session counts come from the replication `Handle::status().peers`
  instead of `/v1/status` `connected_peers`.
- The scenario adds the registry dep `ipnet = "2"` (same version as the
  harness) to name the `IpNet` overlay type; product access stays
  through the harness re-exports.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/addrpolicy/Cargo.toml
```
