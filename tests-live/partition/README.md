# partition (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/partition/`
(`README.md` + `partition_test.go`:
`TestFourNodePartitionIsolationAndHealing`).

## What it proves

Four encrypted, mutually authenticated QUIC nodes form a mesh, split into
two isolated pairs (`{node1, node2}` vs `{node3, node4}`), and write
independently. Each side must remain isolated (equal counts, divergent
digests), then after restoring cross-partition peers all four nodes must
converge on identical ordered state, and a post-heal write must replicate
everywhere.

## Pass criteria

- `Cluster::new` opens 4 meshed nodes under
  `tests-live/runtime/partition/`.
- Baseline insert on node 0 reaches all 4 nodes within 10 s.
- After removing all 8 cross-group peer links (both directions), one
  insert per side converges inside each pair (both nodes at 2 rows)
  within 15 s, with no node ever exceeding 2 rows (leak = fail).
- Intra-group digests match; cross-group digests differ (isolation).
- After re-adding all 8 cross-group links, all 4 nodes converge on 3
  rows within 15 s with bit-identical digests.
- A post-heal insert on node 1 reaches all 4 nodes within 10 s.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records HTTP API (`TypedContentionInsert` /
  `TypedContentionRows`); marker rows use small integer ids
  (1/100/200/300) instead of formatted UUIDs, and the digest is the
  `Debug` rendering of the sorted `(id, name)` pairs instead of `%#v` of
  the typed rows. The Go `AwaitUnlock` / `TypedRecords` / `TypedContention`
  cluster options have no Rust equivalents (no unlock endpoint; SQL
  schema instead of typed fixtures).
- `remove_peer`/`add_peer` errors propagate (`.expect`) instead of Go's
  ignored returns; the split/heal assertions are otherwise identical.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/partition/Cargo.toml
```
