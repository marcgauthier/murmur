# scale-mesh (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/scale-mesh/`
(`README.md` + `scale_mesh_test.go`: `TestTenNodeMeshConvergesBounded`).

## What it proves

Ten encrypted, mutually authenticated nodes mesh with the default fanout of
4 while two origins write 400 rows (300 on node 1, 100 on node 8). Smaller
suites never exceed the fanout, so this is the only live proof that peer
selection and rotation hold at scale: every node must converge to identical
row sets, and a 2 s sampler asserts no node ever selects more than 4
replication targets for the whole run.

## Pass criteria

- `Cluster::new` opens 10 meshed nodes under `tests-live/runtime/scale-mesh/`
  with the default replication fanout (4, verified in
  `replication/src/lib.rs` `Config::new`).
- Node 1 inserts rows `0..300`, node 8 inserts rows `300..400` (one
  transaction per row, ids `%032x`, names `w-{i}`); the second origin proves
  multi-hop forwarding, not just hub fan-out.
- Within 300 s (200 ms poll), `SELECT COUNT(*) FROM records` on every node
  equals 400.
- The PK-ordered `id:name` row vector on every node equals node 1's.
- Every 2 s sample (plus one final sample after convergence) finds at most 4
  connected push-mode peers per node; any breach fails the test.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`records(id TEXT NOT NULL PRIMARY KEY, name TEXT NOT NULL)`) instead of the
  managed typed-contention RIME API (`TypedContentionInsert`,
  `TypedContentionRows`); the `phone`/`score` contention columns have no
  equivalent because the Rust product has no typed-records layer.
- The fanout sampler counts connected peers with `push_mode` in the
  in-process replication `Status` instead of polling `/v1/status`
  `selected_peers` over HTTPS; the engine caps push peers at `fanout` on
  insertion and swaps them one-for-one during rotation
  (`replication/src/engine.rs`), so the bound is the same invariant.
- The sampler is a `std::thread` with an `AtomicBool` stop flag and a
  bounded `sync_channel` (non-blocking `try_send` drops on full) instead of
  a goroutine with `stop`/`breaches` channels.
- Canonical row vectors are compared directly instead of comparing SHA-256
  digests: byte-identical vectors imply identical digests, and this avoids a
  hashing dependency in the scenario crate.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/scale-mesh/Cargo.toml
```
