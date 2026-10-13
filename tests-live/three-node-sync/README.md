# three-node-sync (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/three-node-sync/`
(`README.md` + `sync_test.go`: `TestThreeNodeConcurrentWritesConverge`).

## What it proves

Three encrypted, mutually authenticated nodes each commit 30 uniquely-named
rows concurrently; every node must converge on all 90 rows in canonical
ordered sequence within 25 s.

## Pass criteria

- `Cluster::new` opens 3 meshed nodes under `tests-live/runtime/three-node-sync/`.
- Each node inserts `node-{i}-row-{00..29}` (one transaction per row).
- Within 25 s (100 ms poll), `SELECT name FROM records ORDER BY name` on
  every node equals the full 90-name canonical list.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`records(id INTEGER PRIMARY KEY, name TEXT)`) instead of the
  managed typed-records HTTP API (`/v1/typed/insert`, `/v1/typed/names`);
  canonical order comes from `ORDER BY name` rather than the API response.
- Concurrency via `std::thread::scope` + a start `Barrier` (one worker per
  node, cloned `Database` handles) instead of goroutines + `WaitGroup`.
- Row count follows the Go test code (30 rows/node, 90 total), not the "20
  rows / 60 total" wording in the Go README and the TODO summary, which the
  code (`rowsPerNode = 30`) contradicts.
- No SHA-256 digest step: the Go test body asserts ordered-list equality
  (`fmt.Sprint(got) != fmt.Sprint(want)`); the digest mention in the Go
  README describes the GALVANIZE benchmark this ports the checks from, not
  an assertion in the test itself.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/three-node-sync/Cargo.toml
```
