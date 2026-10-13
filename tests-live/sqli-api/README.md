# sqli-api (Rust port, test-side equivalent)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/sqli-api/`
(`sqli_api_test.go`, `TestSQLApplicationRoutesRemoved`).

## What it proves

Go pins the Phase 6 cutover: former SQL routes reject requests with 410
before parsing them, while the managed typed API stays writable and
replicates. The Rust product has no HTTP routes, so this port pins the
security property those 410s existed to enforce — untrusted SQL text is
never executed — at the layers that do exist: SQL payloads replicate as
inert byte-exact data with the schema untouched, and retired-route
request bytes sprayed at the replication port die on arrival.

## Leg mapping (Go -> Rust)

- `POST /v1/query` + `/v1/exec` -> 410 before parsing becomes: the
  literal retired-route request bytes (full POST requests + JSON
  bodies) sent as UDP datagrams at the replication port attach no
  session and change no rows or schema.
- Stacked/exfil payloads rejected becomes: the same payloads plus DDL
  attempts (`DROP TABLE`, `CREATE TABLE`, stacked `DELETE`) inserted
  through the managed typed path land byte-exact on every node with
  `sqlite_master` snapshots bit-identical before/after on both nodes.
- `before-sql-probe` / `after-sql-probe` typed writes with per-node
  counts are unchanged (managed API writable and replicating).
- The 410 status code and typed-RIME migration guidance have no
  target (no HTTP server); port those assertions if/when the Rust
  product adopts the HTTP API contract.

## Pass criteria

- Pre-probe write converges (20 s); schema baselined both nodes.
- 6 payloads each count exactly 1 everywhere (20 s each); schemas
  bit-identical after.
- Retired-route byte spray: peer counts unmoved, schemas unmoved,
  row set converged.
- Post-probe write converges with per-node counts exact.

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/sqli-api/Cargo.toml
```
