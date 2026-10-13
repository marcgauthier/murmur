# typed-bridge (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/typed-bridge/`
(`typed_bridge_test.go`; the Go dir has no README.md).

## What it proves

Two single-node clusters form the Low (exporter) and High (importer) bridge
domains sharing one staging directory (`runtime/typed-bridge-low/staging/`,
stream `typed-stream`). One `#[test]`
(`typed_bridge_imports_into_native_rime`, from
`TestBridgeImportsIntoNativeRIME`) proves the Go pass criteria: a typed row
inserted on Low is exported and imported through High's typed durable path
(`high_low::import`, with schema/column mapping and transaction verification);
a High write overrides the field (automatic ownership via `promote`); a Low
update while the field is High-owned is imported but suppressed; after
`Bridge::release` the next Low update flows through; and the imported result
survives a High-node restart (`close()` + reopen).

Pass criteria: High converges to 1 row titled `bridge-native-record` after
export+import; after the High override plus a drained Low update, High still
shows `high-owned-record` = 1 and `low-update-before-release` = 0; after
release and a second Low update, High shows `low-after-release` = 1, including
after the restart.

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).
Convergence waits are 30 s with 50 ms polls, matching the Go timeouts.

## Deviations from the Go original

- Plain SQL via `Database::transaction`/`Database::read` replaces the typed
  HTTP API; there is no HTTP server in the Rust product.
- `TypedRename` updates the LWW `title` field rather than the primary key, so
  row identity is preserved (the `typed-records` port convention); this is
  also required because `promote` only claims ownership of non-PK cells.
- `BridgeExport`/`BridgeImport` map to direct `Bridge::capture()` (Low) and
  `Bridge::process()` (High) calls. Per-call bundle counts are not asserted:
  the replication engine also drives capture/process in the background, so
  export/import progress is proven by row convergence (a sync pump runs inside
  every wait poll) instead of `published`/`imported` counters.
- The suppression leg drains the bridge to idle before asserting, proving the
  Low update arrived yet stayed suppressed (bounded 100-pump idle loop, the
  `tests/bridge_files.rs` product-test pattern).
- `BridgeRelease` takes the postcard-encoded PK value vector
  (`encode(&vec![Value::Text(name)])`) instead of Go's hex row ID, per the
  `high_low::release` key contract.
- Bridge keys are fixed test constants (signer seed, X25519 secret, domain
  and node IDs) following the `tests/bridge_files.rs` fixed-key convention;
  the staging dir lives under Low's runtime dir so it is retained on failure
  and cleaned on pass together with the cluster dirs.
- Two clusters in one test means two `RESULT` lines (one per cluster name)
  and two forensics dumps on failure; cluster setup itself is outside the
  `catch_unwind` body, so a setup failure panics without artifact retention,
  matching the single-cluster template behavior.

## Shared-harness addition

`harness/src/cluster.rs` only: `Cluster::node_origin_pubkey(idx)` returns the
node's replication origin public key (ed25519 verifying-key bytes derived
from the stored signing seed). Required because High's
`bridge::Config::receiver(.., trusted_origins)` must pin Low's origin key so
imported transactions verify, and the seeds are generated inside
`Cluster::new` with no accessor. No struct or behavior change.

## Compile gate (only gate; never run)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/typed-bridge/Cargo.toml
```
