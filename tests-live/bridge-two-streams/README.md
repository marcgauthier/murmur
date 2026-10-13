# bridge-two-streams (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/bridge-two-streams/`
(`two_streams_test.go` + `README.md`).

## What it proves

Two independent single-node Low clusters (distinct DBIDs, CAs, keys,
streams, staging dirs) export managed rows into `stream-a` and `stream-b`;
a two-node High cluster imports stream-a on node0 and stream-b on node1
(per-node `bridge::Config::receiver`) and converges both domains' rows over
its mesh (imported shadows/owners replicate as auxiliary records). One
`#[test]` (`two_low_domains_into_one_high`, from
`TestTwoLowDomainsIntoOneHigh`) proves the Go pass criteria: round 1 lands
5 A-rows + 5 B-rows on both High nodes with exact names; a drain-to-idle
leg proves duplicate delivery applies nothing twice (receipt idempotence);
a per-domain Low update flows through to both High nodes, proving
Low-owned provenance; the names are restored; a stream-a-only round 2
(a-5..a-7) arrives; and a stream-b-only round 3 (b-5, b-6) delivers exactly
once, proving stream-b progress was undisturbed by the stream-a round.

Pass criteria: both High nodes converge on exactly the 10 round-1 names,
still exactly those 10 after idle pumps, the 2 renamed probe values then
the restored 10, exactly the 13 round-2 names (`a-0..a-7`, `b-0..b-4`),
and exactly the 15 round-3 names (plus `b-5`, `b-6`).

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).
Convergence waits are 60 s with 200 ms polls, matching the Go timeouts.

## Deviations from the Go original

- Plain SQL via `Database::transaction`/`Database::read` replaces the typed
  HTTP API; there is no HTTP server in the Rust product. Row IDs keep the
  Go `%032x` shape as the `TEXT PRIMARY KEY`.
- `BridgeExport`/`BridgeImport` map to direct `Bridge::capture()` (Low) and
  `Bridge::process()` (High) pumps, with a pump inside every wait poll.
  Per-call bundle counts are not asserted: the replication engine also
  drives capture/process in the background, so progress is proven by row
  convergence instead of `published`/`imported` counters.
- `BridgeStatus` progress has no public Rust API (the applied cursor lives
  in internal `09/bridge/applied/<db>/<stream>` spool keys), so the
  byte-identical progress assertion is replaced by behavior: exact B-row
  content after the stream-a-only round, plus the added stream-b-only
  round 3, which can only deliver exactly once from an intact cursor (a
  cursor skipped forward would gap-skip the new bundles and fail the wait;
  backward resets stay benign via receipt idempotence).
- `Provenance` has no public Rust API (ownership/shadow records are
  internal), so `assertLowOwned` is replaced by behavior: a Low update to
  one row per domain must become visible on both High nodes. A High-owned
  field would suppress the Low update (the `typed-bridge` ownership
  semantics), so flow-through proves Low ownership. Probe names are then
  restored so round 2 keeps the exact Go name vectors.
- The duplicate-delivery gate (named in the Go README) is an explicit
  drain-to-idle leg: after round-1 convergence every bridge is pumped to
  idle (bounded 100-pump loop, the `tests/bridge_files.rs` product-test
  pattern) and both High nodes must still show exactly the 10 names.
- `manual_peers` + `mesh()` after the local migrates (the `typed-records`
  convention) so a local migrate never races schema auto-adoption; bridge
  keys are fixed test constants following the `tests/bridge_files.rs`
  fixed-key convention; each staging dir lives under its Low's runtime dir
  so it is retained on failure and cleaned on pass with the cluster dirs.
- Three clusters in one test means three `RESULT` lines (one per cluster
  name) and three forensics dumps on failure; cluster setup itself is
  outside the `catch_unwind` body, so a setup failure panics without
  artifact retention, matching the single-cluster template behavior.

## NOT ported (missing product behavior, documented, no silent skip)

`TestCrossDomainIdentityCollisionFailsClosed` is not ported. The Go product
rejects a cross-domain same-row-ID import with `ErrIdentityCollision`
(`bridge: imported row identity collides with High-owned data`,
`bridge/import.go`) and leaves first-writer state intact on both High
nodes. The Rust product has no row-identity collision detection:
`high_low::import` (`src/high_low.rs`) stores one shadow per source domain
(`09/bridge/shadow/<db>/...`) and `join` projects coexisting shadows for
the same High row deterministically (later key order wins per cell), so a
second domain's same-ID row silently merges instead of failing closed.
Additionally, `Bridge::process()` (`replication/src/bridge.rs`) traps
import failures into internal `09/bridge/hold/` spool records and returns
`Ok`, and no `Database` API exposes bridge progress, provenance, hold, or
quarantine state — so even a future import-side rejection would surface no
public failing-closed signal to assert. Porting this leg needs a product
change (identity-collision detection on the import path plus an observable
rejection); asserting the current merge behavior would enshrine a fail-open
divergence from the Go gate.

## Shared-harness addition

None. The port uses only the existing harness API (`Cluster` with
per-node bridge configs, `node_origin_pubkey`, `wait_until`,
`artifacts::runtime_dir`).

## Compile gate (only gate; never run)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/bridge-two-streams/Cargo.toml
```
