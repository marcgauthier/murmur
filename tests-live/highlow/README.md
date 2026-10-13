# highlow (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/highlow/`
(`highlow_test.go`, `ownership_reorder_test.go`, `README.md`).

## What it proves

Eight `#[test]` fns (distinct `runtime/` names so parallel execution stays
isolated) port the eight Go tests one for one over a `contacts(id, name,
score)` table plus an `email` column for the schema leg:

- `disconnected_transfer_with_one_way_role_gating`
  (`TestHighLowDisconnectedTransferLive`): distinct Low/High DBIDs, one-way
  role gating (exporter `process()` and importer `capture()` are API-level
  no-ops), and Low/High contact-state equality after export+import.
- `gap_delivery_with_duplicate_recovery`
  (`TestHighLowDuplicateReorderRecoveryLive`): three rounds export three
  bundles; withholding the middle one applies only the first (gap, no
  skip-ahead); restoring it converges all three; duplicate redelivery
  changes nothing and quarantines nothing.
- `two_receivers_converge` (`TestHighLowTwoReceiversConvergeLive`): two
  High receivers import the same staging independently and the High mesh
  converges both to the Low state.
- `forgeries_rejected_without_partial_apply`
  (`TestHighLowForgeriesRejectedLive`): rogue-domain, wrong-recipient,
  tampered, and oversized artifacts each fail closed alone (quarantined,
  High stays empty, no progress recorded); wrong-extension artifacts
  (`.txt`, dotfile) are ignored in place; an unreadable file fails the
  import with an error and no partial apply; the genuine bundle still
  applies afterwards.
- `restart_resumes_without_recapture_or_reapply`
  (`TestHighLowRestartResumeLive`): a Low restart exports nothing new
  (bundle file set unchanged); a High restart re-applies nothing (both
  contacts held exactly).
- `schema_hold_survives_restart_and_releases_on_local_migration`
  (`TestHighLowSchemaHoldMigrationLive`): the unknown `email` column holds
  the bundles (High stays empty, no DDL crosses); the hold survives a High
  restart; a local `ADD COLUMN` migration releases the held bundles and
  the email reads back.
- `signer_rotation_and_outage_catch_up`
  (`TestHighLowKeyRotationAndOutageResumeLive`): round 1 imports under the
  predecessor key; both sides rotate (config edit plus restart) and round
  2 imports on top; a retired-signer replay is quarantined while a
  current-key duplicate stages cleanly; then a 5-event outage queues
  durably across a Low restart and catches up to exactly
  `{ann: 1, bob: 2, backlog: 4}`.
- `high_owned_field_reorder_converges_across_peers`
  (`TestHighLowOwnershipReorderAcrossPeersLive`): peer A
  (override-then-import) and peer B (import-then-override) converge over
  the mesh on the High bytes with High-owned provenance (a further Low
  update arrives yet stays suppressed on both); release on A converges
  both back to the latest Low value.

Pass criteria: per-test contact maps converge exactly as listed; gap,
hold, forgery, and suppression legs assert exact visible row sets;
restart legs assert exact file-set/row stability; rotation asserts
quarantine placement for the replay and clean staging for the duplicate.
Adversarial HLC ordering stays pinned at the DB layer by
`TestBridgeShadowReorderConvergence`, as in Go.

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).
Convergence waits are 30 s with 50 ms polls, matching the Go timeouts.

## Deviations from the Go original

- Plain SQL via `Database::transaction`/`Database::read` replaces the typed
  HTTP API; there is no HTTP server in the Rust product. Sequential
  single-writer `score` increments replace `TypedCounterAdd` (identical
  convergence); `email` replaces the typed note field.
- `BridgeExport`/`BridgeImport` map to direct `Bridge::capture()` (Low) and
  `Bridge::process()` (High) calls.
- Separate Low/High staging dirs plus a one-way test ferry replace Go's
  shared staging dir: the Rust engine also drives capture/process in the
  background, so only ferried bundles may be visible to High. This keeps
  withhold/forgery/outage legs deterministic; bundle filenames sort in
  export order, so the middle-bundle withhold mirrors Go.
- Per-call bundle counts and `BridgeStatus` progress (`Applied`, `Missing`,
  `Holds`, `pending_events`) have no public Rust equivalent and are racy
  under background driving: all are proven behaviorally (exact row sets,
  file-set stability, quarantine placement, exact converged totals).
- `Bridge::process()` quarantines bad artifacts into
  `staging/quarantine/` and returns `Ok` (Go: import error); the forgery
  legs assert quarantine + empty High + no recorded progress instead of an
  error. Only the unreadable-file leg returns `Err`, like Go. That leg is
  created unreadable atomically (no background staging window) and skips
  itself when the file stays readable (e.g. running as root).
- Rust traps schema holds internally and returns `Ok`, so there is no
  `waiting-schema` error to assert; the hold is proven by empty High plus
  `email` still absent (no DDL crossed), then release after migration.
- Rust pins a single signer per source node, so Go's overlap trust during
  rotation is inexpressible: round 1 is imported before the rotation
  instead of after, and the retired-signer replay plus current-key
  duplicate distinguish signature failure from dedup.
- Field provenance (`Provenance` owner tags) has no public Rust API, so
  High-owned is proven by a drained Low update staying suppressed on both
  peers, and release convergence by both peers showing the Low value.
- Bridge keys are fixed test constants shared by all tests in this file
  (signer seeds, X25519 secrets, domain and node IDs) following the
  `tests/bridge_files.rs` fixed-key convention; clusters never interact
  across tests. Each test owns 2-4 `runtime/` names (one `RESULT` line
  each); cluster setup is outside the `catch_unwind` body, matching the
  template behavior.

## Shared-harness addition

`harness/src/cluster.rs` only: `Cluster::set_bridge_config(idx, config)`
replaces a stopped node's stored bridge config (applied on the next
`start_node`; errors if the node is running). Required for the signer
rotation leg (Go `rewriteNodeConfig` parity): bridge configs bind at open
with no runtime update API, so rotation is a config edit plus restart.
No struct or behavior change.

## Compile gate (only gate; never run)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/highlow/Cargo.toml
```
