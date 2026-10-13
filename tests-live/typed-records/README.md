# typed-records (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/typed-records/`
(`typed_records_test.go`, `models_test.go`, `README.md`).

## What it proves

Two in-process nodes share one typed table (`live_typed_records`, TEXT primary
key, PN_COUNTER / OR_SET / MAX / MIN policy columns); seven `#[test]` fns (one
cluster each) prove the Go pass criteria:

| Rust test | Go source |
|---|---|
| `typed_record_replicates_and_rebuilds_across_restart` | `TestTypedRecordReplicatesAndRebuildsAcrossRestart` |
| `typed_schema_migration_adopted_by_older_peer_and_retains_new_fields` | `TestTypedSchemaMigrationAdoptsOnOlderPeerAndRetainsNewFields` |
| `typed_crdt_writes_converge_after_offline_partition` | `TestTypedCRDTWritesConvergeAfterOfflinePartition` |
| `typed_empty_peer_catches_up_through_snapshot` | `TestTypedEmptyPeerCatchesUpThroughSnapshot` |
| `typed_concurrent_schema_branches_merge_across_partition` | `TestTypedConcurrentSchemaBranchesMergeAcrossPartition` |
| `typed_files_replicate_and_fetch_without_sql` | `TestTypedFilesReplicateAndFetchWithoutSQL` |
| `typed_node_local_rows_persist_only_on_their_node` | `TestTypedNodeLocalRowsPersistOnlyOnTheirNode` |

Pass criteria: replicated insert + peer subscription update + explicit-tx
commit/rollback + counter/set/extrema convergence + restart rebuild; additive
migration adopted by the older peer with the new field surviving writes from
both sides and a restart; offline counter sum (18) and set union on both nodes;
12-row snapshot catch-up proven by flat log counters; union schema plus both
branch values on both nodes; file metadata/object/delete-tombstone replication
plus traversal confinement; node-local row present locally and absent remotely.

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
`MURMUR_LIVE_UNLOCK_STALL_SECONDS`, `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).
Convergence waits are 30 s with 50 ms polls, matching the Go timeouts.

## Deviations from the Go original

- Plain SQL via `Database::transaction`/`Database::read` plus `policy.rs` ops
  (`counter_add`, `set_add`/`set_remove`, `min`, `max`) replaces the managed
  typed HTTP API; there is no HTTP server in the Rust product.
- Merge policies are bound at open through the new harness
  `ReplicationOptions::merge_policies` overlay (single shared-harness
  addition; see below).
- Schema "epochs" map to column-presence polls plus the `schema_adoptions`
  metric; `/v1/schema` manifest checks map to `Database::schema()` and
  `PRAGMA table_info` reads.
- `TriggerGC` maps to `Database::collect_retention()`; the snapshots-received
  metric does not exist in the Rust product, so the snapshot leg uses the
  established flat `(received, applied)` counters proof (see the
  `corrupt-snapshot` port). The receiver's snapshot donor is pinned via
  `trusted_snapshot_sources` (the harness trusts nobody by default).
- The branch-merge leg relies on legacy-mode (no `schema_master`) mutual
  adoption of additive manifests; there is no V4 record-type binding step, so
  the union columns themselves are the assertion surface.
- `TypedRename` renames the LWW `title` field rather than the primary key, so
  row identity is preserved without a delete+insert cycle.
- File fetch is automatic (`fetch_interval`, no explicit `FetchFile` call
  exists); traversal probes run against `get_file`/`file_metadata`/
  `search_files` instead of HTTP routes.
- Clusters use `manual_peers` + `mesh()` after the local migrates so a local
  `migrate` never races schema auto-adoption of the identical DDL.
- NOT ported (missing product surface, documented, no silent skip):
  `TestTypedModeRejectsSQLAPIWithoutMutatingRecords` asserts HTTP 410s from
  `/v1/exec` and `/v1/query`, which cannot exist without an HTTP API (same
  class as the product-blocked `sqli-api` scenario); the two `models_test.go`
  suites need `Config.Models` reflection collections with UUIDv5 identity,
  item API, indexed/rich-predicate queries, and `MigrateModels`, none of which
  exists in the Rust product.

## Shared-harness addition

`harness/src/cluster.rs` only: `ReplicationOptions::merge_policies`
(`Option<BTreeMap<(String, String), murmur_replication::policies::Policy>>`),
applied to every node's replication `Config` before open. Required because
policies bind at open (`policy::configure`) and no overlay existed.

## Compile gate (only gate; never run)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/typed-records/Cargo.toml
```
