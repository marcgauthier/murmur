# backup-restore (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/backup-restore/`
(`README.md` + `backup_restore_test.go`).

## What it proves

- `backup_restore_rejoin_mesh` (Go `TestBackupRestoreRejoinMesh`): three
  nodes converge on 20 rows; node3 stops; its data dir is backed up offline
  through the library, wiped to simulate total loss, and restored under a
  fresh writer identity; node3 restarts on the restored data and the mesh
  reconverges with no lost rows, with the restarted node's identity equal
  to the fresh one.
- `restore_rejects_stale_identity` (Go `TestRestoreRejectsStaleIdentity`):
  rollback safety — opening restored data under the old NodeID is
  rejected, while the fresh identity opens cleanly with the row intact.
- `typed_backup_restore_round_trip_live` (Go
  `TestTypedBackupRestoreRoundTripLive`): a typed row written through
  policy ops (PN_COUNTER / OR_SET / MAX / MIN) survives backup of the
  encrypted store, fresh-identity restore, and reopen with identical
  materialized values.

## Env knobs

Standard harness knobs only (`MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`). No scenario-specific knobs.

## Pass criteria

- Rejoin leg: all three nodes hold the same 20 rows within 90 s of the
  restart; `Database::node_id` on node3 equals the fresh id; a
  post-rejoin survivor write converges to 21 rows everywhere.
- Stale-identity leg: open under the old id fails; open under the fresh
  id succeeds and returns the backed-up row.
- Typed leg: counter/set/peak/floor projections after restore equal the
  pre-backup values exactly.

## Deviations from the Go original

- In-process multi-`Database` cluster instead of daemon processes; no
  HTTPS/mTLS admin API exists in the Rust product.
- Plain SQL plus `policy.rs` ops (`counter_add`, `set_add`, `min`, `max`)
  instead of the managed typed-records HTTP API.
- `Cluster::stop_node` / `restart_node_with_identity` instead of
  StopNode/StartNode + config rewrite + unlock polling; the offline
  backup/restore legs use `Database::open` (no replication) +
  `Database::backup` / `Database::restore_with_options`, and the TLS
  reissue + fresh origin key are handled by the harness method (new
  `tls/` files, fresh signing seed registered in the node's own
  `trusted_origins`).
- The `/metrics` `node_id` label assertion becomes
  `Database::node_id() == fresh`.
- Extra post-rejoin survivor write (21st row) proves the restored node is
  a live replication peer again. The restarted node itself must not
  originate until every peer restarts with its fresh origin key
  (transaction verification is by origin; peers only learn origins at
  open); the Go test likewise writes nothing from node3 after rejoin.
- `dst must not exist` for `Database::restore_with_options` matches Go's
  `RemoveAll(data)` + `Restore(Overwrite)` sequence.
