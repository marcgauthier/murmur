# encryption (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/encryption/`
(`README.md` + `encryption_test.go`:
`TestTypedEncryptedStoreKeepsRecordsEncryptedAcrossRestart`).

## What it proves

A single node stores one typed row carrying a secret marker, stops,
and its whole node directory is scanned for the marker in plaintext;
the node restarts, a wrong-key open is rejected, and the correct key
unlocks the row again.

## Pass criteria

- `Cluster::new` opens cluster `encryption` (1 node) under
  `tests-live/runtime/encryption/`.
- The typed table is migrated, the marker row is inserted and flushed,
  and its count reads back as 1 before the restart.
- After `stop_node`, no regular file under the node dir (durable
  spool files plus `tls/`) contains the marker bytes.
- `Database::open` on the stopped node's data dir with an all-zero
  key fails with exactly `Error::WrongKey` (and leaves the store
  alone, so the restart below is unaffected).
- `start_node` (same dir + key) succeeds and the marker row count
  reads back as 1.

## Env knobs

- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- Plain SQL (`records(name TEXT PRIMARY KEY)`, `INSERT` /
  `SELECT count(*)`, `migrate` + `flush`) instead of the managed
  typed-records HTTP API (`Config.Tables`, `TypedInsert`,
  `TypedCount`); the marker is the row key in both.
- Stop/restart is in-process `close()` + reopen via
  `stop_node`/`start_node`, not daemon `SIGTERM` + re-exec.
- The wrong-key leg is a direct `Database::open` asserting
  `Error::WrongKey` (the `tests/persistence.rs` precedent): the Rust
  product has no Remote Unlock endpoint, so there is no unlock HTTP
  status to assert. The zero key mirrors the Go all-zero wrong key.
- The plaintext scan uses byte windows over raw file bytes instead
  of Go's string conversion, so binary spool files are covered too.
- Encrypted replication of sustained concurrent writes stays covered
  by the replication integration scenarios, as in the Go README.
- No shared-harness addition was required by this scenario.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/encryption/Cargo.toml
```
