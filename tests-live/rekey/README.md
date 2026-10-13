# rekey (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/rekey/`
(`README.md` + `rekey_test.go`:
`TestTypedStorageKeyRekeySurvivesRestart`,
`TestRekeyAwaitUnlockRestartsWithKeyID`).

## What it proves

A single node rotates its application (master) storage key without
downtime: records written before and after rotation read back after
reopening with the new key, and the retired key is rejected. The second
leg covers await-unlock/SIGKILL recovery: rotation survives an unclean
restart and the reopened store still requires the new key.

## Pass criteria

- `typed_storage_key_rekey_survives_restart` (cluster `rekey-restart`,
  1 node): `typed-before-rotate` is inserted, `rotate_master_key`
  succeeds, `typed-after-rotate` is inserted; after `stop_node`, a
  direct `Database::open` with the new key reads both rows back with
  count 1, and opening with the retired key fails with exactly
  `Error::WrongKey`.
- `rekey_sigkill_restarts_with_new_key` (cluster `rekey-sigkill`,
  1 node): `pre-rotate` is inserted and the key is rotated; a fresh
  `testnode` fixture process opens the rotated store with the new key
  (ready file observed) and is then SIGKILLed; reopening in-process
  with the new key reads `pre-rotate` back with count 1, a
  `post-restart` write succeeds and reads back, and the retired key is
  rejected with `Error::WrongKey`.

## Env knobs

- Harness knobs apply: `MURMUR_LIVE_RUNTIME`, `MURMUR_LIVE_FAILURES`,
  `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.
- The SIGKILL leg needs the `testnode` fixture binary built
  (`run.sh` builds it and exports `MURMUR_TESTNODE_BIN`;
  `MURMUR_BIN` also honored).

## Deviations from the Go original

- Plain SQL (`records(name TEXT PRIMARY KEY)`, `INSERT` /
  `SELECT count(*)`, `migrate` + `flush`) instead of the managed
  typed-records HTTP API (`Config.Tables`, `TypedInsert`,
  `TypedCount`); the record name is the row key in both.
- Rotation is `Database::rotate_master_key(new_key)` (synchronous:
  flush + worker ack, so durable at return) instead of the
  `RotateKey` admin call; there is no cipher parameter (the store
  cipher is fixed) and no key IDs in the Rust product, so the Go
  `EncryptionStatus` active-`ApplicationKeyID` assertion is dropped
  (the new-key-opens / retired-key-rejected legs carry that proof).
- The new key is fixed (`[0x5a; 32]`) instead of random; only the
  old-vs-new distinction matters.
- Restart is in-process `close()` + direct `Database::open` with the
  new key (the cluster keeps the pre-rotation key, so `start_node`
  cannot reopen a rotated node), not daemon `SIGTERM` + re-exec with
  a rewritten key config.
- The retired-key leg is a direct `Database::open` asserting
  `Error::WrongKey` (the `tests/persistence.rs` precedent) instead of
  `assertNeverReady` (daemon exit without serving health).
- No await-unlock leg: `Database::open` takes the key directly (the
  Rust product has no Remote Unlock endpoint), so the "unlock with
  new material but no key ID 401s" assertion cannot be ported.
- The SIGKILL leg kills a `testnode` fixture holding the rotated
  store open rather than killing the rotating daemon itself (the
  fixture has no rotate command); this still proves a crashed opener
  of a rotated store loses nothing and the new key stays required.
- No shared-harness addition was required by this scenario.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/rekey/Cargo.toml
```
