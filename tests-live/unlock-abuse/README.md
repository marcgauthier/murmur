# unlock-abuse (Rust port, test-side equivalent)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/unlock-abuse/`
(`unlock_abuse_test.go`, `TestUnlockAbuse`).

## What it proves

Go abuses the Remote Unlock HTTP endpoint. The Rust product has no
unlock endpoint (`Database::open` takes the key directly), so this port
proves the same anti-oracle property on direct opens of a stopped
node's store: distinct wrong keys fail with byte-identical generic
messages leaking no key bytes, malformed keys fail cleanly, and the
honest reopen serves intact data and rejoins the mesh.

## Leg mapping (Go -> Rust)

- Certless admin refused (5 routes) + certless `/healthz` 200: NOT
  portable (no admin endpoint, no HTTP server); transport auth is
  covered by the `api-mtls` equivalent instead.
- Wrong-key / unknown-key-id / right-key-unknown-id identical 401s
  become: three distinct 32-byte wrong keys fail direct open with
  byte-identical messages (failure indistinguishability, the
  anti-oracle property).
- No-oracle body checks become: every failure message is non-empty and
  contains neither the tried key's hex nor the real key's hex.
- Unknown-key-id becomes malformed credentials (16- and 64-byte keys):
  must fail cleanly with no key bytes. Cross-identity with the
  wrong-key message is deliberately NOT asserted: length validation
  reflects only the attacker's own input length, which cannot oracle
  key value.
- Honest unlock + audit line becomes: honest reopen serves the intact
  baseline row and rejoins (a post-unlock write converges with equal
  digests). The audit line has no target (no audit sink in the
  product); the data-intact proof remains.

## Pass criteria

- Baseline converges (1 row, 10 s) with equal digests before any abuse.
- Three wrong-key opens fail with byte-identical messages, no key hex.
- Two malformed-key opens fail with no key hex.
- Honest reopen: baseline intact, mesh rejoined, post-unlock write
  converges (2 rows, 10 s) with equal digests.

## Compile check (only gate; never run here)

```sh
CARGO_TARGET_DIR=/home/marc/MURMUR/tests-live/target \
  cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/unlock-abuse/Cargo.toml
```
