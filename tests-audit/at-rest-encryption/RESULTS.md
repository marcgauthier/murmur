# At-rest encryption security audit — RESULTS

- Date: 2026-10-04
- Scope: at-rest encryption end to end — key provisioning, in-memory key
  lifetime, Pebble encryption (container + VFS), registry, rotation/rewrite,
  CLI/harness key handling.
- Tree: HEAD `7211b06a990fe6adb72ee14110d2801e93d33a6b`, working tree dirty
  (333 changed/untracked entries, pre-existing; the audit made no repo edits).
- Method: code inspection of `crypto/`, `encryption.go`, `db.go` open/close,
  `maintenance.go`, `config.go`, `backup/`, `tool/cmd/`, harness key paths;
  plus live verification (repo tests + an independent `/tmp/eap` probe driven
  through the public API, kept out of the repo per scratch-collateral rules).

## Verdict

The core design is sound and the dangerous paths fail closed: all seven
ciphers round-trip, wrong keys are rejected, and byte-level tampering of data
files, headers, and the registry is detected. The serious problems are at the
edges: the shipped CLI encrypts with a public constant when no key is given
and stretches passphrases with a single unsalted SHA-256, and two
tamper-recovery behaviors (empty-file leniency, header-slot fallback) give a
file-tampering attacker silent rollback/delete primitives. No fixes were
applied; this report is findings only.

## Findings

| # | Severity | Title | Status |
|---|----------|-------|--------|
| 1 | High | CLI hardcoded default storage key | Code-read, shipped path |
| 2 | High | CLI passphrase = single unsalted SHA-256 | Code-read, shipped path |
| 3 | Medium | WAL truncate-to-zero silently deletes all data | Proven live |
| 4 | Medium | Zeroed newest header slot silently rolls back one commit | Proven live |
| 5 | Medium | Pre-authentication 1 GiB allocation via checkpoint `sealedLen` | Code-read |
| 6 | Medium | Avoidable long-term key copies in memory, never zeroed | Code-read |
| 7 | Low | `FileProvider` accepts world-readable key files silently | Code-read |
| 8 | Low | `Encryption.KeyID` required but ignored in provider mode | Code-read |
| 9 | Low | Whole-file plaintext buffering in rewrite/rebind | Code-read |
| 10 | Info | Harness `config.json` (0644) carries the operator key | Observed live |
| 11 | Info | CLI uses zero DBID: no cross-DB binding for CLI stores | Code-read |

## Detailed findings

### 1. [High] CLI default storage key is a public constant

`tool/cmd/db_helper.go:100` and `tool/cmd/keys.go:121`: when neither
`--passphrase` nor `--key-hex` is given, the CLI uses
`[]byte("0123456789abcdef0123456789abcdef")` — the same literal that appears
in every example and the test harness. Anyone with read access to the
database directory can decrypt everything. `init` prints `Encrypted: false`
in this case (`tool/cmd/init.go:78`), which understates the situation: the
store *looks* encrypted while protected by a published key.

Fix: refuse to open/init without explicit key material; never ship a default.

### 2. [High] CLI passphrase stretched with single unsalted SHA-256

`tool/cmd/db_helper.go:85-88`, `tool/cmd/keys.go:112-114`:
`sha256.Sum256(passphrase)` is used directly as the 32-byte storage key. This
contradicts `architecture/encryption.md` §40 ("human passwords require a
proper password KDF before entering this API"): it is fast (GPU brute force),
unsalted (rainbow tables; identical passphrases yield identical storage keys
across databases — only the later HKDF `dbID` bind differs), passed via argv
(visible in `ps`/shell history), and the digest is never zeroed.

Fix: argon2id/scrypt with a per-database random salt persisted alongside the
store, TTY/env-file input instead of argv, and zeroed buffers. The library API
is correct to demand pre-derived keys; the CLI must not pretend SHA-256
qualifies.

### 3. [Medium] Truncating a WAL to zero bytes silently deletes data (proven)

`crypto/file.go:108-120` treats any zero-byte file as a valid empty file
(lenient-empty, matching stock Pebble crash semantics). Live probe result:
truncating `data/*.log` after 50 committed inserts → `Open` succeeds,
`SELECT COUNT(*)` returns 0, no error. A file-tampering attacker gets a
silent-delete primitive with no alarm (the `ShortFiles` counter increments,
but nothing surfaces it as a security signal). Truncating `MANIFEST` fails
closed (Pebble rejects it) and `OPTIONS` is benign (defaults), both verified
live.

Fix options: maintain an expected-file inventory across clean shutdowns and
refuse/warn when a previously committed file reads back empty; expose
`ShortFiles` during open as aloud integrity signal; document as accepted risk
under the existing no-rollback-protection limitation if not fixed.

### 4. [Medium] Zeroing the newest header slot rolls back one commit silently (proven)

`crypto/file.go:242-277`: pristine zero slots are skipped, otherwise the
higher valid `slotSeq` wins. Live probe on a WAL with `slotSeq A=32 B=33`:
zeroing slot B → `Open` succeeds on slot A with no error (in this instance
the dropped commit carried no new rows, so the count was unchanged — the
fallback itself is the confirmed primitive). Conversely, flipping one byte in
slot B fails closed with `header seal: cipher: message authentication
failed` (proven). So only *zeroing* attackers get a silent one-commit
rollback per file. This is inherent to dual-commit without monotonic
counters and adjacent to the documented no-rollback-protection limitation,
but it is cheap (256 zero bytes) and file-granular, so it deserves explicit
documentation at minimum.

### 5. [Medium] Up to 1 GiB pre-authentication allocation from `sealedLen`

`crypto/file.go:311-357`: `readRawRecord` trusts the unauthenticated
`sealedLen` u32 (data records capped at 64 KiB + 64 — good; checkpoint
records capped at `1<<30`), and `readSealed` allocates before reading or
authenticating. A file-tampering attacker can force a 1 GiB transient
allocation per file open / `verifyImage` / rewrite-resume — a repeatable
memory-exhaustion DoS. Registry parsing does not share this flaw (lengths
there slice an already-read buffer, counts are inside the seal).

Fix: bound `sealedLen` by the remaining physical file size (or a sane
checkpoint cap derived from entry count) before allocating.

### 6. [Medium] Avoidable key copies live in memory for the process lifetime

- `DB.cfg` retains `Encryption.Key` (`db.go:37`, struct copy shares the
  caller's backing array); `Open` copies it again into a synthetic
  `MapProvider` (`encryption.go:152-162`). Neither copy is zeroed on `Close`.
- `RotateStorageKey` installs a fresh `MapProvider` holding the new key
  (`maintenance.go:55-59`) and abandons the old provider's bytes un-zeroed.
- Only the registry KEK is reliably zeroed (`Registry.Close`, called from
  `DB.Close`/`closeStore`).

The Go-runtime-copies caveat is already documented (`crypto/provider.go:222`,
`architecture/encryption.md` §41); this finding is about *avoidable* copies
the code owns. Fix: zero `cfg.Encryption.Key` once the provider is built,
add zero-on-replace/close to `MapProvider`, and document that caller-owned
slices are retained (or stop retaining them).

### 7. [Low] `FileProvider` silently accepts world-readable key files

`crypto/provider.go:119-130` reads the key file with no mode/ownership check.
An operator mistake (`chmod 644`) silently voids at-rest protection. Fix:
warn or refuse unless the file is 0600 (with an explicit override flag).

### 8. [Low] `Encryption.KeyID` is required but ignored in provider mode

`encryption.go:100-102` requires `KeyID`, yet the `Open` path
(`db.go:147-172`) never compares it against the provider-served material ID —
the registry opens under whatever ID the provider returns. A misconfigured
deployment (config says `key-a`, KMS serves `key-b`) opens without complaint.
Fix: compare and fail closed, or document that `KeyID` is direct-key-only.

### 9. [Low] Rewrite/rebind buffer whole-file plaintext in memory

`Manager.RewriteFile` (`crypto/manager.go:357-369`), `rebindFile`
(`crypto/rebind.go:143-180`), and `verifyImage` hold up to a full file's
plaintext at once (SSTables: tens of MB). Buffers are zeroed after use, but
GC copies are not zeroable and a large file spikes RSS. Observation only;
chunked streaming would bound it.

### 10. [Info] Harness `config.json` (0644) carries the operator key in clear

Live-test logs show the harness writes `config.json` mode 0644 containing
`key_hex`; `plaintext-audit` explicitly scopes it out ("except the
harness-provisioned config file"). Test-tooling only — per the repo's own
service qualification this is a deployment gap, not a library vulnerability —
but any deployment copying the harness layout must provision keys at 0600 via
a `KeyProvider`, never a world-readable config.

### 11. [Info] CLI-created stores share the zero DBID

`tool/cmd/db_helper.go:76` sets `DBID: ids.DBID{}` for every CLI store, so
the KEK derivation and container AAD bind to the same all-zero identity:
CLI stores using the same storage key are mutually portable (registry and
file swap succeeds). Fix: generate and persist a random DBID per store.

## What was verified sound

- **Key hierarchy.** Storage key → HKDF-SHA-256 KEK (salt binds `dbID`,
  info binds key ID) → AES-256-GCM registry → random 32-byte data keys →
  per-file HKDF key → per-epoch HKDF key. Distinct purpose labels at every
  level (`crypto/cipher.go:164-183`, `crypto/format.go:261-327`).
- **Nonce uniqueness.** Random 256-bit writer epoch on every writable open
  and fresh files (`crypto/file.go:184-187,227-229`); 32-bit record counter
  per epoch with rotation before wrap (`allocCounterLocked`) and fail-closed
  epoch exhaustion; headers use random nonces under a separate subkey domain.
  Verified by inspection; no reuse path found.
- **AAD binding.** Records bind dbID, fileID, algorithm, key ID, epoch, all
  sequence counters, chunk index, offsets, and lengths; headers bind the
  envelope; the registry seal covers its whole envelope. Cross-file splices,
  replays, and reordered records fail authentication by construction.
- **Cipher adapters.** All seven AEADs have 16-byte tags (confirmed against
  the pinned `ericlagergren/aegis@v0.0.0-20250325060835` source:
  `TagSize128L = TagSize256 = 16`), so the container's fixed 16-byte-tag
  assumptions hold; the `Overhead() != 16` guards are defense in depth.
- **Fail-closed tamper handling (proven live).** Wrong key, flipped data
  byte, flipped registry byte, and non-zero header tamper are all rejected
  with generic authentication errors that do not oracle key vs. corruption
  detail. Restored files reopen cleanly.
- **Registry durability.** Atomic temp+fsync+rename+dir-fsync persists,
  forward recovery of interrupted rewraps via verified intent, tampered
  snapshots fail closed, keys retire only after a full live/open/
  checkpoint/backup inventory rebuild.
- **Hygiene where it counts.** `Zero()` on chunk buffers, `dataKey/fileKey`,
  `lastPlain` at file close; subkeys and registry payloads zeroed after use;
  registry dir 0700, temp files 0600; no key bytes in errors, logs, or
  `EncryptionStatus` (IDs and metadata only — confirmed by grep and by the
  live wrong-key message text).
- **Rotation semantics (proven live by `tests-live/rekey`).** Storage-key
  rotation without downtime, restart on the new key, old key rejected.

## Verification log

- `go test -tags "" -count=1 ./crypto/`
  → ok (18.8s).
- `go test -tags "" -count=1
  ./tests-live/encryption/` → ok: marker absent at rest, wrong key rejected,
  correct-key reopen reads the row.
- `go test -tags "" -count=1
  ./tests-live/rekey/` → ok: online rotation, new-key restart, old-key
  rejection.
- Independent `/tmp/eap` probe through the public API (7 files, out of repo):
  all 7 ciphers round-trip with marker and key bytes absent at rest; wrong
  key rejected per cipher; data-file flip rejected; registry flip rejected;
  WAL truncate → silent open with 0 rows; MANIFEST truncate → open fails;
  OPTIONS truncate → benign; header-slot zero → silent fallback; header-slot
  flip → auth failure. All checks passed.
- `tests-live/plaintext-audit`: all encryption assertions passed (8 markers
  round-trip; markers absent on both nodes; key material absent outside
  `config.json`); the run then fails at a later offline-reopen step with
  `state: signing key changed under the same NodeID` — a pre-existing
  origin-identity/harness issue, unrelated to encryption (no repo edits were
  made by this audit).
- `tests-live/file-permissions`: fresh-init keys dirs locked down (0700, no
  group/other) on both nodes; same pre-existing offline-reopen failure as
  above before the backup/restore assertions.

## Recommendations (priority order)

1. Remove the CLI default key; refuse keyless operation (finding 1).
2. Replace CLI `SHA256(passphrase)` with argon2id/scrypt + per-DB salt and
   non-argv input (finding 2); persist a random DBID per CLI store (11).
3. Bound checkpoint `sealedLen` allocations by file size (finding 5).
4. Zero `cfg.Encryption.Key` after provider construction; zero abandoned
   provider copies on rotation/close (finding 6).
5. Decide and document the tamper-recovery posture: silent WAL-empty and
   slot-fallback behaviors (findings 3–4) should either gain detection or be
   named explicitly alongside the no-rollback-protection limitation.
6. Mode-check key files (finding 7); validate or document `KeyID` in provider
   mode (finding 8).
