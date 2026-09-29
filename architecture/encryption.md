# Encryption and key management

Encrypted VFS, key providers/caches, registry rotation, and maintenance rewrites.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [39. Encrypted Pebble VFS](#39-encrypted-pebble-vfs)
- [40. Key Provider Interface](#40-key-provider-interface)
- [41. Encryption Key Cache](#41-encryption-key-cache)
- [42. Data-Key Registry and Rotation](#42-data-key-registry-and-rotation)
- [43. Application-Key Rotation and File Rewriting](#43-application-key-rotation-and-file-rewriting)
- [44. QUIC Encryption vs At-Rest Encryption](#44-quic-encryption-vs-at-rest-encryption)

---

## 39. Encrypted Pebble VFS

Default package encryption is **AES-256-GCM**, with a 32-byte application key. Encryption is mandatory. Implement it below Pebble in a reusable `crypto` package providing cipher adapters, a key registry, and an encrypted `vfs.FS`/`vfs.File` wrapper supplied as `pebble.Options.FS`. Remove the prior native Badger encryption and per-value envelope design; Pebble compression must run before VFS encryption.

### Supported authenticated ciphers

| Canonical algorithm | Key bytes |
| --- | ---: |
| AES-128-GCM | 16 |
| AES-192-GCM | 24 |
| AES-256-GCM (default) | 32 |
| AEGIS-128L | 16 |
| AEGIS-256 | 32 |
| ChaCha20-Poly1305 | 32 |
| XChaCha20-Poly1305 | 32 |

Accept `AES-GCM-256` as an alias for canonical `AES-256-GCM`. Treat `ChaCha` as ChaCha20-Poly1305; do not offer unauthenticated stream-cipher modes. Use Go's AES/GCM, `golang.org/x/crypto/chacha20poly1305`, and `github.com/ericlagergren/aegis` (AEGIS-128L/256). Pin the AEGIS dependency at implementation time and verify published vectors and portable/hardware paths before accepting that dependency. Do not implement cipher primitives locally. Use full, untruncated authentication tags from each adapter.

### Coverage and VFS contract

Encrypt every database content file: SSTables, WALs, manifests, OPTIONS, CURRENT, blob/value files if enabled, and temporary content. Keep directory operations and lock files compatible with the underlying filesystem. Authenticate clear format headers, but filenames, physical sizes, and access patterns remain visible. Content authentication does not provide whole-database rollback or deletion detection.

Preserve random `ReadAt`/`WriteAt`, sequential reads/writes, partial-operation error behavior, logical file sizes and EOF, concurrent reads, file reuse, rename, links, preallocation, prefetch, sync operations, and checkpoints. Track logical and physical offsets separately. Expose invalid raw descriptors where required to prevent ciphertext offsets or direct reads bypassing the wrapper; do not provide a usable unwrapped filesystem to Pebble. Locking and directory synchronization still reach the underlying filesystem.

Assign immutable random file identities; rename and hard links retain identity, while reuse creates fresh content identity. Associate checkpoints with their own registry copy and pin keys for their lifetime. Rewriting a linked file must create a replacement inode rather than mutate a checkpoint's copy.

### SQL materialization

The SQLite query materialization is memory resident and rebuildable from
Pebble. SPeD-SQL does not create a query database file. Operating systems may
page process memory to swap; deployments that require protection from that
exposure must disable or encrypt swap at the host level. The encrypted Pebble
VFS remains the at-rest protection for authoritative database files.

### Pragmatic Authenticated File Containers

Rather than attempting to build a full general-purpose copy-on-write filesystem with dynamic chunk replacement and obsolete-chunk compaction inside `vfs.FS`, leverage Pebble's real I/O contracts by implementing a pragmatic dual-mode authenticated container:

1. **Immutable Files (SSTables):**
   - SSTables are written strictly sequentially once during flush or compaction, closed, and thereafter accessed via concurrent `ReadAt`.
   - Written as sequential fixed-size authenticated blocks (64 KiB) with a trailing authenticated index footer.
   - Zero read-modify-write overhead and zero obsolete-record compaction required.

2. **Append-Only Streaming Files (WALs and Manifests):**
   - WAL and Manifest files are strictly sequential appends followed by `Sync`.
   - Written as streaming authenticated frames with a monotonic record sequence counter and AEAD nonce.
   - On reopen, tail recovery authenticates frames up to the last durable commit and trims any uncommitted/torn trailing bytes.

This dual-mode architecture satisfies the complete Pebble VFS contract while eliminating hundreds of lines of fragile chunk-compaction and index-replacement code.

Derive separate per-file and writer-epoch keys using HKDF-SHA-256 with distinct purpose labels. A writable reopen starts a fresh random 256-bit writer epoch, including after a failed write or crash. Derive the epoch key from file key plus epoch ID, and use monotonically increasing record counters encoded at the adapter's nonce size. Never repeat a key/nonce pair or reuse an epoch on writable reopen. Cap each epoch at 2^32 records, then start a new epoch; fail closed on counter exhaustion or random-source failure.

For `Sync`/`SyncData`, append authenticated commit metadata and synchronize it before returning success. Directory synchronization persists create/rename/cutover metadata. Preserve read-your-writes before synchronization.

On recovery use the latest authenticated durable commit and discard only incomplete/uncommitted tail data. Authentication failure inside committed content is corruption and must never be treated as EOF, plaintext, or a reason to fall back to an older commit. Validate footer/index bounds before allocations and require referenced records to authenticate. Fault tests must prove torn-tail handling cannot discard acknowledged writes.

Reference: [Pebble VFS contract](https://github.com/cockroachdb/pebble/blob/v2.1.6/vfs/vfs.go).

---

## 40. Key Provider Interface

Allow direct key material or a provider; do not require environment variables.

```go
type KeyProvider interface {
    Current(ctx context.Context) (KeyMaterial, error)
    Lookup(ctx context.Context, id string) (KeyMaterial, error)
}

type KeyMaterial struct {
    ID        string
    Algorithm EncryptionAlgorithm
    Key       []byte // key length matches Algorithm
}
```

Require exactly one source (`Encryption.Key` or `Encryption.Provider`) and a nonempty key ID. Direct input is copied into package-owned memory; provider lookup resolves historical wrapping keys for interrupted rotation and separately retained backups.

For a new database, omitted write algorithm resolves to AES-256-GCM. Direct material uses `KeyAlgorithm` (defaulting to `Algorithm`); provider material supplies its own `KeyMaterial.Algorithm`. Validate wrapping material against that algorithm's key length independently of the selected data-file cipher. On an existing store, validate the registry's wrapping material algorithm/ID and configured write algorithm; changing `Open` configuration alone must not rotate keys or convert files.

Environment/file providers accept raw or hex keys at the selected algorithm's required length. Never trim arbitrary raw bytes. Vault/KMS/TPM/platform providers can live outside the package. Keys must come from a secure random source or an external key manager; human passwords require a proper password KDF before entering this API.

---

## 41. Encryption Key Cache

Resolve application wrapping keys at open or rotation and retain only keys currently required in process. Do not call Vault/KMS for every Pebble operation.

Cache policy:

- Never log key bytes; redact configuration/status.
- Keep unwrapped data keys only while referenced by files or operations.
- Copy caller-owned direct key material; zero temporary and cached slices when practical.
- Document that Go runtime copies prevent guaranteed secret erasure.
- Keep plaintext buffers bounded; optional platform locked-memory support may be added later.

Failures to resolve required historical keys must stop recovery; do not silently initialize a new registry.

---

## 42. Data-Key Registry and Rotation

Separate application wrapping keys from independently generated data-encryption keys. The application key wraps an authenticated, versioned registry; data keys encrypt files through the VFS.

```go
type EncryptionConfig struct {
    Algorithm       EncryptionAlgorithm // default AES256GCM for newly created files
    KeyAlgorithm    EncryptionAlgorithm // direct wrapping material; defaults to Algorithm
    Key             []byte              // application wrapping material; exclusive with Provider
    KeyID           string
    Provider        KeyProvider
    DataKeyRotation time.Duration       // positive; default constructor uses 10 days
}

type EncryptionAlgorithm string

const (
    AES128GCM EncryptionAlgorithm = "AES-128-GCM"
    AES192GCM EncryptionAlgorithm = "AES-192-GCM"
    AES256GCM EncryptionAlgorithm = "AES-256-GCM"
    AEGIS128L EncryptionAlgorithm = "AEGIS-128L"
    AEGIS256  EncryptionAlgorithm = "AEGIS-256"
    ChaCha20Poly1305  EncryptionAlgorithm = "ChaCha20-Poly1305"
    XChaCha20Poly1305 EncryptionAlgorithm = "XChaCha20-Poly1305"
)
```

`DataKeyRotation` must be positive; the package constructor explicitly sets 10 days and callers may override it. Rotate on open and before creating a file when the active key expires, and provide `RotateDataKey(ctx)` for explicit rotation. Clock rollback must not extend expiry beyond the configured lifetime; use monotonic elapsed time during a process lifetime and persisted creation time on restart.

Persist random key IDs, algorithm, creation time, and wrapped data-key material in the registry. Derive a 32-byte registry wrapping key with HKDF-SHA-256 using stable database-specific context and a separate purpose label, and wrap using AES-256-GCM. The input material retains the entropy of its original supported key size. Authenticate registry version, identity, generation, and key-source ID; fresh random nonces and per-key limits apply to registry replacements.

Registry bootstrap context must be available before opening Pebble; validate it cryptographically against the registry. Store the registry outside its own encrypted VFS recursion through private underlying-filesystem access available only to the encryption manager. It contains no plaintext secrets. Persist a new registry generation with temporary-file sync, atomic rename, and directory sync **before** publishing a data key to any file creator.

New files use the active key and algorithm. Existing files keep recorded key/algorithm IDs and remain readable; live handles do not switch their file key mid-write. Compaction produces newly encrypted files naturally. Old keys are retained until all live files, open handles, checkpoints, and registered backups release references. Scan file headers during recovery to reconstruct references; never retire keys merely because their rotation period elapsed.

Use Badger's data-key registry and lazy-rotation concepts as a reference. Any adapted code must retain Apache-2.0 notices and attribution. Replace its CTR encryption and registry format; do not keep Badger as a runtime dependency.

Reference: [Badger v4.9.1 key registry](https://github.com/dgraph-io/badger/blob/v4.9.1/key_registry.go).

---

## 43. Application-Key Rotation and File Rewriting

### Rewrap the registry

`RotateStorageKey(ctx, material)` requires a new application-key ID, explicit supported algorithm, and matching key length. It rotates the **wrapping key**, leaving data keys and encrypted files unchanged. Its algorithm identifies the input key material; the registry wrapper remains domain-separated AES-256-GCM.

Serialize registry mutations, authenticate the current registry, write and sync a replacement under the new wrapping key, atomically replace it, and sync the directory before returning success. Record key IDs and a recoverable rotation intent, never raw keys. Keep the old/new key sources available until verification completes. Recovery uses provider lookup to complete or roll back a pending rotation before allowing durable writes; direct-key mode requires the matching key for the authoritative registry and returns a descriptive missing-key error otherwise.

After successful rotation, subsequent `Open` uses the new application material. The old key must fail to open the current registry, but remain available separately for backups whose registry is still wrapped under it.

### Change data cipher

`SetEncryptionAlgorithm(ctx, algorithm)` persists a new active write algorithm and data key before creating new files. It does not change application wrapping material or existing files. Old algorithm adapters stay available while any retained files need them. Record the selected write algorithm in metadata; require reopen configuration to agree rather than silently reverting it.

### Explicit maintenance rewrite

`RewriteEncryptedFiles(ctx)` rewrites files using the current data key and write algorithm. Enter maintenance, block local durable writes and remote apply, drain active state operations, flush and close Pebble and its readers, then enumerate live encrypted files. For each file, authenticate/decrypt through the source VFS, write through the destination VFS into a fresh temporary container, sync, atomically replace, and sync its directory. Preserve logical bytes and Pebble filenames while changing physical file identity. Existing checkpoints remain on their original linked inode with their registry/key references pinned.

Persist a resumable per-file rewrite journal. On restart, verify temporary/replaced files and resume or discard incomplete temporary work; never accept partly rewritten content. Registry key retention precedes every file publication. Reopen Pebble, verify identity/generation and logical-state digest, then resume writes and replication. Allow SQL reads during maintenance only against the existing in-memory materialization, explicitly reporting maintenance state.

Retire data keys only after rebuilding the full reference inventory. Registered backup handles pin keys until explicitly released. Separately retained backup bundles contain ciphertext files plus the matching registry and require their original application wrapping key; no claim of global backup-key erasure is made.

Expose status without key bytes:

```go
type EncryptionStatus struct {
    Algorithm           EncryptionAlgorithm
    ApplicationKeyID    string
    ActiveDataKeyID     string
    RegistryGeneration  uint64
    Keys                []KeyReferenceStatus
    Phase               string // idle, rotating, rewriting, or recovering
    FilesDone, FilesTotal uint64
    BytesDone, BytesTotal uint64
}

type KeyReferenceStatus struct {
    ID        string
    Algorithm EncryptionAlgorithm
    CreatedAt time.Time
    LiveFiles, OpenHandles, Checkpoints, Backups uint64
}
``` Do not claim zero-downtime rewriting or whole-database rollback protection.

---

## 44. QUIC Encryption vs At-Rest Encryption

These are separate controls.

At rest:

```text
encrypted VFS authenticated encryption
```

In transit:

```text
QUIC TLS 1.3
```

Do not reuse the Pebble storage key as a TLS private key, PSK, replication payload key, or certificate secret.

Independent key domains.

---
