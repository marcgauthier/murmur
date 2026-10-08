# Encryption and key management

Spool AES-256-GCM storage encryption, key providers/caches, keyring rotation, and maintenance rewrites.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [39. At-Rest Encryption via Spool AES-256-GCM](#39-at-rest-encryption-via-spool-aes-256-gcm)
- [40. Key Provider Interface](#40-key-provider-interface)
- [41. Encryption Key Cache](#41-encryption-key-cache)
- [42. Data-Key Keyring and Rotation](#42-data-key-keyring-and-rotation)
- [43. Application-Key Rotation and Compaction Rewriting](#43-application-key-rotation-and-compaction-rewriting)
- [44. QUIC Encryption vs At-Rest Encryption](#44-quic-encryption-vs-at-rest-encryption)

---

## 39. At-Rest Encryption via Spool AES-256-GCM

Transaction signing uses a separately provisioned Ed25519 key. Do not reuse encryption, TLS or bridge signing keys for this identity. See [origin signatures](origin-signatures.md) for the exact format and trust boundaries.

Default package encryption is **AES-256-GCM**, with a 32-byte application wrapping key. Storage encryption is mandatory. Spool (`spool/`) provides native authenticated encryption for all persistence artifacts at rest:
- Encrypted data segments (`*.seg`) holding mutation records, commits, and sync barriers.
- Encrypted manifest (`manifest.enc`) storing segment metadata, sequence numbers, and generation state.
- Encrypted keyring (`keys.enc`) holding wrapped per-segment data encryption keys.

### Authenticated Cipher and Key Length

Storage encryption uses **AES-256-GCM** with 32-byte key material. Spool binds the cluster database ID (`DBID`) as authenticated context (AAD) for all encrypted persistence files. Direct 32-byte key material or dynamic provider lookup via `KeyProvider` is supported.

### Coverage and File Layout

Spool encrypts every database persistence file under `Config.Path/data`: segments, manifests, keyrings, and temporary compaction outputs. Keep directory operations and lock files compatible with the underlying filesystem. Authenticate clear format headers, but filenames and physical segment sizes remain visible to the OS. Content authentication protects against ciphertext tampering and torn writes.
On open, Spool also enforces mode `0700` on its store root, including an existing
data directory that a caller previously created with broader permissions.

### RIME Materialization

RIME records and indexes are memory resident and rebuildable from Spool. Murmur
does not create a query database file on disk. Operating systems may page
process memory to swap; deployments that require protection from that exposure
must disable or encrypt swap at the host level. Spool's encrypted files remain
the at-rest protection for authoritative database data.

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

Resolve application wrapping keys at open or rotation and retain only keys currently required in process. Do not call Vault/KMS for every storage operation.

Cache policy:

- Never log key bytes; redact configuration/status.
- Keep unwrapped data keys only while referenced by files or operations.
- Copy caller-owned direct key material; zero temporary and cached slices when practical.
- Document that Go runtime copies prevent guaranteed secret erasure.
- Keep plaintext buffers bounded; optional platform locked-memory support may be added later.

Failures to resolve required historical keys must stop recovery; do not silently initialize a new registry.

---

## 42. Data-Key Keyring and Rotation

Spool separates application wrapping keys from independently generated segment data-encryption keys. The application wrapping key protects an authenticated keyring (`keys.enc`); per-segment data keys encrypt data segments (`*.seg`).

```go
type EncryptionConfig struct {
    Key      []byte      // application wrapping material; exclusive with Provider
    KeyID    string
    Provider KeyProvider
}
```

Storage encryption uses AES-256-GCM with 32-byte keys.

Spool manages data keys in its encrypted keyring. `RotateDataKey(ctx)` activates a fresh data key for subsequent segments. monontonic generation numbers track keyring updates.

New segments use the active data key. Older segments remain readable via historical keys retained in `keys.enc`. Compaction reads older segments and writes new compacted segments encrypted under the active data key.

---

## 43. Application-Key Rotation and Compaction Rewriting

### Rewrap the keyring

`RotateStorageKey(ctx, material)` rewraps the Spool keyring (`keys.enc`) under a new application wrapping key ID and 32-byte key material, leaving data segments unchanged.

Spool serializes keyring mutations, authenticates the current keyring, rewraps it with the new key, and atomically replaces `keys.enc` before returning success.

After successful rotation, subsequent `Open` uses the new wrapping key material.
The multi-process `rekey` rehearsal uses only native typed databases. It rotates
the wrapping key, reopens with the new key, verifies records written before and
after rotation, and exercises await-unlock recovery after an unclean restart;
the retired key is rejected.

### Compaction rewriting

To re-encrypt older segments under a new data encryption key, rotate the data key via `RotateDataKey(ctx)` and execute storage compaction. Compaction reads all live entries, writes fresh segments encrypted under the active data key, updates the manifest, and removes obsolete segment files.

---

## 44. QUIC Encryption vs At-Rest Encryption

These are separate controls.

At rest:

```text
Spool AES-256-GCM authenticated encryption
```

In transit:

```text
QUIC TLS 1.3
```

Do not reuse the storage key as a TLS private key, PSK, replication payload key, or certificate secret.

Independent key domains.

---
