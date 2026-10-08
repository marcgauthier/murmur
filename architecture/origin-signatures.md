# Replication origin signatures

[Architecture index](README.md) · [Project README](../README.md)

## Guarantee and trust boundaries

Protocol v4 requires an Ed25519 signature on every new transaction. A relay
can forward the origin's transaction unchanged but cannot change its identity
or mutations, or create a transaction under another provisioned NodeID, without
that origin's private key. QUIC/mTLS remains mandatory for connection admission
and confidentiality; transaction-origin authorization is independent of the
forwarding peer's certificate and `AllowedPeers` policy.

An origin can still sign malicious writes or conflicting history under its own
identity. This feature does not establish Byzantine consensus, availability,
rollback detection, or schema-author authorization. Administrator-provided
baselines and explicitly trusted snapshot sources remain trusted authorities
for merged state. See [snapshots and restore](snapshots-backup-and-restore.md).

## Provisioning and key lifecycle

`Config.OriginSigning` contains a 64-byte Go Ed25519 private key and an
`origin.KeyRegistry` of administrator-provisioned NodeID/public-key bindings.
The registry is scoped to the configured database; use independent registries
when databases authorize different writers. `NewKeyRegistry` copies key bytes;
`Add` is an explicit administrative operation, is safe during replication, and
refuses replacement of an existing binding. Network and discovery handlers
never add keys. Forwarded transactions require the original writer's binding,
even when the receiver has never connected to that writer.

Credentials are required for offline writers too. Startup validates the local
binding and private-key consistency, copies private material, and stores a
SHA-256 public-key fingerprint with the writer identity. A different key under
the same stored NodeID fails closed. Replacement uses a fresh NodeID; retain
historical public keys for log forwarding, recovery, and same-DBID restore.
Same-NodeID rotation and key revocation are not implemented in this release.
After compromise, stop/admit peers through the existing administrative controls
and establish an audited baseline with a registry excluding the compromised
origin if its history can no longer be trusted.

Load and persist signing keys separately from TLS, encryption, and bridge keys;
restrict private-key files to their owner. Never generate a replacement on
ordinary reopen. Example programs use temporary random identities through
`examples/internal/demoidentity`; deterministic keys in `internal/testidentity`
are test fixtures only and must never be used for real databases.

## Canonical signature and encoding

Signature version 1 uses ordinary Ed25519 over this exact byte sequence:

```text
ASCII("murmur/origin-transaction/v1")
DBID[16]
OriginNodeID[16]
OriginSequence[u64 big endian]
TxID[16]
HLC[u64 big endian]
SchemaEpoch[u64 big endian]
SchemaHash[32]
MutationDigest[32]
```

`SchemaID` is the existing epoch/hash pair; no additional schema identifier is
introduced. `MutationDigest` is SHA-256 of the u32 big-endian mutation count and
the ordered canonical mutation bytes used by `codec.EncodeBatch`: TableID,
RowID, ColumnID, flags, value type, and encoded value. It excludes transport
framing, compression and the signature envelope. The order is authenticated.
Protocol 5 additionally authenticates each column policy and its ordered causal records. Historical protocol-4 digests retain their original bytes. Mutation count and the complete payload cannot be changed without detection.

Signed batches add DBID, signature version, digest, and the 64-byte signature
to their header (208 bytes total). Chunk version 2 repeats the signed identity,
including HLC, and retains its independent SHA-256 digest of the entire encoded
batch. The chunk header is 260 bytes. Signature verification authenticates the
announced identity before staging; assembly checks the complete transaction
bytes and recomputes the signed mutation digest before apply. It does not prove
an individual fragment's contents before assembly. Existing staging, transfer,
decode, and transaction limits remain necessary against resource exhaustion.

## Commit, apply, forwarding and diagnostics

Local single and grouped commits assign sequences, finalize metadata, compute
the digest, and sign before merging and publishing the atomic Spool commit.
Signing failure does not publish a sequence, receipt, generation, or state.
Stored logs preserve the signature through gossip, Plumtree, range repair,
chunk regeneration, restart, and same-DBID clone restore.

Network receive handlers verify before schema deferral or chunk staging.
`state.CommitRemote`, `CommitRemoteGroup`, and prepared-record recovery also
verify before merge, prepare persistence, HLC observation, receipts or applied
watermarks. The public `DB.ApplyRemote` APIs cannot bypass these checks.
Duplicates authenticate before deduplication. Retained receipt/log identity
conflicts are rejected; compacted history cannot prove non-equivocation.

`DB.ScanReplicationLog` exposes complete signed transactions for inspection.
`BridgeLogSource` is a projection: when it filters policy mutations, it clears
the origin signature/version/digest. High/Low bridge artifact signatures remain
independent, and bridge-derived local transactions receive the local node's
origin signature.

Replication statistics and exported metrics expose unsigned inputs, unknown
origins, invalid signatures/identities, mutation digest mismatches and denied
snapshot sources. Invalid input does not receive applied progress; ordinary
batch input is dropped, while malformed chunk/snapshot input terminates its
stream through the existing protocol error path.

## Snapshots and strict cutover

`Replication.TrustedSnapshotSources` explicitly authorizes authenticated source
NodeIDs for merged-state snapshots. Empty disables remote snapshot ingestion.
The source is checked before accepting manifests, chunks and completion, and
untrusted peers are not requested as snapshot sources. When their retained logs
cannot repair a gap, diagnostics report that a trusted snapshot source is
required. Local `ApplySnapshotChunk` remains an administrator-authorized import
API: it does not independently prove transaction origins.

The RIME cutover uses replication protocol/minimum version 6, mutation codec
version 4, schema manifest encoding 3, and snapshot manifest format 3. The
handshake requires `CapOriginSignatures` and `CapMergePolicies`; protocol
overrides cannot disable authentication. Older peers are rejected; there is no
unsigned runtime compatibility mode.

Fresh stores use persistent format/minimum reader/minimum writer 6. Format-5
SQL-era stores and earlier legacy formats fail closed without being rewritten.
There is no in-place SQLite-to-RIME conversion in this release: use the previous
release to export legacy data, then open a fresh directory with typed Go table
A same-DBID clone keeps signed history and adopts a fresh signing NodeID/key.
A new-DBID reseed cannot reuse signatures bound to the source DBID: it preserves
current state as a trusted baseline and discards the old replication logs.
Legacy backups must be exported with the previous release before use. Older
binaries must refuse format 6.

## Verification

Canonical encoding vectors, per-field tampering, cross-database replay, group
atomicity, invalid duplicates, staging, key replacement and legacy prepare
recovery are covered by unit/integration tests. The
[origin-signatures live scenario](../tests-live/origin-signatures/README.md)
uses real QUIC/mTLS nodes and a hostile relay. Existing chunk-resume, snapshot,
backup/restore, bridge, partition and upgrade scenarios exercise the same
signed path. `BenchmarkOrigin` measures signing, verification and forwarding
serialization for small and large transactions; performance evidence belongs
in [benchmarks](benchmarks.md).
