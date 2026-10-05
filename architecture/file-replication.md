# Encrypted file replication

Optional extension inspired by GALVANIZE. The `objectstore` package provides
local immutable encrypted payload storage, and `FilesConfig` enables replicated
file metadata with database upload/read/search/delete/status operations. Peer
object transfer (mesh fetching) is still planned: object bytes currently stay
on the uploading node.

[Architecture index](README.md) · [Capability gaps](capability-gaps.md)

## Object data and replicated metadata

### Implemented local encrypted object store

`objectstore.New(root, key)` accepts an independent 32-byte storage key.
`Put(ctx, reader)` streams 64 KiB chunks into a mode-0600 encrypted temporary
file, authenticates each chunk with AES-256-GCM, records an authenticated final
length and SHA-256 digest, and atomically links the completed container under
its digest. No plaintext staging file is created. Repeating the same upload is
idempotent after the existing object verifies. `Read(ctx, digest, writer)`
authenticates each chunk before writing it and checks the final length/digest;
callers must treat the destination as incomplete when it returns an error.
`Has` reports local presence without verifying content. `Close` wipes the
store's configured key copy and rejects later operations.

`List(ctx)` authenticates each stored object and returns its digest and
plaintext length. `Pin(digest)` holds a local reader/export retention pin until
the returned handle closes. `Collect(referenced, grace)` removes unreferenced,
unpinned objects only after the retention interval and syncs the directory.
The caller supplies the reference inventory; replicated reference ownership
is implemented via `FilesGC` over replicated metadata, and key rotation plus
object-inclusive backup/restore are implemented as described below.

### Implemented replicated metadata and file operations

Setting `Files.Enabled` with a 32-byte `Files.ObjectKey` opens a node-local
object store under `<Path>/files` and enables the file APIs. Metadata (name,
digest, size) replicates through the normal durable log as cells of the
reserved `__replicatedb_files` table, which lives outside the application
schema registry: enabling files never changes the replicated schema identity,
mixed clusters stay compatible, and the SQL materializer skips the reserved
table ID. Application schemas must not use the reserved name; `Open` rejects
the collision. Metadata batches carry the current schema epoch/hash so peers
accept them without waiting for schema sync.

`UploadFile(ctx, name, reader)` streams bytes into the local object store
(first, publish-before-acknowledgement) and then commits the metadata in one
durable transaction; the acknowledgement implies local byte availability. A
failed metadata commit leaves an unreferenced object for later collection,
never visible file state. `OpenFile(ctx, name)` returns a verified streaming
`FileReader` (integrity failures fail closed mid-stream). `FileStatus`
reports metadata plus local availability: known-but-absent bytes surface as
`Exists` without `Available`, and reads return `ErrFileUnavailable`.
`ListFiles`/`SearchFiles` scan visible metadata ordered by name.
`DeleteFile` replicates a row tombstone (idempotent; a later upload
resurrects the name). `FilesGC(grace)` reclaims unreferenced, unpinned
objects older than the grace; run it with a grace longer than the slowest
upload so in-flight publishes are never collected.

Deletes replicate metadata/tombstones first; physical collection follows the
caller's retention policy via `FilesGC`. Active readers hold retention pins
until closed. A missing object cannot be repaired by SQL state alone; the
mesh fetch worker below repairs missing bytes from peers, and backups
declare whether they carry objects (see below), so operators must still
account for node-local bytes in backup and capacity planning.

Store large file payloads outside SQL cells in a package-owned encrypted object
store. Replicate metadata identifying the object, immutable content version/digest,
length, content type, and deletion state through normal database transactions.
Local availability and download progress are node-local operational state.

Separate object transfer from [transaction chunking](synchronization-and-overload.md#31-sequence-and-gap-handling):
chunking a SQL value still reconstructs one bounded atomic cell, whereas this model
streams independently stored objects. A metadata commit does not certify that all
peers have the payload. File reads return an explicit unavailable/pending status
until local verification completes.

Use immutable object versions so metadata updates cannot silently mutate existing
content. Publish a local upload's verified encrypted object before acknowledging
its referencing metadata transaction. If the metadata commit fails, the unreferenced
object becomes eligible for later collection rather than visible file state.

## Encryption and bounded mesh fetching

Encrypt objects at rest with authenticated, versioned containers and retained key
references. Do not store plaintext temporary files.

### Implemented object-key rotation

Each object file name carries its key generation: `<digest>.spfo` is
generation 1 (the original format) and `<digest>.g<N>.spfo` is generation N;
the current generation persists in a plaintext `generation` file (absent
means 1). `RotateFileObjectKey(ctx, newKey, progress)` re-encrypts every
node-local object under the new 32-byte key, publishes the new generation,
then deletes superseded files; reads run throughout, concurrent uploads land
on the new key, and a crash anywhere in between reopens with both keys
(`Files.PrevObjectKey` recovers an interrupted rotation) and completes
automatically. Keys never touch disk: the operator supplies the current key
at open and must persist the new `Files.ObjectKey` before the next restart.

Rotation is node-local and must be installed on every node:
`FileObjectKeyGeneration` reports the local generation so operators can
confirm a cluster-wide rotation finished. Mesh fetch verifies containers
with the receiver's local key, so nodes on different generations fail
fetches closed until each node rotates; replicated file metadata is
unaffected by rotation.

Streaming upload/read, metadata lookup/search, delete, and availability
statistics are implemented as the `DB` file operations above, and mesh transfer
is implemented by the `filefetch` package with a `DB` worker. Keep
authorization in application-facing adapters; peer fetches authenticate with the
mesh's mTLS NodeID certificates plus a per-request DBID check.

### Implemented mesh fetch

`Files.FetchAddr` serves object bytes on a dedicated QUIC endpoint sharing the
cluster mTLS credentials (a deliberate deviation from bulk streams on the
shared replication endpoint: bulk bytes never head-of-line-block membership or
control traffic, at the cost of a second listen address; multiplexing fetch
streams onto replication sessions remains possible future work).
`Files.FetchPeers` statically lists sources, and serving nodes additionally
advertise their bound fetch endpoint in SWIM membership metadata, so SQL
replication and object replication share the same dynamic membership model:
fetch sources are the union of static peers and live SWIM members advertising
a fetch endpoint (static entries win conflicts; discovered members honor peer
exclusion; dead members disappear with the membership view). Fetching peers
must run the same object-key generation: receivers verify containers with the
local key, so a generation mismatch fails closed as a corrupt source until
the lagging node rotates.

The background worker scans visible metadata for locally missing objects
(woken early whenever remote file metadata applies) and pulls each with
bounded concurrency (`MaxConcurrentFetches`), a per-file cross-source timeout,
and a staging-directory cap. `FetchFile` performs one on-demand fetch with
rotated source ordering. Sources are tried in turn: a missing object, refusal,
corrupt bytes, or truncation moves to the next source, and per-source failures
are recorded in `FileFetchStats` without failing the file until every source
is exhausted.

Transfers stage ciphertext into per-source files (container bytes differ across
uploads of identical content, so prefixes never mix across sources), resume
from the staged prefix after interruption or restart, and publish atomically
through `objectstore.InstallVerified`, which verifies the replicated digest
and length before linking the object into place. Partial objects are never
visible: availability flips only on verified install. Declared container
lengths are capped against the metadata-anchored maximum, frames are bounded,
servers cap concurrent connections, and `FilesGC` sweeps installed or
long-orphaned staging (never active fetches). No availability announcements
exist: sources are attempted, never trusted.

## Cross-domain artifacts

[High/Low](high-low-replication.md) file metadata and file payloads use the same
authorized source lineage and recipient policy. Seal exported objects for High
using separate bridge credentials; High validates and re-encrypts accepted content
under its own local storage keys. Neither domain shares its storage wrapping key.

Metadata may arrive before an object; expose pending content and retry without
blocking unrelated row imports. Ownership/provenance rules apply to metadata
updates and deletions. Object artifacts are idempotent by stable identity and
digest; conflicting payloads fail closed. High shares verified files within its
own mesh without exporting its files back to Low.

Implemented (see [High/Low file artifacts](high-low-replication.md#file-artifacts)):
metadata crosses as bundle records, bytes as recipient-sealed chunk sidecars
with a durable pending journal and per-object quarantine/retry, installing in
either arrival order with digest verification and full ownership-policy parity.

## Retention, backup, and acceptance

Deletion replicates metadata/tombstones first; physical collection follows a
configured retention policy and verified reference inventory. Pin active readers,
fetches, pending exports, and backups. A missing object cannot be repaired by SQL
state alone, so collection and backup policies must explicitly account for files.

### Implemented file backup and restore

`Backup` with `IncludeFiles` copies the node-local object files (every key
generation present) alongside metadata and records `FilesMode=objects` with
object/byte counts in the manifest; without it the backup is metadata-only.
`Restore` copies included objects back and reports restored counts, also
flagging `FilesSkipped` when the operator passes `SkipFiles`. An
object-inclusive backup whose restore yields fewer objects than declared
logs a warning naming the declared/restored counts rather than claiming
completeness. Restored objects decrypt only under the backup-time object
key (and generation): configure the same `Files.ObjectKey` before opening
the restored copy. Metadata-only restores surface known files as `Exists`
without `Available`; the mesh fetch worker (or `FetchFile`) repairs
payloads from peers afterwards. Restore-time digest verification of
object containers remains future work: restored objects are re-verified on
first read.

Acceptance requires streaming large files with bounded memory, offline peer repair,
alternate-source fetch, duplicate transfer, corrupt/truncated rejection, crash-safe
publication, deletion/retention, rotation/backup/restore, and a two-Low/two-High
scenario proving metadata convergence and verified encrypted payload availability.
The Low-to-Low repeated upload/fetch path and p95 smoke gates are exercised by
`go test -count=1 ./tests-live/files-soak`. The two-Low/two-High bridge path is
covered by `go test -count=1 ./tests-live/files-bridge`; the ten-minute file
soak passed on 2026-09-27 with 10 rounds, 655,360 fetched bytes, 8.51 ms
upload p95, and 35.13 ms metadata-to-verified-fetch p95.
Rotation, both backup modes, and restore-then-read round trips are covered by
`TestFileObjectKeyRotation`, `TestRotateKey`, `TestBackupFilesInclusive`,
`TestFileBackupRestoreInclusive`, and `TestFileBackupRestoreMetadataOnly`.
