# Architecture

```text
Application SQL
      |
One SQLite :memory: connection + session
      |
Collect committed changeset + collection-time version
      |
Ordered, bounded background queue
      |
Changeset iterator -> Spool metadata reads
      |
Atomic Spool group: changeset + versions + tombstones + clock
      |
Zstd -> AES-256-GCM -> encrypted append-only segments
```

The database thread owns the single SQLite connection and its session. Normal
writes execute only application SQL and transaction control; they do not execute
CRDT SQL. Session extraction and resource admission are the coordination boundary.
Compression, encryption, disk access, metadata generation, and changeset compaction
run behind that boundary. Explicit migrations, exports, and incoming row application
need coordinated database access.

## Collection time

The writing time of a batch is its collection time, assigned immediately after
session extraction. HLC ordering uses physical milliseconds, a logical counter,
and node identity. The counter handles equal or backward clocks; a remote clock
can move the watermark forward. Every captured cell and deletion in the batch
uses this version. Retry and worker delay never replace it with processing time.
Intermediate operations coalesced by SQLite have no independently recoverable time.

## Metadata ownership

Versions, row generations, and tombstones are encrypted Spool records. The worker
reads the relevant records from disk and constructs an atomic update group. It
owns neither a SQL replica nor a persistent Rust map of these values. The Spool
key/location index is the read index; record values are decoded temporarily.

Storage key namespaces:

- `00/identity`: format, database ID, node ID.
- `01/snapshot/head`: published archive boundary and hash.
- `02/snapshot/<sequence>/...`: chunked compacted replay archive.
- `03/log/<sequence>/...`: immutable, chunked ordered events.
- `04/meta/row/<table-id>/<key-hash>`: generation and cell versions.
- `05/meta/tomb/<table-id>/<hash>`: generation tombstones.
- `06/clock`: durable HLC watermark.

A log event and its metadata mutations share an atomic, synchronous Spool commit.
Chunks are independently encrypted by Spool framing; payload descriptors verify
length, chunk order, and BLAKE3 hash. Application wrapping keys are authenticated
before opening existing storage for mutation. Data-key rotation remains in Spool.
The current SQLite persistence format is version 3; earlier development formats
are rejected instead of silently interpreted.

## Recovery and compaction

Open reads the committed ordered archive/log and reconstructs the sole SQLite
connection before making it available. Failed-storage retry reloads Spool records
without opening another SQLite database. Pending events retain their original
collection version and are replayed in queue order.

Snapshots compact changesets between schema transitions using SQLite Changegroup,
which processes changeset blobs without a database connection. The snapshot head
is published durably before covered log records are deleted and Spool reclaims
segments. Metadata keys stay live independently of the replay archive. Schema
migrations retain their replay dependencies, so frequent migrations can grow the
archive. Snapshot processing uses temporary buffers, not a continuously resident
copy of SQL rows.

## Conflict resolution

Per-column versions choose a deterministic LWW winner within a row generation.
Deletes retain a tombstone for the generation, so an update of that generation
cannot resurrect it. A subsequent captured INSERT receives a new generation.
A delete/reinsert that SQLite coalesces into UPDATE is indistinguishable from an
ordinary update and keeps its generation.

Offline state export and joining are supported. Export explicitly reads SQL rows
and retrieves versions from Spool. The worker validates schema/identity and joins
states; the writer applies accepted SQL rows atomically. Full state transfer is
currently the offline API; incremental peer transport, QUIC membership, alternate
ciphers, and operation-based CRDT types are deferred.

## Schema authority

Schema sync rides the replication session, not the SQLite writer. Hello and
Welcome exchange canonical schema bytes as an unsigned mismatch hint; adoption
happens only through signed manifests (`murmur/sqlite-schema/v1`, Ed25519 over
database, signer, monotonic sequence, and payload digest). When `schema_master`
is configured, exactly that node id is adopted from and the master itself never
adopts; otherwise any trusted origin is adopted (legacy symmetric mode).
Sequence cursors and last-accepted manifests live under
`07/replication/schema/` in Spool, so replays stay idempotent across restarts
and old payloads referencing since-retired tables still apply. Payloads carry
their origin so receivers filter retired tables per sender; unknown senders
keep strict checking. Adoption stays additive (tables, nullable columns,
indexes) and local-only tables are excluded from sent schemas and untouched by
adoption.
