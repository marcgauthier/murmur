# High/Low replication and provenance

Optional extension inspired by GALVANIZE. Roles, sealed bundles, export/import
journals, schema holds, and embedded status/replay controls are implemented.
Atomic delivery, encrypted outbox storage, aggregate capacity enforcement,
definition-level schema validation, and local provenance/ownership policy are
implemented. Convergence when High policy and Low value mutations arrive in a
different order across High peers remains pending. Remaining work is tracked in
[TASKS_PENDING.md](../TASKS_PENDING.md#pending-tasks).

[Architecture index](README.md) · [Capability gaps](capability-gaps.md)

## Domains and data direction

Support a Low domain that exports logical changes to a High domain. Each domain
keeps its own DBID, schema administration, identities, storage keys, and ordinary
authenticated QUIC mesh. High can distribute accepted imports among its own peers;
it never exports its database changes back to Low through this bridge.

Expose explicit disabled, Low-exporter, and High-receiver roles through a future
package-owned configuration. Configure trusted source streams/signers, permitted
recipients, artifact transport, staging limits, retention, and retry policy. Roles
do not designate leaders or replace SWIM within either domain.

Implemented (roles): the `bridge` package provides `Role` (`RoleDisabled` by
default, `RoleLowExporter`, `RoleHighReceiver`), package-owned `Config` with
domain DBID/stream/limits validation, and role-gated `Exporter`/`Receiver`
handles. Each handle binds to its domain's DBID, so a Low exporter cannot be
wired to a High database and vice versa; cross-role construction is rejected.
One-way flow holds by construction (`Exporter` publishes only, `Receiver`
receives only) over directional `Publisher`/`ArtifactSource` interfaces, with
a shared artifact filename/size policy and a logical `Batch`/`Record` carrier
(logical values, never storage encodings) that the bundle codec, outbox,
inbox, and provenance slices build on.

The bridge transports logical records rather than raw Pebble files or ordinary
cross-DBID mesh frames. Preserve source transaction boundaries and lineage while
mapping validated imports into the High domain's schema and mutation model. Low
origin sequences are bridge progress, not High mesh receive watermarks.

One-way describes permitted application data flow. Directory/removable-media
transfer can operate disconnected; HTTP or FTP adapters may exchange transport
requests and responses. Do not claim a physical data diode guarantee.

## Sealed bundles and transport

Define a versioned canonical bundle/manifest with bundle identity, source domain
and stream identity, contiguous sequence range, transaction identities, schema
identity, payload digest, encoded/decompressed limits, signer/key identity, and
recipient/key identity. Authenticate the routing and integrity metadata as well as
the payload; tampering with sequence, schema, or recipient information must fail.

Sign exports and encrypt each payload for its intended High recipient using
separate bridge keys and fresh content keys. At-rest wrapping/data keys and QUIC
credentials are not bridge credentials. Support verification/decryption key
rotation with explicit trusted key identities and retention for pending bundles.
Use reviewed cryptographic primitives and version the suite; GALVANIZE uses
Ed25519 signatures, XChaCha20-Poly1305 payload encryption, and RSA-OAEP recipient
wrapping, but compatibility with that bundle format is not assumed.

Implemented (bundles): `bridge/bundle.go` suite v1 (`SPB1` magic) carries a
canonical manifest (bundle/transaction identities, source domain + stream,
contiguous sequence range, schema epoch/hash, payload digest) plus canonical
logical batches, zstd-compressed, sealed with a fresh XChaCha20-Poly1305
content key per bundle. The content key wraps to the recipient via X25519
ECDH + HKDF with identity-bound AAD (no RSA dependency); an Ed25519
signature covers the header and sealed payload (verify before decrypt).
Bridge signing/recipient keys are generated only in `bridge` and never reuse
storage/mesh credentials. `TrustStore` holds trusted signer keys with
per-stream authorization plus the receiver's own decryption keys, with
explicit add/remove for rotation and retention. Open enforces encoded-size,
decompression, transaction-count, and name-length bounds, verifies the
SHA-256 payload digest, requires exact sequence contiguity and manifest/batch
transaction agreement, and authorizes signer + stream. Duplicate/replay
suppression across bundles belongs to the durable inbox slice.

Start with a directory transport usable with removable media, then HTTP(S) and
FTP(S) adapters as required. Publish the payload before an atomic manifest/commit
marker; receivers ignore incomplete artifacts. Reuse backup transport plumbing
only where its semantics fit. SFTP remains unsupported until its implementation
and acceptance tests exist.

Validate signatures, authorized sender/stream, recipient, hashes, format, schema,
sequence ranges, and encoded/decompressed bounds before applying changes. Treat
all artifact content and filenames as untrusted. Transfer integrity is independent
of whether the artifact transport uses TLS.

## Durable export and import state

Persist the Low export event/outbox atomically with the corresponding durable
exportable transaction. Include eligible changes learned from other Low mesh
members, preserving their source transaction identity so forwarding cannot create
duplicate exports or omit writes made away from the exporter. A crash between a
commit and an export wakeup must not lose that change. Bundle generation and
delivery are retryable; track publication
separately from event creation. Retain pending events/artifacts until an explicit
delivery/retention policy allows collection. A one-way bridge cannot depend on
High returning mesh acknowledgements; expose backlog and exhaustion rather than
silently deleting undelivered changes.

Implemented (export/outbox): `bridge/outbox.go` journals transaction-linked
events (batch + source identity + writer schema + export seq) crash-safely
(tmp/rename/fsync per event, resume in synced state) and replays them after
restart; published history is reclaimed by keep-last-N retention while
pending/failed events are always retained. `bridge/capture.go` drains every
origin log (`LogSource`, fed by `DB.BridgeLogSource`) with per-origin resume
stored in the outbox. Uncaptured origin logs are protected from log GC via
`BridgeLogSource.ProtectResume` / `Store.SetBridgeExportResume`, ensuring that crashes
before capture, restarts, delayed polling, and concurrent GC cannot lose an exportable
transaction. Tombstones collapse per row with delete-wins, puts merge last-wins,
IDs resolve via `DB.BridgeSchema` (additive registries resolve history), and
unknown IDs or missing logs stall loudly instead of skipping.
`bridge/publish.go` packs contiguous same-schema runs into bundles, seals via
`NewBundleSealer`, and publishes with bounded exponential-backoff retry;
transport failures stop the drain (events stay queued) while poison runs are
quarantined as failed. Directory/removable-media, HTTP(S) PUT, and FTP(S)
STOR adapters wrap the tested `backup.Destination` blob stores. No mesh
acknowledgements are required; backlog is visible as pending/failed events.

Implemented (outbox encryption): `bridge/outbox_crypto.go` seals each event
file under XChaCha20-Poly1305 with a per-file random nonce and the
sequence/key identity as associated data (`OpenOutboxEncrypted`, keys
re-supplied by the caller on every open and never journaled). Plaintext
journals keep opening unencrypted, but mixing modes fails loudly in both
directions; unknown keys, wrong key bytes, ciphertext/nonce tampering,
and cross-file transplants all fail closed at open. Rotation adds the
successor to the readable ring before `Rekey` converges files onto it
and the predecessor retires, preserving pending exports across every
step and restart. Mapping journal backpressure into commit admission
and bulk-transfer budgets still awaits those systems.

High keeps an encrypted inbox, stable bundle/transaction identities, per-stream
highest observed and highest contiguous applied sequence, missing ranges, import
status, and replay jobs. Staging/download completion is not an applied watermark.
Reject conflicting content for an already known identity; identical replays are
idempotent. Hold later sequences behind gaps and resume when earlier artifacts
arrive. Restart recovers outstanding work without losing accepted data.

Implemented (inbox/import): `bridge/inbox.go` journals staged bundles with
per-stream observed/applied watermarks, explicit gap lists, quarantine
records, and a bounded digest window; identical replays are idempotent while
conflicting content is quarantined terminally (bytes preserved, never
re-driven) without blocking the known original. `bridge/import.go` applies
each bundle atomically in authoritative storage (`ApplyBundle`), preserving
source transaction boundaries and recording stable source-transaction receipts
(`DB.RecordTransactionReceipt` / `Store.RecordReceipt` / `ReceiptKey`) in Pebble
alongside contiguous stream progress (`Store.SetBridgeStreamProgress` /
`BridgeProgressKey`). Batches with existing receipts are skipped, ensuring that
replays after crashes or independent imports by concurrent High receivers
deduplicate without creating fresh local writes, duplicate mutations, or
diverging HLC timestamps. Each import transaction mints a fresh local TxID
(source identity stays in provenance and receipts): reusing the source
BundleID would give independent importers identical TxIDs, and on the High
mesh those batches hit duplicate-TxID acknowledgement without origin
watermark advance, gaping every later batch forever. `Drain` reconciles inbox watermarks with authoritative
storage on startup and recovery via `Inbox.SyncAuthoritativeProgress`.
Schema-incompatible bundles enter durable `waiting-schema` holds instead of
quarantine: `bridge/holds.go` journals the hold with the bundle's required
schema epoch/hash and the missing objects, `NextImport` skips held heads
(other streams still drain), and every `Drain` rechecks held heads in order
against the live schema, so a local `Migrate` releases waiting bundles
automatically with no DDL transfer and no partial effects. The pre-apply
gate checks tables, columns, primary keys, per-value type compatibility
(exact match through SQLite-ish aliases, plus lossless INTEGER into REAL),
and NULL against NOT NULL columns. Only apply-time infrastructure failures
(e.g. a storage error mid-commit) quarantine, preserving the bytes for
idempotent replay after recovery; an unreadable schema leaves the bundle
staged for a later drain.

The current schema gate checks object presence, not compatible types or full
required definitions. Definition-level validation and holds remain pending.

Commit accepted transaction effects, provenance/ownership changes, receipt, and
contiguous stream progress atomically in authoritative High storage. Materialize
through the normal apply path. Mesh forwarding preserves accepted policy metadata;
it does not generate a fresh Low event or duplicate bridge receipt. Serialize
competing import attempts, including separate High receivers, by stable source
transaction identity so duplicates cannot become new local writes.

Bound inbox/outbox bytes, bundle sizes, transaction counts, decompression, replay
jobs, and worker queues. Share the [writer scheduler](synchronization-and-overload.md#local-and-replication-write-scheduling)
and bulk-transfer budgets; a disconnected destination must not exhaust storage or
prevent local reads indefinitely. Report explicit backpressure when durable export
capacity cannot accept another exportable commit.

Implemented (journal capacity): `bridge/capacity.go` enforces aggregate
`MaxStagingBytes`/`MaxStagingEntries` at admission — outbox appends and
inbox receipts (including conflict forensics) fail with a
`*BackpressureError` (`errors.Is` `ErrBackpressure`) before writing
anything, so a full journal never silently drops queued or incoming
work; held bytes stay counted in staging, quarantine moves are
byte-neutral, and only published/applied history is ever reclaimed.
Accounting is restart-safe (recomputed from the journal dirs on open,
with crash-orphaned temp files swept) and race-guarded under the
journal locks; `Outbox.Usage`/`Inbox.Usage` and the status snapshots
expose usage against bounds. There are no bridge worker pools to bound
(drains are synchronous). Mapping backpressure into commit admission
and bulk-transfer budgets awaits those systems.

## Schema boundary and holds

Never transport DDL or automatically adopt Low schema changes. Each domain applies
its own migrations. Every bundle identifies the canonical replicated schema it
requires; do not hash local indexes, FTS objects, or incidental SQL formatting.

An incompatible bundle enters a durable `waiting-schema` state without partial
effects or advancing stream progress. After an administrator applies a compatible
schema, retry held bundles in sequence automatically. Permanent signature, sender,
or content failures are quarantined with an explicit reason, not treated as schema
waits. Manual replay does not bypass validation or duplicate detection.

## Provenance and High-owned fields

Persist source domain/stream, original row and transaction identity, first/last Low
sequence, and High override/field ownership metadata. Expose read-only provenance
inspection and controlled ownership/replay operations through the embedded API.

An accepted Low value may update an imported field only while that field remains
Low-owned. A High local edit marks that field High-owned; subsequent Low imports
cannot overwrite it merely by carrying a newer HLC. Ordinary High mesh writes
continue to use the existing deterministic LWW rules among eligible High versions.

Implemented: row and field policy records are hidden replicated mutations in
authoritative state, so normal High mesh replication, snapshots, and backups
carry them with their values. Local High edits mark Low-owned fields High-owned;
Low updates skip those fields when the ownership record is already present.
`DB.ReleaseBridgeOwnership` explicitly returns a field to Low ownership,
`DB.ReleaseBridgeRowOwnership` releases a High-held imported row, and
`DB.ReleaseBridgeFileOwnership` / `DB.ReleaseBridgeFileRowOwnership` do the
same for file metadata. Provenance inspection is available through
`DB.BridgeRowProvenance`, `DB.BridgeFieldProvenance`,
`DB.BridgeHighOwnedColumns`, and the `DB.BridgeFile*` counterparts.

Reordered policy/value delivery reconciles through same-row ownership shadow
cells (`bridge_shadow.go`), so a newer-HLC Low value can never overwrite a
High-owned field on any peer. Every High write to an imported field commits
its value into a shadow column (`c^0x80000000`, wrapped set/clear encoding);
Low imports never write shadows, and merge stays plain LWW everywhere, so
all peers converge on identical cells. Reads resolve each field to its
shadow value while a set-shadow wins (SQL via winner resolution plus a
resolving state reader over rebuild/repair/apply; file metadata via direct
effective-cell reads), tombstones over shadowed rows suppress with a stored
tombstone re-drive once shadows clear, and releases/High deletes/accepted
Low deletes clear shadows to restore Low visibility. Schema validation
rejects column IDs ambiguous with the shadow mapping; legacy collisions fail
shadow writes closed. A High write committed while blind to a concurrent
import (no policy visible locally) still arbitrates that field by HLC; once
any peer's import lands, later High writes take shadowed ownership.
Covered by `TestBridgeShadowReorderConvergence`,
`TestBridgeShadowDeleteReorderConvergence`,
`TestBridgeShadowFileReorderConvergence`, and the shadow codec/mapping/
resolve/schema unit tests.

Low deletes of rows with High-owned fields enter a durable policy hold.
`Importer.ResolvePolicyHold` supports `ResolutionKeepHigh` to consume the Low
delete while preserving the row, or `ResolutionAcceptLowDelete` to delete it and
release field ownership. Unmodified Low-owned rows accept Low deletes. A row
already created in High, or attributed to another Low domain/stream, is
quarantined as an identity collision rather than implicitly transferred.

## File artifacts

File metadata crosses as ordinary bundle records under the reserved file
table; object bytes cross as recipient-sealed chunk sidecars. Capture splits
file mutations into their own batches; after each bundle publishes, the file
publisher streams the referenced objects' verified plaintext into bounded
chunks sealed for High's bridge recipient key. The Low at-rest key never
crosses the domain boundary.

Implemented: `bridge/filechunk.go` (chunk framing, seal/open with the shared
envelope, artifact naming), `bridge/fileexport.go` (chunk emission with a
durable outbox pending journal for objects whose bytes are missing, retried
every drain), `bridge/fileinbox.go` (disk staging with journaled progress,
plaintext-identity idempotency, per-object quarantine and retry), and
`bridge/fileimport.go` (install once metadata is present in either arrival
order, streaming re-encryption under High's storage key, replicated-digest
verification before availability flips). Import honors the row ownership
policy: file provenance uses the same hidden policy store
(`DB.BridgeFileProvenance`, `DB.BridgeFileHighOwnedColumns`), High-local
uploads mark files High-owned, Low deletes of High-owned files hold for an
explicit decision, identity collisions quarantine, and a High without files
holds file bundles until files are enabled. Accepted metadata notifies High
mesh replication, so verified files redistribute within High without ever
exporting back to Low. Orphaned staging (objects whose metadata never
arrives, e.g. deleted before sending bytes) stays staged under the staging
caps; sweeping it is future work.

## Diagnostics and acceptance

Expose role, stream progress/gaps, pending/held/quarantined bundles, oldest backlog
age, publication/import failures, key identities, replay status, and provenance.
Do not log plaintext payloads or keys. Controls stay authenticated when wrapped by
an optional service adapter.

Implemented (status/diagnostics): `bridge/status.go` composes the embedded
snapshot — `Outbox.ExportStatus` (pending/failed events with attempts,
errors, oldest age, exact totals), `Inbox.ImportStatus` (bounded
per-stream progress), `TrustStore.KeyStatus` (public identities only),
and `Inbox.ReplayStatus`/`RetryAllQuarantined` (retryable vs terminal
conflicts, batch replay that never re-drives forensics) — via
`Bridge.Describe`, with populated applied/observed/exported watermarks.
Gap enumeration is capped with an exact arithmetic total (a far-ahead
bundle cannot hang listing), every section carries exact totals alongside
capped lists, and summaries exclude record payloads and private key
material by construction. Provenance is inspectable through the DB APIs above;
service-adapter wrapping stays a future integration.

Acceptance requires:

- Low-to-High transfer across disconnected periods with no High-to-Low exports.
- Forged signatures, wrong recipients/senders, corrupt payloads, oversized or
  expanding compressed data, and malicious paths rejected without partial apply.
- Duplicate/reordered bundles, missing sequence recovery, multiple High receivers,
  and replay preserving one accepted transaction identity and contiguous progress.
- Crashes around event capture, artifact publication, import commit, materialization,
  and replay cursors recovering without lost commits or duplicate effects.
- Schema holds surviving restart and resuming after compatible migration; no DDL
  crosses the bridge.
- High-owned fields surviving later Low updates, protected deletes requiring High
  resolution, and all High peers converging under reordered policy/value delivery.
- Key rotation and prolonged destination outage staying within configured budgets.
