# RIME migration schema and codec

This document tracks the implementation state of the rich Go record schema
described in [the migration plan](migration-plan.md#4-record-schema-encoding-and-evolution).
The production cutover has removed the legacy SQL engine. The typed facade
provides
available through `Define[T]`, `Config.Tables`, `TableOf[T]`, and
`DB.WriteTxContext`; typed mode disables SQL entry points. `Open` binds the
compiled definitions to a private RIME database, rebuilds it from encrypted
Spool on open and after completed snapshots, and routes local and accepted
remote writes through the adapter. Typed opens accept compatible additive
schema subsets and use the persisted manifest as authoritative; older writers
retain unknown top-level cells in Spool. The adapter supports LWW BLOB fields,
numeric MIN/MAX fields (updated with `RecordMin` and `RecordMax`), top-level
int64 PN_COUNTER fields (updated with `RecordCounterAdd`), plus
top-level `[]string` OR_SET fields (updated with `RecordSetAdd` and
`RecordSetRemove`). These use managed CRUD/batches, read-only query
filters/order/page/count,
compiled positional parameters, aggregates/grouping, and snapshot-bound inner
and left joins. Query and join outputs are cloned through the registered codec.
Successful durable local typed commits advance the shared observer cursor and
wake the replication sender. `RecordTable.Subscribe` emits typed initial and
update snapshots, coalesces changes, and resets on bounded-buffer overflow. The
live `typed-records` scenario verifies two-process propagation, counter and set
convergence, peer restart reconstruction, additive runtime migration, older-peer
adoption, and preservation of a new field across an older-peer write/restart.
Snapshot application rebuilds and
switches the private RIME materializer only after durable Spool
publication. `DB.MigrateRecords` now publishes additive runtime definitions,
rebuilds RIME from the new manifest and durable rows, and rebinds typed handles.
Peers with compatible older runtime descriptors can adopt additive descendants
and continue retaining unknown fields. Destructive and incompatible changes
remain rejected. Remaining operational release qualification does not restore
or depend on SQLite. The live `rekey` scenario verifies typed records survive
application-key rotation and restart. The live `backup-restore` scenario
backs up an encrypted typed node, restores under a fresh writer identity, and
verifies the record after reopening its RIME materializer. Transaction callbacks may
prepare concurrently; durable commits and publication pass through one adapter
coordinator. Terminal Spool errors return an uncertain outcome with the
transaction ID as `*CommitOutcomeUncertainError` and fail the adapter closed.
After reopening, `HasTransactionReceipt` resolves whether that typed transaction
was recovered.

The bridge import schema gate reads the persisted table manifest, so its
descriptor, primary-key, merge-policy, type, and nullability checks work with
native typed schemas. Typed row-existence checks use registered RIME tables,
and typed imports commit imported cells, ownership-policy state, generated
provenance and source receipts atomically through Spool before RIME publication.
The `typed-bridge` live scenario verifies Low-to-High import and High restart
reconstruction. Bridge imports now require managed typed tables; the SQL
transaction/import fallback has been removed.

## Descriptor compiler

`internal/recordcodec` compiles a Go record type using an explicit table ID,
primary field and stable field IDs. The replicated primary key must be a
16-byte array. Every exported persisted field, including fields in nested
structs, requires a nonzero ID supplied separately from Go declaration order.
The compiler retains Go field names only as local bindings; IDs and value kinds
are the durable identities.

The current descriptor covers booleans, signed and unsigned integers, float32
and float64, strings, bytes, arrays, nested structs, slices, maps, pointers,
`Optional[T]`, `time.Time`, and registered custom codec identities. Map keys are
limited to scalar bool/integer/string types and byte arrays. Interfaces and
unsupported kinds fail descriptor compilation. Recursive Go type graphs reuse
their compiled descriptor nodes.

Merge policy metadata supports LWW, numeric minimum/maximum, integer counters,
and OR-sets over slices, maps or bytes. The typed adapter currently maps MIN/MAX
numeric scalars and counters plus top-level `[]string` OR_SET operations. The
compiler rejects incompatible field types; custom CRDT value semantics still
require explicit codec/application integration. When schema evolution widens
an integer MIN/MAX field to float32 or float64, durable integer extrema are
projected through Go's numeric conversion with destination overflow checks; this
keeps the rebuilt RIME record consistent with the widened descriptor.

## Remaining implementation

`internal/recordcodec` now provides the initial RGV1 canonical record format.
It sorts top-level and nested fields by stable ID and maps by encoded key,
preserves nil versus empty bytes/slices/maps and explicit optional presence,
retains unknown top-level and nested-struct field payloads for round trips, and
bounds value bytes, nesting and collection counts during decode. Unknown nested
fields carry the stable-ID path of their enclosing record fields; edits to
known sibling fields preserve the opaque payload and canonical ID ordering.
Absent pointer and `Optional[T]` field encodings consume their explicit
presence tag during decode and round-trip as absent values. Nil byte slices
also consume their nil-presence tag and remain nil after field decoding;
regression tests cover all three forms.
This extends the in-memory `UnknownField` metadata without changing RGV1 bytes,
so existing encoded values remain readable. It supports fixed-width float bits
and UTC instants. Executable custom codecs register through
`RecordOptions.Codecs` with a stable ID/version and encode, decode, clone, and
equality functions. The adapter invokes the custom clone hook at ownership
boundaries and the equality hook while coalescing durable field changes.

Compiled descriptors can be serialized as RSD1 bytes with stable numeric field
IDs and without Go field names. `schema.TableSchema.RecordDescriptor` carries
those bytes into SMF3 manifests; the manifest content hash covers them. SMF1
and SMF2 remain byte-compatible for tables without rich descriptors. Schema
compatibility now permits recursively additive fields and deterministic unions
of concurrent additions; existing field types, codec identities, merge rules,
table IDs and primary keys must remain unchanged. The adapter preserves unknown
fields in nested structs, arrays, slices and map values when an older writer
changes that top-level field. Opaque payload paths use collection element
indexes and canonical encoded map keys. Encoder allocation is not streamed
against a budget. Runtime typed schema re-registration is implemented for
additive table/field declarations. The typed live suite now partitions two
peers, creates different compatible additive branches, reconnects them, and
checks that both nodes bind the merged record type with both branch values
retained. Delivery permutations and broader schema fault qualification remain
open. The
adapter now has a remote apply seam: once `state.CommitRemote` accepts a merge,
`ApplyRemote` reads affected registered rows from one bounded point-in-time
state snapshot, including tombstones and all winning fields, then stages all
affected rows in one RIME transaction. Invisible rows are deleted from RIME;
visible rows are decoded and upserted after checking their primary key against
the durable row ID. This path does not capture local mutations or echo remote
changes to Spool. A post-acceptance read, validation, or publication failure
makes the adapter fail closed. It streams up to 100,000 distinct affected rows
from one state snapshot into one RIME transaction, avoiding the previous 4096
row bulk-read limit while retaining an explicit bound.
Runtime storage and replication continue using the current cell schema and
codec versions. Root typed snapshot apply rebuilds a private RIME database from
the completed durable snapshot and switches table handles under a lock; it
does not invoke the legacy SQL rebuild path. Runtime schema adoption accepts
additive descendants when current typed bindings remain compatible and rebuilds
the private materializer from the adopted Spool manifest. Incompatible
descriptors still fail closed.

Sources: [descriptor.go](../internal/recordcodec/descriptor.go),
[RIME presence types](../rime/optional.go), and
[the migration plan](migration-plan.md#4-record-schema-encoding-and-evolution).
