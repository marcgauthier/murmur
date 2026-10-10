# Automatic models and item API

[Architecture index](README.md) · [Application usage](../USAGE.md)

## Registration and durable identities

`Config.Models` accepts named struct exemplars and compiled definitions from
`Model[T]`. It is combined with `Config.Tables` before opening storage. The
combined schema undergoes the existing duplicate, reserved-ID, codec, scope,
merge-policy and compatibility validation. No storage or replication format is
introduced by model registration.

Models require an exported `ID ids.RowID` tagged `rime:"ID"`, or the legacy
`ID rime:"primary"` form. `ModelOptions` embeds `RecordOptions` for explicit
identity-field selection, field IDs, merge rules, codecs, scope and depth;
`Name` and `TableID` override the default collection identity. `Model[T]`
is the only table-definition API.

Automatic IDs use the existing SHA-256 name derivation in `schema`: tables use
`table:<lowercase model name>`; root fields use
`column:<lowercase model name>:<lowercase logical field name>`. Nested fields
use `column:<lowercase model name>:nested:<lowercase logical field name>` in
separate descriptor scopes. Reordering fields or registering models in another
order does not change durable identities. Numeric collisions, logical-name
collisions and reserved identities fail registration. Renames need logical
name or numeric-ID overrides. Nested Go type names do not affect durable IDs.
An explicit field-ID override on a reused nested Go type applies to every use
of that field; conflicting overrides fail registration.

## Row identity

The physical row ID is separate from an optional business field tagged
`primary`. At most one business field is allowed: string, signed/unsigned
integer or a 16-byte UUID, including named equivalents. It receives an equality
index and uses LWW encoding. A nonzero caller ID is authoritative. For a zero
ID, a nonzero business key generates
`UUIDv5(rime.TableNamespace(modelName), canonicalFieldBytes)`; no business key
means cryptographic UUIDv4. The canonical bytes come from the existing record
codec, without formatted values or a process-local sequence.

Local full replacements, upserts and callback updates reject business-key
changes. `UpdateFields` rejects physical and business identity assignments.
These rules run at the managed adapter boundary, covering typed model handles
as well as item methods. Authoritative remote apply and materializer rebuild
retain existing conflict resolution and do not run local identity generation
or immutability checks. Explicit distinct IDs can share a business key; model
primary tags do not introduce distributed uniqueness. Applications must deploy
consistent model identity rules to their replicas.

IDs populate writable caller records before staging and are retained after a
rollback or uncertain outcome. Get/update/delete can derive UUIDv5 from a key;
they never invent UUIDv4 references. Existing commit uncertainty and receipt
contracts remain unchanged; see [transactions](transactions.md).

## Shared engine and ownership

RIME's `Register[T]` and `RegisterType` share schema compilation, accessor
initialization, indexes, shards, MVCC and commit coordination. `RegisterType`
uses `Table[any]` with a `*any` carrier containing a native struct pointer.
Reflective accessors unwrap that carrier; typed tables retain direct field
loads. Prepared changes unwrap carriers to native records before durable codec
capture. This is a registration representation, not a new storage engine.

`Model[T]` installs explicitly identified adapter handles. Plain exemplars
install runtime handles for the item API. A struct registered for more than
one table is ambiguous for the item API and must use distinct model names.
The adapter shares descriptor validation, encrypted persistence, snapshot
reconstruction, bridge checks and remote reconciliation for both forms.
RIME remains restricted to the Go standard library.

Reads and query results are detached through the registered codec and recursive
copying. Updates clone after application callbacks, preventing retained input
maps and slices from becoming mutable published state. Runtime result bindings
check native types and return errors if a model has been rebound to another Go
type.
Pinned read snapshots retain their original model bindings across migration;
live queries built for a replaced Go type return a schema error.

## Operations, batches and queries

`InsertItem`, `GetItem`, `UpdateItem`, `SaveItem`, `UpdateFields` and `DeleteItem`
resolve collections by Go type. `UpdateItem` requires an existing row;
`SaveItem` upserts. `Transaction` uses the existing managed callback coordinator.
`InsertMany`, `UpdateMany`, `SaveMany` and `DeleteMany` accept homogeneous struct
or struct-pointer slices and stage one atomic batch.

RIME `Tx.Batch` exposes the existing batch savepoint. Failure truncates new
writes and restores superseded flags on preceding writes, preserving their
commit eligibility. The adapter also restores staged causal-operation lengths.
Errors identify the operation and input index. Caller objects, including
assigned IDs and partial-update values, are outside transaction rollback.

`q` matchers compile to ordinary RIME predicates; hash and ordered indexes keep
the same planning behavior. Queries support ordering/page limits, transaction
bindings, detached destination slices, first/count/existence results, and
`Explain`. `Find` and `FindOne` infer the registered model from their
slice/struct destination; `Count` and `Exists` require an exemplar. Transaction
shortcuts read staged overlays, and snapshot shortcuts resolve captured model
bindings across migrations. `Update` validates `Set` assignments, rejects
duplicates, and delegates to `UpdateFields`.

The simple matcher bridge supports named scalar types and exact `time.Time`.
Checked runtime RIME fields preserve planner-visible operator and field metadata;
existing built-in scalar comparisons retain their typed accessors. Timestamp
query operands and index extractors normalize to UTC without monotonic state.
Comparison uses wall-clock instants without epoch-nanosecond narrowing. Hash,
ordered, unique, and compound timestamp indexes agree with scans. Stored records
and codec formats remain unchanged. Prefix matching retains trie planning;
substring and suffix matching use filtered scans. `Between` and `NotIn` compose
existing comparison and logical predicates. Nested, optional, and slice querying
are deferred.

Arbitrary maps without registered struct schemas are unsupported.
See [query and search](query-and-search.md) for the underlying planner.

## Migration and qualification

`MigrateModels` compiles the complete registration set and delegates to
`MigrateRecords`. Open creates a fresh schema but never implicitly publishes
additions to an existing one. Compatibility, unknown-field retention,
manifest-before-materializer publication and remote adoption use the existing
migration coordinator. See [record schema evolution](rime-migration-schema.md).

Model tests cover registration failures, canonical UUID vectors, stable IDs,
typed/runtime CRUD, encrypted reopen, immutable keys, batch rollback, legacy
array identities and additive migration with older-binding writes. The live
`typed-records` model scenario starts two separate QUIC/TLS node processes and
checks replication, indexed query results, batch insertion, peer restart,
`MigrateModels`, deletion and tombstone recovery. The query-shortcuts live
scenario qualifies named string predicates, timestamp ranges across zones,
explicit assignments, destination-inferred reads, and restart. Performance benchmarking is
deferred.
