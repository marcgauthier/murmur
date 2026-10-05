# Schema-level CRDT merge policies

## 1. Column contract

`schema.ColumnSchema.MergePolicy` is immutable once a column is published.
Omitting it selects `schema.LWW`. Additive migration can add policy columns;
changing an existing column's policy, including conversion of existing JSON or
integer data, is rejected. Allocate a new column and migrate application data
explicitly. Primary keys always use LWW.

| Policy | Schema type | SQL projection | Writes and merge |
| --- | --- | --- | --- |
| LWW | Existing supported types | Original scalar | Largest `(HLC, NodeID)` wins. |
| PN_COUNTER | TEXT | Exact signed decimal integer | Explicit deltas; each actor's positive and negative components merge by maximum, then sum. |
| OR_SET | TEXT | Canonical typed JSON array | Explicit additions and observed removals; concurrent unobserved additions survive. |
| MAX | INTEGER or REAL | Numeric scalar | Largest numeric value; NULL is neutral. |
| MIN | INTEGER or REAL | Numeric scalar | Smallest numeric value; NULL is neutral. |

Counters use arbitrary precision integers. `CounterValue` returns a copied
`*big.Int`; SQL returns decimal TEXT, avoiding SQLite integer overflow and
floating-point loss. The existing operation/value and transaction limits still
apply to each causal record and signed transaction. SQL arithmetic on a TEXT
counter does not implement a distributed counter.

Set elements are null, boolean, int64, finite float64, or valid UTF-8 string.
Integer `1`, real `1`, boolean `true`, and string `"1"` remain distinct. Real
negative zero normalizes to positive zero; NaN and infinity are rejected.
JSON objects carry `type` and, except null, `value`. Integer and real values
are decimal strings. Entries sort by canonical binary encoding, not lexical
string order. `SetValues` returns typed elements; their `Encode` and
`MarshalJSON` methods preserve identity. Arrays, nested objects and blobs are
not set elements.

MAX/MIN compare integer/real values exactly using rational conversion, and break
numerically equal representation ties by canonical encoded bytes (largest for
MAX, smallest for MIN). A cell's row-visibility version advances independently
of whether its numeric value changes. Only finite numeric values are accepted.

## 2. Transaction API

Declare columns in `SchemaConfig`:

```go
schema.ColumnSchema{Name: "count", Type: schema.ColText, MergePolicy: schema.PN_COUNTER}
schema.ColumnSchema{Name: "tags", Type: schema.ColText, MergePolicy: schema.OR_SET}
schema.ColumnSchema{Name: "peak", Type: schema.ColInteger, MergePolicy: schema.MAX}
```

Initialize counters with `'0'` and sets with `'[]'` when inserting a row; nullable
columns may start at NULL. Counter/set UPDATE and non-neutral INSERT through
ordinary SQL are rejected at commit and rolled back. SQL change capture checks
individual events so a forbidden assignment cannot hide behind a subsequent
valid operation in the same transaction. MAX/MIN use ordinary numeric SQL writes.

```go
tx, err := database.BeginTx(ctx, nil)
if err != nil { return err }
defer tx.Rollback()
if err := tx.CounterAdd(ctx, "items", "count", row, big.NewInt(3)); err != nil { return err }
if err := tx.SetAdd(ctx, "items", "tags", row, murmur.SetString("ready")); err != nil { return err }
if err := tx.SetRemove(ctx, "items", "tags", row, murmur.SetString("pending")); err != nil { return err }
return tx.Commit()
```

`CounterValue(ctx, table, column, row)` and
`SetValues(ctx, table, column, row)` read within a transaction, including its own
operations. Rows must exist. Removing an unseen element and adding a zero delta
are no-ops. There is no assignment/reset API: create a new row identity to reset.
Existing LWW row tombstone/resurrection rules still control visibility; delete
and reinsert of the same row identity retains its causal history.

## 3. Durable state and authenticated replication

Pebble prefix `0x0e` stores individual causal records, scoped by table, row and
column. Counter keys are `(positive/negative, DBID, NodeID)`; set keys are
`(add/remove, DBID, NodeID, TxID, operation index)`. Component magnitudes are
canonical unsigned big-endian integers. Set removal records retain the exact
observed addition identity and typed element. Records merge idempotently;
conflicting elements for an existing set tag are rejected.

Caches under the existing cell prefix hold SQL projections. Durable causal
records, projections, logs, receipts and watermarks publish in one commit.
Group commits resolve counter deltas against preceding staged components before
signing. Prepared transactions, startup rebuild and encrypted backup preserve
causal records. Restore retains historical actor namespaces and requires a fresh
writer identity as before; subsequent writes allocate a new actor component.

[Origin signatures](origin-signatures.md) authenticate the policy byte and every
causal record in protocol 5. Peers forward the original signed transaction.
Native components and additions in the local DBID must belong to the signed
origin; foreign database actors require the explicit bridge import flag.
Ordinary remote policy mismatches and malformed records fail before progress.
The imported source proof is the separately authorized bridge signature.

Snapshot manifest version 2 carries bounded individual metadata records, not
whole set/counter values. Counter/set scalar projections export bounded neutral placeholders (preserving
NULL versus initialized identity) and rebuild after causal records join. Atomic and resumable snapshot modes preserve
removals and component history and publish coverage only after digest validation.
Snapshot donors still require explicit trust: snapshot state is not individually
origin-signed. See [snapshots and restore](snapshots-backup-and-restore.md).

## 4. High/Low bridge

Bundle suite 2 carries policy and causal payloads under the artifact signature.
Schema holds include policy mismatch. Replaying source components through another
stream from the same source domain does not multiply counts or additions.
Source row-transaction receipts commit atomically with imported row state;
stream/inbox publication remains resumable and idempotent.

Low causal state keeps merging while High ownership masks its projection. First
High takeover seeds a separate branch with the observed Low baseline, retaining
source actor/tag identity. Native High operations then merge in that branch.
Releasing ownership reveals the latest Low state. A later takeover uses a new
branch epoch tied to the release marker, preventing delayed operations from a
retired branch from restoring ownership. The same rules apply to counter, set
and extrema columns. See [High/Low provenance](high-low-replication.md).

## 5. Upgrade and operational limits

New stores require format/minimum reader/minimum writer 5, mutation codec 3,
protocol/minimum protocol 5 and `CapMergePolicies` plus `CapOriginSignatures`.
All-LWW schema manifest bytes and content hashes retain their historical encoding.
Schemas with policy columns use SMF2. Historical protocol-4 signed logs retain
their original encoding and digest; they can still be read and forwarded inside
the upgraded cluster. Peers below protocol 5 are rejected.

Stop writers, take a recoverable encrypted backup, and call
`murmur.MigrateMergePolicies(ctx, cfg)` offline to upgrade a signed format-4
store. Ordinary Open refuses that store. The migration preserves historical
signed logs and schema identities and disables replication during migration.
Unsigned format-2/3 stores still require `MigrateOriginBaseline`. Downgrade after
migration is unsupported. Bridge peers must upgrade together to suite 2.

Causal history is retained without age-based garbage collection: set removals
cannot safely disappear merely because a node has been offline for a long time.
Record count and storage therefore grow with actors/additions/removals. Projection
work and memory scale with a cell's retained history; this implementation scans
that history on merge. Use bounded cells and application-level row rollover for
long-lived workloads. `DB.CRDTRecords` returns a copied durable read cut for
inspection of underlying Low records; it does not report an active High branch.
It does not flush deferred SQL materialization or pending group commits.
`DB.MergePolicyStats(ctx)` scans metadata on demand and reports logical bytes,
actor components, additions/removals, process merge attempts/total nanoseconds,
and rejected policy batches. Poll infrequently on large databases. Merge timing
measures join work, including attempts that later fail durability; it does not
measure snapshot publication or end-to-end commit latency.

## 6. Verification

`merge_policy_test.go` covers transaction operations, reopen, remote convergence,
atomic/resumable snapshots and High ownership. `state/merge_policy_test.go`
checks permutation/duplicate joins, forged actors, signed payload tampering and
format-4 migration preserving signed bytes. `bridge/merge_policy_test.go`
exercises capture, sealed bundles, replay, multiple streams and High takeover.
The live `tests-live/merge-policies` scenario uses three real encrypted daemons
with QUIC/mTLS, disconnected writes, a counter beyond int64, origin-offline
forwarding, causal-state comparison, restart and observed removal.
