# Schema-level CRDT merge policies

## 1. Column contract

`schema.ColumnSchema.MergePolicy` is immutable once a column is published.
Omitting it selects `schema.LWW`. Additive migration can add policy columns;
changing an existing column's policy, including conversion of existing JSON or
integer data, is rejected. Allocate a new column and migrate application data
explicitly. Primary keys always use LWW.

| Policy | Typed field | Projection and merge |
| --- | --- | --- |
| LWW | Any supported record field | Largest `(HLC, NodeID)` wins. |
| PN_COUNTER | `int64` | Explicit deltas; each actor's positive and negative components merge by maximum, then sum. |
| OR_SET | Top-level `[]string` | Explicit additions and observed removals; concurrent unobserved additions survive. |
| MAX | Numeric field | Largest numeric value; NULL is neutral. |
| MIN | Numeric field | Smallest numeric value; NULL is neutral. |

Counter components use canonical arbitrary-precision integers in durable
metadata, while the managed Go API exposes an `int64` field and `int64` deltas.
The operation/value and transaction limits apply to each causal record and
signed transaction. Ordinary assignment cannot update an existing counter.

The managed OR_SET API accepts strings in a top-level `[]string` field. The
wire codec retains typed element identities for causal records, so additions
and observed removals remain deterministic across peers.

MAX/MIN compare integer/real values exactly using rational conversion, and break
numerically equal representation ties by canonical encoded bytes (largest for
MAX, smallest for MIN). A cell's row-visibility version advances independently
of whether its numeric value changes. Only finite numeric values are accepted.

## 2. Managed record API

Declare merge policies with the typed record definition:

```go
type Item struct {
    ID    ids.RowID `rime:"primary"`
    Count int64
    Tags  []string
    Peak  int64
}

definition, err := murmur.Model[Item](murmur.ModelOptions{
    Name: "items", TableID: 1,
    RecordOptions: murmur.RecordOptions{
        PrimaryField: "ID",
        MergePolicies: map[string]murmur.RecordMergePolicy{
            "Count": murmur.RecordMergeCounter,
            "Tags":  murmur.RecordMergeORSet,
            "Peak":  murmur.RecordMergeMax,
        },
    },
})
```

Use managed CRDT operations in the same transaction as the row write. Direct
assignment to existing CRDT-owned fields is rejected, and zero deltas or
removal of an absent set element are no-ops.

```go
return database.WriteTxContext(ctx, func(tx *murmur.Tx) error {
    if err := tx.InsertItem(&Item{ID: row, Peak: 4}); err != nil { return err }
    key := &Item{ID: row}
    if err := tx.CounterAdd(key, "Count", 3); err != nil { return err }
    if err := tx.SetAdd(key, "Tags", "ready"); err != nil { return err }
    return tx.Max(key, "Peak", int64(5))
})
```

Read the merged projection with `GetItem`. There is no assignment/reset
API for CRDT-owned fields: create a new row identity to reset. Existing LWW row
tombstone/resurrection rules still control visibility; delete and reinsert of
the same row identity retains its causal history.

## 3. Durable state and authenticated replication

Storage key prefix `0x0e` stores individual causal records, scoped by table, row and
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

The RIME cutover uses format/minimum reader/minimum writer 6, mutation codec 4,
protocol/minimum protocol 6, snapshot manifest format 3, and
`CapMergePolicies` plus `CapOriginSignatures`. Schemas with policy columns use
SMF2; rich RIME descriptors use SMF3. Format-5 SQL-era stores fail closed and
must be exported with the previous release before applications start with a
fresh typed database. Peers below protocol 6 are rejected. Downgrade is
unsupported; bridge peers must upgrade together to suite 2.

Causal history is retained without age-based garbage collection: set removals
cannot safely disappear merely because a node has been offline for a long time.
Record count and storage therefore grow with actors/additions/removals. Projection
work and memory scale with a cell's retained history; this implementation scans
that history on merge. Use bounded cells and application-level row rollover for
long-lived workloads. `DB.CRDTRecords` returns a copied durable read cut for
inspection of underlying Low records; it does not report an active High branch.
It does not flush RIME materialization or pending group commits.
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
with QUIC/mTLS and the managed typed API. It covers disconnected PN_COUNTER,
OR_SET and MAX/MIN writes, forwarding after the original writer stops,
unobserved versus observed set removal, and restart reconstruction. The public
typed counter currently uses int64 projections; arbitrary-precision schema
counters remain part of the legacy-only API and are removed with that path.
