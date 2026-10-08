# Storage and materialization

Spool layout, in-memory radix tree, startup/rebuild, caching, compaction, and a log-GC example.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [13. Spool and In-Memory Radix Store as the Durable Source of Truth](#13-spool-and-in-memory-radix-store-as-the-durable-source-of-truth)
- [14. Storage Key Layout](#14-storage-key-layout)
- [20. Materialization Generation](#20-materialization-generation)
- [21. Startup](#21-startup)
- [22. Query Materializer Rebuild](#22-query-materializer-rebuild)
- [25. Memory Management and Caching](#25-memory-management-and-caching)
- [47. Spool Compaction and Physical Space Reclamation](#47-spool-compaction-and-physical-space-reclamation)
- [83. Example Log GC](#83-example-log-gc)

---

## 13. Spool and In-Memory Radix Store as the Durable Source of Truth

Authoritative state is held in-memory using an immutable Radix Tree (`github.com/hashicorp/go-immutable-radix`) and persisted to disk via Spool (`spool/`).

The store maintains four distinct classes of information:

1. Current winning state.
2. Replication log.
3. Replication/node metadata.
4. Schema/system metadata.

The current state must be sufficient to rebuild the configured query materializer
without replaying historical mutations. Managed `Config.Tables` definitions
rebuild a private RIME database from the current state.

The `state.Reader` contract exposes current rows, cells, and tombstones to a
materializer without making the durable state package depend on a query engine.
`state.RowKey` identifies materialization work independently of RIME types.
Spool and the in-memory state index remain authoritative; RIME is a rebuildable
query materializer.

---

## 14. Storage Key Layout

Signed transactions persist their complete origin proof. Fresh stores use format/minimum reader/minimum writer 6; format-5 SQL-era stores fail closed without rewrite and require export with the previous release. See [origin signatures](origin-signatures.md) for the exact format and trust boundaries.

Binary key encoding preserves lexicographical sort order.

Key prefixes:

```text
0x01 = current cell state
0x02 = row tombstone
0x03 = replication log
0x04 = local receive watermark
0x05 = peer acknowledgement
0x06 = schema
0x07 = system metadata
0x08 = transaction receipt / idempotency
0x09 = snapshot metadata
0x0a = peer exclusion
0x0b = membership record
0x0c = bridge stream progress
0x0d = transaction-chunk staging
0x0e = CRDT causal records
0x10 = node-local typed cell state
0x11 = node-local transaction receipt
```

Node-local typed cells and their receipts use a separate durable namespace.
They are absent from replicated cell iteration and snapshot export; the
`state.Store.CommitLocalRecords` primitive supports LWW cells and row
tombstones without replication. `state.Store.CommitLocalWithRecords` commits
replicated mutations and node-local LWW cells/tombstones in one Spool batch;
local cells are excluded from the signed log payload. Both paths share normal
transaction size limits and receipts. Binding node-local definitions to the
typed RIME adapter routes persistent writes through these primitives.
Ephemeral RIME tables have no Spool keys, receipts, or snapshot representation;
the adapter rejects transactions that mix them with durable tables.

### Current state

```text
01 | tableID:u32 | rowUUID:16 | columnID:u32
```

Value:

```go
type CellState struct {
    Version Version
    Type    ValueType
    Value   []byte
}
```

### Row tombstone

```text
02 | tableID:u32 | rowUUID:16
```

Value:

```go
type Tombstone struct {
    Version Version
}
```

### Replication log

```text
03 | originNode:16 | sequence:u64
```

Value:

```text
encoded MutationBatch
```

### Receive watermark

```text
04 | originNode:16
```

### Transaction receipts

```text
08 | TxID
```

Used for idempotent retry.

### In-Memory Radix Tree & Ordered Scans

The state store maintains an immutable Radix Tree in memory:
- Bytewise lexicographical key ordering is preserved.
- Provides point lookups, bounded prefix and range iteration, and snapshot roots.
- Pinned immutable roots allow row reconstruction, log serving, snapshot export, and query-materializer rebuild without holding writer locks.

---

## 20. Materialization Generation

Maintain a monotonic state generation:

```text
state_generation = uint64
```

Every committed mutation batch increments it.

The live DB tracks query visibility in memory:

```text
materialized_generation
```

After startup rebuild or a successful bulk apply:

```text
materialized_generation == state_generation
```

If the values differ during rebuild or required recovery:

```text
RIME has not yet published all durable state
```

Managed queries are unavailable until RIME publishes accepted state and
advances the materialized generation. The marker is not persisted: on every open, the
configured materializer is rebuilt from authoritative state, then the marker
is initialized to the current state generation. Failed materialization rebuilds
from state; if recovery fails, the node rejects reads and writes.

---

## 21. Startup

Startup sequence:

```text
1. Load configuration; require typed table definitions, Spool, and encryption settings and validate them, including replication budgets.
2. Reject legacy Pebble and Badger directories; resolve wrapping key and authenticate keyring/context.
3. Open Spool with AES-256-GCM encryption and configure background workers.
4. Validate database format version and physical compatibility.
5. Load records into in-memory Radix tree root; apply deletion markers.
6. Enforce fresh-identity/reseed policy before writable networking.
7. Validate typed record descriptors and compatibility with persisted metadata under Section 6.
8. Bind typed record descriptors, rebuild a private RIME database and install it atomically.
9. Build configured secondary indexes in the private RIME materializer.
10. Initialize the in-memory materialized generation from state after the selected materializer is ready.
11. Start shared QUIC listener and restore persisted peer retirement/GC obligations.
12. Start SWIM membership, asynchronous bootstrap retry, and bounded peer replication/anti-entropy.
13. Start GC/maintenance workers.
14. Mark DB ready and admit external reads/writes.
```

Do not replay the full replication history to rebuild RIME.

Only scan current state plus row tombstones.

---

## 22. Query Materializer Rebuild

Rebuild operates from a pinned immutable Radix snapshot. Databases load into a
private RIME generation and switch only after it is complete.

For each replicated table:

```text
Radix prefix scan
      |
      v
assemble rows
      |
      +----------------------+
      |                      |
      v                      v
RIME private load
```

Benchmark RIME rebuild:

- 100K rows.
- 1M rows.
- 10M rows.
- Wide rows.
- BLOB-heavy rows.

Track:

```text
rows/sec
cells/sec
time to query-ready
peak RAM
Spool reload throughput
index-build time
```

---

## 25. Memory Management and Caching

The current state and causal history live in the Radix tree. RIME maintains its own query records, indexes, and MVCC history; measure both resident copies.

Expose memory usage in `DB.Status().SpoolMemBytes` and `DB.Status().SpoolDiskBytes`. RIME owns its query indexes and MVCC history; Spool owns durable state and replication history.

---

## 47. Spool Compaction and Physical Space Reclamation

Spool manages segment files, write buffers, and background compaction passes. Murmur state commits write records and deletion markers through the state writer coordinator.

Application replication-log GC deletes logical keys through the state writer coordinator; Spool background compaction later reclaims physical segment space. Deleting a log key is not the same as removing an old segment or rotating an encryption key.

Track compaction progress, segment count, disk usage, and pending memory in `DB.Status()`.

---

## 83. Example Log GC

Node A has:

```text
A/1001
A/1002
A/1003
A/1004
A/1005
```

Acknowledgements:

```text
B has A through 1005
C has A through 1004
D has A through 1003
```

Assume B, C, and D are authenticated admitted, non-retired members with unexpired retention obligations. D contributes to the floor even when only B and C are selected replication targets; SWIM marking D suspect/dead does not remove it from this calculation.

Low watermark:

```text
1003
```

After minimum retention policy is satisfied, A-origin log entries up to 1003 may be removed.

Current `/state/...` values remain untouched.

If D stays offline beyond log retention and logs through its needed sequence are removed, D is required to snapshot-resync.

---

Current schema-level counter, set and extrema behavior, causal storage, signed wire formats, bridge ownership and upgrade requirements are specified in [merge policies](merge-policies.md). LWW remains the default.
