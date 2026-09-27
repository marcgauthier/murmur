# Storage and materialization

Pebble layout, startup/rebuild, caching, compaction, and a log-GC example.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [13. Pebble as the Durable Source of Truth](#13-pebble-as-the-durable-source-of-truth)
- [14. Pebble Key Layout](#14-pebble-key-layout)
- [20. Materialization Generation](#20-materialization-generation)
- [21. Startup](#21-startup)
- [22. Fast LumoSQL Rebuild](#22-fast-lumosql-rebuild)
- [25. Pebble Caching](#25-pebble-caching)
- [47. Pebble Compaction and Encrypted-File Reclamation](#47-pebble-compaction-and-encrypted-file-reclamation)
- [83. Example Log GC](#83-example-log-gc)

---

## 13. Pebble as the Durable Source of Truth

Pebble stores four distinct classes of information:

1. Current winning state.
2. Replication log.
3. Replication/node metadata.
4. Schema/system metadata.

The current state must be sufficient to rebuild LumoSQL without replaying historical mutations.

---

## 14. Pebble Key Layout

Use binary prefixes.

Suggested layout:

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
```

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

Value:

```text
highest contiguous sequence stored locally
```

### Peer acknowledgement

```text
05 | peerNode:16 | originNode:16
```

Value:

```text
highest contiguous sequence peer confirmed
```

### Schema

```text
06 | "current_manifest"
```

Value: binary-encoded `SchemaManifest` containing `Version:u64`, `CreatedOnNode:16`, `TimeCreated:u64`, `Hash:32`, and all serialized `TableSchema` definitions.

### System metadata

Examples:

```text
07 | local_node_id
07 | local_sequence
07 | hlc
07 | format_version
07 | schema_epoch
07 | last_materialized_generation
07 | gc_floor
```

### Transaction receipts

```text
08 | TxID
```

Used for idempotent retry.

### Pebble Comparer & Prefix Bloom Filters

Pebble uses Block and Table Bloom filters to skip SSTables during lookups. To optimize range scans and point queries on cells:
- Define a custom `pebble.Comparer` whose `Split(key []byte) int` returns 21 for cell keys:
  ```go
  // Prefix: 0x01 (1B) + TableID (4B) + RowID (16B) = 21 bytes
  func (c *PebbleComparer) Split(k []byte) int {
      if len(k) >= 21 && k[0] == prefixCell {
          return 21
      }
      return len(k)
  }
  ```
- This configures Pebble's 10-bit Bloom filter at **row granularity**, allowing Pebble iterators and `Get` operations to skip entire SSTables when scanning or rebuilding a row.

---

## 20. Materialization Generation

Maintain a monotonic Pebble generation:

```text
state_generation = uint64
```

Every committed Pebble mutation batch increments it.

LumoSQL maintains:

```text
materialized_generation
```

Runtime invariant:

```text
materialized_generation == state_generation
```

If false:

```text
materializer = stale/dirty
```

Queries may either:

- block until repair, or
- be allowed only if explicitly configured for stale reads.

Default should be to block or return a materialization error rather than silently serving known-stale data.

---

## 21. Startup

Startup sequence:

```text
1. Load configuration; require schema, Pebble, and encryption settings and validate them, including replication budgets.
2. Reject legacy Badger directories; resolve wrapping key and recover registry/VFS, snapshot-generation, schema-publication, and restore intents.
3. Open Pebble with encrypted VFS, WAL enabled, and uniform compression settings.
4. Validate database format version.
5. Load NodeID, sequence, HLC and schema metadata; enforce fresh-identity/reseed policy before writable networking.
6. Validate application schema and compatibility with persisted metadata under Section 6.
7. Start LumoSQL.
8. Create SQL schema.
9. Bulk rebuild current state from Pebble.
10. Build non-unique secondary indexes only.
11. Build FTS structures.
12. Verify materialized generation.
13. Start shared QUIC listener and restore persisted peer retirement/GC obligations.
14. Start SWIM membership, asynchronous bootstrap retry, and bounded peer replication/anti-entropy.
15. Start GC/maintenance workers.
16. Mark DB ready.
```

Do not replay the full replication history to rebuild the SQL database.

Only scan current state plus row tombstones.

---

## 22. Fast LumoSQL Rebuild

Rebuild should operate table-by-table.

For each replicated table:

```text
Pebble prefix scan
      |
      v
assemble rows
      |
      v
prepared INSERT
      |
      v
large LumoSQL transaction
```

Avoid one SQL transaction per cell.

### Preferred rebuild algorithm

1. Create tables.
2. Delay nonessential secondary-index creation.
3. Scan Pebble current-state prefix in key order.
4. Group consecutive cells by row UUID.
5. Build one row.
6. Insert row with a prepared statement.
7. Commit in large controlled batches if one giant transaction is not practical.
8. Create non-unique secondary indexes only.
9. Build FTS index.
10. Set materialized generation.

Benchmark:

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
Pebble read throughput
LumoSQL insertion throughput
index-build time
FTS-build time
```

---

## 25. Pebble Caching

Expose `PebbleConfig.CacheBytes` for Pebble's unified block cache; do not retain separate Badger block/index cache settings. Pebble caches decoded blocks above the encrypted VFS, so cache memory contains plaintext.

```go
type CacheConfig struct {
    StatementCacheEntries int
}
```

Default to a 16 MiB Pebble block cache because the SQL dataset also lives in LumoSQL memory. Benchmark cache sizing and include memtables, encrypted-VFS indexes and buffers, replication queues, and SQL indexes in the total memory budget.

Expose cache usage/hit rate, memtable bytes, encryption buffer/index bytes, replication queues, and statement-cache usage. Close/unref owned Pebble cache resources during shutdown.

---

## 47. Pebble Compaction and Encrypted-File Reclamation

Pebble owns flush, compaction, and obsolete-file reclamation. Do not schedule Badger value-log GC or retain value-log sizing/settings. WAL stays enabled and state writes use synchronized batches.

Application replication-log GC deletes logical keys through the state writer coordinator; Pebble compaction later reclaims physical space. Deleting a log key is not the same as removing an SSTable or retiring an encryption key.

Track compaction debt, stalls, live/obsolete bytes, WAL bytes, encrypted-container overhead, and old-key file references. File-removal notifications must reconcile key references with open handles and checkpoints. Use explicit maintenance rewriting for files that normal compaction does not replace, including durable metadata containers.

No remote/object-store bypass of the encrypted VFS is supported in v1.

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

