# Conflict resolution

Mutation model, HLC/LWW ordering, tombstones, and conflict examples.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [10. Replicated Mutation Model](#10-replicated-mutation-model)
- [11. Version and Conflict Resolution](#11-version-and-conflict-resolution)
- [12. Delete Semantics](#12-delete-semantics)
- [82. Example Remote Conflict](#82-example-remote-conflict)

---

## 10. Replicated Mutation Model

A committed transaction becomes one mutation batch.

```go
type MutationBatch struct {
    ProtocolVersion uint16

    TxID        [16]byte
    OriginNode  NodeID
    Sequence    uint64
    HLC         uint64
    SchemaEpoch uint64

    Mutations []Mutation
}
```

Mutation:

```go
type Mutation struct {
    TableID  uint32
    RowID    [16]byte
    ColumnID uint32

    Type  ValueType
    Value []byte

    Flags MutationFlags
}
```

Do not repeat node ID, sequence, HLC, or schema version for every cell if all mutations were created by the same local transaction.

That metadata belongs in the batch header.

### Value types

Use a binary encoding preserving SQLite type semantics:

```text
NULL
INTEGER
REAL
TEXT
BLOB
```

Never serialize normal mutations as JSON.

JSON is convenient for debugging but wastes space and CPU.

Provide a debug formatter separately.

---

## 11. Version and Conflict Resolution

Use two separate concepts:

### Conflict version

Hybrid Logical Clock:

```text
HLC + origin NodeID
```

Used solely to decide which concurrent/competing value wins.

### Replication sequence

```text
origin NodeID + monotonically increasing sequence
```

Used solely to determine which mutation batches another node has received.

Do not use the replication sequence as the conflict clock.

### Version comparison

For every cell, compare:

1. HLC
2. If HLC is equal, NodeID lexical order as deterministic tie-breaker.

Example:

```go
type Version struct {
    HLC    uint64
    NodeID NodeID
}
```

```go
func CompareVersion(a, b Version) int
```

All nodes must implement exactly the same byte-level comparison.

### HLC behavior

Local write:

```text
hlc = clock.Now()
```

Remote receive:

```text
clock.Observe(remoteHLC)
```

The next local write must be greater than all observed remote clocks according to the HLC algorithm.

Persist enough HLC state in Pebble so a restart cannot move the logical clock backwards.

---

## 12. Delete Semantics

Use a row-level tombstone.

Pebble stores:

```text
row tombstone version
```

and independent cell versions.

Recommended v1 behavior:

> Row deletion is LWW using the same version ordering as cell updates.

A row is visible if at least one current cell version is newer than the row tombstone, or if no active tombstone exists.

This permits later updates to resurrect a deleted row.

Example:

```text
delete row: version 100
phone update: version 101
```

Result: row exists.

Reverse:

```text
phone update: version 100
delete row: version 101
```

Result: row deleted.

#### Partial Resurrection and `NOT NULL` Columns

When a row is resurrected because a single cell update has a higher HLC than the row tombstone (e.g. `phone` updated at version 101 > delete at version 100), any other columns whose last update was older than the tombstone are treated as absent/NULL.

If any non-updated column has a `NOT NULL` constraint in the SQL schema:
- Materializing the row with absent columns will violate SQL constraints.
- Therefore, a resurrection transaction must either:
  1. Supply values for all required `NOT NULL` columns, or
  2. The schema definition must declare default values for non-primary-key `NOT NULL` columns, allowing the engine to safely materialize the resurrected row.

Document this behavior clearly.

A future mode may add remove-wins semantics, but do not mix policies inside version 1.

---

## 82. Example Remote Conflict

Node A offline:

```text
phone = "111"
version = HLC 500 / Node A
```

Node B offline:

```text
phone = "222"
version = HLC 510 / Node B
```

Reconnect.

Both eventually receive both versions.

Comparison:

```text
510 > 500
```

Both store:

```text
phone = "222"
```

If HLC is exactly equal:

```text
NodeID deterministic tie-break
```

Arrival order does not matter.

---

