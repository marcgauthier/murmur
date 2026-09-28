# Transactions and change capture

Change capture, transaction coalescing, commit/apply ordering, and idempotency.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [8. Change Capture: Use SQLite Pre-Update Hook First](#8-change-capture-use-sqlite-pre-update-hook-first)
- [9. Transaction Delta Coalescing](#9-transaction-delta-coalescing)
- [15. Pebble Atomic Batch for a Local Mutation](#15-pebble-atomic-batch-for-a-local-mutation)
- [16. Local SQL Commit Ordering](#16-local-sql-commit-ordering)
- [17. Alternative Write Optimization](#17-alternative-write-optimization)
- [18. Remote Mutation Apply Path](#18-remote-mutation-apply-path)
- [19. Prevent Replication Echo](#19-prevent-replication-echo)
- [49. Idempotency](#49-idempotency)
- [81. Example End-to-End Local Write](#81-example-end-to-end-local-write)

---

## 8. Change Capture: Use SQLite Pre-Update Hook First

Preferred mechanism:

```text
sqlite3_preupdate_hook()
```

SQLite exposes a pre-update hook when compiled with:

```text
SQLITE_ENABLE_PREUPDATE_HOOK
```

The callback receives INSERT, UPDATE, and DELETE operations and can retrieve old/new column values.

This is a better primary design than generating a trigger for every replicated column.

### Advantages

- No persistent trigger objects required.
- No per-table replication log table in LumoSQL.
- Captures writes regardless of SQL statement shape.
- Multi-row updates naturally produce per-row events.
- Old and new values are available.
- Nested changes caused by triggers can be observed.
- Less schema-generation logic.
- Easier to disable capture while applying remote mutations.

### Required build check

The LumoSQL amalgamation must be built with:

```text
SQLITE_ENABLE_PREUPDATE_HOOK
```

Create an integration test that fails the build/test suite if the hook is unavailable.

### Important limitation

The pre-update hook does not fire for virtual tables.

That is acceptable for this architecture because FTS/search virtual tables are local derived structures and should not replicate directly.

### Trigger fallback

Implement a generated-trigger capture backend only if the pre-update hook cannot be used reliably with the chosen LumoSQL build.

Define:

```go
type ChangeCapture interface {
    Begin(txID TxID)
    Events() []RawChange
    Reset()
}
```

Implementations:

```text
PreUpdateCapture
TriggerCapture
```

Do not implement CR-SQLite as the fallback.

---

## 9. Transaction Delta Coalescing

A SQL transaction can modify the same cell multiple times.

Example:

```sql
BEGIN;
UPDATE contacts SET phone='111' WHERE id=?;
UPDATE contacts SET phone='222' WHERE id=?;
COMMIT;
```

Replication should normally emit only:

```text
phone = "222"
```

not both intermediate values.

Maintain a transaction-local delta:

```go
type CellKey struct {
    TableID  uint32
    RowID    UUID
    ColumnID uint32
}

type TxCellDelta struct {
    OriginalPresent bool
    OriginalValue   Value

    FinalPresent    bool
    FinalValue      Value
}
```

Coalesce events by:

```text
(table ID, row UUID, column ID)
```

At commit:

- INSERT then UPDATE -> one final INSERT state.
- UPDATE then UPDATE -> one final value.
- INSERT then DELETE within the same transaction -> no externally visible row mutation.
- UPDATE then DELETE -> row tombstone.
- DELETE then INSERT with same UUID -> define as row resurrection and emit final row state.
- Setting a value to its existing value -> omit mutation.

This reduces replication volume substantially.

---

## 15. Pebble Atomic Batch for a Local Mutation

Once a transaction has been accepted for durable replication, a single Pebble batch must atomically:

- Merge/update all winning cell states.
- Update tombstone states.
- Add the mutation batch to the origin log.
- Advance the local origin sequence.
- Persist HLC state.
- Write transaction receipt.
- Update a materialization generation counter.

Commit the batch with `pebble.Sync` and keep WAL enabled. Do not write these as independent batches.

Ordinary remote apply writes the complete encoded transaction to a synced prepare record first. It then atomically commits winning cells, log, receipt, receive watermark, HLC, state generation, and removal of that record in one Pebble batch. Store open replays any surviving prepare record through the same idempotent apply path before exposing state. Thus a crash before the final batch leaves the transaction unapplied and recoverable; a crash after it leaves the complete transaction committed and no prepare record. Materialization generation is updated after applying winners to SQL; on restart, SQL is rebuilt from committed state before its generation is advanced.

This prepare record is the recovery boundary needed before any ordinary remote winner state is staged with `DB.Ingest`. The current ordinary apply implementation still uses the atomic batch to install winners; it does not yet use external SSTable ingestion. Any future ingestion optimization must keep the prepare record until the final metadata batch commits, and recovery must tolerate replay after ingestion by applying the same CRDT versions idempotently. Large snapshot imports already use SSTable ingestion because their candidate is staged separately, progress is replayable, SQL remains gated, and snapshot watermarks/generation publish only in the final batch. Local commits continue to use one synced batch.

Pebble batches do not provide Badger-style optimistic transaction conflict detection. Serialize the entire read/merge/write operation for all local, remote, metadata, acknowledgement, and GC mutations through one state-store writer coordinator. Use an indexed batch where a merge needs to read its own pending writes. Independent readers use Pebble snapshots and bounded iterators; close snapshots, iterators, and value closers on every path, and copy borrowed bytes before retaining them.

This is one of the most important correctness requirements.

---

## 16. Local SQL Commit Ordering

There is no distributed ACID transaction shared by LumoSQL and Pebble.

Because LumoSQL is disposable, exploit that fact instead of attempting a complex two-phase commit between two embedded engines.

Recommended local write path:

```text
1. Acquire local write transaction context.
2. Begin LumoSQL transaction.
3. Execute SQL.
4. Pre-update hook records all row/cell changes.
5. Coalesce transaction delta.
6. Validate the coalesced transaction's encoded size against MaxTransactionBytes; roll back SQL on overflow. Otherwise COMMIT LumoSQL (fast in-memory/mmap commit; PRAGMA synchronous = OFF / MDB_NOSYNC).
7. Build final MutationBatch.
8. Commit MutationBatch + current state atomically to Pebble (batch.Commit(pebble.Sync) performs the single authoritative fsync).
9. Record SQL materialization generation = Pebble generation.
10. ACK success to application.
```

Why commit SQL first?

Because COMMIT can still fail due to SQL constraints or deferred checks. The package should not durably replicate a transaction that LumoSQL itself rejected. Furthermore, because LumoSQL runs with `PRAGMA synchronous = OFF` (`MDB_NOSYNC`), Step 6 takes microseconds; physical disk synchronization happens strictly once per transaction in Step 8 on Pebble.

### Failure after LumoSQL commit but before Pebble commit

The application must not receive success.

The in-memory query database is now ahead of durable state.

Immediately:

```text
mark materializer DIRTY
block subsequent writes
rebuild affected rows or rebuild complete LumoSQL state from Pebble
resume
return error to caller
```

Because LumoSQL is non-authoritative, this failure is recoverable.

### Failure after Pebble commit but before client receives success

The client may retry.

Therefore every mutation batch must have a TxID and duplicate TxIDs must be idempotent.

This is a normal ambiguous-commit scenario and must have explicit tests.

---

## 17. Alternative Write Optimization

Write optimizations must not compromise CRDT invariants or crash consistency.

```text
SQL transaction
      |
      v
committed local delta
      |
      v
Pebble commit (pebble.Sync or pebble.NoSync)
```

Acknowledging before Pebble durability changes the durability contract. SPeD-SQL provides an explicit configuration:

```go
DurabilitySynchronous // Default: fsync before acknowledging commit (pebble.Sync)
DurabilityAsync       // Asynchronous: acknowledged at memory/WAL speed without waiting for fsync (pebble.NoSync)
```

Default is synchronous (`DurabilitySynchronous`).

When `DurabilityAsync` is explicitly configured:
- Transactions commit at RAM speed via `pebble.NoSync` without blocking on disk sync.
- Acknowledgements explicitly carry a weaker durability contract: in an ungraceful crash or power-loss scenario, transactions acknowledged since the last sync may not have reached disk.
- Applications can invoke `db.Sync(ctx)` at any time to establish an explicit durable sync point to disk.
- Sync uses a WAL-only Pebble record with `pebble.Sync`; an empty Pebble batch would be skipped and would not establish a durability barrier.
- Set `Durability.SyncInterval = time.Second` to schedule a Pebble sync about once per second. The interval is opt-in, valid only with `DurabilityAsync`, and zero leaves synchronization manual. A slow or failed sync can extend the loss window; a failed scheduled sync makes the database fail closed. Graceful close performs a final sync and reports an error if it fails.
- `pebble.NoSync` still appends each commit to the WAL. The interval reduces fsync frequency; it does not guarantee exactly one physical disk write per second.
- Replica convergence and CRDT invariants remain intact: query engine rebuilds and version ordering remain authoritative from the persisted Pebble state.

---

## 18. Remote Mutation Apply Path

Remote apply order is different.

Pebble is authoritative, so:

```text
1. Receive a complete MutationBatch, or durably stage and validate all its chunks before entering this apply path.
2. Validate protocol/schema/origin/sequence.
3. Check duplicate TxID/sequence.
4. Observe remote HLC.
5. Merge mutations against Pebble current state.
6. Atomically store winners + log + receive watermark.
7. Commit Pebble.
8. Add winning row identities to the in-memory materialization map.
9. Send the durable-receive acknowledgement.
10. At the next one-second tick or when 1,000 received transactions are queued, read each affected row's final state from Pebble and apply all rows in one SQLite transaction.
```

The interval and transaction threshold are configurable through `QueryStore.RemoteApplyInterval` and `QueryStore.RemoteApplyMaxTransactions`. The map coalesces repeated writes to a row. A local SQL write flushes pending remote rows before it starts, and again at commit if remote data arrived during the transaction. Queries may see the previous SQLite state until the flush. `StateGeneration` advances with Pebble commits; the in-memory `MaterializedGeneration` advances only after SQLite catches up. Startup always rebuilds SQLite from Pebble and reinitializes that marker, so an interrupted process cannot lose queued changes or require a separate Pebble marker write.

If the bulk SQLite apply fails:

```text
Pebble remains correct.
mark materializer DIRTY.
rebuild LumoSQL.
```

Do not roll back Pebble because of a cache/materialization failure.

Applying several complete remote transactions in one synchronized Pebble commit may amortize fsync costs, but preserve each transaction's identity and all-or-nothing mutation set. Advance only contiguous origin progress for validated complete batches; chunk receipts and gap-buffer entries are not applied transaction acknowledgements.

---

## 19. Prevent Replication Echo

Applying a remote update to LumoSQL must not create a new local mutation.

With the pre-update-hook implementation, associate an apply mode with the SQL connection:

```go
type CaptureMode uint8

const (
    CaptureLocal CaptureMode = iota
    CaptureSuppressed
)
```

During:

- remote apply
- startup rebuild
- snapshot restore

set:

```text
CaptureSuppressed
```

The callback returns without recording mutations.

For trigger fallback, register a connection-local SQL function such as:

```text
repl_capture_enabled()
```

and generate triggers with a guard.

---

## 49. Idempotency

Every local transaction receives a random 128-bit TxID.

Before applying a remote batch:

```text
if receipt(TxID) exists:
    treat as duplicate
    ack existing sequence
    do not reapply
```

Receipts can potentially be garbage-collected after their origin sequence is safely below the local retention floor.

Sequence identity remains the primary ordered replication identity; TxID protects ambiguous retries and protocol duplication.

---

## 81. Example End-to-End Local Write

Application executes:

```sql
UPDATE contacts
SET phone = '613-555-0100',
    email = 'marc@example.test'
WHERE id = ?;
```

Flow:

```text
LumoSQL UPDATE
      |
      v
pre-update hook
      |
      +-- phone old/new
      +-- email old/new
      |
      v
TxDelta coalescer
      |
      v
SQL COMMIT succeeds
      |
      v
allocate:
    TxID
    local sequence 82911
    HLC 0x...
      |
      v
MutationBatch {
    origin = A
    seq = 82911
    changes = [
        phone,
        email
    ]
}
      |
      v
Pebble synchronized batch
    merge current cells
    write log/A/82911
    write tx receipt
    advance local seq
    advance state generation
      |
      v
Pebble batch.Commit(pebble.Sync)
      |
      v
application receives success
      |
      v
replication sender wakes
```

---
