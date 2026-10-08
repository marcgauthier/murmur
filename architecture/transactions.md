# Transactions and change capture

Change capture, transaction coalescing, commit/apply ordering, and idempotency.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [8. RIME Prepared-Change Capture](#8-rime-prepared-change-capture)
- [9. Transaction Delta Coalescing](#9-transaction-delta-coalescing)
- [15. Spool Atomic Commit for a Local Mutation](#15-spool-atomic-commit-for-a-local-mutation)
- [16. Durable-First Typed Commit Ordering](#16-durable-first-typed-commit-ordering)
- [17. Durability Modes](#17-durability-modes)
- [18. Remote Mutation Apply Path](#18-remote-mutation-apply-path)
- [19. Prevent Replication Echo](#19-prevent-replication-echo)
- [49. Idempotency](#49-idempotency)
- [81. Example End-to-End Local Write](#81-example-end-to-end-local-write)

---

## 8. RIME Prepared-Change Capture

Managed RIME transactions expose an immutable prepared-change set before
publication. Each change carries stable table and row identity, operation,
old/new values, base versions and final per-field state. Preparation validates
schema, codecs, ownership, limits and merge policy before Spool durability.
Application writes never rely on SQL hooks or asynchronous change queues.

Preparation owns all data needed by durable encoding and later publication.
Mutable inputs are cloned before staging, and values in the prepared set are
not exposed for mutation. A failed preparation aborts the RIME transaction and
leaves durable state unchanged.

## 9. Transaction Delta Coalescing

Repeated changes to one record are coalesced to the final durable delta while
operation hooks retain their documented execution count. The adapter converts
prepared RIME changes into canonical Spool mutations, assigns transaction and
origin metadata, and enforces per-value, mutation-count and encoded-byte caps
before submitting durable work.

## 15. Spool Atomic Commit for a Local Mutation

Local transactions are signed after assigning their origin sequence, before the atomic Spool commit; remote transactions verify before any durable prepare, merge, receipt or HLC observation. See [origin signatures](origin-signatures.md) for the exact format and trust boundaries.

Transaction methods serialize staging against cancellation, so cancellation
cannot roll back concurrently with an executing mutation. Read results are
detached from the transaction and do not require SQL row-handle closure.

Once a transaction has been accepted for durable replication, a single Spool commit must atomically:

- Merge/update all winning cell states.
- Update tombstone states.
- Add the mutation batch to the origin log.
- Advance the local origin sequence.
- Persist HLC state.
- Write transaction receipt.
- Update a materialization generation counter.

Commit the mutations to Spool at the configured durability level. Do not write these as independent commits.

Ordinary remote apply writes the complete encoded transaction to a synced
prepare record first. It then atomically commits winning cells, log, receipt,
receive watermark, HLC, state generation, and removal of that record in one
Spool commit. Store open replays any surviving prepare record through the same
idempotent apply path before exposing state. Thus a crash before the final
commit leaves the transaction unapplied and recoverable; a crash after it leaves
the complete transaction committed and no prepare record. RIME is rebuilt from
committed state before its materialized generation is advanced. Before final
snapshot publication, typed callback writes drain and new callbacks wait until
the rebuilt RIME generation is installed, so a snapshot cannot invalidate a
staged callback's generation.

The state store maintains an in-memory Radix tree representing current authoritative state. Serialize the entire read/merge/write operation for all local, remote, metadata, acknowledgement, and GC mutations through one state-store writer coordinator. Transactions can read their own pending writes before persistence. Independent readers use pinned Radix tree snapshots; with synchronous group commit ([Section 16](#16-durable-first-typed-commit-ordering)), one synced Spool commit carries a whole group of local transactions instead of one; the serialization requirement is unchanged.

This is one of the most important correctness requirements.

---

## 16. Durable-First Typed Commit Ordering

RIME is a rebuildable materializer and Spool is authoritative. Every accepted
local transaction follows this order:

```text
1. Stage and validate typed changes in a managed RIME transaction.
2. Acquire writer admission and the ordered local commit position.
3. Encode the final mutation batch, origin signature, HLC and transaction ID.
4. Commit the batch, receipt, local sequence and state generation to Spool.
5. Publish the prepared managed RIME transaction in the same commit order.
6. Notify subscriptions and replication, then acknowledge the caller.
```

A rejection before Spool durability aborts RIME publication and returns a
definite error. A failure after Spool durability freezes the adapter and reports
an uncertain outcome with the transaction ID; restart reconstructs RIME from
Spool, and `HasTransactionReceipt` resolves whether the commit landed. Remote
transactions use the same durable-before-publication boundary after signature,
schema and merge validation.

### Synchronous group commit

Synchronous typed writes are grouped by default when the configured delay is
positive. A group batches ordered mutation batches in one `Store.CommitLocalGroup`
call and one Spool sync. Each prepared RIME transaction is then published in
the original group order. Callers receive success only after both durability
and publication complete. Node-local writes and ephemeral writes use the direct
path when their payload is ineligible for replicated group commit.

Close stops admission, lets active staging finish, flushes the pending group,
and waits for all admitted members. A shared Spool failure fails every member;
no member is acknowledged. A process exit before the durable group leaves no
member visible after reopen. A failure after durability is treated as an
uncertain outcome and the materializer is rebuilt or the node fails closed.

Focused `TestGroupCommit*` coverage exercises concurrent writers, conflicting
updates to one row, one shared ENOSPC failure, process exit before Spool commit,
and Close with queued members. Broader storage-fault and multi-peer process
qualification remains part of the migration release gates.

## 17. Durability Modes

Write optimizations must not compromise CRDT invariants or crash consistency.

Synchronous durability is the default. Managed writes return only after Spool
sync and RIME publication; asynchronous mode acknowledges before its next
explicit or scheduled sync.

Acknowledging before Spool durability changes the durability contract. Murmur-SQL provides an explicit configuration:

```go
DurabilitySynchronous // Default: fsync before acknowledging commit
DurabilityAsync       // Asynchronous: acknowledged at memory/WAL speed without waiting for fsync
```

Default is synchronous (`DurabilitySynchronous`).

When `DurabilityAsync` is explicitly configured:
- Transactions commit at RAM speed without blocking on disk sync.
- Acknowledgements explicitly carry a weaker durability contract: in an ungraceful crash or power-loss scenario, transactions acknowledged since the last sync may not have reached disk.
- Applications can invoke `db.Sync(ctx)` at any time to establish an explicit durable sync point to disk.
- Sync appends an explicit sync barrier record and performs a segment sync.
- Set `Durability.SyncInterval = time.Second` to schedule a Spool sync about once per second. The interval is opt-in, valid only with `DurabilityAsync`, and zero leaves synchronization manual. A slow or failed sync can extend the loss window; a failed scheduled sync makes the database fail closed. Graceful close performs a final sync and reports an error if it fails.
- Async commits still append each mutation to Spool's active segment. The interval reduces fsync frequency; it does not guarantee exactly one physical disk write per second.
- Replica convergence and CRDT invariants remain intact: RIME rebuilds and
  version ordering remain authoritative from persisted Spool state.

---

## 18. Remote Mutation Apply Path

Remote apply order is different.

Spool is authoritative, so:

```text
1. Receive a complete MutationBatch, or durably stage and validate all its chunks before entering this apply path.
2. Validate protocol/schema/origin/sequence.
3. Check duplicate TxID/sequence.
4. Observe remote HLC.
5. Merge mutations against current authoritative state in RAM.
6. Atomically commit winners, log, receipt, watermark and generation to Spool.
7. Publish the winning typed records through the managed RIME adapter.
8. Acknowledge the apply after RIME publication completes.
```

RIME is rebuilt from Spool on open and after a completed snapshot. A failure
after the durable Spool commit cannot undo that commit; the node fails closed
and reconstructs the materializer from Spool on restart. The former deferred
SQLite worker and its one-second/1,000-transaction thresholds have been removed.

Applying several complete remote transactions in one synchronized Spool commit may amortize fsync costs, but preserve each transaction's identity and all-or-nothing mutation set. Advance only contiguous origin progress for validated complete batches; chunk receipts and gap-buffer entries are not applied transaction acknowledgements.

---

## 19. Prevent Replication Echo

Remote apply merges accepted batches into authoritative Spool state, then
publishes winning rows through the RIME adapter. It never calls the local write
coordinator, so remote updates cannot create new local mutation batches.

Startup rebuild and snapshot publication also materialize directly from durable
state rather than routing rows through application writes.

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

Application stages a typed update:

```go
err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
    return contacts.Update(tx, id, func(row *Contact) error {
        row.Phone = "613-555-0100"
        row.Email = "marc@example.test"
        return nil
    })
})
```

The managed coordinator prepares coalesced field changes, writes the mutation
batch and receipt to Spool, then publishes the RIME transaction. Synchronous
mode returns success after durable Spool commit and required RIME publication;
replication is notified after the local commit completes.

---
