# Synchronization and overload

Gap/range repair, transaction chunks, batching, budgets, wire encoding, compression, acknowledgements, and large values.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [31. Sequence and Gap Handling](#31-sequence-and-gap-handling)
- [32. Mutation Batching](#32-mutation-batching)
- [33. Wire Encoding](#33-wire-encoding)
- [34. Compression](#34-compression)
- [35. Replication Acknowledgements](#35-replication-acknowledgements)
- [48. Large Values / BLOBs](#48-large-values--blobs)
- [Local and replication write scheduling](#local-and-replication-write-scheduling)

---

## 31. Sequence and Gap Handling

Maintain the highest contiguous sequence of fully validated, atomically applied transactions for each origin. QUIC repairs packet loss and preserves byte order within one stream; independent streams, peer suppliers, retries, and reconnects can still deliver origin transactions out of order. A sequence gap is an application-level missing transaction, not proof of a dropped QUIC packet.

If expected:

```text
100
```

but peer sends:

```text
102
```

do not advance the contiguous watermark.

Required behavior:

```text
request missing 100-101
stage complete 102 within the configured pending budget
apply/acknowledge only after earlier missing transactions are resolved
```

Serialize staging/deduplication by mutation identity across supplying peers. Bound pending bytes, transactions, and missing-range entries per peer and globally; defer/reject excess input with a retryable overload response while leaving the contiguous watermark unchanged. Coalesce overlapping missing ranges and retry with jitter, using another eligible peer when a source cannot serve them. Ignore already applied duplicates; reject conflicting transaction digests. Restart reloads durable staging and missing ranges without acknowledging incomplete data.

Implemented queue budgets: replication accounts queued control frames and inbound `Need` entries with the reusable `overload.Counter`. Defaults are 64 MiB and 8192 entries globally, 4 MiB and 128 entries per peer, and at most 4096 peer records. Queued bytes include frame payload plus header; leases release after drain or shutdown. A separate eight-entry per-peer response queue preserves `ErrOverloadedCode` responses when normal control admission is exhausted. A valid origin/sequence hint backs the sender off for one second before retrying. Bulk batches, transaction chunks, and snapshot chunks share global/per-peer token buckets (64/16 MiB/s, 16/8 MiB bursts). Durable staging retains its 256 MiB/4096-transfer disk cap; missing-range repair fans out to at most two alternate peers, and chunk availability caches at most 4096 transfers per peer.

### Detailed synchronization state

Exchange, with bounded/paginated messages:

- Each origin's highest contiguous applied sequence and highest staged/observed sequence, explicitly distinguished.
- Missing inclusive sequence ranges and partial transaction identities with missing chunk ranges.
- The earliest retained log sequence and latest available sequence per origin, plus snapshot availability.

Implemented today: optional paginated applied/observed and retained-history advertisements; observed reflects the highest durable staged sequence. Peers also exchange paginated staged transaction IDs and chunk bitmaps. Missing tails can be fetched from another connected peer that advertises the required retained sequence, with snapshot fallback when no retained source is available. Oversized transactions use canonical `TXCH` frames. Each validated fragment is committed to Spool storage (encrypted at rest with AES-256-GCM in production), under a 256 MiB and 4096-transfer global cap. Partial transactions survive restart, request missing chunk indexes from the original and alternate peers, and advance applied progress only after digest-verified reassembly and the normal atomic whole-batch commit. A source that cannot serve requested chunks returns `ErrRangeUnavailable`; the receiver tries other peers and falls back to a snapshot if all sources are unavailable. A crash before staging cleanup is safe because the committed receipt deduplicates replay.

Request only missing data the selected source advertises as available. A source that lacks a range returns an explicit unavailable-history response; try another source or obtain a current-state snapshot. Advertised staged/observed heads, chunk availability, and `IHAVE` notices are hints, never durable applied watermarks or GC acknowledgements.

Limit one active retrieval for a transaction identity locally while accepting identical chunks from other sources for completion. The shared scheduler admits repair work under existing caps and ensures repeated gaps cannot starve new local dissemination. Unknown schema data is not applied or acknowledged; finish the compatible schema exchange first.

### Transaction chunks and staging

Transactions fitting a frame may use the existing complete-batch encoding. Larger transactions use versioned chunks carrying origin/sequence, TxID, schema version/hash, total encoded length, canonical transaction digest, and chunk index/count. Use protocol-wide 64 KiB payload chunks, with an exact shorter final chunk, independent of the supplying peer; the negotiated frame limit must accommodate one chunk plus headers. This makes partial chunk indexes stable across source changes. Validate all lengths, indexes, counts, total-byte limits, and schema/identity compatibility before allocation or staging.

The `codec` package implements the version-2 `TXCH` frame and canonical transaction encoding/assembly in `EncodeTransactionChunks`, `EncodeTransactionChunk`, `DecodeTransactionChunk`, and `AssembleTransactionChunks`. It fixes payload chunks at 64 KiB, bounds the transaction at the configured codec limit (64 MiB by default), rejects inconsistent metadata and duplicate/missing chunks, and verifies the canonical digest and mutation identity before returning an assembled batch. `state.Store.StageTransactionChunk` persists fragments and the received count atomically; `replication.Manager` resumes incomplete transfers from the source or up to two alternate peers and applies only the fully assembled batch. Progress pages advertise the observed head, and separate cursor pages advertise the durable chunk bitmap.

Stage chunks in package-owned encrypted Spool storage outside authoritative current state, atomically persisting chunk receipt and transfer metadata. Byte-identical duplicates are harmless; conflicting digest/length/chunk definitions fail closed. Advertise only durably staged chunks for resumable transfer. When all chunks validate against the transaction digest, reconstruct the bounded transaction and atomically commit its complete mutations, log, receipt, and contiguous watermark through the normal apply coordinator. Staged data never appears in SQL queries. A crash before the apply commit resumes/retries staging; a crash after it deduplicates by the committed identity.

Bound staging disk bytes and transfer count globally and per peer. Expired, canceled, or over-budget unapplied transfers may be evicted and requested again; eviction must not advance applied watermarks or delete committed data. Release staging only after the apply commit succeeds or an explicit resumable-transfer eviction. Pin source log entries during active chunk reads within bounded transfer leases; after expiry, use retained-history/snapshot repair rather than claiming the chunks still exist.

Keep each sender's origin sequence ordered within its peer session for efficiency; correctness must not depend on a single supplier or stream.

---

## 32. Mutation Batching

Batch for throughput.

Target configurable constraints:

```text
max mutations
max encoded bytes
max batching delay
```

Initial candidates:

```text
MaxBatchBytes      = 1-4 MiB
MaxBatchMutations  = 10,000
MaxBatchDelay      = 5-20 ms
```

These are benchmark values, not protocol constants.

Do not batch unrelated transactions into one replication identity. A network frame may contain several `MutationBatch` objects, but each original transaction keeps its TxID and sequence.

`MaxBatchBytes` limits a frame, not a complete transaction. `MaxTransactionBytes` limits the canonical encoded transaction and is checked before local Spool commit, including coalesced multi-row operations. Transactions above one frame use [Section 31](synchronization-and-overload.md#31-sequence-and-gap-handling)'s chunk protocol; outbound chunk encoding and retained-log repair visit one reusable frame at a time instead of retaining a second transaction-sized frame slice. The canonical batch remains bounded in memory for digesting. Transactions above the total limit fail the local transaction without success acknowledgement. Bound decode/reassembly memory for the one apply coordinator separately from queue and staging budgets; chunking is not permission to allocate the entire database.

### Overload budgets and defaults

Add an embedded `ReplicationOverloadConfig` with the following resolved defaults. Use `int64` for byte/rate/burst fields, `int` for entry/transfer/range/transaction-count fields, and `time.Duration` for TTL/delay fields. Zero values select defaults; negative or inconsistent budgets are rejected before startup.

| Setting | Default | Purpose |
| --- | --- | --- |
| `QueueBytes` / `PeerQueueBytes` | 64 MiB / 8 MiB | Aggregate and per-peer queued network payload bytes |
| `QueueEntries` / `PeerQueueEntries` | 4,096 / 256 | Also bound tiny-message/notification overhead |
| `SendBytesPerSecond` / `PeerSendBytesPerSecond` | 10 MiB/s / 2 MiB/s | Global/per-peer application payload rate, before compression |
| `SendBurstBytes` / `PeerSendBurstBytes` | 10 MiB / 2 MiB | Token-bucket bursts; each must accommodate one frame |
| `StagingBytes` / `PeerStagingBytes` | 512 MiB / 128 MiB | Encrypted partial/gap transfer storage on disk |
| `StagingTransfers` / `PeerStagingTransfers` | 64 / 8 | Concurrent staged transactions |
| `MissingRanges` / `PeerMissingRanges` | 4,096 / 256 | Missing transaction/chunk range descriptors |
| `CacheBytes` / `CacheEntries` | 32 MiB / 4,096 | Plumtree payload and seen-cache aggregate bounds |
| `CacheTTL` / `TransferTTL` | 60 s / 5 min | Transient cache and idle unapplied-transfer expiry |
| `MaxConcurrentRepairs` | 1 | Locally initiated range/snapshot repair jobs |
| `ApplyMinTransactions` / `ApplyMaxTransactions` | 1 / 64 | Adaptive remote apply grouping |
| `ApplyBatchBytes` / `ApplyBatchDelay` | 64 MiB / 10 ms | Encoded apply-group budget and maximum batching wait |

Validate per-peer budgets against aggregate budgets, repair concurrency against reserved session slots, and apply/staging byte budgets against `MaxTransactionBytes`. Count referenced payload buffers once globally and count each queued target entry; a shared buffer must remain charged until the last queued reference is released. Pinning/active retrieval shares cache/staging budgets rather than creating an unbounded exception.

Treat snapshot candidate storage separately from partial-transaction `StagingBytes`: add `MaxSnapshotStagingBytes` (default 1 GiB) and preflight expected candidate bytes against that limit and available disk space, including preserved local state. Larger datasets require explicit capacity configuration. Admit one installing snapshot per DB; the bound includes candidate generation metadata/WAL and must not be bypassed by spilling to unaccounted files. The old active generation remains separately accounted until safe retirement.

Keep membership/control queues separately bounded with reserved capacity. Prioritize membership, acknowledgements, and schema control; schedule new mutations, repairs, and snapshots with weighted fair service so bulk work makes progress under sustained load. Control bytes are charged to the shared rate budget with reserved tokens; do not let unlimited high-priority requests bypass accounting. QUIC wire overhead/retransmissions are monitored separately; application token buckets do not replace QUIC congestion control.

Under pressure, coalesce commit wakeups, missing-range requests, and redundant `IHAVE` notices; discard redundant forwarding/`PRUNE` before locally originated notifications. Persisted mutation logs, committed current state, and durable receipts are never shed. Overloaded inbound work receives a retryable bounded response or flow-control backpressure. Lost transient work is recovered by retained-log scans and anti-entropy. Do not block local durable commits on a full gossip queue; a coalesced notification is sufficient because the log is authoritative.

Implemented overload controls: the `overload` package provides `Counter` byte/entry leases and hierarchical token buckets. Replication accounts queued control frames and `Need` entries, rate-limits batches/chunks/snapshot chunks, caps durable transaction staging at 256 MiB/4096 transfers, and bounds chunk availability caches and alternate repair sources. Queue and transfer rejection returns `ErrOverloadedCode`; sequence-tagged send and request hints retry after one second. Send cursors rewind only for rejected data, while rejected requests are rescheduled. A bounded reserved response queue preserves overload replies when normal control admission is full. Detailed defaults and remaining work below are the target configuration, not all current runtime settings.

Implemented send fairness and wake coalescing: local commit wakeups use a one-slot nonblocking channel. The send loop rotates its starting peer each round, caps ordinary log service at 128 batches per peer per round, drains at most 16 queued control frames per peer per round, and serves chunk repairs in groups of at most 16 chunks before yielding back to control traffic. Explicit range requests are coalesced and served in bounded batches. This bounds work between peers and message classes; an individual frame write still follows the transport's configured deadline.

Implemented remote apply groups start at one transaction, double after successful groups up to 64, and halve after a failed group. Groups contain only contiguous sequences from one origin and are capped at 64 MiB of encoded batches. `Store.CommitRemoteGroup` writes each transaction log row and receipt, each contiguous watermark, merged winners, HLC, and per-transaction generation increments in one synced Spool commit. A gap aborts the whole group. `DB.ApplyRemoteGroup` publishes final winning rows through the managed RIME adapter after the durable commit and before returning the acknowledgement. `MaterializedGeneration` tracks RIME query visibility; no deferred SQLite queue or worker remains. The manager uses the grouped applier when available and falls back to single-batch applies for other appliers.

---

## 33. Wire Encoding

Transaction chunk v2 repeats the signed immutable identity and verifies it before staging. Complete assembly validates both the encoded-batch digest and signed mutation digest. See [origin signatures](origin-signatures.md) for the exact format and trust boundaries.

Use a compact versioned binary encoding.

Options worth benchmarking:

- Custom fixed/varint binary codec.
- Protocol Buffers.
- FlatBuffers/Cap'n Proto only if benchmarks justify complexity.

Start with protobuf or a very small custom codec.

Requirements:

- Explicit protocol version.
- Length-delimited messages.
- Maximum message sizes.
- No unbounded allocations from network-provided lengths.
- Versioned transaction chunks, missing-range requests, retained-history responses, and optional Plumtree control messages.
- Bounds on encoded and decompressed bytes, counts, nested range lists, and aggregate staging/queue usage.
- CRC/checksum optional because QUIC is already protected, but useful for snapshot chunk verification.
- Fuzz decoder extensively.

Do not use JSON on the replication hot path.

---

## 34. Compression

### Spool storage compression

Spool supports configurable compression modes:
- `CompressNone`: records and segments stored uncompressed.
- `CompressSnappy`: Snappy compression for fast write/read throughput.
- `CompressDeflate`: Zstandard compression (default level 3; configurable levels 3, 9, 12).

Spool applies compression to segment payload records before encryption. Configuration changes affect newly created segments; existing segments remain readable.

### Replication compression

Do not compress each tiny mutation individually.

Compress larger replication frames/snapshot chunks only when beneficial.

Zstandard is a reasonable candidate.

Use:

```text
none
zstd
```

as negotiated capabilities.

Benchmark because encrypted/high-entropy BLOBs may expand or waste CPU.

---

## 35. Replication Acknowledgements

A peer periodically reports:

```text
origin -> highest contiguous sequence received durably
```

"Received" means committed to Spool, not merely received over QUIC.

Acknowledgement must never be sent before the Spool commit succeeds.

Distinguish transfer/staging confirmations from applied transaction acknowledgements on the wire. Only the latter advances contiguous progress or renews a GC obligation. `IHAVE` and observed heads never substitute for a durable acknowledgement.

Store peer acknowledgements persistently so log GC survives restart.

---

## 48. Large Values / BLOBs

Per-column replication means a BLOB column update sends the complete BLOB unless a separate chunking model is implemented.

Set a v1 limit:

```go
MaxReplicatedValueBytes
```

Default to 16 MiB and allow an explicit higher limit only when the total transaction/apply/staging budgets accommodate it.
Typed record writes that exceed the configured per-value limit fail with
`ErrValueTooLarge` before the Spool commit and leave the materialized row
unchanged.

For very large files, use the planned [separate encrypted object store](file-replication.md)
rather than allocating whole files as SQL values. A content-addressed chunk model
may represent it as:

```text
cell stores blob manifest/hash
blob chunks replicated separately
```

Do not allow a 1 GiB cell to become one in-memory mutation allocation.

The decoder and QUIC protocol must enforce maximum frame/value sizes.

[Section 31](synchronization-and-overload.md#31-sequence-and-gap-handling)'s transaction chunking can split the encoding of a large value across transport frames without changing its atomic cell semantics. This is distinct from the future content-addressed BLOB model. Enforce total encoded transaction limits before local commit and bound reassembly; chunking does not remove the value limit.

---

---

## Local and replication write scheduling

Planned addition inspired by GALVANIZE's writer scheduler. Current write locks
serialize operations but provide no configured writer-time shares. This scheduling
policy complements network budgets and adaptive apply grouping; High/Low security
domains are a separate feature.

Add a package-owned writer scheduling configuration with local and replication
percentages defaulting to 90 and 10. Resolve an all-zero configuration to defaults;
otherwise require positive percentages totaling 100. Classify local user writes,
mesh/High-Low remote apply, and maintenance separately.

Use one authoritative writer admission coordinator across local commit, remote
apply, snapshot publication, and state maintenance. Admission precedes conflicting
SQL/state locks so lock acquisition cannot bypass scheduling. Measure active writer
time, not transaction count or network bytes; exclude queue wait and idle time.
Track bounded service debt to restore the target share when both queues are busy.
When one class is idle, the other borrows its unused capacity without accumulating
unlimited future debt. Serve maintenance when interactive queues are empty and
bound/defer its incremental work; required recovery/publication barriers remain
explicit lifecycle operations.

Active transactions are not preempted. Bound remote apply groups and maintenance
units to limit local latency, and respect transaction cancellation/deadlines while
waiting for admission. A long user transaction can still exceed a latency target;
90/10 is a service-time target under contention, not a hard per-request guarantee.
Never reorder phases within a transaction or acknowledge before durability.

Expose queue wait/age, busy time and share by class, service debt, cancellations,
and local/remote commit latency. Read admission stays independent by default;
measure any optional read throttling before enabling it.

Acceptance: sustained replication plus local writes approaches configured writer
time shares over a declared measurement window; either class uses idle capacity;
replication still converges under local load; maintenance, canceled waiters, and
shutdown do not deadlock or leak permits. Benchmark local latency against the
unscheduled baseline and test that all state mutation paths use the coordinator.

Implementation status: `scheduler.go` implements one exclusive fair
coordinator with virtual-time fair queueing over measured grant-to-release
service time (`WriterSchedulingConfig`: 90/10 default shares, 1s debt
bound; all-zero resolves to defaults, otherwise positive shares totaling
100). Local transactions hold a local ticket end to end, remote
apply/snapshot/schema adoption hold remote tickets, and migrations, file
rewrites, and per-unit log/receipt GC hold maintenance tickets (granted
with no interactive waiters, plus a 1-in-100 reserve for a waiting
maintenance ticket so sustained interactive load cannot starve background
work forever); admission precedes all conflicting locks
and honors context cancellation and shutdown. Open-time rebuild and Close
drains predate/follow service and bypass the coordinator. Diagnostics ride
`MetricsSnapshot.Scheduler` and the `metrics` collectors (per-class
acquisitions, cancels, wait/service totals, waiters, oldest wait, debt).
Per-class dual-contention service (`spedsql_sched_dual_service_seconds_total`,
granted while the other interactive class had waiters) isolates the share
policy from idle borrowing: it is the only service-time ratio the 90/10
target binds, and the `write-priority` live suite gates on it.
Uncontended admission costs ~0.6µs (`BenchmarkSchedulerAdmitRelease`).
