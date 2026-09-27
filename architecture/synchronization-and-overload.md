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

### Detailed synchronization state

Exchange, with bounded/paginated messages:

- Each origin's highest contiguous applied sequence and highest staged/observed sequence, explicitly distinguished.
- Missing inclusive sequence ranges and partial transaction identities with missing chunk ranges.
- The earliest retained log sequence and latest available sequence per origin, plus snapshot availability.

Request only missing data the selected source advertises as available. A source that lacks a range returns an explicit unavailable-history response; try another source or obtain a current-state snapshot. Advertised staged/observed heads, chunk availability, and `IHAVE` notices are hints, never durable applied watermarks or GC acknowledgements.

Limit one active retrieval for a transaction identity locally while accepting identical chunks from other sources for completion. The shared scheduler admits repair work under existing caps and ensures repeated gaps cannot starve new local dissemination. Unknown schema data is not applied or acknowledged; finish the compatible schema exchange first.

### Transaction chunks and staging

Transactions fitting a frame may use the existing complete-batch encoding. Larger transactions use versioned chunks carrying origin/sequence, TxID, schema version/hash, total encoded length, canonical transaction digest, chunk index/count, and a chunk integrity check. Use protocol-wide 64 KiB payload chunks, with an exact shorter final chunk, independent of the supplying peer; the negotiated frame limit must accommodate one chunk plus headers. This makes partial chunk indexes stable across source changes. Validate all lengths, indexes, counts, total-byte limits, and schema/identity compatibility before allocation or staging.

Stage chunks in package-owned encrypted Pebble storage outside authoritative current state, atomically persisting chunk receipt and transfer metadata. Byte-identical duplicates are harmless; conflicting digest/length/chunk definitions fail closed. Advertise only durably staged chunks for resumable transfer. When all chunks validate against the transaction digest, reconstruct the bounded transaction and atomically commit its complete mutations, log, receipt, and contiguous watermark through the normal apply coordinator. Staged data never appears in SQL queries. A crash before the apply commit resumes/retries staging; a crash after it deduplicates by the committed identity.

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

`MaxBatchBytes` limits a frame, not a complete transaction. `MaxTransactionBytes` limits the canonical encoded transaction and is checked before local SQL/Pebble commit, including coalesced multi-row statements. Transactions above one frame use [Section 31](synchronization-and-overload.md#31-sequence-and-gap-handling)'s chunk protocol; transactions above the total limit fail the local transaction without success acknowledgement. Bound decode/reassembly memory for the one apply coordinator separately from queue and staging budgets; chunking is not permission to allocate the entire database.

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

Grow remote apply groups with queue pressure up to transaction/byte caps and flush no later than `ApplyBatchDelay`; an individual transaction is never divided between apply commits. Grouped commits preserve original transaction identities and emit acknowledgements only after synchronized persistence. Measure queue bytes/age, rate-limit waits, drops/coalescing, staging eviction, repair deferrals, and apply group size/latency.

---

## 33. Wire Encoding

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

### Pebble storage compression

Default to enabled Zstd level 3 on every LSM level. After initializing all level options, apply `pebble.UniformDBCompressionSettings(block.ZstdCompression)` with `Options.ApplyCompressionSettings`. Use the v2.1.6 `sstable/block` profile, whose Zstd setting is level 3. Explicit none/snappy map to their uniform built-in profiles. Reject unsupported modes and any nonzero Zstd level other than 3; a nonzero Zstd level with none/snappy is invalid.

```go
opts.EnsureDefaults()
opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
    return pebble.UniformDBCompressionSettings(block.ZstdCompression)
})
// block is github.com/cockroachdb/pebble/v2/sstable/block.
// opts.FS must already be the encrypted VFS before pebble.Open.
```

Pebble may store blocks uncompressed when compression does not achieve its profile's minimum reduction. This is compatible with compression being enabled. Configuration changes affect newly written tables; existing tables remain readable until compaction rewrites them. WAL/manifests are encrypted by the VFS but do not gain SSTable compression. Do not add a second compression layer inside the VFS.

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

"Received" means committed to Pebble, not merely received over QUIC.

Acknowledgement must never be sent before the Pebble synchronized batch succeeds.

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

For very large files, future architecture should use content-addressed chunks:

```text
cell stores blob manifest/hash
blob chunks replicated separately
```

Do not allow a 1 GiB cell to become one in-memory mutation allocation.

The decoder and QUIC protocol must enforce maximum frame/value sizes.

[Section 31](synchronization-and-overload.md#31-sequence-and-gap-handling)'s transaction chunking can split the encoding of a large value across transport frames without changing its atomic cell semantics. This is distinct from the future content-addressed BLOB model. Enforce total encoded transaction limits before local commit and bound reassembly; chunking does not remove the value limit.

---

