# Spool design and implementation

<img src="logo.png" alt="Spool logo: encrypted delta persistence" width="512">

Spool is an embedded Go persistence package optimized for **fast
concurrent writes** for applications that keep all their data in memory. This
document describes the existing package in sections 1–43 and the planned
Murmur integration requirements in section 44. Original section numbers are
retained.

## Implementation status

Implemented: sharded write buffers and index (§3–§7), block codec with
CRC-gated headers (§8, §26), block-level Zstd (§9), AES-256-GCM data
keys (standard library only) with `keys.enc` hierarchy, data-key and
master-key rotation (§10–§13), size-rolled append-only segments
(§14–§15), epoch-scoped seal-time sequence ordering (§16), commit-time
index/FileStats updates (§17), background file deletion and live-ratio
compaction with free-space reserve and write-pressure pause (§18–§21),
newest-first exactly-once loads with `LoadFunc` callbacks (§22–§24),
torn-tail crash recovery (§25), Async/Flush/Sync durability with a
syncing `Flush` barrier (§27), pending-bytes backpressure with `TryPut`
(§28), large-value single-block writes (§29), manifest with
clean-shutdown marker (§31), ordered block commits (§35), tombstone
deletes (§36), `Stats` (§37), and batch write APIs (§2).

Deferred: optional index snapshots (§33) — the index always rebuilds
from segments. Murmur uses Spool for durable storage.

[Section 44](#44-required-changes-for-murmur-integration) records the
completed Spool-side requirements for Murmur integration. Atomic groups
provide recovery boundaries across blocks.

Verify with `go test -count=1 ./spool/` (plus `-race`), the live
crash/soak scenario in `tests-live/`, and cross-compiles for
windows/darwin (file locking has per-OS files).

Applications keep their read data in RAM. Spool persists mutations, loads data
at startup, and compacts obsolete disk records in the background. It deliberately
has no disk `Get(key)` API. Its current-state index stores key locations,
not a permanent copy of every value.

## Plan navigation

| Area | Sections |
| --- | --- |
| Architecture and public API | [1–2](#1-high-level-architecture) |
| Indexes and buffered writes | [3–7](#3-in-memory-index) |
| Block format, compression, and encryption | [8–13](#8-block-format) |
| Segments, ordering, and compaction | [14–21](#14-segment-files) |
| Loading, recovery, and durability | [22–27](#22-loadall) |
| Resource limits, lifecycle, and metadata | [28–33](#28-backpressure) |
| Concurrency, deletes, metrics, and layout | [34–38](#34-concurrency-model) |
| Implementation phases and defaults | [39–42](#39-implementation-phases) |
| External dependencies | [43](#43-external-package) |
| Planned Murmur integration requirements | [44](#44-required-changes-for-murmur-integration) |

Section numbers are retained from the original plan. Additional requirements
are grouped into their relevant sections.

## 1. High-level architecture

```text
concurrent writers
        │
sharded write cache
        │
time / count / size flush
        │
build block → compress → encrypt → append to segment
                                      │
                         ┌────────────┴────────────┐
                         │                         │
                  memory key index          file statistics
```

Proposed disk layout:

```text
data/
    manifest
    keys.enc
    segments/
        000000000001.spool
        000000000002.spool
        ...
```

## 2. Primary API

Keep the public API small. The exported surface is `Open`,
`OpenAndLoad`, `Load`, `LoadWithOptions`, and the `Store` methods
`Put`, `TryPut`, `PutBatch`, `Delete`, `DeleteBatch`, `Flush`,
`RotateKey`, `RotateMasterKey`, `Reclaim`, `Stats`, `LastFlushErr`,
`LastReclaimErr`, and `Close`.

```go
store, err := spool.Open(options)
// Handle err before using store.

err = store.Put(key, value)
err = store.Delete(key)
err = store.Flush()
err = store.Close()
```

`PutBatch()` and `DeleteBatch()` let callers with existing batches avoid
repeated per-call locking and backpressure waits. Admission is
all-or-nothing (a size violation rejects the batch before anything is
buffered); commits follow the normal flush path and are not atomic as a
group.

The planned atomic mixed-write API and explicit `Sync()` are separate
extensions; see [section 44.1](#441-atomic-mixed-write-api).

Loading the dataset should support a block-level callback:

```go
err := spool.Load(path, masterKey, func(records []spool.Record) error {
    // Apply records to application memory.
    return nil
})
```

Alternatively, combine opening and loading:

```go
store, err := spool.OpenAndLoad(options, func(records []spool.Record) error {
    return nil
})
```

The startup operation is exported as `Load`, `LoadWithOptions`, and
`OpenAndLoad`. `Flush()` is a full barrier that always fsyncs, so no
separate `Sync()` is needed; see [section 27](#27-durability-modes).

## 3. In-memory index

Maintain a sharded `map[string]Location`. Locations identify the current disk
version and support recovery, obsolete-record accounting, compaction, and
file reclamation.

```go
type Location struct {
    FileID      uint64
    BlockID     uint64
    BlockOffset int64
    RecordIndex uint32
    Sequence    uint64
}

type indexShard struct {
    sync.RWMutex
    values map[string]Location
}
```

Use a fast key hash to select a shard; start with 256 shards. Each shard has
its own lock. Normal reads use application RAM, so random disk-read
optimization is unnecessary.

## 4. File statistics index

Track each segment's live and obsolete records in memory:

```go
type FileStats struct {
    FileID       uint64
    TotalRecords uint64
    LiveRecords  uint64
    DeadRecords  uint64
    TotalBytes   uint64
    LiveBytes    uint64
    CreatedAt    time.Time
}
```

When a newer committed record replaces an older one, decrement the old
segment's live count, increment its dead count, and count the new record as
live in its segment. Update byte accounting as well.

These counters should normally make compaction and reclamation decisions
possible without scanning disk.

## 5. Write path

The normal asynchronous `Put()` path should:

1. Assign a mutation sequence number.
2. Copy or take ownership of the key and value.
3. Append the record to a shard buffer.
4. Return once accepted.

Compression, encryption, file opening, syncing, and compaction belong in
background workers. `Put()` never waits for durability; only
backpressure (a full pending buffer) makes the caller wait, and
`TryPut()` reports that case with `ErrBackpressure` instead.

## 6. Write buffering

Seal a buffer when the first configured count, byte, or delay threshold is
reached:

```go
type FlushPolicy struct {
    MaxRecords int
    MaxBytes   int64
    MaxDelay   time.Duration
}

// Example policy; benchmark before choosing final defaults.
FlushPolicy{
    MaxRecords: 10_000,
    MaxBytes:   8 << 20,
    MaxDelay:   100 * time.Millisecond,
}
```

Batching turns many small mutations into one block compression, one block
encryption, and one sequential append. The initial default block target is
4 MiB; the example above illustrates an alternative 8 MiB threshold.

## 7. Concurrent buffering

Use multiple write shards with separate locks:

```go
type writeShard struct {
    mu      sync.Mutex
    records []pendingRecord
    bytes   int
}
```

Sealed shard buffers feed a shared block queue and the segment writer.
This avoids one global buffer lock limiting concurrent `Put()` calls.

## 8. Block format

Each flush produces a self-contained block:

| Part | Contents |
| --- | --- |
| Header | Magic, format version, feature flags, block sequence, key ID, record count, uncompressed/compressed sizes, nonce, checksum/header authentication. |
| Encrypted payload | Compressed records. |
| Authentication tag | AEAD tag. |

The plaintext contains a record count followed by records with their
sequence, flags, key length, value length, key, and value. A delete has a
tombstone flag and zero value length.

Include format versions and feature flags in both segment and block framing
from the start. Bound lengths and counts before allocating buffers; see
[section 26](#26-checksums-and-authentication).

The existing independently recoverable blocks do not define a transaction
boundary. The planned format adds authenticated commit groups spanning
multiple blocks; see [section 44.2](#442-transactional-format-and-segment-boundaries).

## 9. Compression

Compress whole blocks before encryption. `CompressionDeflate` is the default;
`CompressionNone` stores uncompressed blocks, and custom codecs can be
registered through `spool.Options.Codec`. `spool.DefaultOptions` sets the
default explicitly. A struct literal that selects the zero-valued
`CompressionNone` must also set `CompressionSet: true`, so normalization can
distinguish it from an omitted setting.

```text
records → compression → encryption → disk
```

Encrypted data cannot usefully be compressed afterward.

## 10. Encryption

Use authenticated encryption. The cipher is AES-256-GCM from the Go
standard library (`crypto/aes`, `crypto/cipher`); it is the default and
the only cipher. Any future alternative must also come from the
standard `crypto` packages.

Every block needs a unique nonce for its data key; never reuse a key/nonce
pair. Store the key ID and nonce in the header, never the encryption key.
`EncryptionNone` remains an explicit opt-out for tests and local
development.

## 11. Key hierarchy

Use data keys for blocks rather than encrypting blocks directly with the
user's master key:

```text
Master key → decrypt keys.enc → historical data keys + current data key
```

```go
type DataKey struct {
    ID        uint32
    Key       [32]byte
    CreatedAt int64
    Status    KeyStatus
}
```

Encrypt `keys.enc` (format version 2, AES-256-GCM) with a key derived
from the supplied master key. Protect keyring updates with atomic
replacement (temp file, fsync, rename, directory fsync): losing the
keyring means losing access to the store, and atomic replacement keeps
exactly one durable generation instead of redundant `keys.1`/`keys.2`
copies.

Named wrapping keys and authenticated database-context binding are planned
in [section 44.7](#447-encryption-key-management-and-maintenance).

## 12. Master key

Supply the master key at open/load time and never persist it in plaintext:

```go
spool.Open(spool.Options{
    Path:      "./data",
    MasterKey: masterKey,
})
```

Prefer an application-provided, generated 256-bit key. If passphrases are
supported, use an appropriate password KDF such as Argon2id.

Avoid converting keys to strings, zero temporary key buffers where practical,
and retain decrypted keys only as long as needed.

## 13. Key rotation

Data-key rotation should:

1. Generate the next data key.
2. Persist it safely in the keyring.
3. Mark it current.
4. Use it for all new blocks.

Keep historical keys so old blocks remain decryptable. Normal compaction
gradually re-encrypts live data under the current key without requiring an
immediate store-wide rewrite.

Master-key rotation is separate: `RotateMasterKey()` decrypts and
re-encrypts the small keyring under the new master key (or passphrase)
without rewriting data segments. The container is persisted before the
in-memory protector switches, so a failure leaves the old material
working.

## 14. Segment files

Append multiple blocks to each segment and roll to a new segment at the
configured size. Start with `MaxSegmentSize: 256 << 20` (256 MiB); benchmark
64, 128, 256, and 512 MiB alternatives.

Optionally preallocate segment space to reduce fragmentation while retaining
logical append behavior. Bound open descriptors using short-lived opens
or a small descriptor cache during loading and compaction.

## 15. Append-only disk behavior

Never modify existing blocks. Sequential appends simplify locking,
encryption, recovery, and compaction while reducing small random writes.

Publish newly constructed or compacted files atomically: write a temporary
file such as `123.tmp`, sync it, rename it to `123.spool`, then sync the
directory. Define active-segment publication so recovery can recognize its
incomplete tail; do not expose unfinished replacement segments as complete.

Directory syncing must cover creation, renaming, and deletion of segment,
keyring, and manifest files when required for durable filesystem metadata.

## 16. Sequence numbers

Assign every mutation a monotonically increasing sequence:

```go
seq := atomic.AddUint64(&sequence, 1)
```

For the same key, sequence `93820` supersedes sequence `93811`. The highest
assigned sequence wins even if goroutines, compression workers, or appends
finish out of order. Wall-clock time does not determine the winner.

Sequences combine a per-open epoch (high 16 bits, bumped and persisted on
every open) with a counter, so restarts never reuse sequence space.
Numbers are assigned at seal time rather than in `Put()`: one seal
completes fully before the next begins, keeping cross-flush sequences
strictly increasing with append order, which the tombstone age rule in
[section 36](#36-delete-semantics) relies on.

## 17. Updating the index

Once a block reaches the requested durability level, process its records:

1. Compare each sequence with the current index entry.
2. If it is newer, mark the previous location obsolete.
3. Install the new location and update file statistics.
4. Account for superseded records without making them current.

The index must only point to data that reached the requested durability
level. Completion order must not override logical sequence order.

## 18. File reclamation

A low-priority background worker periodically inspects file statistics.
A segment with `LiveRecords == 0` can be deleted once it is no longer
referenced by a writer, loader, or compactor and tombstone retention permits
deletion. Only sealed segments are eligible.

Make deletion durable using the lifecycle rules in
[section 30](#30-file-lifecycle).

## 19. Partial-file compaction

Start with a live-record threshold of 20%:

```text
eligible when LiveRecords / TotalRecords <= 0.20
```

For a segment with 100,000 records and 8,000 current records, rewrite those
8,000 records, durably publish the replacements, then reclaim the old file.

Compaction needs space for both old and replacement files; rewrite passes
are refused below the configurable `CompactionMinFreeBytes` reserve (0
disables the check; one segment size is a reasonable starting point).
Passes are also skipped while the write buffer is over half full so
foreground writes take priority. Tombstone drops and dead-file deletion
still run in both cases: they are cheap and free space.

## 20. Compaction correctness

For every source record, verify that the index still points to its exact
file, block, record, and sequence. Copy only current records.

Recheck when installing a replacement location so concurrent writes cannot
be overwritten by an older compacted value.

Compaction rewrites into the active segment (no replacement files, so no
publication metadata is needed) and assigns rewritten records fresh
sequences rather than preserving them. Either copy winning after an
interrupted compaction is therefore correct: the rewrite carries the
same value or tombstone with a higher sequence, the older copy reads as
dead, and the next reclamation pass finishes the job. Rewrites are
idempotent and old-versus-new needs no durable generation to stay
authoritative: sequences decide.

The planned transactional format changes this behavior: rewrites preserve
logical sequences and publish replacement segments through manifest
generations. See [section 44.5](#445-transaction-aware-compaction-and-reclamation).

## 21. Compaction and encryption rotation

Compaction writes using the current compression settings, encryption key,
and format version. It can therefore apply data-key rotation, compression
changes, and future format migrations gradually.

## 22. LoadAll

Startup should favor sequential I/O:

1. Read the manifest and decrypt the keyring.
2. Find segments and sort them by file ID.
3. Scan blocks; validate framing, authenticate/decrypt, and decompress.
4. Parse records and rebuild the index using mutation sequences.
5. Deliver records to application memory in block-sized callbacks.
6. Loading runs from the newest to the oldest file (and newest to oldest
block within a file), delivering each key's newest version and ignoring
all older occurrences of that key.

```go
type LoadFunc func([]Record) error
```

Block callbacks avoid millions of per-record function calls. Reuse bounded
buffers and deliver data incrementally; loading a 500 GB store must not
require a second store-sized temporary allocation.

Offer configurable parallel decryption/decompression if benchmarks justify
it. Sequential disk access may still be preferable on HDDs. Final state
must remain independent of worker completion order.

## 23. Startup conflict handling

Replace an index entry only when the scanned record has a higher sequence.
Tombstones follow the same rule. Sequence comparisons make chronological
segment loading unnecessary for correctness, though ordered scanning may
improve I/O efficiency.

The callback contract is exactly-once current state: each key is
delivered at most once per load with its current version, in newest-file
order, and tombstones are delivered for keys whose current version is a
deletion (so a reload over dirty application memory still converges).
Blocks with no current versions produce no callback. Sequential
application code needs no sequence tracking; sequence-tolerant code
keeps working unchanged.

Newest-first delivery describes the existing format. Planned loading must
resolve winners by logical sequence after validating complete commit groups;
physical order alone will no longer determine the winner. See
[section 44.4](#444-recovery-loading-and-format-compatibility).

## 24. Loading into application memory

Spool's permanent RAM structures should contain key locations, file
statistics, and metadata. The application owns its values in a map,
database, cache, or index.

Record slices borrow the decoded block buffer and are valid only for the
duration of the callback; applications retaining data must copy them.
Ownership is never transferred, so decoded blocks are released or reused
as soon as the callback returns.

## 25. Crash recovery

Every block must be independently recoverable. A clearly incomplete final
append can be discarded while retaining earlier complete blocks.

Choose an explicit corruption policy, potentially `Fail`, `SkipBlock`, or
`Salvage`. The proposed default is fail-fast except for a clearly truncated
final block. Authentication failures or corruption in earlier blocks must
not be silently treated as ordinary trailing truncation.

Interrupted lifecycle steps are safe by construction: manifest and
`keys.enc` updates are atomic file replacements; segment appends beyond
the last complete block are torn tails; compaction rewrites are
idempotent fresh-sequence copies (see [section 20](#20-compaction-correctness));
and data-key rotation persists `keys.enc` before the manifest, so the
manifest never references an undurable key. The amount of lost data depends
on the selected durability mode and pending buffers, not only on whether
the final disk block was complete.

## 26. Checksums and authentication

AEAD provides encrypted-payload integrity. Add CRC32C over framing/header
data for cheap structural-corruption detection before expensive processing;
it does not replace cryptographic authentication.

Validate magic, version, header size, compressed size, uncompressed size,
record count, and configured limits before trusting disk values or allocating
memory. Bound decompressed output to prevent compression bombs.

## 27. Durability modes

Proposed modes:

```go
type Durability int

const (
    DurabilityAsync Durability = iota
    DurabilityFlush
    DurabilitySync
)
```

| Mode | Acknowledgement boundary | Tradeoff |
| --- | --- | --- |
| Async | Accepted into memory. | Fastest; buffered writes can be lost on process or OS failure. |
| Flush | Block written to the OS/filesystem. | OS-buffered writes can still be lost on OS failure or power loss. |
| Sync | Required file and filesystem metadata syncs completed. | Strongest planned durability; higher latency. |

`Flush()` must act as a barrier for every write submitted before the call,
including records still in shards or worker queues. Sealed batches carry
monotonic generations and `Flush()` waits every generation through its own,
so completion is deterministic.

`Flush()` always commits through fsync regardless of the configured
background durability, so no separate `Sync()` exists. The durability
mode governs background flushes only; `Put()` itself acknowledges on
memory staging in every mode (plus any backpressure wait).

The planned `Commit()` chooses durability per atomic group and adds an explicit
`Sync()` barrier while retaining syncing `Flush()`. See
[section 44.3](#443-durability-ordering-and-storage-failures).

## 28. Backpressure

Bound buffered memory with `MaxPendingBytes`; start with 512 MiB. When the
limit is reached, writers wait for block-writer progress rather than growing
RAM indefinitely.

Optionally provide `TryPut()` returning `ErrBackpressure` instead of waiting.
Account for queued and in-flight work when enforcing the bound.

## 29. Large values

Configure `MaxKeySize`, `MaxValueSize`, and `MaxBlockSize`. Write a value
larger than the normal block target in its own block, provided it fits the
hard limits; avoid expanding a shared buffer around it.

## 30. File lifecycle

```text
Active → Sealed → Compacting → Obsolete → Deleted
```

Only active segments accept appends. Sealed segments are immutable, and
active segments must never be compacted.

Replacement publication and deletion must be crash-safe. Track generations,
sync files and directory metadata, and wait for outstanding references before
reclaiming files. A zero-live sealed segment can become obsolete directly
when reclamation rules permit it.

## 31. Manifest

Keep store-level metadata small: format version, store UUID, next file ID,
current encryption key ID, and feature flags. Update it atomically.

The complete key index belongs outside the manifest and remains rebuildable
from segments. Disk data and durable lifecycle metadata are authoritative.

On clean close, record a clean-shutdown marker: the segment file count
and total segment bytes. The next open requires the scan to reproduce
that shape (same count, no fewer bytes), failing loudly on external
file removal, addition, or committed-byte loss. Extra trailing bytes
stay a normal torn tail. The marker adds verification only; it never
skips integrity checks.

## 32. Memory index recovery

Rebuild the ephemeral index by scanning segments during startup. An index
journal should not be required for version 1.

Optional snapshots may later reduce startup time for multi-terabyte stores,
but the index must always be reconstructable from authoritative disk data.

## 33. Optional index snapshots

Defer this feature beyond version 1. A future `index.snapshot` could contain
the last processed segment/block, key locations, and file statistics.

Startup would load the snapshot and scan newer segments. Snapshots must
remain disposable and rebuildable.

## 34. Concurrency model

```text
application goroutines
        │
64 / 128 write shards
        │
sealed buffers
        │
compression workers
        │
encryption workers
        │
sequential append coordinator
        │
       disk
```

Compression and encryption can run in parallel. Keep disk appends
predominantly sequential and benchmark worker counts against CPU and storage
limits.

## 35. Preserve ordering

Assign a sequence to each sealed block. Workers may finish blocks in the
order `101, 100, 102`; the initial append coordinator should reorder them
to `100, 101, 102` for simpler recovery and debugging.

Logical mutation sequences remain authoritative even if physical block
ordering changes in a later implementation.

## 36. Delete semantics

`Delete(key)` writes a tombstone. Keep enough tombstone sequence/location
metadata to prevent an older value from reappearing during loading or
compaction. Remove that metadata only when recovery and compaction permit it.

A tombstone can be garbage-collected only when no older version of that key
exists in any surviving segment. Seal-time sequencing places
older-sequence records in older-or-equal files, so once the oldest file
holds no live records its tombstones contradict nothing newer and drop
atomically with the file's deletion. Reclamation fences all in-flight
batches before evaluating that condition.

That age rule depends on the existing physical-order invariant. The planned
format requires proof that no older value survives across authoritative
segments and publication generations. Murmur's replicated row tombstones
remain ordinary values, distinct from these Spool deletion markers; see
[section 44.5](#445-transaction-aware-compaction-and-reclamation).

## 37. Metrics

Expose inexpensive statistics for runtime inspection and benchmarking:

```go
type Stats struct {
    Keys             uint64
    Segments         uint64
    LiveRecords      uint64
    DeadRecords      uint64
    DiskBytes        uint64
    LiveBytes        uint64
    PendingRecords   uint64
    PendingBytes     uint64
    BlocksWritten    uint64
    BytesWritten     uint64
    CompressionRatio float64
    Compactions      uint64
    CompactionBytes  uint64
}
```

## 38. Package structure

Suggested layout, grouped by responsibility:

```text
spool/
    spool.go
    options.go
    errors.go
    stats.go

    writer.go
    writer_shard.go
    block.go
    record.go
    segment.go
    manifest.go

    index.go
    filestats.go
    loader.go
    recovery.go
    compaction.go

    compression.go
    crypto.go
    keyring.go

    internal/
        codec/
        checksum/
```

## 39. Implementation phases

| Phase | Deliverables | Validation |
| --- | --- | --- |
| 1. Basic engine | Versioned segment/block formats, `Put`, `Delete`, `Flush`, loading, memory index, file statistics. Begin without compression/encryption as an internal prototype. | Run the engine on disk, reload data, and establish a throughput baseline. |
| 2. Buffered concurrent writes | Sharded buffers, count/byte/timer thresholds, background writer, barriers, backpressure, batch APIs. | Benchmark 1, 8, 32, 128, and 512 writers; verify same-key ordering and memory bounds. |
| 3. Compression | Block-level Zstd and bounded decompression. | Measure records/s, MiB/s, CPU use, and compression ratio. |
| 4. Encryption | AEAD blocks, keyring, master/data keys, nonces, authentication. | Verify disk encryption, reload with correct keys, and reject wrong keys or tampering. |
| 5. Rotation | `RotateKey()`, historical keys, master-key rotation, current-key compaction support. | Reload across rotations and inject failures during keyring replacement. |
| 6. Compaction | Live/dead counters, zero-live reclamation, low-live selection, index validation, generation tracking, safe publication/deletion, throttling, disk reserve. | Exercise live writes during compaction; verify no stale-value resurrection or unsafe deletion. |
| 7. Crash recovery | Recovery policies, lifecycle recovery, fault injection, clean-shutdown metadata. | Kill actual processes during buffering, compression, encryption, append, fsync, segment rotation, compaction, and key rotation. |
| 8. Optimization | Profile allocations, copies, contention, hashing, codecs, syscalls, block construction, and index memory. | Repeat representative benchmarks and compare profiles before tuning. |

Validation must include live disk/process tests, not only unit tests:

- Inject faults after significant writes, renames, syncs, compaction steps,
  and key rotations.
- Generate random `Put`/`Delete`/`Flush`/`Crash`/`Reload`/`Compact` sequences
  and compare recovered state with an authoritative reference map that
  accounts for the chosen durability guarantees.
- Benchmark hot-key updates, tiny values, 1 KiB and 100 KiB values,
  compressible and incompressible data, 1–256 goroutines, and NVMe versus
  slower disks.

Document every newly introduced dependency in [VENDORS.md](../VENDORS.md)
in the same change that adds it.

## 40. Performance design principle

In asynchronous mode, `Put()` should usually interact only with sharded RAM
and atomic counters. Batch construction, compression, encryption, append,
sync, compaction, and deletion run asynchronously.

Large sequential disk writes are the target. Backpressure and explicitly
requested durability remain valid reasons for a writer to wait.

## 41. Initial recommended defaults

These are proposed starting points, all configurable and subject to
benchmark results:

| Setting | Initial value |
| --- | --- |
| Write shards | 128 |
| Index shards | 256 |
| Block target | 4 MiB |
| Maximum records per block | 10,000 |
| Flush interval | 100 ms |
| Segment size | 256 MiB |
| Compression | Zstd fast |
| Encryption | AES-256-GCM (standard library only) |
| Compaction eligibility | At most 20% live records |
| Compaction workers | 1 |
| Compaction free-space reserve | 0 (disabled) |
| Maximum pending data | 512 MiB |

The planned atomic-group limit is 256 MiB. It is a separate bound from block
size, the segment-size target, and pending-memory capacity; see
[section 44.8](#448-resource-limits-diagnostics-and-fault-injection).

## 42. Most important architectural rule

Separate logical write order from physical disk organization. Mutation
sequences determine which version wins; segment/block locations determine
where that version is stored.

Preserve this rule across concurrent writes, compaction, key rotation,
recovery, and format changes. The remaining API and recovery decisions
are settled for the existing format as follows:

- Loading is `Load`/`LoadWithOptions`/`OpenAndLoad` with exactly-once
current-state callbacks; record buffers are borrowed, copy to retain.
- `Put` acknowledges on memory staging; `Flush` is the fsync barrier and
no separate `Sync` exists.
- The keyring and manifest update atomically; compaction rewrites into
the active segment with fresh sequences, needing no generations.
- Tombstones drop with their drained oldest file under a reclamation
fence; corruption fails fast except for a clearly truncated final
block.

The future transaction, loading, compaction, and durability contracts in
[section 44](#44-required-changes-for-murmur-integration) explicitly replace
the corresponding existing-format rules when implemented.

## 43. External package.

Try to limit the usage of external package and favor using standard go package when possible.

Spool's non-test external imports are exactly two, both without a
standard-library equivalent: standard library `compress/flate` for deflate block
compression (Go ships no zstd codec) and `golang.org/x/crypto/argon2`
for the memory-hard passphrase KDF required by
[section 12](#12-master-key). Encryption (AES-256-GCM), key derivation
(HKDF-SHA256), hashing, randomness, and the store identity all come
from the standard library.

## 44. Required changes for Murmur integration

**Status: implemented and tested.** This section records the Spool-side work
required by the Murmur migration; every checkbox below is implemented, with
unit, race, fault-injection, reference-model, benchmark, and live process-kill
evidence. Murmur's production backend migration and its live acceptance tests
are complete.

The target is Spool as Murmur's sole persistence backend, with readable
application state held in RAM and persisted in encrypted Spool files. Fresh
databases and new-format backups are supported; conversion from older storage
pre-transactional Spool stores is outside this migration. Existing SQL,
replication, bridge, file, backup/restore, and key-rotation capabilities must
remain available after the future Murmur integration.

Murmur needs one commit to cover current state, causal records, replication
logs, receipts, sequence/HLC metadata, watermarks, schema changes, and storage
generation updates. A crash must never expose only part of that mutation set.
See the [Murmur transaction contract](../architecture/transactions.md#15-spool-atomic-commit-for-a-local-mutation)
and [snapshot requirements](../architecture/snapshots-backup-and-restore.md#37-snapshot--full-seed).

### 44.1. Atomic mixed-write API

- [x] Add the following API. Signatures below are planned and must not be
  presented as runnable examples until implemented:

```go
type Mutation struct {
    Key     []byte
    Value   []byte
    Deleted bool
}

func (s *Store) Commit(mutations []Mutation, durability Durability) error
func (s *Store) Sync() error
```

- [x] Validate all mutations and their total encoded size before admitting
  any part of a group. Copy accepted keys and values; callers retain ownership.
  A deleted mutation must have no value. Empty groups are no-ops and do not
  establish a durability barrier; use `Sync()` for that purpose.
- [x] Preserve operation order within a group. The final operation on a
  repeated key wins. All validation precedes admission, and all disk recovery
  and index publication are atomic for the complete group.
- [x] Retain `Put`, `TryPut`, `Delete`, `PutBatch`, and `DeleteBatch` with their
  documented admission behavior. Do not silently promise atomicity for those
  existing batch APIs. Their buffered writes must still use valid transactional
  framing and participate in sync/checkpoint fences.
- [x] Allow Murmur to submit a synchronous group of local transactions as one
  atomic Spool commit. Spool treats its keys and values as opaque bytes and
  does not interpret SQL, origin signatures, receipts, or distributed versions.

### 44.2. Transactional format and segment boundaries

- [x] Introduce a new versioned segment/block format and matching manifest
  format. Identify supported features explicitly and reject unknown required
  features before loading records.
- [x] Frame each commit group as bounded data blocks followed by an
  authenticated completion record. Bind the group ID, ordered block identity,
  record count, encoded length, and SHA-256 content digest to that completion.
  Validate block ordering, declared counts, sizes, and digest before accepting
  the group. Retain AEAD for payloads and CRC32C for bounded framing checks.
- [x] Keep an entire group within one segment. Reserve its space and rotate
  before starting it when necessary. If one permitted group exceeds the normal
  segment target, give it a dedicated oversized segment and seal that segment
  after the group completes. Never split a group across segment files.
- [x] Serialize segment appends for entire groups so data/completion records
  cannot interleave. Compression and encryption may run in parallel, but the
  append coordinator preserves admitted group order.
- [x] Assign mutation sequences in that order, retaining restart-safe sequence
  allocation. Reserve enough sequence space for the entire group before
  admission; exhaustion rejects the whole group without partial writes.
- [x] Persist segment creation and manifest membership before acknowledging
  synchronous commits. Manifest metadata identifies the authoritative active
  and sealed segments and compaction generations; directory listings alone
  are insufficient to decide which replacement files to load.

### 44.3. Durability, ordering, and storage failures

| `Commit()` mode | Successful acknowledgement |
| --- | --- |
| `DurabilityAsync` | The complete group is accepted into owned, ordered pending memory. No disk durability is promised. |
| `DurabilityFlush` | Every data block and the completion record have been written to the OS/filesystem. |
| `DurabilitySync` | The complete group and required file/directory metadata have been synced. |

- [x] Publish committed disk locations and file statistics only after the
  complete group reaches its requested boundary. An async caller may publish
  its application RAM state after acceptance, but must retain the distinction
  between accepted and synced progress.
- [x] Implement `Sync()` as a fence over all writes accepted before the fence
  is captured, including buffered individual writes, queued groups, and
  in-flight workers. Keep `Flush()` as the same full syncing barrier for
  compatibility. Writes accepted afterward may belong to the next fence.
- [x] Make `Close()` drain accepted groups and perform a syncing barrier before
  recording a clean shutdown; return failures rather than claiming a clean
  close. Failed close must release resources without clearing failure state.
- [x] Expose a sticky `ErrStorageFailed`, a `StorageError()` accessor, and an
  optional `OnStorageError func(error)` notification in options. Notify outside
  internal locks so the hosting database can reject subsequent operations.
- [x] Treat uncertain append/sync failures and failures of authoritative
  manifest/keyring publication as terminal until reopen. Do not retry uncertain
  groups automatically or assign replacement sequences to them. Recovery may
  retain a complete unacknowledged group or discard it, but never retain a
  partial mutation set. Validation, capacity rejection, cancellation before
  admission, and refusal to compact below the free-space reserve are ordinary
  operation errors rather than terminal storage failures.

### 44.4. Recovery, loading, and format compatibility

- [x] Recover only complete, validated groups. A structurally incomplete final
  group at the authoritative active tail is discarded as a whole, including
  any individually complete data blocks that lack its completion record.
  Authentication failures, bad digests, and corruption of complete groups
  fail closed; do not classify them as harmless truncation.
- [x] Resolve each key's winner by highest logical mutation sequence across
  authoritative segments. Compaction can relocate an older version into a
  newer physical file, so first-seen physical order is not sufficient.
- [x] Preserve exactly-once current-state callbacks, including retained Spool
  deletion markers. Validate groups and determine winning locations in a first
  pass, then decode winning records in bounded blocks for callback delivery.
  Keep only key/location metadata between passes, not another permanent copy
  of all values. Preserve callback-buffer borrowing and error propagation.
- [x] Discard staged load results if open/loading fails. Murmur must not expose
  RAM state or start networking until complete recovery and identity/schema
  validation succeed. Callback delivery during startup does not supply a
  runtime transaction subscription or a consistent online read API.
- [x] Reject unsupported store versions before opening a writer or changing
  the store. Murmur rejects legacy and pre-transactional storage directories
  without migrating or modifying them. Plaintext mode remains available only
  for standalone development/tests; Murmur requires encryption.
- [x] Extend clean-shutdown checks to the authoritative manifest generation and
  committed group boundary. A clean marker must never bypass authentication
  or admit orphan replacement files.

### 44.5. Transaction-aware compaction and reclamation

- [x] Replace fresh-sequence rewrites into the active segment with replacement
  segments that preserve each record's logical sequence. Do not let relocation
  create a newer logical mutation or resurrect a stale value.
- [x] Select records using exact file/block/record/sequence identity and
  revalidate candidates under the commit coordinator before replacement
  publication. Concurrent newer writes win. Replacement records form their
  own complete validated groups; superseded members of an original group
  need not be retained together once their surviving records are safely
  represented in the replacement generation.
- [x] Sync replacements, atomically publish the manifest generation selecting
  old or replacement segments, and sync its directory before unlinking sources.
  After interruption, recovery selects the authoritative generation and removes
  unreferenced temporary/replacement files. An uncertain publication failure
  stops further writes rather than guessing which generation won.
- [x] Reclaim only sealed segments with no needed values or deletion markers
  and no outstanding loader, writer, compactor, or checkpoint references.
  Key retirement obeys the same reference constraints.
- [x] Drop a Spool deletion marker only after proving no older value for that
  key remains in any authoritative segment. The existing oldest-file shortcut
  cannot be reused once physical and logical order diverge. Durable replacement
  publication must precede reclamation that depends on it.
- [x] Keep Murmur row tombstones, causal records, and replication logs as opaque
  ordinary values. Murmur decides their logical retention; Spool never infers
  that a replicated tombstone can be dropped from segment age.
- [x] Retain configurable live-ratio selection, write-pressure pausing, and
  free-space reserve checks. Account for both source and replacement storage;
  avoid compacting active segments and prioritize foreground commits.

### 44.6. Consistent encrypted checkpoints

- [x] Add the planned checkpoint API:

```go
func (s *Store) Checkpoint(ctx context.Context, destination string) (*Checkpoint, error)
func (c *Checkpoint) Release() error
```

- [x] Capture one committed cut: drain and sync accepted writes, seal the active
  segment, and capture its manifest generation and matching encrypted keyring.
  Exclude commit, rotation, and compaction publication during capture. Once
  capture is complete, resume live writes before archive streaming.
- [x] Hard-link immutable authoritative segments into staging on the same
  filesystem. Copy manifest/keyring bytes atomically into the checkpoint;
  never hard-link metadata that may be changed in place. Reject cross-filesystem
  staging without silently falling back to full-database copying.
- [x] Make the checkpoint independently openable with its captured context and
  wrapping key. Later live compaction, rewrites, rotation, and key retirement
  must not invalidate checkpoint contents. Rewrites create replacement inodes
  rather than modify linked segments.
- [x] Return a handle owning checkpoint retention registrations. `Release()` is
  idempotent and releases those registrations; the backup caller owns deletion
  of the completed staging directory. Failed or canceled creation removes
  partial staging and releases registrations. Checkpoint destinations must be
  new directories, and cleanup must not remove unrelated caller data.
- [x] Support bounded ciphertext archive streaming with no value decoding in
  the backup pipeline. Document actual checkpoint timings from benchmarks,
  rather than inheriting performance claims from another storage engine.

### 44.7. Encryption, key management, and maintenance

- [x] Keep AES-256-GCM and generated unique nonces. Accept 32-byte wrapping
  material for Murmur, zero owned temporary key buffers where practical, and
  never persist plaintext keys.
- [x] Add a wrapping-key identifier to open/load options and persist it in the
  authenticated keyring envelope. Expose bounded key metadata for the Murmur
  adapter to select `KeyProvider.Lookup(id)`; metadata used before decryption
  is only a lookup hint until the envelope authenticates. Keep provider logic
  application-owned rather than importing Murmur into Spool.
- [x] Add an expected 16-byte database-context ID to open/load options. Bind the
  context, immutable store identity, group/block identities, and relevant header
  fields to authentication and key derivation. A provided context must match
  authenticated persisted context before writable open. Standalone stores may
  use their generated store identity as the default context.
- [x] Rotate the wrapping key and its identifier together through atomic
  keyring replacement. Preserve existing standalone rotation calls and add
  a named-key rotation entrypoint for Murmur. Resolve interrupted replacement
  using the authoritative envelope and matching supplied key before writes.
- [x] Support configurable data-key age rotation, checking on open and before
  new groups when the current key expires. Persist the new key before any
  group can reference it; historical keys remain usable while referenced.
- [x] Add maintenance operations to rewrite all authoritative encrypted data
  under the current key and to rebind the database context for reseed. Persist
  a recoverable intent, generate replacement files, verify and sync them, then
  publish the matching manifest/keyring generation. Gate writes during these
  operations and resume an interrupted operation before writable open.
- [x] Preserve existing checkpoints under their original context and keyring
  during rewrites/reseed. Retire a historical live-store key only after no
  authoritative file, open handle, pending group, or registered checkpoint
  depends on it; archived backups retain their own keyring copies.
- [x] Expose non-secret key inventory, references, active wrapping-key ID,
  active data-key ID, keyring generation, maintenance phase, and progress for
  Murmur's diagnostics and rotation APIs.

### 44.8. Resource limits, diagnostics, and fault injection

- [x] Add `MaxAtomicBatchBytes`, default 256 MiB, measured over the entire
  encoded group including record/group framing. Validate integer arithmetic,
  counts, and lengths before allocation. Keep the 4 MiB block target and
  hard per-block/per-value limits independent from this group bound.
- [x] Charge accepted group copies, queued work, and in-flight owned record
  bytes to pending accounting. Enforce capacity for the whole group before
  accepting it; do not use the large-single-record bypass for atomic groups.
  Reject a group that cannot fit the configured pending capacity rather than
  waiting forever. Bound worker buffers separately and report their memory.
- [x] Expose accepted, written, and synced group progress; pending groups/bytes;
  commit/sync counts and latency; incomplete groups discarded; storage failures;
  checkpoint activity; compaction bytes/debt; and maintenance progress.
- [x] Add internal test hooks for writes, short writes, sync, rename, directory
  sync, deletion, checkpoint links, and publication boundaries. Use a small
  local I/O abstraction or hooks; do not introduce an external filesystem dependency.
- [x] Preserve standalone statistics and distinguish advisory compaction
  deferrals from failures that make authoritative storage unusable.

### 44.9. Integration boundary and dependency policy

Spool remains a write/load persistence engine without a disk `Get()` API.
Murmur owns readable values, ordered memory indexes, immutable read snapshots,
read/merge/write serialization, and atomic publication of its RAM view after
the selected Spool acknowledgement boundary. Spool's commit groups provide the
durable foundation for that publication.

Replication staging, resumable snapshot progress, final watermark publication,
bridge receipts, schema metadata, and restored writer identities are opaque
keys committed through the new API. Their protocols and merge rules remain
in Murmur. Spool's checkpoint provides encrypted files and a consistent cut;
Murmur owns archive versions, destinations, file-object inclusion, and restore
identity policy.

Use standard Go crypto, hashing, synchronization, and filesystem facilities.
No new external dependency is required by these extensions. Any dependency
later introduced must be documented in [VENDORS.md](../VENDORS.md) in the same
change. Murmur's future memory-index dependency changes belong to the Murmur
integration, not to Spool.

### 44.10. Implementation order and acceptance tests

| Step | Deliverable | Required evidence |
| --- | --- | --- |
| 1 | Versioned commit groups, mixed mutations, bounds, and ordered publication. | Unit/property tests for repeated keys, mixed deletes, multi-block groups, rollover, oversize rejection, and sequence exhaustion. |
| 2 | Recovery, syncing barriers, close semantics, and terminal failures. | Live process kills and injected I/O faults before/after data blocks, completion, file sync, and directory sync; recovered groups are complete or absent. |
| 3 | Logical-sequence loading and generation-based compaction. | Concurrent hot-key writes/deletes during compaction, restart at every publication step, bounded callback memory, and no stale-value resurrection. |
| 4 | Context binding, named wrapping keys, rotation, rewriting, and reseed. | Wrong-key/context rejection, tamper rejection, rotation across restart, and recoverable maintenance at each publication boundary. |
| 5 | Consistent checkpoints and reference-safe cleanup. | Live backup capture during writes/compaction/rotation, standalone checkpoint reopen, cancellation cleanup, cross-filesystem rejection, and repeated release. |
| 6 | Diagnostics, platform checks, and performance measurement. | Race checks, supported OS builds, encryption audits, and measured throughput/latency/RAM/disk amplification. |

- [x] Test synchronous group commits and async `Sync()`/`Flush()` fences against
  an authoritative reference model. A crash can lose unsynced async groups or
  recover complete unacknowledged groups; it must never recover part of a group.
- [x] Verify callbacks after recovery cannot expose a receipt, watermark,
  progress marker, or generation without the corresponding surviving state
  from its atomic mutation set.
- [x] Include real process termination with actual disk files, not only mocks
  or unit tests. Exercise disk exhaustion and short writes, completed-group
  corruption, incomplete tails, orphan replacements, and open-lock exclusion.
- [x] Benchmark isolated and grouped synchronous writes, hot-key updates,
  compressible/incompressible values, concurrent writers, reload, compaction,
  checkpoint interference, and full rewrite. Record results without assuming
  the new format is faster than prior storage formats.
- [x] Run package unit/race checks and the live Spool scenario, then require
  Murmur's separate live replication, snapshot, bridge, file, and backup tests
  before changing its production backend. Completing this README does not
  satisfy that acceptance gate.

### 44.11. Implementation notes and evidence

This subsection records how the implementation interprets a few checklist
items, and where the evidence lives. It adds no new requirements.

**Verification record.** `go test ./spool/` (unit, ~5-18s),
`go test ./spool/ -race` (~10-17s), `bash tests-live/run.sh spool` (five live
scenarios incl. SIGKILL crash recovery, group atomicity, kill-during-rebind,
and backup-under-write, ~30-36s), plus `GOOS=windows`/`darwin` builds. Key
test files: `spool_model_test.go` (reference model),
`spool_fault_test.go` (injected I/O faults), `spool_ckpt_test.go`
(checkpoints), `spool_recovery_test.go` (corruption/tails/orphans),
`spool/tests-live/spool_live_test.go` (process kills).

**Ordering of buffered writes vs commit groups (44.1).** `Commit` seals
buffered individual writes into admitted groups before admitting its own
group, so an accepted `Put` never outranks a later `Commit` on the same key.
A staged-write dirty counter keeps this to one atomic load for commit-only
callers; `Put` throughput is unaffected. Pinned by
`TestCommitOrdersAfterBufferedPuts` and `TestReferenceModel`.

**Short writes (44.8, 44.10).** There is no short-write hook: a short write
is observable on disk only as a torn tail or truncated file, which is exactly
what the tail-truncation recovery tests exercise (`TestTornTailRecovery`,
`TestTornTailFullFrame`, mid-file corruption tests). The loader cannot
distinguish a short write from a crash mid-append, so hook coverage would add
no new behavior. Disk exhaustion is exercised through error-returning fault
hooks at every durability boundary.

**Callback atomicity (44.10).** Recovery admits only complete groups, so any
marker (receipt, watermark, generation) loaded from disk always has its full
mutation set present in the same load. `Load` delivers records in block
chunks, so Murmur must still publish its state root only after the whole load
succeeds; the migration contract requires exactly that discipline. Covered by
the group-atomicity live test and the reference-model checkpoint-cut checks.

**Pruning needs no capture exclusion (44.5, 44.6).** Pruning scans live
segment files for key usage; capture exclusion keeps every linked member
live for the whole snapshot-plus-links section; and retired keys gain no new
references. A keyring copy taken inside the section is therefore complete for
every linked segment without serializing pruning against capture.

**Rotation IDs are manifest-allocated (44.5).** Writer rotation and
compaction replacement share one file-ID allocator. Deriving successors as
sealed+1 could collide with a live replacement (wedged writer, or a rename
clobbering the writer's file); allocation plus `O_EXCL` creation closes the
race. Pinned by `TestCompactionWriterIDCollision`.

**Resume clears a stale clean marker (44.4).** A clean close written while a
maintenance intent is pending carries a pre-resume shape; when resume
publishes a new manifest generation it clears the marker, since the
post-rebuild check would otherwise compare shapes across the resume.
Integrity still holds: resume verifies every staged file and the rebuild
validates every group. Pinned by `TestFaultRenameMaintenanceResumes`.

**Checkpoint captures seal first (44.6).** Capture seals buffered individual
writes into admitted groups before fencing, so the cut includes every
accepted write. Rotation, compaction publication, and maintenance start
serialize against the capture section; commits block briefly, then proceed.

**Benchmark results (44.10).** Recorded on an Intel i5-6500, Linux, AES-256-GCM
encrypted stores. Methodology per benchmark name in `spool_bench_test.go`;
figures are point measurements for capacity planning, not comparisons
against other formats.

| Benchmark | Result |
| --- | --- |
| `PutWriters/1` (128B values) | 207 MB/s |
| `PutWriters/8` | 244 MB/s |
| `PutWriters/32` | 191 MB/s |
| `PutWriters/128` | 176 MB/s |
| `GroupSyncWrites/1` | 706 groups/s (1.42 ms) |
| `GroupSyncWrites/20` | 731 groups/s, 1.87 MB/s |
| `HotKey` (same-key updates) | 404k puts/s |
| `FlushThroughput/compressible` (1KB) | 306 MB/s sustained |
| `FlushThroughput/incompressible` (1KB) | 75 MB/s sustained |
| `Reload` (50k keys) | 250k keys/s |
| `Compaction` (100-key cycle) | 21.9 ms |
| `RotateKey` | 169 rotations/s (5.9 ms) |
| `CheckpointCapture` (5MB store) | 13.5 ms |
| `CheckpointUnderWrite` (1.2M puts/s flood) | ~1.9 s/capture, backlog- and scan-dominated; highly load-dependent |
| `Rewrite` (full re-seal) | 39.5 MB/s |
