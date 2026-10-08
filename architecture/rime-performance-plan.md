# RIME performance implementation plan

Status: substantially implemented (2026-10-07); this document is now the
historical roadmap. Implemented: frozen baselines, stages 3.1–3.4, the
batch path, kind-specialized (typed) index keys, and the reads/scans/joins
work — see [stage 3.1](rime-benchmarks.md#stage-31-results-2026-10-07)
through [stage 3.6 results](rime-benchmarks.md#stage-36-results-reads-scans-joins-2026-10-07)
and the [closing SQLite scorecard](rime-benchmarks.md#closing-sqlite-scorecard-2026-10-07).
Deferred by decision: stage 3.5 parallel commits (optional polish; the
concurrency target was met) and the dense-scan layout for full
table scans (design in [rime/IMPROVEMENTS.md](../rime/IMPROVEMENTS.md#1-full-table-scan-path)).
See [the current architecture](../rime/ARCHITECTURE.md) for the implemented
single-write fast path and lazy staged map. RIME remains a native Go, in-memory engine.
Use only standard-library dependencies. Preserve its existing public API,
snapshot isolation, immutable records and transactional guarantees.

## 1. Objective and evidence

Improve both individual mutation cost and throughput with four writers sharing
one database. Optimize insert, update and delete on disjoint keys, while keeping
hot-key workloads and long-lived snapshots practical. Preserve point-read speed.

The [write profiles](rime-benchmarks.md#write-profiles-2026-10-07) establish:

- 99.4% of recorded mutex contention is at the global commit lock in the
  four-writer disjoint-update capture. This is aggregate waiting, not CPU time.
- Single-row updates allocate about 2,578 bytes and 36 allocations per row.
  Pending-write construction, commit maps and unique-claim bookkeeping dominate.
- Hotspot updates allocate about 22,810 bytes per row; copying version history
  accounts for 88.4% of allocation volume in that capture without timed GC.
- Batching 256 rows reduces disjoint-update allocation to about 1,420 bytes and
  23 allocations per row. Index locks are not the current measured contention
  bottleneck, because their work is already inside the global commit lock.

Engineering targets, to be validated rather than treated as promised speedups:
reduce single-row update bytes and allocation counts by at least 50%; double
four-writer disjoint mutation throughput against the frozen unprofiled baseline;
achieve at least 2x one-writer throughput with four writers on disjoint single-row
transactions after the concurrent-commit phase. History append cost should be
bounded or logarithmic in retained history rather than copying its full length.

## 2. Freeze reproducible measurements

Before production changes, extend the existing harnesses with isolated,
unprofiled mutation benchmarks. Keep the matched-index SQLite comparison and
RIME-only profiles as separate measurements. Record Go version, CPU, GOMAXPROCS,
shard count, index configuration, GC policy, transaction size and operation mix.

Use one fixed host, GOMAXPROCS=4, at least five repeated unprofiled runs and
median results. Do not run other benchmarks concurrently. Capture CPU/allocation
and mutex/block profiles separately when diagnosing each milestone.

Sweep writers 1/2/4/8; transaction sizes 1/16/256; 1K/100K rows and a capacity
check at 1M. Include no secondary indexes, the current matched index set, and
unique/compound/prefix constraints. Test disjoint keys, one hot key, 64 hot keys,
uniform access and multi-table transactions. Keep schedules, successful work
counts and transaction boundaries identical between engines; report retries.

Measure rows/s, bytes/row, allocations/row, commit p50/p95/p99, conflict rate,
aggregate lock waits, retained heap and read latency. Quantify percentile
sampling overhead separately. Include hook/event subscribers enabled and
disabled, GC enabled and disabled, and readers pinning historical snapshots.

Deliverable: a saved baseline and reproducible commands in
[rime-benchmarks.md](rime-benchmarks.md). Define comparable acceptance workloads
before implementation; profile timings are not acceptance baselines.

## 3. Implementation sequence

### 3.1. Reduce transaction allocations

Primary files: `rime/tx.go`, `rime/table.go`, `rime/index.go`.

- Add inline storage for the common single-write transaction. Promote to the
  existing map-based staged lookup only when the transaction grows; keep large
  batches and repeated-key staged reads efficient.
- Replace per-mutation check/validate/apply closures with table dispatch methods
  over explicit pending-write data. Collect post-commit changes without creating
  a callback closure for every row. Preserve mutation order and callback counts.
- Specialize single-write commit bookkeeping so it does not build the general
  seen-table, checked-key, final-write and nested unique-claim maps.
  Promote to the general path if a BeforeCommit hook adds mutations.
- Compute only required unique ownership changes. Avoid release/reclaim work
  for unchanged values where safe, retaining final-state validation for swaps,
  deletes/reinserts and temporary value reuse. Immutable primary-key updates
  should not require the general unique-swap machinery.
- Pre-size large-batch scratch structures. Introduce bounded scratch reuse only
  if profiles justify it; clear record/table references when releasing buffers.

Keep commit serialization during this phase so algorithmic and concurrency
changes can be verified separately. Target at least 50% fewer bytes and
allocations on the frozen single-row update workload. Reprofile to identify
remaining lock-holder work and GC cost.

### 3.2. Replace full-history copying

Primary files: `rime/mvcc.go`, `rime/table.go`, `rime/gc.go`, `rime/shard.go`.

Prototype immutable chunked version storage with a persistent search structure.
Append by copying a bounded active chunk and its lookup path, not every retained
version. Keep latest reads cheap and historical lookup efficient. Evaluate the
prototype against current history lengths before selecting chunk size/layout.

Retain all versions required by active snapshots. GC must preserve the boundary
version and all newer versions, including deletion and reinsertion history.
Existing readers must safely use old structures after replacement. Preserve
`ReadAt` rejection once history is genuinely reclaimed.

Gate: compare append allocation across increasing retained histories; growth
must not remain linear per append. Run the same hotspot schedule with GC off,
GC on and a pinned old snapshot. Require exact historical model equivalence,
bounded retention after snapshot release and no material point-read regression.
Do not count dropping required history as a performance improvement.

### 3.3. Reduce index and field-access overhead

Primary files: `rime/index.go`, `rime/schema.go`, `rime/table.go`.

- Compile compact metadata lists for indexed, defaulted and NOT NULL fields;
  avoid repeated whole-schema work where no relevant fields exist.
- Avoid duplicate primary-key/hash storage when a unique lookup already serves
  that operation, after adapting planning/statistics and checking all index paths.
- Reduce one-key hash bucket allocation on highly distinct indexed values.
  Preserve duplicate-value queries and transitions from one key to many keys.
- Reduce reflected field boxing where profiles show a material cost. Evaluate
  safe primitive accessors first, preserving named types and existing schema
  semantics. Keep reflection as a correct fallback.

Choose changes from fresh profiles after phases 3.1/3.2. Require gains on insert
and update workloads with the representative index set; recheck ordered range,
compound, prefix, unique, cached-plan and historical-query equivalence.

### 3.4. Shorten the serialized commit section

Separate pure preparation from live-state validation and publication. Prepare
write summaries and immutable allocations before acquiring the global lock,
then validate optimistic bases under synchronization before publishing.

Keep constraint/hook semantics: callbacks and live-state checks cannot simply
move outside the lock. Prepared data must be rebuilt or rejected if its base
changed. Preserve cancellation before publication, atomic rollback and callback
execution after visibility. Keep the existing atomic publication barrier here.

Measure lock hold time, waiting and throughput. This is an incremental reduction
in serialization, not parallel commit support. Reassess whether its remaining
cost justifies the concurrent design below.

### 3.5. Introduce concurrent commits for independent transactions

This phase needs a concurrency protocol and deterministic tests before rollout.

- Reserve affected keys and unique values, including old/new ownership, in a
  stable global order. Disjoint transactions may validate and prepare together;
  conflicting keys/claims coordinate. Do not rely solely on locking all storage
  shards: a 256-row transaction can span all 64 shards and block other batches.
- Start with a conservative fast path for independent single-row transactions.
  Retain a coordinated serialized path for arbitrary BeforeCommit/CHECK
  callbacks and cross-record constraints until their stable-view semantics are
  supported by the parallel protocol. Fast and fallback paths must exclude
  unsafe overlaps through one shared coordination scheme.
- Assign commit tickets after validation and resource reservation. Install
  versions tagged as in-flight, then advance the visible commit frontier only
  through fully completed tickets. Readers ignore unpublished versions.
  A successful write returns and emits callbacks only once its entire commit
  is visible. Specify how aborts retire tickets and how stalled earlier commits
  affect later visibility.
- Update reader/index planning together. Mutable indexes can lose old candidates
  during preparation; checking a record's TxID alone cannot prevent missing rows.
  Use stable table generations checked around candidate capture and an in-flight
  state that forces a pinned historical scan when safety is uncertain. Use
  versioned candidates if fallback cost proves excessive. Make generation
  updates monotonic even when preparation finishes out of ticket order.
- Partition index locks only when they limit the new parallel path. Partition
  hash/unique claims by value; evaluate per-storage-shard ordered trees and the
  cost of merging range results. Account for batches touching many partitions.
- Coordinate snapshot registration and incremental shard-local GC with the same
  visibility/retention protocol; remove whole-database GC exclusion only after
  safe reclamation under concurrent publication is established.

Gate: at least 2x one-writer throughput with four disjoint single-row writers
on the designated fixed-host acceptance workload, while preserving read latency
and the correctness gates below. This is a target, not a guarantee for hot keys,
large cross-shard batches or callback-heavy transactions.

### 3.6. Profile and improve reads, scans and joins

After mutation work, profile point joins, COUNT/AVG, ordered ranges and full
joins separately. The [scan improvement backlog](../rime/IMPROVEMENTS.md#1-full-table-scan-path)
specifies the proposed sequence and its publication, reclamation and measurement
requirements; these changes are not implemented.

Start with streaming, fused aggregates and bounded result-capacity estimates.
Then prototype a dense, safely published row directory to remove map enumeration
and temporary chain-pointer copies. Preserve deletion/reinsertion visibility and
live cursors during compaction; measure directory memory and mutation overhead.
Latest visibility already has a direct head-version fast path. Further inline
head work must retain a coherent record/commit/tombstone state and the pinned
snapshot check, even when no older reader exists.

Specialize predicates only for filtered workloads where profiles justify it.
Evaluate bounded parallel scanning after reducing single-worker costs, including
its effect on four-writer throughput and read/commit tail latency. Avoid unnecessary
intermediate join allocations and repeated plan work. Cached planning currently
locks every storage shard to count map entries; measure that cost before replacing
it with maintained counts.

Compare equivalent outputs: aggregate SQL joins must be compared with equivalent
RIME aggregates, and materialized joins with materialized SQL results. Keep
snapshot consistency, contexts, caps and historical fallback behavior. Accept
changes only against the corresponding read workload, with concurrent-writer,
historical-snapshot and churn/retention checks. Preserve aggregate pagination
semantics. SQLite parity is an overall target; each milestone needs measured gains
against RIME's frozen baseline and an assessment of write and memory regressions.

## 4. Correctness and live acceptance

Follow [RIME qualification](rime-testing.md) throughout implementation. Each
milestone must pass relevant model tests, the race suite, the matched-index
comparison with exact final-state validation and a four-reader/four-writer live
workload with GC, indexed reads and atomic two-table transfers. Run the longer
nightly qualification before accepting the concurrent-commit protocol.

Add deterministic regressions for out-of-order preparation/publication, an
aborted or stalled earlier ticket, a writer returning before visibility, two
writers claiming the same unique value, opposite lock acquisition order,
serialized fallback versus fast-path overlap, index candidate capture racing
publication, snapshot registration racing GC and shutdown/cancellation races.
Exercise retained snapshots across updates, tombstones, reinsertion and GC.

On the fixed host, use a provisional 10% regression threshold for point-read
latency, retained heap and commit p99 in comparable workloads, investigating
repeatable regressions rather than rejecting noisy individual samples. Long
history and indexed-range reads must be included when evaluating new storage.
Publish limits when gains depend on transaction size, indexes or key distribution.

## 5. Delivery and reporting

Deliver phases as reviewable changes, recording before/after unprofiled results,
profiles, allocation counts, relevant checks and limitations in
[rime-benchmarks.md](rime-benchmarks.md). Update
[the engine contract](rime.md), qualification instructions and README examples
when implemented behavior changes. Reorder later optimizations only when fresh
measurements support it. Mark planned capabilities as implemented only after
correctness and live acceptance pass.
