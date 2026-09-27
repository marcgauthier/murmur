# Implementation roadmap

Implementation phases, prototype files, MVP boundaries, and recommended next steps.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [67. Phase 0 — Feasibility Spike](#67-phase-0--feasibility-spike)
- [68. Phase 1 — Local Durable State Engine](#68-phase-1--local-durable-state-engine)
- [69. Phase 2 — SQL Change Capture and Transaction Manager](#69-phase-2--sql-change-capture-and-transaction-manager)
- [70. Phase 3 — CRDT Merge](#70-phase-3--crdt-merge)
- [71. Phase 4 — SWIM Membership and Bounded QUIC Replication](#71-phase-4--swim-membership-and-bounded-quic-replication)
- [72. Phase 5 — Snapshot and Log GC](#72-phase-5--snapshot-and-log-gc)
- [73. Phase 6 — Encryption and Rotation](#73-phase-6--encryption-and-rotation)
- [74. Phase 7 — Search and Performance](#74-phase-7--search-and-performance)
- [75. Phase 8 — Hardening](#75-phase-8--hardening)
- [76. First Prototype File List](#76-first-prototype-file-list)
- [77. Minimum Viable Prototype](#77-minimum-viable-prototype)
- [87. Recommended First Engineering Decision](#87-recommended-first-engineering-decision)
- [91. Recommended Immediate Next Step](#91-recommended-immediate-next-step)

---

## 67. Phase 0 — Feasibility Spike

### Objective

Prove the risky integration points before building the replication engine.

### Tasks

- Build LumoSQL amalgamation suitable for CGO.
- Create minimal Go binding.
- Open in-memory DB.
- Create table.
- INSERT/UPDATE/DELETE.
- Compile with `SQLITE_ENABLE_PREUPDATE_HOOK`.
- Receive pre-update callbacks in Go/C bridge.
- Read old/new values safely.
- Execute multi-row update and verify callbacks.
- Execute trigger-generated updates and inspect pre-update depth.
- Suppress capture on a designated apply connection/mode.
- Build FTS5 if intended.
- Confirm in-memory connection model.
- Open encrypted Pebble.
- Write/read transaction.
- Configure block cache.
- Verify internal encryption data-key rotation.
- Establish quic-go mTLS connection.
- Send framed binary message.
- Carry memberlist packets in QUIC DATAGRAMs and reliable exchanges in deadline-aware streams on the same endpoint.
- Prove identity/DBID admission, datagram size limits, and reserved membership capacity during bulk replication.

### Exit criteria

Do not proceed until all of these are proven in one Go test binary.

---

## 68. Phase 1 — Local Durable State Engine

### Objective

Build the authoritative Pebble representation with no networking.

Implement:

- NodeID.
- DBID.
- HLC.
- origin sequence.
- stable schema registry.
- binary Pebble keys.
- `CellState`.
- tombstones.
- `MutationBatch`.
- TxID receipt.
- serialized read/merge/write with atomic `batch.Commit(pebble.Sync)`.
- encrypted VFS and registry bootstrap before opening Pebble.
- uniform Zstd level 3 compression by default.
- current-state iteration.
- rebuild into LumoSQL.

Tests:

- local INSERT.
- UPDATE.
- DELETE.
- resurrection.
- multi-column update.
- same cell changed twice in transaction.
- restart/rebuild.
- million-row rebuild benchmark.

Exit criterion:

```text
delete LumoSQL completely -> reopen -> identical SQL-visible database
```

---

## 69. Phase 2 — SQL Change Capture and Transaction Manager

Implement:

- explicit package transaction API.
- pre-update capture.
- transaction coalescing.
- SQL commit -> Pebble commit sequence.
- dirty materializer recovery.
- TxID idempotency.
- prepared statement cache.
- write serialization.

Failure injection at every commit boundary.

Exit criterion:

No injected failure may result in Pebble state that cannot rebuild into the expected authoritative database.

---

## 70. Phase 3 — CRDT Merge

Implement:

- Version comparison.
- remote HLC observation.
- per-cell LWW.
- row tombstone LWW.
- deterministic tie-break.
- winner-only materialization.

Property tests:

```text
same mutation set
different arrival orders
=> same final state
```

Run thousands/millions of randomized cases.

---

## 71. Phase 4 — SWIM Membership and Bounded QUIC Replication

Implement:

- mTLS.
- peer identity.
- handshake.
- protocol version.
- schema checks.
- watermarks.
- per-origin log fetch.
- sender.
- receiver.
- durable ack.
- reconnect.
- retries.
- duplicate detection.
- forwarding mutations from other origins.
- memberlist SWIM discovery, metadata, event reconciliation, and bootstrap retry.
- custom memberlist `NodeAwareTransport` over QUIC DATAGRAMs and streams.
- shared authenticated QUIC endpoint, stream dispatch, and bounded connection/handshake pool.
- bounded peer selection, rotation, membership-priority admission, and jittered anti-entropy.
- bootstrap-compatible `Peers`/`AddPeer`, persistent local `RemovePeer` exclusions, and budgeted `ForceSync`.
- membership and replication diagnostics.
- cross-peer missing-range synchronization, retained-history discovery, and durable transaction chunk staging.
- optional Plumtree control/payload routing with bounded caches and protected eager neighbors.
- byte/entry/token-bucket budgets, retryable overload, fair scheduling, and adaptive remote apply groups.
- canonical schema ancestry/merge exchange, equal-epoch hash conflicts, and strict refusal without mutation acknowledgement.

Initial topology:

```text
A <-> B
```

then:

```text
A <-> B <-> C
```

Exit criterion:

All nodes discover each other from partial seeds and converge after arbitrary offline periods while retained logs remain available. Multi-hop/rotation, cross-peer gaps, interrupted transaction chunks, both dissemination modes, and schema conflict/merge tests pass. Default fanout/session/connection caps hold in [Section 57](testing.md#57-network-partition-tests)'s scaling simulations; byte/rate/staging budgets hold under overload. SWIM remains responsive under bulk transfer, and shutdown leaks no goroutines.

---

## 72. Phase 5 — Snapshot and Log GC

Implement:

- logical current-state snapshot.
- snapshot manifest.
- chunk checksums.
- new-node bootstrap.
- stale-node full resync.
- peer acknowledgement tracking.
- active/retired peer state.
- persistent admission/acknowledgement deadlines independent of SWIM liveness and selected peers.
- GC low-watermark.
- minimum retention.
- snapshot-required response.
- consistent snapshot state/watermark cut, encrypted candidate staging, and merge of acknowledged offline writes.
- crash-safe storage-generation publication and resumable transfer validation.
- fresh-identity backup restore/clone and explicit new-DBID coordinated reseed.

Exit criterion:

A node whose required logs have been deleted can rejoin by merging a snapshot without blindly discarding its acknowledged offline state. Publication recovers cleanly at every crash boundary. Restore cannot reuse old writer sequence identities; reseeded clusters reject the previous DBID. Unselected members retain GC obligations until durable progress, expiry, or explicit retirement; restart and rediscovery preserve this policy.

---

## 73. Phase 6 — Encryption and Rotation

The minimum encrypted VFS and registry are prerequisites for opening a production Pebble store in earlier phases. This phase completes and hardens:

- All seven cipher adapters with published-vector validation.
- Versioned chunk containers, authenticated indexes, bounded buffers, and full VFS semantics.
- Direct/provider key resolution, key lookup, caching, and registry bootstrap.
- Lazy periodic and explicit data-key rotation, cipher switching, and reference inventory.
- Atomic application-key rewrap and resumable maintenance file rewriting.
- Checkpoint/backup key pinning, key retirement, encryption status, and redacted logging.
- Crash recovery around file, registry, and directory synchronization.

Exit criterion:

Repeated writes/replication remain correct across data-key rotation and cipher changes. Rewrap and maintenance rewrite recover after injected crashes at every publication stage. No acknowledged durable mutation is lost and no committed authentication failure is hidden.

---

## 74. Phase 7 — Search and Performance

Implement/tune:

- indexes.
- FTS.
- prepared statement cache.
- Pebble cache/memtable budgets and VFS index/buffer memory.
- replication batch sizes.
- optional zstd wire compression.
- bulk rebuild and encrypted SSTable-ingestion optimization.
- large transaction handling.

Establish performance baselines before changing data formats.

---

## 75. Phase 8 — Hardening

Add:

- fuzzing.
- race tests.
- corruption tests.
- malformed peer tests.
- oversized payload rejection.
- disk-full behavior.
- certificate expiry behavior.
- schema mismatch behavior.
- long-running soak tests.
- upgrade compatibility tests.
- package documentation/examples.

Version all persistent and network formats before first stable release.

---

## 76. First Prototype File List

A practical first coding order:

```text
go.mod

config.go
db.go

crdt/hlc.go
crdt/version.go
crdt/merge.go

codec/value.go
codec/mutation.go

state/keys.go
state/store.go
state/merge.go
state/log.go

lumosql/driver.go
sqlengine/engine.go
sqlengine/preupdate.go
sqlengine/delta.go
sqlengine/rebuild.go

transaction.go

replication/protocol.go
replication/membership.go
replication/scheduler.go
transport/quic.go
transport/memberlist.go
transport/pool.go
```

Do not begin with a large abstraction framework.

Build the smallest complete path:

```text
SQL write
 -> capture
 -> Pebble
 -> close
 -> reopen
 -> rebuild
 -> query
```

Then add a second node.

---

## 77. Minimum Viable Prototype

The MVP should support only:

```text
one database
fixed schema
UUID PK
INTEGER/REAL/TEXT/BLOB/NULL
INSERT
UPDATE
DELETE
transactions
per-cell LWW
row delete
encrypted VFS authenticated encryption
two-node QUIC replication
SWIM discovery from partial seeds over QUIC
bounded fanout, peer rotation, and periodic anti-entropy
startup rebuild
```

Explicitly defer:

```text
dynamic DDL
secondary unique conflict handling
distributed foreign-key semantics
huge BLOB chunk replication
complex membership consensus
HTTP APIs
admin UI
vector search
multi-database-per-instance
```

This keeps the core convergence model testable.

---

## 87. Recommended First Engineering Decision

The first code should answer one question:

> Can one embedded Go process run LumoSQL in memory, capture every base-table INSERT/UPDATE/DELETE through `sqlite3_preupdate_hook`, convert those callbacks into a transaction delta, commit the winning current state into encrypted Pebble, then delete/recreate LumoSQL and rebuild an identical database?

If the answer is yes, the central architecture is proven.

Only then add QUIC.

That sequence avoids spending time on networking before the most important local durability/materialization boundary is known to work.

---

## 91. Recommended Immediate Next Step

Create a small `prototype/` package with exactly this path:

```text
Go
 |
 v
LumoSQL :memory:
 |
 | sqlite3_preupdate_hook
 v
TxDelta
 |
 v
Pebble encrypted state
 |
 v
Close everything
 |
 v
Open Pebble
 |
 v
Rebuild LumoSQL
 |
 v
SELECT and compare
```

Do not add QUIC until this prototype passes restart and crash tests.

Once this works, add:

```text
Node A <---- QUIC ----> Node B
```

using the exact same `MutationBatch` and Pebble merge APIs. The networking layer should not need to know SQL.
