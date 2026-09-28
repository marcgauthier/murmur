# Testing and acceptance

Crash/convergence/network/encryption tests and alpha acceptance criteria.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [55. Crash-Recovery Tests](#55-crash-recovery-tests)
- [56. Convergence Property Testing](#56-convergence-property-testing)
- [57. Network Partition Tests](#57-network-partition-tests)
- [58. Encryption and Storage Tests](#58-encryption-and-storage-tests)
- [89. Initial Acceptance Criteria](#89-initial-acceptance-criteria)
- [Additional capability acceptance](#additional-capability-acceptance)

---

## 55. Crash-Recovery Tests

Inject crashes/failures after every important boundary.

Local write:

```text
after SQL BEGIN
after SQL statement
after capture
before SQL COMMIT
after SQL COMMIT
before Pebble commit
during Pebble commit
after Pebble commit
before ACK to application
```

Remote apply:

```text
after network receive
after merge calculation
during Pebble synchronized batch
after Pebble commit
before ACK
after ACK while SQLite rows are queued
during bulk SQLite apply
after bulk SQLite commit but before materialized-generation publication
```

Restart must always converge to Pebble authoritative state.

Also inject crashes during chunk staging/completion, grouped apply, snapshot candidate merge/validation, intent sync, watermark/generation publication (atomic batch, or chunked merge with durable resume plus final publication batch), registry pinning, and post-publication SQL rebuild. Staging checks cover restart after a partial transfer, a pre-commit interruption with clean retry, and an over-budget transfer eviction; none may advance the applied watermark before complete apply. Before snapshot publication the old generation remains authoritative; afterward recovery completes from the published candidate. Never expose partial transaction effects or mixed-generation watermarks. Inject failures during schema manifest publication and backup restore identity rewriting; the persisted restore marker must prevent same-identity writable rollback on restart.

Single remote apply writes a synced complete-batch prepare record before final apply. The crash test interrupts after prepare and reopens the store; recovery must atomically install winners, log, receipt, receive watermark, HLC, and state generation, remove the prepare record, and make a repeated delivery idempotent. SQL materialization generation is reconciled from authoritative state during open/rebuild.

Remote materialization acceptance checks keep query rows out of SQLite until the
one-second timer or 1,000 received-transaction threshold, confirm coalesced
updates/delete/resurrection, and verify a local SQL write flushes pending rows.
An open SQLite reader must not block durable remote Pebble receipt. Acknowledged
Pebble progress may lead `MaterializedGeneration`; restart rebuilds from Pebble.

---

## 56. Convergence Property Testing

Create a deterministic in-process simulator.

Generate random operations:

```text
insert
update one column
update multiple columns
delete
resurrect
offline node
network partition
reconnect
duplicate batch
delayed batch
out-of-order origin delivery
node restart
interrupted transaction chunks and cross-peer chunk duplicates
snapshot merge with unpropagated offline writes
compatible concurrent schema branches and repeated merges
```

Apply the same logical mutation set in different delivery orders.

The CRDT package also checks comparison reflexivity, antisymmetry, and
transitivity, folds a competing set of cell versions through several delivery
orders, and exercises equal-version and equal-HLC tombstone visibility
boundaries. These focused checks protect the merge ordering primitives used by
the larger replication convergence simulator.

SQL engine read tests verify column metadata, synchronous single-row reads,
idempotent row close, and that closing a streaming result releases the read
lock so a waiting writer can proceed.

Focused `state` tests cover origin-log scan pagination, making progress when a
single batch exceeds the requested byte budget, and preserving the scan cursor
when a callback fails. Replication protocol tests cover watermark-list round
trips and reject missing counts, truncated entries, and absurd entry counts.
Run them with `go test ./state ./replication`.

The live integration port at `tests-live/crdt-contention/` runs three encrypted
QUIC nodes and verifies that concurrent updates to disjoint columns all
survive replication. Run it with `go test -count=1 ./tests-live/crdt-contention`.
It covers the disjoint-column invariant from GALVANIZE's CRDT contention
scenario; shared-cell contention and delete pruning remain unported.

The managed-view integration port at `tests-live/views/` checks view results on
three replicas and after one replica reopens and rebuilds. Run it with
`go test -count=1 ./tests-live/views`.

The large-payload port at `tests-live/large-payload/` sends a 1.5 MiB value in
the same transaction as 250 rows across two encrypted QUIC nodes, then checks
the exact value digest and row count. Run it with
`go test -count=1 ./tests-live/large-payload`. It covers the direct mesh data
path; High/Low bundle transport remains unimplemented.

The allow-nodes port at `tests-live/allow-nodes/` uses valid NodeID-bound
certificates while restricting one peer to a loopback `/32`; it verifies the
allowed peer replicates and the disallowed address does not connect. Run it
with `go test -count=1 ./tests-live/allow-nodes`.

The rekey port at `tests-live/rekey/` rotates an application's wrapping key,
reopens with the new material, checks prior rows and a post-rotation write, and
rejects the old key. Run it with `go test -count=1 ./tests-live/rekey`.

The encryption port at `tests-live/encryption/` writes a unique plaintext
marker, verifies it is absent from durable storage, rejects a wrong key, and
reopens with the correct key to read the value. Run it with
`go test -count=1 ./tests-live/encryption`.

The `tests-live/three-node-sync/` smoke port concurrently commits on three
encrypted QUIC nodes, waits for 60 replicated rows, and checks equal ordered
SHA-256 digests. Run it with `go test -count=1 ./tests-live/three-node-sync`.
It covers convergence mechanics from GALVANIZE's sustained-sync benchmark,
not its throughput or five-minute duration targets.

The `tests-live/crash-recovery/` scenario runs three daemon processes with
concurrent application writes, kills two nodes with `SIGKILL` while their SQL
requests are in flight, restarts their existing encrypted stores, and verifies
acknowledged rows and ordered digests converge. It then commits a new write.
Each write window defaults to 12 seconds and can be adjusted with
`SPEDSQL_CRASH_WRITE_SECONDS`; `SPEDSQL_CRASH_SETTLE_SECONDS` controls the final
convergence deadline. Run it with
`go test -count=1 ./tests-live/crash-recovery`.

The `tests-live/crdt-contention/` scenario performs concurrent disjoint-column
updates followed by shared-cell contention from three daemon processes. It
requires the independent values to survive and all processes to expose the same
LWW cell winner and ordered logical-state digest. Run it with
`go test -count=1 ./tests-live/crdt-contention`.

The `tests-live/long-running-five-node/` scenario keeps five encrypted daemon
processes writing over a full mesh, restarts two node directories during load,
and verifies retained rows, common logical-state digests, materialized
generations, and persistent-store capacity. Its smoke run is 30 seconds; use
`SPEDSQL_FIVE_NODE_DURATION_SECONDS=3600
SPEDSQL_FIVE_NODE_SETTLE_SECONDS=300 go test -count=1 -timeout=75m
./tests-live/long-running-five-node` for the one-hour acceptance profile.

The four-node partition smoke test at `tests-live/partition/` splits a full
mesh into two pairs, verifies writes stay isolated, heals the mesh, compares
ordered state digests, and checks a later write replicates. Run it with
`go test -count=1 ./tests-live/partition`. It does not cover SWIM discovery,
multi-hop forwarding, churn, or scaling limits.

The local encrypted object-store tests stream multi-chunk content through
`objectstore.Put` and `objectstore.Read`, check digest/idempotency and absence
of plaintext at rest, and reject wrong keys, tampering, truncation, and
cancellation. They also verify inventory, reference/grace collection, explicit
reader pins, and collection racing with an active streaming read. Run them
with `go test ./objectstore`.

The `tests-live/chaos-load/` scenario keeps all three daemon processes writing
while node3 is partitioned from node1/node2, checks group row counts and
divergent digests, then re-admits peers while writes continue and verifies
convergence plus a post-heal write. The partition and healing windows default
to 12 seconds each and can be configured with
`SPEDSQL_CHAOS_PARTITION_SECONDS` and `SPEDSQL_CHAOS_HEAL_SECONDS`. Run it with
`go test -count=1 ./tests-live/chaos-load`. Abrupt process death is covered by
the separate crash-recovery test.

The `tests-live/files-soak/` scenario uploads files repeatedly over an
encrypted two-node QUIC mesh, checks metadata replication and search, fetches
each payload from its peer, validates SHA-256, and reports upload/fetch p95
latencies against broad smoke SLO bounds. Run the short form with
`go test -count=1 ./tests-live/files-soak`; set
`SPEDSQL_FILES_SOAK_DURATION_SECONDS=600` and
`SPEDSQL_FILES_SOAK_INTERVAL_SECONDS=60` with `-timeout=12m` for the
ten-minute acceptance run.

The `tests-live/files-bridge/` scenario covers two Low peers, a recipient-sealed
Low-to-High file import, and two High peers. High-2 learns metadata through its
mesh and fetches the verified object from High-1. Run it with
`go test -count=1 ./tests-live/files-bridge`.

The `tests-live/soak-slo/` scenario writes through three encrypted daemon
processes and their HTTP SQL endpoints, then gates logical digest convergence,
writer p95/max latency, service readiness, materialized generation, queue
depth, and disk size. The five-second smoke command is
`go test -count=1 ./tests-live/soak-slo`; the two-hour acceptance run is
`SPEDSQL_SLO_DURATION_SECONDS=7200 SPEDSQL_SLO_SETTLE_SECONDS=300 go test
-count=1 -timeout=3h ./tests-live/soak-slo`.

Recorded acceptance on 2026-09-27: the ten-minute file run completed 10
rounds and verified 655,360 fetched bytes (upload p95 8.51 ms; metadata,
fetch, and verification p95 35.13 ms). A 15-second three-node write run
committed 4,199 rows (1,410/1,361/1,428 per node), converged to digest
`6ed3f99c5abb450a6ffcc6e7507cd66c21d008d03aa9e8897c23a245ebe5ce68`, and
measured local-write p95 17.52 ms and maximum 322.72 ms. Queue, readiness,
materialized-generation, and disk-size gates passed.

The `tests-live/highlow/` scenarios move logical writes from a real Low
database to real High databases across a directory drop: disconnected
transfer with one-way role gating, gap delivery with duplicate/reordered
recovery, two-receiver convergence, forgery/corruption/misnaming rejection
without partial apply, outbox/inbox restart resumption, schema-hold
survival across restart with release after a local High migration, and
signer rotation plus outage catch-up, and High-owned-field reorder
convergence across two meshed High peers with opposite import/override
orders (plus release convergence). Run them with
`go test -count=1 ./tests-live/highlow`. Hostile-HLC policy/value
interleavings are pinned at the DB layer by
`TestBridgeShadowReorderConvergence`, single-peer override/release and
Low-delete protection by `bridge/ownership_test.go`, and file transfer by
`tests-live/files-bridge/`; staging-byte budgets remain unasserted.

The `tests-live/addrpolicy/` scenario launches three standalone daemon
processes in isolated node directories. A loopback CIDR admits node1's path to
unrestricted node3, while a TEST-NET policy prevents node2 from forming any
loopback session. Service API writes verify permitted replication and that
node2's write remains local. Run it with
`go test -count=1 ./tests-live/addrpolicy`.

The `tests-live/allow-nodes/` scenario launches three daemon processes with
valid certificates from one CA. Node2 allows only node1, while node1 and node3
accept all peers. Concurrent writes run for three minutes by default; the test
keeps node2's direct peer count at one, then checks row-count and ordered digest
convergence through node1. Set `SPEDSQL_ALLOW_NODES_WRITE_SECONDS` to adjust
the workload duration. Run it with `go test -count=1 ./tests-live/allow-nodes`.

The `tests-live/subscribe/` scenario holds a live subscription across a
two-node split: the partitioned write does not leak, and healing
delivers the backlog with no reset. Run it with
`go test -count=1 ./tests-live/subscribe`.

The `tests-live/loadshare/` scenario saturates one node with 240 local
writes while its peer replicates 20 rows, then requires full
convergence, acquisitions in both scheduler classes, service debt
within the 1s bound, and reports the local-commit latency distribution.
Run it with `go test -count=1 ./tests-live/loadshare`.

Invariant:

```text
all nodes eventually have identical current state
```

for every:

```text
table + row + column + tombstone
```

Then rebuild each SQL materialization and compare query-visible rows.

This test is more important than basic happy-path integration tests.

---

## 57. Network Partition Tests

Minimum integration topology:

```text
A <-> B <-> C
```

Test:

```text
A cannot reach C directly.
A writes.
B forwards A-origin mutation.
C stores it under origin A.
A later connects directly to C.
watermarks prevent duplicate logical changes.
```

Then:

```text
partition all three
write conflicting cell values
reconnect in different orders
verify deterministic convergence
```

Also test full-mesh reachability and hub/spoke allow-list topologies while the replication scheduler retains bounded fanout. Full reachability does not authorize a permanent connection to every member.

### SWIM and QUIC integration tests

- Start nodes from overlapping partial seed lists and verify discovery without enumerating every peer. Reject an existing-cluster join without the shared DBID; unreachable seeds permit local operation and eventual retry.
- Exercise direct and indirect probes, reliable fallback, suspicion/refutation, packet loss, membership repair, process restart with stable identity, advertised-address changes, graceful leave, and partition recovery.
- Verify that probes/gossip use QUIC DATAGRAMs and membership exchanges use QUIC streams on the shared endpoint. Check `net.Conn` deadlines, source addresses, queue saturation, datagram negotiation/size failures, and shutdown without leaked goroutines.
- Reject wrong CA, unauthorized NodeID, certificate/claimed-identity mismatch, wrong DBID, incompatible protocol, malformed metadata/envelopes, and oversized messages. Accept legitimate relayed records only as hints until direct authentication.
- Force multi-hop forwarding and peer rotation with conflicting offline writes, duplicate delivery, gaps, and concurrent snapshots. Verify deterministic convergence without permanent all-peer links and preserve durable acknowledgement ordering.
- Verify `ForceSync`, inbound sessions, concurrent joins, and rejected/queued repairs obey the same budgets. Sustain bulk snapshot traffic while membership and acknowledgements continue making progress.
- Verify an unselected member with missing acknowledgements gates GC, SWIM failure does not release that obligation, unchanged acknowledgements do not extend its deadline, restart preserves deadlines, expiry requires snapshot recovery when logs are gone, and explicit local retirement survives rediscovery/restart.

### Scaling acceptance tests

Use an instrumented transport and deterministic scheduler simulation for clusters of 10, 100, and 1,000 members, plus real QUIC integration coverage on smaller clusters. Under defaults, assert selected outbound replication targets never exceed three, total replication sessions never exceed eight, and total admitted connections plus handshakes never exceed 32. Include inbound bursts and transient repair work in peak accounting.

Measure convergence latency, bytes sent, dial/handshake churn, evictions, queue pressure, and probe failures across cluster sizes. Verify eventual convergence under finite loss/partitions and repeated successful peer selection. Distinguish fixed fanout and connection bounds from O(N) membership/watermark metadata and membership-size-dependent gossip retransmission traffic.

Implemented acceptance coverage: `replication.TestPeerScalingCapsAcrossChurn` simulates 10, 100, and 1,000 discovered members, checks selected fanout and configured pool bounds, and removes a quarter of the membership at each scale to verify selection refills. `replication.TestPlumtreeNegotiatedThreeNodeForwarding` exercises forwarding beyond the local selected target set. `transport.TestMemberlistQUICRemainsResponsiveWithReplicationSlotsSaturated` fills all eight replication slots, then establishes an authenticated QUIC membership session and delivers a SWIM datagram through the reserved connection capacity. These are bounded simulations and small-cluster transport checks; long-duration impaired-network soak measurements remain separate acceptance work.

### Recovery, dissemination, and overload acceptance

- Supply the same origin out of order from several peers; retain missing ranges across restart, switch repair sources, and never acknowledge observed/staged heads as applied progress. Exercise unavailable retained history and snapshot fallback.
- Transfer a transaction larger than a frame using chunks; interrupt/restart, duplicate chunks across peers, inject conflicting digests/indexes, and verify durable contiguous acknowledgement followed by atomic SQL visibility at the materialization flush. Reject a local transaction above `MaxTransactionBytes` before success and enforce decompression/reassembly limits.
- Recover an offline node whose acknowledged insert/update/delete was never propagated. A peer snapshot must preserve its winning cells/tombstones and valid local sequence, while legitimately newer competing versions may win. Test source writes after the snapshot cut, candidate publication crashes, cancellation, and schema incompatibility.
- Restore an older backup under its original identity and verify writable startup is rejected. Restore with a fresh identity/certificate and repair with the existing DBID; reseed a cluster with a new DBID and verify old nodes are isolated. Verify historical origin identities are preserved and source acknowledgement/retirement state is not inherited.
- Merge independent compatible schema additions with equal and unequal epochs in different orders; repeat exchanges without epoch churn. Reject same-column type/default/nullability conflicts and identity collisions, preserve local additions, and leave mutation watermarks unchanged under strict policy or conflict.
- The standalone `plumtree` state machine tests bounded eager/lazy selection, forwarding, duplicate pruning, GRAFT cache replies, conflicting identities, and cache/neighbor limits. After replication-manager integration, also test `IHAVE` delay scheduling, protected ring neighbors, payload-log fallback, partition repair, incompatible-mode rejection, and compare payload bytes/duplicates against default gossip without relaxing session/connection caps.
- Saturate byte and entry queues, staging storage, token buckets, and apply/repair workers. Verify bounded accounting of shared buffers, retryable overload, safe coalescing/shedding, eventual anti-entropy repair, and progress for membership/control plus bulk traffic. Grouped apply must keep each original transaction atomic and acknowledge only synchronized commits.

---

## 58. Encryption and Storage Tests

Required:

- Published vectors and round trips for all seven ciphers, including portable and accelerated AEGIS paths.
- Default AES-256-GCM, alias normalization, key-length validation, direct/provider exclusivity, and wrong/missing-key rejection.
- No plaintext fallback or obvious application plaintext in any database content file.
- VFS conformance for sequential/random operations, partial writes, EOF/logical sizes, concurrency, preallocation, reuse, rename, links, checkpoints, and sync semantics.
- Header/index/chunk tampering, cross-file substitution, malformed lengths, and committed-content corruption fail authentication.
- Nonce uniqueness across reuse, failed writes, writable reopen, counter limits, and crashes.
- Fault injection before/after data sync, commit metadata, registry rename/directory sync, and maintenance file replacement; acknowledged writes survive every restart.
- Reopening large SSTables uses authenticated indexes without reading/decrypting the entire file.
- Lazy rotation with mixed old/new keys and all cipher combinations preserves reads, writes, rebuild, replication, and restart.
- Explicit rewrite resumes after interruption and does not mutate linked checkpoints.
- Rewrap leaves file ciphertext unchanged; new key opens and old key fails after completion; historical backup registry/key remains usable.
- Key retirement waits for live files, handles, checkpoints, and registered backups.
- Snapshot/replication never transmits at-rest keys; config, metrics, and logs redact key bytes.
- Existing Badger directories return `ErrUnsupportedStorageFormat` without modification.
- Default compression is enabled Zstd level 3 on every LSM level; none/snappy overrides work and unsupported levels fail validation.
- Compression is applied before encryption; compare compressible and incompressible data.
- Serialized state merges, atomic synchronized batches, and iterator/snapshot/value-closer lifecycle pass regressions and race checks.

---

## 89. Initial Acceptance Criteria

A first serious alpha should not be called successful until all of the following work:

- Embedded Go API with no standalone service.
- LumoSQL opens in memory.
- Base table writes captured through pre-update hook.
- Multi-statement transactions coalesce correctly.
- Pebble is authoritative.
- Pebble uses encrypted VFS with AES-256-GCM by default.
- encryption-manager data-key rotation configured/tested.
- Startup rebuild recreates identical query-visible state.
- Per-column LWW convergence tested.
- Delete/resurrection semantics tested.
- Two nodes replicate over quic-go with mTLS.
- Three nodes forward multi-origin changes.
- Partial seed lists discover members through SWIM over authenticated QUIC without native UDP/TCP membership listeners.
- QUIC membership/datagram/stream adapters handle identity, DBID, size limits, deadlines, and graceful shutdown.
- Default fanout three, replication session cap eight, and total QUIC connection/handshake cap 32 hold at 10/100/1,000 simulated members.
- Peer rotation and anti-entropy converge after missed forwarding, membership churn, and partitions.
- Offline conflicting writes converge.
- Replication logs garbage-collect safely.
- Unselected members retain GC obligations; SWIM failure cannot release them, and acknowledgement deadlines/retirements survive restart.
- Stale/new node can snapshot-resync.
- Snapshot source cuts are consistent; merged publication preserves winning offline writes/tombstones and recovers from crashes without partial state.
- `TestSnapshotManifestCellsAndTombstonesShareOneReadCut` commits a cell, tombstone, schema update, and generation change during export, then checks the manifest and all streamed cells remain from the original Pebble cut. `TestSnapshotExportContextStopsAndReleasesSourceCut` checks cancellation stops export and permits later durable writes.
- Snapshot candidate tests reject over-budget/invalid manifests, manifest changes between chunks, and conflicting duplicate chunks without publishing cells or advancing watermarks. Existing import coverage also reopens after staging to verify resume, and rejects a corrupted final digest.
- Transaction-chunk codec tests cover reverse-order assembly, frame and transaction limits, malformed metadata, missing/duplicate chunks, and canonical digest verification. Durable encrypted staging and cross-peer atomic apply still require integration tests.
- Fresh-identity restore and new-DBID reseed prevent mutation identity reuse and old-cluster contamination.
- Cross-peer missing ranges and resumable transaction chunks preserve atomic visibility and contiguous durable acknowledgements.
- Compatible concurrent schema merges converge without repeated epoch bumps; incompatible definitions and strict refusal leave affected mutation watermarks unchanged.
- Optional Plumtree reduces duplicate payload traffic under measured workloads while retaining bounded eager/lazy targets and repair fallbacks.
- Queue byte/entry, bandwidth, cache, staging, and repair limits hold under overload; adaptive grouped apply preserves durability and control/bulk progress.
- The standalone `overload` package tests global/per-peer byte and entry admission, idempotent release, bounded peer state, token-bucket bursts, context cancellation, and peer-slot reuse. Replication queue/staging/cache integration and retryable protocol responses still need acceptance coverage.
- Key rotation procedure survives restart/failure testing.
- FTS/local indexes rebuild without replication.
- No application success acknowledgement before Pebble durable commit.
- `go test -race` passes.
- Randomized convergence test passes repeatedly.
- Crash-injection suite passes.

---

---

## Additional capability acceptance

The existing core crash/fuzz/convergence suite does not cover High/Low domains,
file objects, writer priority, address filtering, or resumable query subscriptions.
Keep these extensions outside MVP completion claims until their subsystem criteria
and integration scenarios pass.

- Run High/Low fault and schema-hold scenarios from
  [the bridge acceptance criteria](high-low-replication.md#diagnostics-and-acceptance),
  including multiple High receivers and reordered ownership/value delivery.
- Run a two-Low/two-High encrypted-file scenario from
  [file acceptance](file-replication.md#retention-backup-and-acceptance), checking
  metadata convergence and payload digest/availability independently.
- Measure local write latency and writer-time share under sustained replication,
  then verify eventual convergence after draining the workload. Test idle borrowing,
  canceled admission, maintenance, and shutdown as specified in
  [writer scheduling](synchronization-and-overload.md#local-and-replication-write-scheduling).
- Exercise [address policy](membership-and-transport.md#ip-and-cidr-admission-policy)
  and [subscription continuity/reset](query-and-search.md#reactive-query-subscriptions)
  without weakening identity authorization or blocking durable writes.
- Interrupt a multi-chunk snapshot after its first chunk, restart the receiver,
  and resume the same transfer. Verify staged cells and watermarks stay hidden,
  reject a corrupt digest without publication, then confirm atomic publication
  and tail repair. State tests cover restart staging and digest rejection;
  end-to-end tail repair remains a live acceptance scenario.
- Commit a source write from inside a snapshot chunk callback and verify the
  manifest's watermarks and exported cells still describe the original Pebble
  read cut; deliver the later write through normal tail replication.

Use isolated directories, node identities, and ports for live scenarios. Check
canonical logical state digests and file content digests rather than encrypted
container bytes. Retain failed scenario artifacts with secrets redacted. Document
which scenarios ran; available test files are not evidence of a passing run.
