# Testing and acceptance

Crash/convergence/network/encryption tests and alpha acceptance criteria.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [55. Crash-Recovery Tests](#55-crash-recovery-tests)
- [56. Convergence Property Testing](#56-convergence-property-testing)
- [57. Network Partition Tests](#57-network-partition-tests)
- [58. Encryption and Storage Tests](#58-encryption-and-storage-tests)
- [89. Initial Acceptance Criteria](#89-initial-acceptance-criteria)

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
before LumoSQL apply
during LumoSQL apply
before ACK
```

Restart must always converge to Pebble authoritative state.

Also inject crashes during chunk staging/completion, grouped apply, snapshot candidate merge/validation, intent sync, active-generation pointer publication, registry pinning, and post-publication SQL rebuild. Before snapshot publication the old generation remains authoritative; afterward recovery completes from the published candidate. Never expose partial transaction effects or mixed-generation watermarks. Inject failures during schema manifest publication and backup restore identity rewriting; the persisted restore marker must prevent same-identity writable rollback on restart.

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

### Recovery, dissemination, and overload acceptance

- Supply the same origin out of order from several peers; retain missing ranges across restart, switch repair sources, and never acknowledge observed/staged heads as applied progress. Exercise unavailable retained history and snapshot fallback.
- Transfer a transaction larger than a frame using chunks; interrupt/restart, duplicate chunks across peers, inject conflicting digests/indexes, and verify atomic SQL visibility and durable contiguous acknowledgement. Reject a local transaction above `MaxTransactionBytes` before success and enforce decompression/reassembly limits.
- Recover an offline node whose acknowledged insert/update/delete was never propagated. A peer snapshot must preserve its winning cells/tombstones and valid local sequence, while legitimately newer competing versions may win. Test source writes after the snapshot cut, candidate publication crashes, cancellation, and schema incompatibility.
- Restore an older backup under its original identity and verify writable startup is rejected. Restore with a fresh identity/certificate and repair with the existing DBID; reseed a cluster with a new DBID and verify old nodes are isolated. Verify historical origin identities are preserved and source acknowledgement/retirement state is not inherited.
- Merge independent compatible schema additions with equal and unequal epochs in different orders; repeat exchanges without epoch churn. Reject same-column type/default/nullability conflicts and identity collisions, preserve local additions, and leave mutation watermarks unchanged under strict policy or conflict.
- In Plumtree mode, test `IHAVE`/`GRAFT`/`PRUNE`, protected ring neighbors, eager/lazy budget saturation, cache expiry, payload-log fallback, partition repair, and incompatible-mode rejection. Compare payload bytes/duplicates against default gossip without relaxing existing session/connection caps.
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
- Fresh-identity restore and new-DBID reseed prevent mutation identity reuse and old-cluster contamination.
- Cross-peer missing ranges and resumable transaction chunks preserve atomic visibility and contiguous durable acknowledgements.
- Compatible concurrent schema merges converge without repeated epoch bumps; incompatible definitions and strict refusal leave affected mutation watermarks unchanged.
- Optional Plumtree reduces duplicate payload traffic under measured workloads while retaining bounded eager/lazy targets and repair fallbacks.
- Queue byte/entry, bandwidth, cache, staging, and repair limits hold under overload; adaptive grouped apply preserves durability and control/bulk progress.
- Key rotation procedure survives restart/failure testing.
- FTS/local indexes rebuild without replication.
- No application success acknowledgement before Pebble durable commit.
- `go test -race` passes.
- Randomized convergence test passes repeatedly.
- Crash-injection suite passes.

---

