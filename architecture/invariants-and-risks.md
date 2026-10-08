# Invariants, risks, and security

Design risks, non-negotiable architecture invariants, and security review requirements.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [78. Key Risks](#78-key-risks)
- [79. Architecture Invariants](#79-architecture-invariants)
- [85. Security Review Checklist](#85-security-review-checklist)

---

## 78. Key Risks

### Risk 1: Durable commit and RIME publication ordering

Mitigation:

- Commit accepted mutations to Spool before acknowledging synchronous writes.
- Publish candidates in commit order and rebuild RIME from authoritative state after an uncertain publication.
- Resolve ambiguous outcomes by transaction receipt and exercise process-kill boundaries.

### Risk 2: Duplicate in-memory state

Mitigation:

- Budget the Spool state index and RIME records, indexes, and MVCC history separately.
- Measure representative rich records, snapshots, and pinned readers.
- Bound transient queues, snapshot staging, and transaction reassembly.

### Risk 3: Optimistic conflicts and group commit

Mitigation:

- Keep same-row conflicts retryable before durability.
- Preserve Spool sequence and RIME publication order across groups.
- Test contended LWW and CRDT operations, shared-fsync failures, close, cancellation, and restart.

### Risk 4: Replicated constraints

Mitigation:

- Restrict guarantees that cannot be preserved under offline writes.
- Add distributed uniqueness or referential semantics only with explicit conflict rules.

### Risk 5: Unbounded replication history

Mitigation:

- Use persisted watermarks, peer retirement, retention windows, and snapshot resync.
- Do not let SWIM suspicion release durable retention obligations.

### Risk 6: Large startup rebuild

Mitigation:

- Scan current Spool state once into a private RIME generation.
- Measure rebuild time and retained heap across representative cardinalities and rich values.
- Publish only after the private materializer is complete.

### Risk 7: Encrypted storage and key rotation

Mitigation:

- Test authenticated registry rewrap and directory sync independently.
- Test resumable rewrites, links/checkpoints, and key-reference accounting.
- Retain historical keys until verified reference inventories allow retirement.

### Risk 8: Membership traffic starvation or accidental full mesh

Mitigation:

- Share QUIC resources without creating permanent workers/connections for every discovered member.
- Reserve membership capacity, bound inbound handshakes and queues, and test under bulk snapshot load.
- Rotate peers and run anti-entropy so a bounded subset does not become a permanent isolated overlay.
- Keep durable acknowledgement/retention policy independent of SWIM failure detection.

---

## 79. Architecture Invariants

Treat these as non-negotiable.

### Invariant A

Spool current state can recreate the complete replicated typed-record state in RIME.

### Invariant B

RIME contains no unique authoritative user data; Spool is authoritative.

### Invariant C

Every durable local transaction has one stable TxID and one origin sequence.

### Invariant D

Conflict resolution does not depend on message arrival order.

### Invariant E

Replication acknowledgements are sent only after Spool durability.

### Invariant F

Remote materialization never generates a new local replication event.

### Invariant G

Replication history can be garbage-collected without deleting current state.

### Invariant H

New nodes can join without replaying the database's complete lifetime history.

### Invariant I

At-rest encryption keys are never transmitted as replication credentials.

### Invariant J

Schema incompatibility fails closed rather than silently corrupting state.

### Invariant K

Membership and replication use authenticated QUIC; discovery never requires a permanent full mesh. All connections and replication sessions obey their configured budgets.

### Invariant L

SWIM reachability is not a durable acknowledgement. Changing the selected peer subset or suspecting a member cannot prematurely release its GC obligation.

### Invariant M

Snapshot recovery merges acknowledged local state and tombstones by version; a snapshot never silently erases offline writes or publishes a partial storage generation.

### Invariant N

Restoring an old backup cannot reuse the original writer's mutation sequence identities. Ordinary restart, fresh-identity clone, and new-DBID reseed are distinct operations.

### Invariant O

Only complete, validated transactions with resolved contiguous progress are acknowledged as applied. Transport chunks, missing-range heads, and broadcast announcements do not imply durability of transaction effects.

### Invariant P

Compatible schema merges are deterministic and idempotent; conflicts fail closed without advancing affected mutation watermarks. Overload may discard transient work but never acknowledged authoritative data.

### Invariant Q

An enabled High/Low bridge exports Low application changes to High only. Signed,
recipient-encrypted artifacts and schema validation are required even when the
transport is trusted. Import staging is not applied stream progress.

### Invariant R

High ownership and provenance are durable, replicated policy state. Later Low
updates cannot bypass High-owned fields through timestamp ordering, duplicate
import, replay, snapshots, or arrival order across High peers.

### Invariant S

File metadata convergence is distinct from verified local payload availability.
Incomplete or unverified objects are never returned as complete file content;
object collection respects reader, backup, and pending-export references.

---

## 85. Security Review Checklist

Before stable release:

- mTLS peer authentication.
- shared membership/replication DBID admission and DATAGRAM capability checks.
- bounded handshakes, streams, datagrams, queues, and connection/session admission.
- peer allow-list.
- certificate-to-NodeID binding.
- replay/idempotency checks.
- frame-size bounds.
- value-size bounds.
- snapshot-size sanity.
- no unsafe decoder allocations.
- no keys in logs.
- no secrets in panic output where avoidable.
- wrong storage key fails closed.
- schema mismatch fails closed.
- Spool directory permissions.
- certificate/key file permissions.
- fuzz replication decoder.
- fuzz snapshot decoder.
- test malicious sequence gaps.
- test mutation claiming another origin.
- verify forwarding preserves original authenticated origin metadata rules.
- when enabled, enforce IP/CIDR policy alongside identity on inbound/outbound and migrated QUIC paths.
- validate High/Low signatures, recipients, source streams, schema holds, replay identities, and bounded artifact/decompression handling.
- verify High ownership protection and file-object integrity across imports, snapshots, and restore.
- keep optional service administration/unlock authorization separate from mesh permissions.

Forwarded transactions must authenticate their immutable identity and mutation digest with the origin's Ed25519 key. mTLS authenticates the forwarding peer independently. Unsigned input is rejected before applied progress. Merged-state snapshots require separately configured trusted sources; signatures do not provide Byzantine consensus or schema authorization. See [origin signatures](origin-signatures.md).

---
