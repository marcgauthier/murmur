# Invariants, risks, and security

Design risks, non-negotiable architecture invariants, and security review requirements.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [78. Key Risks](#78-key-risks)
- [79. Architecture Invariants](#79-architecture-invariants)
- [85. Security Review Checklist](#85-security-review-checklist)

---

## 78. Key Risks

### Risk 1: SQLite driver integration

Mitigation:

- Prove it first.
- Pin the mattn driver and use its bundled SQLite build with pre-update
  capture and FTS5 enabled.
- Test the default CGO build and optional modernc build in CI on supported OSes.

### Risk 2: In-memory SQL connection model

Mitigation:

- Validate connection behavior in Phase 0.
- Start with one controlled handle if necessary.

### Risk 3: Cross-engine transaction boundary

Mitigation:

- Pebble authoritative.
- Do not ACK before Pebble.
- Mark/rebuild the SQLite materialization on post-SQL/pre-Pebble failures.
- TxID idempotency.
- Heavy crash injection.

### Risk 4: Unique constraints and foreign keys

Mitigation:

- Restrict v1 schema.
- Add distributed semantics only deliberately.

### Risk 5: Unbounded replication log

Mitigation:

- watermarks
- peer retirement
- retention window
- snapshot resync

### Risk 6: Very large startup rebuild

Mitigation:

- optimized current-state scan
- bounded multi-row inserts with at most 900 bind parameters per statement and
  transaction commits in groups of at most 5,000 rows
- indexes after load
- optional disposable persistent materialization later

### Risk 7: Encrypted VFS and key rotation correctness

Mitigation:

- Test authenticated registry rewrap and directory sync independently.
- Test resumable VFS rewrites, links/checkpoints, and key-reference accounting.
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

Pebble current state can recreate the complete replicated SQL-visible state.

### Invariant B

SQLite contains no unique authoritative user data.

### Invariant C

Every durable local transaction has one stable TxID and one origin sequence.

### Invariant D

Conflict resolution does not depend on message arrival order.

### Invariant E

Replication acknowledgements are sent only after Pebble durability.

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
- Pebble directory permissions.
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

If forwarded origin records are accepted, the batch must be cryptographically attributable or trusted according to the cluster trust model. In an all-trusted-node cluster, mTLS plus protocol validation may be sufficient. If nodes are not mutually trusted, add origin signatures in a future security phase.

---
