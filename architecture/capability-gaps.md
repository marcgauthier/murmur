# Capability gaps and implementation status

Implementation inventory with remaining acceptance work, reconciled with the
current RIME worktree on 2026-10-08. Implementation labels do not claim that
every release gate has passed.

The [TODO list](../TODO.md) is the live list of remaining work.
This document provides comparison context and design priorities;
keep its status summaries consistent when checklist items are completed or changed.

[Architecture index](README.md) · [Project README](../README.md)

## Current foundation

Implemented foundations include encrypted authoritative Spool state, managed
RIME records and indexes, schema-level LWW/PN_COUNTER/OR_SET/MAX/MIN and row
tombstones, offline writes, authenticated QUIC replication, memberlist SWIM
discovery, bounded transport pools, snapshot merging, key rotation, typed
subscriptions and checkpoint backup. Production has no SQLite engine or SQL
application API; managed query state rebuilds from Spool.

Existing fuzz, crash, adversarial-peer, and multi-node convergence tests provide
coverage. Their presence does not establish acceptance for the remaining
storage-fault, delivery-interleaving, long-soak, and performance gates below.

## Implemented requirements and remaining acceptance

| Capability | Current gap | Target document |
| --- | --- | --- |
| Membership and bounded dissemination | Peer selection/rotation, anti-entropy, the shared bounded QUIC pool, Plumtree integration, scaling, and seed-only live discovery are implemented. Seed-only discovery passed in the 2026-10-08 sequential live release gate; long-duration membership/soak qualification remains | [Membership](membership-and-transport.md), [dissemination](replication-and-dissemination.md) |
| Detailed synchronization | Separate streams, the transaction-chunk codec, durable chunk staging, paginated progress, and cross-peer partial-range repair are implemented | [Synchronization](synchronization-and-overload.md) |
| Overload controls | Aggregate/per-peer queue byte/entry budgets, staging caps, token buckets, and retryable overload responses are implemented | [Overload](synchronization-and-overload.md#32-mutation-batching) |
| Safe snapshot publication | Consistent read cuts, digest-validated durable staging, atomic cell/watermark publication, chunked merge with durable resume and bounded source tail-history leases are implemented. Broader process-kill and multi-peer qualification remains | [Snapshot safety](snapshots-backup-and-restore.md) |
| Restore identity safety | Fresh writer identity, rollback rejection, inherited-membership-policy clearing, and crash-safe ciphertext rebinding for new-DBID reseed are implemented | [Backup and restore](snapshots-backup-and-restore.md#38-spool-online-zero-downtime-backup-and-restore) |
| Metrics integration | Status, membership/pool diagnostics, replication/writer counters, and a JSON HTTP renderer exist; dedicated retention-lease metrics and complete SWIM runtime metric coverage remain follow-ups; those subsystems themselves have implementations | [Diagnostics](runtime-and-diagnostics.md#52-metrics-and-diagnostics) |

## Additional GALVANIZE capabilities

These optional extensions are outside the initial embedded replication MVP.
Several have implementations; remaining integration and acceptance requirements
are tracked separately. Their acceptance criteria live with each subsystem.

| Addition | Implementation and remaining capability |
| --- | --- |
| [High/Low replication](high-low-replication.md) | Roles, sealed bundles, encrypted journals, atomic export/import obligations and progress, aggregate capacity, definition-level schema validation, schema holds, and status/replay controls are implemented |
| High ownership and provenance | Implemented source stream/sequence tracking with High-owned field protection against subsequent Low updates (including reordered policy/value reconciliation via same-row shadow cells), protected deletes with explicit resolution, and policy/shadow state in High-domain durability and replication; file ownership release APIs exist |
| [Writer scheduling](synchronization-and-overload.md#local-and-replication-write-scheduling) | Implemented configurable local/replication writer-time shares, bounded maintenance, idle borrowing, and cancellation-aware admission; sustained workload acceptance remains tracked |
| [Encrypted file replication](file-replication.md) | Local authenticated immutable objects plus replicated metadata with streaming upload/read, search/list, delete tombstones, availability status, grace-based collection, bounded mesh fetch from static peers plus SWIM-discovered serving members, recipient-sealed High/Low file artifacts with High-local re-encryption, object key rotation with generations, and object-inclusive versus metadata-only backup/restore exist |
| [Address policy](membership-and-transport.md#ip-and-cidr-admission-policy) | Implemented IP/CIDR filtering alongside certificate/NodeID authorization |
| [Query subscriptions](query-and-search.md#reactive-query-subscriptions) | Implemented bounded subscriptions to committed typed-record changes with explicit resume/reset behavior |
| [Optional service adapter](runtime-and-diagnostics.md#optional-service-adapter) | Removed: no HTTP handler or client SDK ships with the library; the embedded Go API is the interface and any HTTP is the hosting process's own; the internal test-node HTTPS routes remain test tooling |

High/Low identifies security domains and permitted data direction. It is distinct
from local/replication writer priority, and neither implies a network bandwidth
split. Ordinary mesh replication remains multi-writer within a domain.

## Reference boundaries

GALVANIZE's reference implementations are `galv-highlow`, `galv-write-scheduler`,
`galv-files`, and the Corrosion agent integration. Its live scenarios cover forged
or corrupted bundles, replay, missing sequences, schema holds, writer priority,
and file transfers. Port the behavior and acceptance cases, not CR-SQLite-specific
storage or mutation formats: NOMADSQL retains Spool authority and its own HLC/LWW
protocol. Wire compatibility with GALVANIZE is not currently promised.

GALVANIZE's High/Low directory, HTTP(S), and FTP(S) transports have implementations.
Its SFTP entry points currently return configuration errors; listing a transport
in a reference configuration does not establish support. The Murmur-SQL bridge has
directory, HTTP(S), and FTP(S) publication adapters plus recipient-sealed
encrypted file-object transfer with High-local re-encryption (proven by
`tests-live/files-bridge`).

## Delivery order

The implementation order below is complete; remaining release qualification is
tracked in the [migration plan](migration-plan.md#8-implementation-milestones)
and [release inventory](release-status.md#1-verified-feature-matrix).

1. Snapshot generation publication, restore policy cleanup/reseed, bounded
   dissemination, detailed synchronization, and overload are implemented. The
   sequential release gate passed; broader multi-peer source-lease and tail
   catch-up qualification under sustained writes/GC remains.
2. Writer scheduling/address admission, sustained-load acceptance, and
   membership/pool diagnostics are implemented.
3. High/Low atomic delivery, encrypted outbox storage, capacity and schema
   gates, and replicated provenance/ownership are implemented.
4. File transfer is implemented (replicated metadata, bounded mesh fetch,
   sealed High/Low artifacts with High-local re-encryption, object key
   rotation, object-inclusive backup/restore, file service operations);
   dedicated SWIM fetch-discovery and longer-duration file-soak acceptance
   remain. The HTTP service adapter is intentionally outside the embedded
   library API; callers provide explicit `RowID` primary keys.
5. Remaining work: long-duration impaired-network and multi-hour soak
   acceptance, plus the storage-fault and replication-interleaving gates in
   `migration-plan.md`.

Update the [TODO list](../TODO.md) and project status only when the
corresponding implementation and acceptance checks exist.

Column policies and the explicit counter/set API are implemented; see [merge policies](merge-policies.md) for retained-history costs and supported element types.
