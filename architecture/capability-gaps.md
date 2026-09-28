# Capability gaps and implementation status

Source comparison with the sibling GALVANIZE checkout on 2026-09-27. This is an
implementation inventory and a set of target additions, not a claim that these
features are available. Update this inventory as code and acceptance tests change.

The pending checklist in [TASKS_PENDING.md](../TASKS_PENDING.md#pending-tasks) is the live list
of remaining work. This document provides comparison context and design priorities;
keep its status summaries consistent when checklist items are completed or changed.

[Architecture index](README.md) · [Project README](../README.md)

## Current foundation

Implemented foundations include encrypted authoritative Pebble state, an in-memory
SQL materializer, HLC/per-cell LWW and row tombstones, offline writes, authenticated
QUIC replication, memberlist SWIM discovery over QUIC, a bounded shared connection
pool, separate control/data/snapshot streams, log GC, snapshot merging,
key rotation, FTS, and checkpoint backup transports. The default SQL engine uses
`modernc.org/sqlite`; a build-tagged LumoSQL driver, pre-update hook, and MVCC
connection path are present, while building and validating against an actual
LumoSQL LMDB library remains pending.

Existing fuzz, crash, adversarial-peer, and two-node convergence/soak tests are
useful foundations. Their presence does not establish acceptance for the additions
below; this comparison did not execute either repository's tests.

## Existing requirements still awaiting implementation

| Capability | Current gap | Target document |
| --- | --- | --- |
| Membership and bounded dissemination | SWIM discovery and the shared bounded QUIC pool are implemented; peer selection/rotation and anti-entropy remain actively tracked, while Plumtree integration and scaling acceptance remain pending | [Membership](membership-and-transport.md), [dissemination](replication-and-dissemination.md) |
| Detailed synchronization | Separate streams and the transaction-chunk codec are implemented; durable chunk staging, paginated progress, and cross-peer partial-range repair remain pending | [Synchronization](synchronization-and-overload.md) |
| Overload controls | No complete aggregate/per-peer queue, bandwidth, staging, and repair budgets or adaptive apply scheduler | [Overload](synchronization-and-overload.md#32-mutation-batching) |
| Safe snapshot publication | Consistent read cuts, bounded source tail-history leases, digest-validated durable staging, atomic cell/watermark publication, and chunked merge with durable resume plus atomic publication for snapshots above the atomic threshold are implemented up to 512 MiB | [Snapshot safety](snapshots-backup-and-restore.md#current-implementation-and-remaining-gap) |
| Restore identity safety | Fresh writer identity and rollback rejection are implemented; clearing inherited membership policy remains pending; crash-safe ciphertext rebinding for new-DBID reseed is implemented | [Backup and restore](snapshots-backup-and-restore.md#38-pebble-online-zero-downtime-backup-and-restore) |
| Metrics integration | Status, replication/writer counters, and Prometheus collectors exist; complete SWIM/shared-pool diagnostics and metrics for pending subsystems remain outstanding | [Diagnostics](runtime-and-diagnostics.md#52-metrics-and-diagnostics) |

## Additional GALVANIZE capabilities

These optional extensions are outside the initial embedded replication MVP.
Several have implementations; remaining integration and acceptance requirements
are tracked separately. Their acceptance criteria live with each subsystem.

| Addition | Implementation and remaining capability |
| --- | --- |
| [High/Low replication](high-low-replication.md) | Roles, sealed bundles, journals, schema holds, and status/replay controls exist; atomic export/import obligations and progress, encrypted outbox payloads, aggregate capacity, and definition-level schema validation remain pending |
| High ownership and provenance | Implemented source stream/sequence tracking with High-owned field protection against subsequent Low updates (including reordered policy/value reconciliation via same-row shadow cells), protected deletes with explicit resolution, and policy/shadow state in High-domain durability and replication; file ownership release APIs exist |
| [Writer scheduling](synchronization-and-overload.md#local-and-replication-write-scheduling) | Implemented configurable local/replication writer-time shares, bounded maintenance, idle borrowing, and cancellation-aware admission; sustained workload acceptance remains tracked |
| [Encrypted file replication](file-replication.md) | Local authenticated immutable objects plus replicated metadata with streaming upload/read, search/list, delete tombstones, availability status, grace-based collection, bounded mesh fetch from static peers, recipient-sealed High/Low file artifacts with High-local re-encryption, object key rotation with generations, and object-inclusive versus metadata-only backup/restore exist; SWIM fetch discovery remains pending |
| [Address policy](membership-and-transport.md#ip-and-cidr-admission-policy) | Implemented IP/CIDR filtering alongside certificate/NodeID authorization |
| [Query subscriptions](query-and-search.md#reactive-query-subscriptions) | Implemented bounded subscriptions to committed SQL-visible changes with explicit resume/reset behavior |
| [Optional service adapters](runtime-and-diagnostics.md#optional-service-and-administration-adapters) | Implemented optional authenticated SQL/status/subscription HTTP handler and SDK plus separate admin unlock controls; file operations await file replication |

High/Low identifies security domains and permitted data direction. It is distinct
from local/replication writer priority, and neither implies a network bandwidth
split. Ordinary mesh replication remains multi-writer within a domain.

## Reference boundaries

GALVANIZE's reference implementations are `galv-highlow`, `galv-write-scheduler`,
`galv-files`, and the Corrosion agent integration. Its live scenarios cover forged
or corrupted bundles, replay, missing sequences, schema holds, writer priority,
and file transfers. Port the behavior and acceptance cases, not CR-SQLite-specific
storage or mutation formats: NOMADSQL retains Pebble authority and its own HLC/LWW
protocol. Wire compatibility with GALVANIZE is not currently promised.

GALVANIZE's High/Low directory, HTTP(S), and FTP(S) transports have implementations.
Its SFTP entry points currently return configuration errors; listing a transport
in a reference configuration does not establish support. NOMADSQL's existing
whole-database backup transports alone do not establish delta or file transfer.
The bridge now has directory, HTTP(S), and FTP(S) publication adapters; encrypted
file-object transfer remains pending.

## Delivery order

1. Complete snapshot generation publication, tail-history retention, restore policy
   cleanup/reseed, bounded dissemination, detailed synchronization, and overload.
2. Retain implemented writer scheduling/address admission and complete their
   sustained-load acceptance and membership/pool diagnostics.
3. Complete High/Low atomic delivery, encrypted outbox storage, capacity and schema
   gates, and replicated provenance/ownership before treating imports as hardened.
4. File transfer exists (replicated metadata, bounded mesh fetch, sealed
   High/Low artifacts with High-local re-encryption, object key rotation,
   and object-inclusive backup/restore); remaining discovery work is SWIM
   fetch advertisement.
   Subscriptions and optional adapters already exist, with file service
   operations still pending.

Update [pending tasks](../TASKS_PENDING.md) and project status only when the
corresponding implementation and acceptance checks exist.
