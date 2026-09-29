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
bundled SQLite through `mattn/go-sqlite3`; an optional `modernc` build tag
provides a pure-Go SQLite backend (`modernc.org/sqlite`). Both use an in-memory
query materialization rebuilt from Pebble.

Existing fuzz, crash, adversarial-peer, and two-node convergence/soak tests are
useful foundations. Their presence does not establish acceptance for the additions
below; this comparison did not execute either repository's tests.

## Existing requirements still awaiting implementation

| Capability | Current gap | Target document |
| --- | --- | --- |
| Membership and bounded dissemination | Peer selection/rotation, anti-entropy, the shared bounded QUIC pool, Plumtree integration, and scaling acceptance are implemented; only runtime SWIM discovery wiring remains pending (static peers; the QUIC transport adapter is unit-tested) | [Membership](membership-and-transport.md), [dissemination](replication-and-dissemination.md) |
| Detailed synchronization | Separate streams, the transaction-chunk codec, durable chunk staging, paginated progress, and cross-peer partial-range repair are implemented | [Synchronization](synchronization-and-overload.md) |
| Overload controls | Aggregate/per-peer queue byte/entry budgets, staging caps, token buckets, and retryable overload responses are implemented | [Overload](synchronization-and-overload.md#32-mutation-batching) |
| Safe snapshot publication | Consistent read cuts, digest-validated durable staging, atomic cell/watermark publication, and chunked merge with durable resume plus atomic publication are implemented up to 512 MiB; only the bounded source tail-history lease remains pending | [Snapshot safety](snapshots-backup-and-restore.md#current-implementation-and-remaining-gap) |
| Restore identity safety | Fresh writer identity, rollback rejection, inherited-membership-policy clearing, and crash-safe ciphertext rebinding for new-DBID reseed are implemented | [Backup and restore](snapshots-backup-and-restore.md#38-pebble-online-zero-downtime-backup-and-restore) |
| Metrics integration | Status, membership/pool diagnostics, replication/writer counters, and Prometheus collectors exist; metrics for still-pending subsystems (SWIM runtime, tail lease) remain outstanding | [Diagnostics](runtime-and-diagnostics.md#52-metrics-and-diagnostics) |

## Additional GALVANIZE capabilities

These optional extensions are outside the initial embedded replication MVP.
Several have implementations; remaining integration and acceptance requirements
are tracked separately. Their acceptance criteria live with each subsystem.

| Addition | Implementation and remaining capability |
| --- | --- |
| [High/Low replication](high-low-replication.md) | Roles, sealed bundles, encrypted journals, atomic export/import obligations and progress, aggregate capacity, definition-level schema validation, schema holds, and status/replay controls are implemented |
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
in a reference configuration does not establish support. The SPeD-SQL bridge has
directory, HTTP(S), and FTP(S) publication adapters plus recipient-sealed
encrypted file-object transfer with High-local re-encryption (proven by
`tests-live/files-bridge`).

## Delivery order

The delivery order below is complete except for the noted residuals; see the
[verified release status](release-status.md#1-verified-feature-matrix) for
current evidence.

1. Snapshot generation publication, restore policy cleanup/reseed, bounded
   dissemination, detailed synchronization, and overload are implemented;
   residual: source tail-history retention lease.
2. Writer scheduling/address admission, sustained-load acceptance, and
   membership/pool diagnostics are implemented.
3. High/Low atomic delivery, encrypted outbox storage, capacity and schema
   gates, and replicated provenance/ownership are implemented.
4. File transfer is implemented (replicated metadata, bounded mesh fetch,
   sealed High/Low artifacts with High-local re-encryption, object key
   rotation, object-inclusive backup/restore, file service operations);
   residual: SWIM fetch discovery.
5. Residual runtime work: SWIM discovery wiring (static peers only),
   multi-hour/impaired-network soak acceptance and the TODO.md
   automatic-PK request. The daemon API requires HTTPS with verified client
   certificates except for public `GET /healthz`.

Update [pending tasks](../TASKS_PENDING.md) and project status only when the
corresponding implementation and acceptance checks exist.
