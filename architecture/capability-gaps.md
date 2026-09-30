# Capability gaps and implementation status

Source comparison with the sibling GALVANIZE checkout on 2026-09-27. This is an
implementation inventory with remaining acceptance work, reconciled with code on
2026-09-28. Implementation labels do not claim a fresh passing test run.

The [TODO list](../TODO.md) is the live list of remaining work.
This document provides comparison context and design priorities;
keep its status summaries consistent when checklist items are completed or changed.

[Architecture index](README.md) · [Project README](../README.md)

## Current foundation

Implemented foundations include encrypted authoritative Pebble state, an in-memory
SQL materializer, HLC/per-cell LWW and row tombstones, offline writes, authenticated
QUIC replication, memberlist SWIM discovery over QUIC, a bounded shared connection
pool, separate control/data/snapshot streams, log GC, snapshot merging,
key rotation, FTS, and checkpoint backup transports. The default SQL engine uses
bundled SQLite through `mattn/go-sqlite3`; an optional `modernc` build tag
provides a pure-Go SQLite backend (`modernc.org/sqlite`). Both use an in-memory query materialization rebuilt from Pebble.

Existing fuzz, crash, adversarial-peer, and two-node convergence/soak tests are
useful foundations. Their presence does not establish acceptance for the additions
below; this comparison did not execute either repository's tests.

## Implemented requirements and remaining acceptance

| Capability | Current gap | Target document |
| --- | --- | --- |
| Membership and bounded dissemination | Peer selection/rotation, anti-entropy, the shared bounded QUIC pool, Plumtree integration, and scaling acceptance are implemented; runtime SWIM wiring starts with configured bootstrap seeds. Seed-only live discovery acceptance remains pending; the named discovery test currently uses explicit `AddPeer` calls | [Membership](membership-and-transport.md), [dissemination](replication-and-dissemination.md) |
| Detailed synchronization | Separate streams, the transaction-chunk codec, durable chunk staging, paginated progress, and cross-peer partial-range repair are implemented | [Synchronization](synchronization-and-overload.md) |
| Overload controls | Aggregate/per-peer queue byte/entry budgets, staging caps, token buckets, and retryable overload responses are implemented | [Overload](synchronization-and-overload.md#32-mutation-batching) |
| Safe snapshot publication | Consistent read cuts, digest-validated durable staging, atomic cell/watermark publication, chunked merge with durable resume plus atomic publication up to 512 MiB, bounded source tail-history leases, and live acceptance covering slow transfer, sustained writes, aggressive GC, restarts, and tail catch-up are fully verified | [Snapshot safety](snapshots-backup-and-restore.md#current-implementation-and-remaining-gap) |
| Restore identity safety | Fresh writer identity, rollback rejection, inherited-membership-policy clearing, and crash-safe ciphertext rebinding for new-DBID reseed are implemented | [Backup and restore](snapshots-backup-and-restore.md#38-pebble-online-zero-downtime-backup-and-restore) |
| Metrics integration | Status, membership/pool diagnostics, replication/writer counters, and Prometheus collectors exist; dedicated retention-lease metrics and complete SWIM runtime metric coverage remain follow-ups; those subsystems themselves have implementations | [Diagnostics](runtime-and-diagnostics.md#52-metrics-and-diagnostics) |

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
| [Optional service adapter](runtime-and-diagnostics.md#optional-service-adapter) | Removed: no HTTP handler or client SDK ships with the library; the embedded Go API is the interface and any HTTP is the hosting process's own; the internal test-node HTTPS routes remain test tooling |

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
in a reference configuration does not establish support. The Murmur-SQL bridge has
directory, HTTP(S), and FTP(S) publication adapters plus recipient-sealed
encrypted file-object transfer with High-local re-encryption (proven by
`tests-live/files-bridge`).

## Delivery order

The delivery order below is complete except for the noted residuals; see the
[release inventory and verification record](release-status.md#1-verified-feature-matrix) for
current evidence.

1. Snapshot generation publication, restore policy cleanup/reseed, bounded
   dissemination, detailed synchronization, overload, and live source-lease safety
   acceptance through receiver tail catch-up under sustained writes/GC are fully verified.
2. Writer scheduling/address admission, sustained-load acceptance, and
   membership/pool diagnostics are implemented.
3. High/Low atomic delivery, encrypted outbox storage, capacity and schema
   gates, and replicated provenance/ownership are implemented.
4. File transfer is implemented (replicated metadata, bounded mesh fetch,
   sealed High/Low artifacts with High-local re-encryption, object key
   rotation, object-inclusive backup/restore, file service operations);
   residual: SWIM fetch discovery.
5. Remaining work: seed-only SWIM discovery acceptance,
   service-adapter file routes, and multi-hour/impaired-network
   soak acceptance. Automatic primary-key derivation is not implemented; callers
   still supply explicit `BLOB(16)` keys.

Update the [TODO list](../TODO.md) and project status only when the
corresponding implementation and acceptance checks exist.
