# MURMUR-SQL Architecture

*Coordinated starling flock flight.*

The architecture and implementation documentation has moved from the root `ARCHITECTURE.md` into these topic documents.

These documents describe the target design and implementation plan, including planned capabilities. See the [project README](../README.md#status) for current implementation status.

Use [Capability gaps](capability-gaps.md) and [Release status](release-status.md)
for outstanding work and release requirements.
[Release status](release-status.md) records the implementation inventory and
verification history.

Start with the [overview](overview.md), then read the topics relevant to your change. Original section numbers are preserved so existing section references remain identifiable; prose references link directly to their new locations.

For the implementation inventory, configured platform checks, and deployment
instructions, read [Release status](release-status.md); it is the single
source of truth when older status paragraphs disagree. For the original
code-versus-design comparison with GALVANIZE, read
[Capability gaps and implementation status](capability-gaps.md).
High/Low implements roles, sealed bundles, encrypted journals, capacity
enforcement, atomic delivery with receipts and stream progress, schema holds,
status/replay controls, and ownership policy. Replicated file metadata with
bounded mesh fetch and cross-domain sealed payload transfer is implemented;
dedicated SWIM-based file-fetch discovery acceptance remains pending. SWIM
membership runtime wiring supports configured bootstrap seeds, and seed-only
live discovery passed in the 2026-10-08 release gate. Long-duration membership
soaks remain. The query materialization is in-memory only.

| Document | Topics | Original sections |
| --- | --- | --- |
| [Capability gaps](capability-gaps.md) | Remaining capabilities, limitations, and future work. | New topic |
| [Capability gaps and implementation status](capability-gaps.md) | Implemented foundations, pending core requirements, GALVANIZE comparison, and delivery priorities. | New topic |
| [High/Low replication and provenance](high-low-replication.md) | One-way domain bridge, sealed bundles, durable delivery, schema holds, High ownership, and replay. | New topic |
| [Encrypted file replication](file-replication.md) | Separate encrypted objects, metadata, peer fetching, cross-domain artifacts, and retention. | New topic |
| [Architecture overview](overview.md) | Goals, RIME/Spool ownership, Go build contract, and the production system diagram. | 1, 2, 3, 4, 88 |
| [API and configuration](api-and-configuration.md) | Public Go API, startup progress callback, configuration example, and internal interfaces. | 5, 66, 80 |
| [Schema and migrations](schema.md) | Schema restrictions, stable identities, concurrent additive migrations, and local-only objects. | 6, 7, 50, 51 |
| [Transactions and prepared changes](transactions.md) | Change capture, transaction coalescing, commit/apply ordering, and idempotency. | 8, 9, 15, 16, 17, 18, 19, 49, 81 |
| [Merge policies](merge-policies.md) | Distributed counters, typed observed-remove sets, numeric extrema, API, bridge and upgrade contract. | New topic |
| [Conflict resolution](conflict-resolution.md) | Mutation model, HLC/LWW ordering, tombstones, and conflict examples. | 10, 11, 12, 82 |
| [Storage and materialization](storage.md) | Spool persistence layout, in-memory radix tree state, startup/rebuild, caching, compaction, and a log-GC example. | 13, 14, 20, 21, 22, 25, 47, 83 |
| [Spool persistence](../spool/README.md) | Embedded write-optimized persistence package serving as the sole on-disk storage backend with AES-256-GCM encryption. | Separate document: 1–44 |
| [Origin signatures](origin-signatures.md) | Ed25519 transaction identities, origin trust, snapshots and strict migration. | New topic |
| [Membership and transport](membership-and-transport.md) | SWIM/memberlist over QUIC, discovery, authentication, and IP/CIDR admission. | 26, 27 |
| [Replication and dissemination](replication-and-dissemination.md) | Bounded peer connections, handshakes, multi-origin forwarding, and optional Plumtree. | 28, 29, 30 |
| [Synchronization and overload](synchronization-and-overload.md) | Gap/range repair, transaction chunks, budgets, writer scheduling, wire encoding, acknowledgements, and large values. | 31, 32, 33, 34, 35, 48 |
| [Snapshots, backup, and restore](snapshots-backup-and-restore.md) | Log retention, safe snapshot merging/publication, backups, restore identities, and reseeding. | 36, 37, 38 |
| [Encryption and key management](encryption.md) | Encrypted VFS, key providers/caches, registry rotation, and maintenance rewrites. | 39, 40, 41, 42, 43, 44 |
| [Runtime and diagnostics](runtime-and-diagnostics.md) | Lifecycle, startup progress, workers, metrics, concurrency, and optional service adapter. | 45, 46, 52, 53, 54, 62, 63, 64 |
| [Query and search](query-and-search.md) | Indexes, FTS, prepared statements, and reactive query subscriptions. | 23, 24, 65 |
| [RIME in-memory engine](rime.md) | Package overview, API guide, scope, concurrency limits and implementation/qualification references. | New topic |
| [Murmur migration plan](../MIGRATION_PLAN.md) | In-progress replacement of SQLite/SQL with RIME, rich Go records, Spool persistence and masterless replication; breaking API/format changes and qualification gates. | New topic |
| [Release notes](../RELEASE_NOTES.md) | Breaking typed API, storage/protocol format changes, legacy export requirements, and qualification status. | New topic |
| [RIME migration schema](rime-migration-schema.md) | Recursive descriptor compiler, canonical record codec, and typed facade/adapter migration status. | New topic |
| [RIME detailed architecture](../rime/ARCHITECTURE.md) | Implemented storage, snapshots, commit paths, locks, index structures, query execution, maintained views, hooks, GC and performance tradeoffs. | New topic |
| [RIME usage and syntax](../rime/USAGE.md) | Public Go API, schema syntax, CRUD, transaction-bound handles, queries, relations, maintained views, events, limits and optional code generation. | New topic |
| [RIME production qualification](rime-testing.md) | Correctness gates, model fuzzing, memory retention, standalone live workload, and release acceptance. | New topic |
| [RIME benchmarks](rime-benchmarks.md) | Benchmark methods, measured development samples, and remaining baseline work. | New topic |
| [RIME performance plan](rime-performance-plan.md) | Profile-driven mutation, MVCC history, concurrent commit and query optimization milestones with measurement and correctness gates. | New topic |
| [RIME improvement backlog](../rime/IMPROVEMENTS.md) | Deferred scan-layout and field-extraction work with correctness and measurement constraints. | New topic |
| [Testing and acceptance](testing.md) | Crash/convergence/network/encryption tests and alpha acceptance criteria. | 55, 56, 57, 58, 89 |
| [Operational rehearsals](operational-rehearsals.md) | Backup restore, lost-key, certificate-renewal, disk-full, and recovery-time drills. | New topic |
| [Benchmarks](benchmarks.md) | Search, write/replication, RIME qualification, and retained historical SQLite baselines. | 59, 60, 61 |
| Implementation roadmap (removed; content now in [Capability gaps](capability-gaps.md) and [Release status](release-status.md)) | Implementation phases, MVP boundaries, active and future tasks. | 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 87, 91 |
| [Invariants, risks, and security](invariants-and-risks.md) | Design risks, non-negotiable architecture invariants, and security review requirements. | 78, 79, 85 |
| [Versioning, release, and references](versioning-and-release.md) | Compatibility/versioning, packaging, platform support, and architecture sources. | 84, 86, 90 |
| [Operational tooling](18-operational-tooling.md) | Standalone CLI utility (`murmur`), REPL shell, storage diagnostics, backup lifecycle, and cluster operations. | New topic |
| [Release status](release-status.md) | Current implementation inventory, configured platform checks, and release evidence requirements. | New topic |

Keep these documents, their links, and the project README current when behavior, APIs, configuration, storage formats, or implementation status change. Repository guidance is in [AGENTS.md](../AGENTS.md).
