# SPeD-SQL Architecture

The architecture and implementation documentation has moved from the root `ARCHITECTURE.md` into these topic documents.

These documents describe the target design and implementation plan, including planned capabilities. See the [project README](../README.md#status) for current implementation status.

Start with the [overview](overview.md), then read the topics relevant to your change. Original section numbers are preserved so existing section references remain identifiable; prose references link directly to their new locations.

| Document | Topics | Original sections |
| --- | --- | --- |
| [Architecture overview](overview.md) | Goals, SQL engine choices, Go/CGO boundaries, and the target system diagram. | 1, 2, 3, 4, 88 |
| [API and configuration](api-and-configuration.md) | Public Go API, configuration example, and internal interfaces. | 5, 66, 80 |
| [Schema and migrations](schema.md) | Schema restrictions, stable identities, concurrent additive migrations, and local-only objects. | 6, 7, 50, 51 |
| [Transactions and change capture](transactions.md) | Change capture, transaction coalescing, commit/apply ordering, and idempotency. | 8, 9, 15, 16, 17, 18, 19, 49, 81 |
| [Conflict resolution](conflict-resolution.md) | Mutation model, HLC/LWW ordering, tombstones, and conflict examples. | 10, 11, 12, 82 |
| [Storage and materialization](storage.md) | Pebble layout, startup/rebuild, caching, compaction, and a log-GC example. | 13, 14, 20, 21, 22, 25, 47, 83 |
| [Membership and transport](membership-and-transport.md) | SWIM/memberlist over QUIC, discovery, transport configuration, and authentication. | 26, 27 |
| [Replication and dissemination](replication-and-dissemination.md) | Bounded peer connections, handshakes, multi-origin forwarding, and optional Plumtree. | 28, 29, 30 |
| [Synchronization and overload](synchronization-and-overload.md) | Gap/range repair, transaction chunks, batching, budgets, wire encoding, compression, acknowledgements, and large values. | 31, 32, 33, 34, 35, 48 |
| [Snapshots, backup, and restore](snapshots-backup-and-restore.md) | Log retention, safe snapshot merging/publication, backups, restore identities, and reseeding. | 36, 37, 38 |
| [Encryption and key management](encryption.md) | Encrypted VFS, key providers/caches, registry rotation, and maintenance rewrites. | 39, 40, 41, 42, 43, 44 |
| [Runtime and diagnostics](runtime-and-diagnostics.md) | Lifecycle, workers, metrics, logging/errors, resource budgeting, and concurrency. | 45, 46, 52, 53, 54, 62, 63, 64 |
| [Query and search](query-and-search.md) | Indexes, FTS, derived search objects, prepared statements, and FTS maintenance. | 23, 24, 65 |
| [Testing and acceptance](testing.md) | Crash/convergence/network/encryption tests and alpha acceptance criteria. | 55, 56, 57, 58, 89 |
| [Benchmarks](benchmarks.md) | Search, write/replication, and startup performance scenarios. | 59, 60, 61 |
| [Implementation roadmap](implementation-roadmap.md) | Implementation phases, prototype files, MVP boundaries, and recommended next steps. | 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 87, 91 |
| [Invariants, risks, and security](invariants-and-risks.md) | Design risks, non-negotiable architecture invariants, and security review requirements. | 78, 79, 85 |
| [Versioning, release, and references](versioning-and-release.md) | Compatibility/versioning, packaging, platform support, and architecture sources. | 84, 86, 90 |

Keep these documents, their links, and the project README current when behavior, APIs, configuration, storage formats, or implementation status change. Repository guidance is in [AGENTS.md](../AGENTS.md).
