# MURMUR-SQL

<p align="center">
  <img src="murmur-sql.png" alt="MURMUR-SQL logo" width="600"/>
</p>

A murmuration is a flock of starlings wheeling across the sky as
one: no bird leads, each one simply watches its neighbors and adjusts,
and from those small local reactions emerges a single shape that holds
together even as birds drift apart and rejoin. Murmur-SQL is built on
that pattern. Every node is a fully writable database that keeps
working offline, exchanges only change deltas with the peers it can
currently reach, and converges with the rest of the flock without a
leader — so a cluster that is sometimes partitioned, sometimes
disconnected, still acts in unison and heals into one consistent
shape whenever its nodes talk again.

Murmur-SQL is a Golang embedded package that provides an in-memory SQLite database with masterless/offline distributed cluster with persistence on disk via Pebble saving only change deltas.

- **SQL queries** through an embedded SQLite engine held in memory by default and rebuilt
  from Pebble on startup
- **Durable state** in Pebble (authoritative; the SQL database is rebuildable)
- **Masterless multi-writer replication** over QUIC with mutual TLS
- **Offline writes** on every node, per-column last-writer-wins via hybrid
  logical clock (CR-SQLite-style cell model, engine-independent)
- **Encrypted storage** with multiple cipher options, storage-key rotation, snapshots for new or stale nodes, replication-log garbage collection
- **Cross-domain unidirectional airgap**: allow data to transfer from a low domain flock to a high domain flock

> **Want the assembled database instead of the building block?**
> [`murmurd`](murmurd/README.md) is the same engine as a runnable
> server: configure it with `config.toml` and connect over the
> MySQL or PostgreSQL wire protocols. This page documents the
> embeddable Go package.

The architecture and implementation documentation is in [architecture/](architecture/README.md) folder, split into smaller topic documents.  Start with the [architecture overview](architecture/overview.md).

## Reused packages

The implementation leans on existing Go packages instead of reinventing them:

| Concern | Package |
|---|---|
| Durable LSM KV + block cache + compression | `github.com/cockroachdb/pebble/v2` |
| At-rest AEADs (AES-GCM, AEGIS, ChaCha20-Poly1305, XChaCha20) | `github.com/ericlagergren/aegis`, `golang.org/x/crypto` |
| SQL + pre-update hook + FTS5 | `mattn/go-sqlite3` with bundled SQLite (default), optional `modernc.org/sqlite` (`-tags modernc`) |
| QUIC transport with TLS 1.3 | `github.com/quic-go/quic-go` |
| SWIM membership discovery & transport | `github.com/hashicorp/memberlist` |
| Node/row/tx/cluster IDs | `github.com/google/uuid` |
| Prepared-statement cache | `github.com/hashicorp/golang-lru/v2` |
| Snapshot/wire compression | `github.com/klauspost/compress/zstd` |
| SQL pooling surface, mTLS PKI, structured logging | stdlib (`database/sql`, `crypto/x509`, `log/slog`) |

The default build uses mattn SQLite. The optional pure-Go `modernc.org/sqlite`
driver can replace it under the `modernc` build tag without changing replication,
durability, or conflict resolution.

The default SQL materialization is memory resident, non-authoritative, and rebuilt from
Pebble on open. Reads hold the engine read lock until their rows are closed or
exhausted; writes wait for active reads. See the [SQLite backend guide](architecture/sqlite-backends.md).

Startup rebuild materializes current Pebble state with bounded multi-row SQL
inserts (900 bind parameters per statement, commits at most every 5,000 rows).
Remote receives commit to Pebble first; affected rows are coalesced in memory
and applied to SQLite in one transaction every second or after 1,000 received
transactions, whichever comes first. Set `QueryStore.RemoteApplyInterval` and
`QueryStore.RemoteApplyMaxTransactions` to change those thresholds. Remote rows
can appear in queries after the receive acknowledgement; a local SQL write
flushes pending rows before it starts. SQLite materialization progress is held
in memory; startup rebuilds SQLite from Pebble without a separate progress
write to the durable store.

The optional `admin` package provides a TLS-only unlock/status/lock handler with
a bearer token separate from mesh identity. It starts no listener by itself;
applications explicitly mount it on their own TLS server. See
[runtime and diagnostics](architecture/runtime-and-diagnostics.md#optional-service-and-administration-adapters).

## Quick start

```go
db, err := replicateddb.Open(ctx, replicateddb.Config{
    Path:   "./node-data",
    NodeID: replicateddb.MustNodeID("..."),
    Schema: replicateddb.SchemaConfig{
        Version: 1,
        Tables: []schema.TableSchema{{
            Name: "contacts",
            Columns: []schema.ColumnSchema{
                {Name: "id", Type: schema.ColBlob},
                {Name: "name", Type: schema.ColText, Nullable: true},
                {Name: "phone", Type: schema.ColText, Nullable: true},
            },
        }},
    },
    Pebble: replicateddb.DefaultPebbleConfig(),
    Encryption: replicateddb.EncryptionConfig{
        Key:             key32, // or Provider for KMS/file/env sourcing
        KeyID:           "app-key-1",
        DataKeyRotation: 24 * time.Hour,
    },
    Replication: replicateddb.ReplicationConfig{
        ListenAddr: "127.0.0.1:7443",
        TLS:        &replicateddb.TLSCredential{CertPEM: cert, KeyPEM: key, CAPEM: ca},
        Peers:      []replicateddb.Peer{{NodeID: peerID, Addrs: []string{"127.0.0.1:7444"}}},
    },
})
if err != nil {
    return err
}
defer db.Close()

id := replicateddb.NewRowID()
_, err = db.ExecContext(ctx,
    `INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
    id[:], "ann", "613-555-0100")
```

For local applications that accept a short loss window after a crash, set
`Durability: replicateddb.DurabilityConfig{Mode: replicateddb.DurabilityAsync,
SyncInterval: time.Second}` in `Config`. Individual commits return without a
disk sync; the database syncs Pebble about once per second and also syncs on
graceful close. The default remains a disk sync before each write acknowledgement.
Pebble still appends to its WAL on each commit, so this setting controls sync
frequency rather than the exact number of physical disk writes.

Run the runnable example (the SQLite feature tags are required):

```sh
go run -tags "sqlite_preupdate_hook sqlite_fts5" ./example
```

A graduated progression — single node, encrypted node with transactions and
reopen recovery, then a 3-node replicated mesh — lives in `examples/`
(see [examples/README.md](examples/README.md)).

## Schema rules (v1)

Every replicated table must have an explicit `BLOB(16)` primary key
(generated by the application, e.g. `replicateddb.NewRowID()` — never SQLite
rowid/autoincrement). Tables/columns get stable numeric IDs for replication;
derived deterministically unless set explicitly. Additive evolution only:
`DB.Migrate` adds tables/columns (never renames/removes); the schema manifest
is stored in Pebble, replicated to peers with ancestry/merge exchange before
mutation sync, and applied under the `AcceptRemoteSchema` policy (auto-upgrade
or strict refusal). Secondary `UNIQUE` constraints, virtual tables, and
non-additive DDL are out of scope for v1. Foreign keys are
application-level (not enforced during remote apply/rebuild).

### Local-only tables, indexes, views, and FTS

Not every table needs to be replicated. `SchemaConfig.LocalDDL` holds SQL
statements for objects that live only on the local node — secondary indexes,
views, FTS5 virtual tables, or entire tables that are never captured or sent
over the wire. `LocalDDL` is re-applied after every open and rebuild, so
these objects are always consistent with the current replicated state but
carry zero replication overhead:

```go
Schema: replicateddb.SchemaConfig{
    Version: 1,
    Tables: []schema.TableSchema{{
        Name: "contacts",
        Columns: []schema.ColumnSchema{
            {Name: "id", Type: schema.ColBlob},
            {Name: "name", Type: schema.ColText, Nullable: true},
        },
    }},
    LocalDDL: []string{
        `CREATE INDEX IF NOT EXISTS idx_contacts_name ON contacts(name)`,
        `CREATE VIRTUAL TABLE IF NOT EXISTS contacts_fts USING fts5(name, content=contacts, content_rowid=rowid)`,
        `CREATE TABLE IF NOT EXISTS local_cache (key TEXT PRIMARY KEY, value BLOB)`,
    },
},
```

Only `Tables` entries are replicated. Everything in `LocalDDL` stays local.

## Repository layout

```text
MURMUR-SQL/               public API (db.go, transaction.go, config.go, ...)
  architecture/          architecture, subsystem design, testing, and implementation roadmap
  crdt/                  HLC clock, version comparison, LWW merge
  codec/                 binary value / mutation-batch / snapshot encodings
  schema/                stable table/column registry + validation
  state/                 Pebble-backed authoritative store (commit, log, GC, snapshot)
  sqlengine/             SQL materialization: capture, delta, apply, rebuild
  replication/           framed QUIC protocol + peer manager (batches, acks, snapshot)
  plumtree/              bounded eager/lazy dissemination state machine
  overload/              bounded global/per-peer accounting and token-bucket primitives
  transport/             QUIC listener/dialer, mTLS, NodeID-bound certificates
  crypto/                at-rest encryption: AEADs, key registry, encrypted VFS, rotation
  objectstore/           local immutable encrypted file payload storage
  ids/                   128-bit identity types
  service/               optional authenticated HTTP adapter + remote SDK (status, SQL, subscriptions)
  metrics/               Prometheus collectors over DB.Status (caller-owned registry)
  admin/                 TLS-only unlock/status/lock HTTP handler (app-mounted, no listener)
  bridge/                one-way Low-to-High logical replication roles and transfer types
  backup/                backup/restore with local/HTTPS/FTP destinations, file objects
  filefetch/             bounded mesh fetch of file object bytes (client/server protocol)
  example/               runnable single-node example
  examples/              graduated examples: single node → 3-node mesh
  murmurd/               assembled database daemon (MySQL + Postgres frontends, config.toml)
  tests-live/             live multi-process integration test scenarios
```

`objectstore/` stores immutable file payloads in authenticated encrypted
chunks, separate from SQL rows. `Files.Enabled` adds replicated file metadata
with streaming upload/read, search/list, delete tombstones, availability
status, grace-based collection, and bounded mesh fetch of object bytes from
statically configured peers (separate QUIC endpoint sharing cluster mTLS
credentials); object-key rotation advances per-file key generations, and
backups optionally include objects (`IncludeFiles`) or stay metadata-only
with mesh repair after restore;
see [encrypted file replication](architecture/file-replication.md).

The `tests-live/encryption` scenario checks wrong-key rejection, plaintext
absence from durable files, and correct-key restart recovery. Run it with
`go test -count=1 ./tests-live/encryption`.

The `tests-live/three-node-sync` and `tests-live/crash-recovery` scenarios
check three-node state convergence and encrypted-store recovery after an abrupt
process kill. Run them with `go test -count=1 ./tests-live/three-node-sync`
and `go test -count=1 ./tests-live/crash-recovery`.

The `tests-live/partition` scenario checks isolation and healing across a
four-node, two-pair network partition. Run it with
`go test -count=1 ./tests-live/partition`.

The `tests-live/chaos-load` scenario keeps writers active while a third node
is partitioned and while the mesh heals. Run it with
`go test -count=1 ./tests-live/chaos-load`.

The `tests-live/files-soak` scenario repeatedly uploads encrypted file objects
over a two-node QUIC mesh, checks metadata/search and peer-fetched content
digests, and records latency SLOs. Run a short pass with
`go test -count=1 ./tests-live/files-soak`; configure the ten-minute soak with
`SPEDSQL_FILES_SOAK_DURATION_SECONDS=600` and
`SPEDSQL_FILES_SOAK_INTERVAL_SECONDS=60`, passing `-timeout=12m` to `go test`.

The `tests-live/files-bridge` scenario checks encrypted file transfer from a
two-node Low mesh through a recipient-sealed bridge into a two-node High mesh,
with four isolated nodes driven over TLS (upload, peer fetch,
sealed staging with a no-plaintext check, High re-encryption, search, and a
delete cascade). Run it with `go test -count=1 ./tests-live/files-bridge`.
The `tests-live/soak-slo` scenario measures continuous writes and convergence
on three encrypted replicas. Both have short smoke commands in their local
READMEs and environment-configurable longer acceptance runs.

## Build, vet, test

The default mattn CGO build requires SQLite feature tags. Use one of the two
supported build configurations:

```sh
TAGS="sqlite_preupdate_hook sqlite_fts5"   # default mattn SQLite backend
# TAGS="modernc"                           # pure-Go backend (CGO_ENABLED=0)

go build -tags "$TAGS" ./...
go vet -tags "$TAGS" ./...
go test -tags "$TAGS" ./...
go test -tags "$TAGS" -race ./...   # required: no data races
go test -tags "$TAGS" ./... -short  # skip soak + 100K benchmarks

# Cross-compilation targets (pure-Go backend)
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -tags modernc ./...
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags modernc ./...

# Live multi-process scenarios (applies SPEDSQL_TAGS automatically)
bash tests-live/run.sh three-node-sync
bash tests-live/run.sh all   # full live matrix (long)
```

All `go test ./tests-live/...` commands elsewhere in this README need the
same `-tags` prefix, or use `tests-live/run.sh <scenario>` instead.

Continuous integration (.github/workflows/ci.yml) checks the default mattn CGO backend and pure-Go (`modernc.org/sqlite` via `-tags modernc`) configuration. See [versioning and release](architecture/versioning-and-release.md#86-packaging-and-release) for supported platforms and licensing.

Benchmarks (`benchmark/`): point/indexed/range/order/join/group reads, FTS
term/prefix, single/multi-cell writes, 10/1000/10000-row transactions,
Pebble commit latency, replication throughput (2-node, 5-node, backlog,
snapshot seed), cipher/compression matrix, checkpoint, maintenance
rewrite, startup components, and stock SQLite
baseline. Baselines below are illustrative only (10K contacts + 20K
orders, Intel i5-6500, encryption enabled; see
[Benchmarks](architecture/benchmarks.md) for commands and full results):

```text
PK lookup              5.9 µs/op   (p50)
Indexed equality       3.6 µs/op   (p50)
Indexed range (100)  109   µs/op   (p50)
ORDER BY + LIMIT 20   22   µs/op   (p50)
JOIN                  315   µs/op   (p50)
GROUP BY (scan)         3.5  ms/op (p50)
FTS term               20   µs/op   (p50)
FTS prefix             25   µs/op   (p50)
Single-cell txn         2.9  ms/op (~335 durable txns/s)
1000-row txn           19   ms      (~51K rows/s)
10000-row txn         171   ms      (~57K rows/s)
Pebble commit           1.4  ms/op (~700 commits/s)
Full open (30K rows)  ~1.0  s
2-node sync (30K)     ~1.0  s convergence after writes
5-node star (30K)      9.5  s to last convergence
```

Run them with (`-tags "sqlite_preupdate_hook sqlite_fts5"` throughout,
or `-tags modernc` for the pure-Go backend):

```sh
go test ./benchmark/ -bench . -short -benchtime 1s   # fast pass, 10K rows
go test ./benchmark/ -bench . -benchtime 1s          # 10K + 100K datasets
REPLICATEDDB_BENCH_ROWS=1000000 go test ./benchmark/ -bench .  # 1M rows
SPEDSQL_LOCAL_WRITE_BENCH_SECONDS=10 go test ./benchmark/ -run '^TestLocalWriterThroughput$' -v -count=1 -timeout=90s  # direct local API, 1 vs 4 writers
SPEDSQL_LOCAL_WRITE_BENCH_SECONDS=10 go test ./benchmark/ -run '^TestLocalPeriodicSyncThroughput$' -v -count=1 -timeout=90s  # one-second disk sync, 1 vs 4 writers
SPEDSQL_LOCAL_BATCH_BENCH_SECONDS=5 go test ./benchmark/ -run '^TestLocalTransactionBatchThroughput$' -v -count=1 -timeout=300s  # 1/10/100/1000 inserts per transaction
SPEDSQL_LIVE_WRITER_BENCH_SECONDS=10 go test ./tests-live/benchmark/ -run '^TestWriterThroughput$' -v -count=1 -timeout=90s  # live multi-process cluster, 1 vs 4 writers
```

The direct local writer benchmark reports acknowledged SQL inserts per second
for one and four goroutines on one encrypted database. It reopens the store and checks the durable row count. The separate live writer benchmark measures multi-node cluster throughput.
`TestLocalPeriodicSyncThroughput` runs the same workload with opt-in
one-second synchronization. On the Intel i5-6500, it measured about 5.2K
single-row writes/sec for both one and four writers, versus about 320/sec
with synchronous durability. These results include one scheduled WAL sync
about each second and a durable row-count check after graceful close.
`TestLocalTransactionBatchThroughput` compares both durability modes across
1, 10, 100, and 1,000 inserts per SQL transaction, using one or four writers.
It reports transactions/sec and rows/sec after verifying the durable row count.
On the Intel i5-6500, one synchronous writer rose from 340 rows/sec with one
insert per transaction to 13,557 rows/sec with 1,000; the one-second-sync
mode measured 3,950 and 13,816 rows/sec at those sizes. See the full
[transaction-size matrix](architecture/benchmarks.md#62-running-the-matrix).

Fuzz targets live next to the decoders (`codec`, `replication`):

```sh
go test ./codec/ -run XXX -fuzz FuzzBatch -fuzztime 30s
go test ./replication/ -run XXX -fuzz FuzzFrame -fuzztime 30s
```

## Status

The [release inventory and verification record](architecture/release-status.md)
separates implementation, available tests, and historical execution results.
Snapshot source retention leases and bootstrap-configured SWIM runtime wiring
exist. Remaining acceptance includes source-lease safety through receiver tail
catch-up, actual seed-only discovery, and multi-hour/impaired-network soaks.
Automatic primary-key derivation is not implemented; applications supply keys.
No verified release revision is recorded; the static inventory does not certify
the latest commit or working tree.

Working: local durable engine, pre-update capture, transaction coalescing,
 per-cell LWW + row tombstones, encrypted Pebble, startup rebuild, two-node
 QUIC replication with mTLS, conflicting-write convergence, log GC,
 snapshot resync, storage/data-key rotation, maintenance file rewrite, FTS5,
 adaptive remote apply groups (up to 64 contiguous transactions / 64 MiB in one
 synced state commit while preserving per-transaction receipts and sequences),
 crash-recoverable prepare records for single remote applies, finalized
 atomically with winners, logs, receipts, watermarks, HLC, and state generation,
 prepared-statement cache, online additive schema migration (`DB.Migrate`)
 with replicated schema-manifest sync (ancestry/merge exchange,
 `AcceptRemoteSchema` policy), `database/sql` driver wrapper
 (`sql.Open("replicateddb", ...)` / `NewConnector`), optional IP/CIDR peer
admission (`Replication.AllowedNetworks` / `Replication.AllowedPeers`), reactive query subscriptions
(`DB.Subscribe` / `DB.SubscribeWithOptions` with cursor resumption and reset notifications),
canonical `MaxTransactionBytes` pre-commit validation with rollback, handshake receive limit advertising, and bounded decode/reassembly,
 fresh-writer-identity backup restore/clone with a durable restore marker (same-identity rollback rejected),
coordinated new-DBID reseed with crash-safe ciphertext rebind and old-cluster traffic rejection,
optional authenticated HTTP service adapter + remote SDK (`service/`),
High/Low bridge (`bridge/`): domain-isolated one-way roles, sealed
 recipient-encrypted bundles, durable outbox (capture/publish, dir/HTTP/FTP
 adapters) and inbox (contiguous progress, gaps, quarantine, restart
 recovery), atomic bundle imports with preserved source boundaries, stable
 source-transaction receipts and contiguous stream progress in authoritative
 storage (`Store.RecordReceipt`, `Store.SetBridgeStreamProgress`), deduplication
 across replays and concurrent High receivers, durable `waiting-schema` holds with ordered
 automatic retry after local migration, and embedded status/replay
 diagnostics (`Bridge.Describe`, bounded, no payloads or key material),
 and enforced journal capacity with explicit backpressure, and
 authenticated outbox encryption with key rotation, plus recipient-sealed
 file object transfer (chunked sidecars, pending journal, per-object
 quarantine/retry, either-order install with digest verification and
 ownership-policy parity),
 replicated files (`Files.Enabled`): streaming upload/read, search/list,
 delete tombstones, availability status, grace-based collection, bounded
 mesh fetch, node-local object-key rotation with generations, and
 object-inclusive vs metadata-only backup/restore.
 Diagnostics: extended `DB.Status` with per-peer records (session/schema/
 watermark/lag/traffic/queue state), node-local writer/apply/GC/schema
 counters (`DB.Metrics`), aggregate replication counters, and Prometheus
 collectors (`metrics` subpackage; caller-owned registry, no HTTP endpoint).
 Membership persistence: admission records with ack-progress retention
 deadlines gate log GC independently of session liveness; `RemovePeer`
 persists retirement/exclusion across restart and `AddPeer` readmits with a
 fresh obligation. Compatibility: stores record format plus minimum
 reader/writer versions enforced at open, and handshakes refuse unknown
 required capabilities while ignoring unknown optional ones. Writer
 scheduling: fair local/replication time shares (default 90/10) with idle
borrowing, bounded debt, maintenance deferral, and cancellation-aware
admission for all state writers.

 `plumtree/` provides a bounded eager/lazy state machine integrated with
 `replication.Manager`. Bounded gossip remains the default; opt in with
 `ReplicationConfig.Dissemination = DisseminationPlumtree` on every cluster
 member. Capability negotiation rejects mixed modes. Eager data, delayed
 IHAVE/GRAFT, duplicate PRUNE, retained-cache replies, and durable Need fallback
 share the existing authenticated sessions and anti-entropy repair.
 Replication also exchanges paginated applied/observed heads and retained-log
 bounds. When the first peer lacks a missing tail, repair can fetch it from a
 second peer advertising that sequence, or request a snapshot when no retained
 source exists; progress pages report applied and durably observed heads, with
 chunk availability advertised separately.
 Oversized transactions use bounded 64 KiB `TXCH` chunks persisted in Pebble
 staging (encrypted by the configured production filesystem), resume after
 restart, and request missing indexes from alternate peers. Applied watermarks
 advance only after all chunks validate and the whole transaction commits.
 `overload/` provides reusable byte/entry queue leases and hierarchical token
 buckets. Replication accounts queued controls and range requests against a
 64 MiB/8192-entry global budget and 4 MiB/128-entry per-peer budget, limits
 bulk transfer rates, and uses a bounded reserved queue for retryable overload
 responses. Durable transaction staging is capped at 256 MiB/4096 transfers.
 Large outbound transactions and retained-log chunk repairs encode and send
 one reusable chunk frame at a time, avoiding a second transaction-sized
 frame collection in memory.
 The sender coalesces commit wakeups, rotates peer service each round, and
 bounds control and repair work so queued control traffic gets service during
 sustained bulk replication.

 Hardening: decoder fuzz targets, crash-injection suite at every commit
 boundary, adversarial-peer tests (bad magic/frames, oversize values,
 invalid batches, wrong DBID/schema), cert-expiry rejection, fail-closed
 schema/format checks, disk-full fail-closed acceptance (ENOSPC injection
 fails the write with a typed storage error, the node rejects reads/writes
 until restart, and restart recovers the last durable state), and a
 two-node randomized soak test with convergence assertion.

 Transaction chunking and SWIM runtime wiring over the shared bounded QUIC
 pool are implemented. Long-duration impaired-network measurements, multi-hour
 soak acceptance, and seed-only live discovery verification remain outstanding;
 short historical runs do not establish those guarantees.

 The [capability inventory](architecture/capability-gaps.md) distinguishes existing
 code from target requirements, including remaining bounded dissemination,
 detailed synchronization, overload controls, and snapshot/restore safety work.
 Snapshot export now uses a consistent Pebble read cut, and receivers validate
 indexed chunks and a canonical digest before atomically publishing state and
 watermarks. Encrypted staging survives restart. Large snapshots merge CRDT
 winners through encrypted-VFS SSTable ingestion with durable progress and
 atomic watermark publication. `Replication.MaxSnapshotBytes` defaults to
 512 MiB, with a 10-minute source transfer timeout. Source tail-history leases
 exist and release when export returns or expires; end-to-end protection through
 receiver publication and tail catch-up remains an acceptance requirement.

Implemented High/Low replication includes replicated row/field provenance,
High-owned field protection, durable Low-delete holds, identity-collision
quarantine, explicit High resolution (including file ownership release),
recipient-sealed encrypted file object transfer with High-local
re-encryption, and reordered-delivery reconciliation between Low values and
High ownership records via same-row shadow cells (all High peers converge
on High-owned state regardless of arrival order).
 Optional service/administration adapters remain separate from the embedded core.
