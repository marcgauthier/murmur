# Release status (verified)

Single verified source for what SPeD-SQL implements, which platforms it
supports, how to deploy it, and which exact commit those claims attach to.
When this document disagrees with an older status paragraph elsewhere, this
document wins; the older paragraph is stale and should be fixed.

[Architecture index](README.md) · [Project README](../README.md) ·
[Versioning and release](versioning-and-release.md)

Last verified: 2026-09-28 against the current uncommitted tree
listed in [§5](#5-release-commit). Re-verify (builds, vet, unit suites,
live suites) before moving these claims to a new commit.

> Build-tag contract: the default CGO build does **not** compile without
> tags. Every `go build` / `go vet` / `go test` command in this document
> needs `-tags "sqlite_preupdate_hook sqlite_fts5"`, or `-tags modernc`
> for the pure-Go backend. `tests-live/run.sh` defaults
> `SPEDSQL_TAGS` to the CGO set. See [§2](#2-supported-platforms).

## Contents

- [1. Verified feature matrix](#1-verified-feature-matrix)
- [2. Supported platforms](#2-supported-platforms)
- [3. Deployment](#3-deployment)
- [4. Verification record](#4-verification-record)
- [5. Release commit](#5-release-commit)

---

## 1. Verified feature matrix

State is one of **Implemented** (code plus passing tests exist),
**Partial** (some of the capability is missing), or **Pending**.
Evidence names the code path and command for each capability. Refactor-specific
verification is recorded in [§4](#4-verification-record).

| # | Capability | State | Evidence |
|---|---|---|---|
| 1 | Embedded Go API, schema v1 (explicit `BLOB(16)` PK, additive `DB.Migrate`, replicated manifest, `AcceptRemoteSchema`) | Implemented | `db.go`, `schema/`, `schema_sync.go`; `go test -tags ... ./tests-live/schema-evolution` |
| 2 | Durable encrypted Pebble store (AES-256-GCM default, key registry, storage/data-key rotation, maintenance rewrite) | Implemented | `crypto/`, `state/`; `go test -tags ... ./tests-live/encryption ./tests-live/rekey` |
| 3 | In-memory SQLite materialization rebuilt from Pebble (default mattn/go-sqlite3; optional `modernc` pure-Go driver) | Implemented | `sqlengine/`; both backend SQL engine suites |
| 4 | Pre-update capture, transaction coalescing, per-cell LWW + row tombstones, offline writes, deterministic convergence | Implemented | `sqlengine/capture_*.go`, `crdt/`; `go test -tags ... ./tests-live/three-node-sync ./tests-live/crdt-contention` |
| 5 | QUIC + mTLS replication over a static mesh: multi-origin forwarding, rotation, anti-entropy, partition healing | Implemented | `replication/`, `transport/`; `go test -tags ... ./tests-live/partial-mesh ./tests-live/partition ./tests-live/chaos-load ./tests-live/crash-recovery` |
| 6 | Bounded fanout/selection: selected targets, peer rotation, anti-entropy, shared pool caps (defaults: fanout 4, 32 sessions, 64 connections, 2 repairs) | Implemented | `replication/manager.go` (`reconcileSelectedLocked`), `transport/pool.go`, `config.go`; `go test -tags ... ./replication -run 'TestPeerScalingCapsAcrossChurn|TestPeerSelectionBoundedByFanout|TestPeerRotation'` |
| 7 | Optional Plumtree dissemination with required-capability negotiation (mixed modes refuse) | Implemented | `replication/plumtree.go`, `plumtree/`; `go test -tags ... ./tests-live/plumtree-live` |
| 8 | Transaction chunks (64 KiB, durable encrypted staging, restart resume, cross-peer repair) and paginated applied/observed progress | Implemented | `replication/transaction_chunks.go`, `replication/progress.go`, `replication/chunk_availability.go`, `state/transaction_stage.go`; `go test -tags ... ./tests-live/large-payload` |
| 9 | Overload budgets: global/per-peer queue byte/entry caps, 256 MiB durable staging cap, token buckets, retryable overload responses | Implemented | `overload/`, replication queue accounting; `go test -tags ... ./tests-live/overload-budgets` |
| 10 | Snapshot resync: consistent Pebble read cut, manifest v1, canonical digest, encrypted restart-safe staging, atomic (≤8 MiB) and chunked SSTable-ingest merge (≤512 MiB), atomic watermark publication, stall watchdog with bounded re-request, explicit busy deferral, per-peer transfer progress | Implemented | `state/snapshot*.go`, `replication/manager.go` (watchdog/deferral), `replication/snapshot_progress_test.go`; `go test -tags ... ./tests-live/snapshot-resync` (single + dual requester) |
| 11 | Snapshot source tail-history retention lease | Pending | Transfer timeout (10 min read-cut hold) is implemented (`config.go`, `replication/manager.go`); no retention-lease code exists, so a source under log-GC pressure can still age out tail history mid-transfer. |
| 12 | Log GC gated by persisted member admission/ack-progress retention deadlines; `RemovePeer` retirement persists across restart, `AddPeer` readmits | Implemented | `state/members.go`, `db.go`; `go test -tags ... ./tests-live/churn-retirement` |
| 13 | Backup/restore with fresh-writer identity, durable restore marker (same-identity rollback rejected), new-DBID reseed | Implemented | `backup/`, `state/restore.go`; `go test -tags ... ./tests-live/backup-restore` |
| 14 | SWIM/memberlist over QUIC | Partial | Transport adapter implemented and unit-tested (`transport/memberlist.go`, `transport/memberlist_test.go`), but no runtime discovery wiring exists (`memberlist.Create` appears only in tests); clusters use static `Replication.Peers`. |
| 15 | High/Low bridge: domain roles, recipient-sealed bundles, durable outbox (encrypted payloads) / inbox journals, aggregate capacity with backpressure, atomic imports with stable source receipts + contiguous stream progress, waiting-schema holds, status/replay diagnostics, ownership/provenance with reorder convergence, sealed file-object transfer | Implemented | `bridge/`; `go test ./bridge/`; `go test -tags ... ./tests-live/highlow ./tests-live/files-bridge ./tests-live/bridge-two-streams` |
| 16 | Encrypted file objects: replicated metadata, streaming upload/read, search/list, delete tombstones, availability, grace collection, bounded mesh fetch from static peers, per-file key generations, object-inclusive vs metadata-only backup | Implemented | `objectstore/`, `files*.go`; `go test ./objectstore`; `go test -tags ... ./tests-live/files-soak`; SWIM fetch discovery is pending (see #14). |
| 17 | Writer scheduling (90/10 shares, idle borrowing, bounded debt, cancellation-aware admission), reactive subscriptions, IP/CIDR admission, `database/sql` driver, optional service/admin HTTP adapters, daemon HTTPS API with mTLS, `DB.Status`/`DB.Metrics`/Prometheus collectors | Implemented | `scheduler.go`, `subscriptions.go`, `transport/addrs.go`, `driver.go`, `service/`, `admin/`, daemon TLS middleware; `go test -tags ... ./tests-live/api-mtls ./tests-live/loadshare ./tests-live/subscribe ./tests-live/addrpolicy`; `go test -tags ... ./service/ ./admin/ ./metrics/` |
| 18 | Hardening: decoder fuzz targets, crash-injection suite, adversarial-peer tests, cert-expiry rejection, disk-full fail-closed, short multi-process soaks | Implemented | `codec/`, `replication/fuzz_test.go`, `crash_test.go`, `diskfull_test.go`, `soak_test.go`; `go test -tags ... ./tests-live/soak-slo ./tests-live/long-running-five-node` (smoke durations; multi-hour/impaired-network acceptance remains pending) |
| 19 | Benchmark matrix (§59–§61) with recorded 10K/100K results, cipher/compression matrix, local/live writer throughput, transaction-size matrix, concurrent-reader suite | Implemented | `benchmark/`; [Benchmarks](benchmarks.md#62-running-the-matrix) |
| 20 | Automatic primary-key derivation (default `id` UUIDv4 / UUIDv5 from a flagged column) | Pending | Requested in [TODO.md](../TODO.md); v1 still requires the application to supply an explicit `BLOB(16)` PK. Not release-blocking. |

## 2. Supported platforms

Pinned by [CI](../.github/workflows/ci.yml) and re-verified locally on
2026-09-28 (builds, both vets, unit suites, nine live suites).

| OS / arch | Backend (build tags) | CGO | Proven by |
|---|---|---|---|
| Linux amd64 | Default mattn/go-sqlite3, `-tags "sqlite_preupdate_hook sqlite_fts5"` | yes (gcc) | CI race suite; local `go build`, `go vet`, `-short` suites, live suites |
| Linux amd64 | Pure Go (`modernc.org/sqlite`), `-tags modernc` | no | CI suite; local `go build`, `go vet` |
| Linux arm64 | Pure Go, `-tags modernc` | no | CI cross-build; local `GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags modernc ./...` |
| Windows amd64 | Pure Go, `-tags modernc` | no | CI native suite + cross-build; local `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -tags modernc ./...` |

Requirements: Go 1.26.x, gcc for the CGO configuration. Dependency pins
live in [`go.mod`](../go.mod); licensing notes live in
[Versioning and release](versioning-and-release.md#86-packaging-and-release).

Known build constraint: `sqlengine/capture_mattn.go` (build `!modernc`)
uses the pre-update-hook API, so a bare `go build ./...` fails. Always
pass one of the two tag sets above.

## 3. Deployment

Two deployment shapes are supported: link the module into a Go
application, or run the `spedsql` daemon.

### 3.1 Embedded library

```sh
go get github.com/nomadsql/replicateddb
```

Open a node as in the [project README quick start](../README.md#quick-start):
`Config{Path, NodeID, Schema, Pebble, Encryption, Replication}` with a
cluster-shared `DBID`, one CA, and per-node certificates carrying the
`replicateddb://node/<uuid>` URI SAN (see §3.3). Every replicated table
needs an explicit `BLOB(16)` primary key; see
[Schema rules](../README.md#schema-rules-v1).

### 3.2 Daemon build and layout

```sh
go build -tags "sqlite_preupdate_hook sqlite_fts5" -o bin/spedsql ./cmd/spedsql
```

Give each node its own directory (`node1/`, `node2`, …) containing a JSON
config, a `pebble/` store (created on first start), TLS files, and logs.
The live harness uses exactly this layout; see
[`tests-live/harness/harness.go`](../tests-live/harness/harness.go) and any
`tests-live/*/README.md`.

Minimal two-node config (`node1/config.json`; node2 mirrors it with
swapped ports and peer entries):

```json
{
  "node_id": "<uuid>",
  "db_id": "<shared cluster uuid>",
  "data_dir": "node1",
  "listen_addr": "127.0.0.1:17443",
  "api_addr": "127.0.0.1:18080",
  "await_unlock": true,
  "key_id": "app-key-1",
  "tls_ca_cert_file": "node1/tls/ca.crt",
  "tls_node_cert_file": "node1/tls/node.crt",
  "tls_node_key_file": "node1/tls/node.key",
  "peers": [{"node_id": "<node2 uuid>", "addrs": ["127.0.0.1:17444"]}],
  "schema": {"version": 1, "tables": [
    {"name": "items", "columns": [
      {"name": "id", "type": 4},
      {"name": "name", "type": 3, "nullable": true}
    ]}
  ]}
}
```

Column `type` is numeric (`schema.ColumnType`: 1=INTEGER, 2=REAL,
3=TEXT, 4=BLOB); string names are not accepted in JSON configs.

Optional sections: `allowed_peers` / `allowed_networks` (admission),
`replication` (`dissemination: "plumtree"` must match on every member;
retention overrides are test-only), `limits` (commit budgets; defaults
are 16 MiB values / 64 MiB transactions), `files` (object replication),
`bridge` (High/Low role). Full key list:
[`cmd/spedsql/main.go`](../cmd/spedsql/main.go) (`NodeConfigFile`);
files/bridge key shapes follow the harness writers (`cfgJSON`,
`bridgeJSON` in `harness.go`).

Start, unlock, and check:

```sh
./bin/spedsql agent --config node1/config.json
./bin/spedsql unlock --api https://127.0.0.1:18080 --ca-cert ca.crt --client-cert client.crt --client-key client.key --key-hex <64 hex chars>
./bin/spedsql status --api https://127.0.0.1:18080 --ca-cert ca.crt --client-cert client.crt --client-key client.key
./bin/spedsql exec --api https://127.0.0.1:18080 --ca-cert ca.crt --client-cert client.crt --client-key client.key \
  "INSERT INTO items (id, name) VALUES (x'00112233445566778899aabbccddeeff', 'hello')"
./bin/spedsql query --api https://127.0.0.1:18080 --ca-cert ca.crt --client-cert client.crt --client-key client.key 'SELECT id, name FROM items'
```

(`unlock` posts the storage key to `/v1/admin/unlock`; without
`await_unlock` the daemon uses `key_hex` from the config. The CLI sends
the SQL statement as one positional argument; `/v1/exec` and `/v1/query`
also accept an `args` JSON array. File operations live
under `/v1/files/*`, bridge controls under `/v1/admin/bridge/*`, peer
churn under `/v1/admin/add_peer` / `/v1/admin/remove_peer`, schema
migration under `/v1/admin/migrate`. Every route, including `/metrics`,
requires HTTPS and a valid client certificate signed by the configured CA.
`GET /healthz` is the only route that permits clients without a certificate,
and it is still HTTPS-only.)

The API listener has no plaintext mode. It uses `tls_ca_cert_file` to verify
client certificates and `tls_node_cert_file` / `tls_node_key_file` as its
HTTPS server identity. Client certificates must be valid for client
authentication and chain to the configured CA. The server certificate must
also contain a DNS or IP SAN matching the hostname used by clients. The CLI
requires `--ca-cert`, `--client-cert`, and `--client-key` on each API command.

### 3.3 Certificates

Replication is mTLS-only. Each node certificate must carry its NodeID as
a URI SAN `replicateddb://node/<uuid>`; the daemon loads
`tls_ca_cert_file` / `tls_node_cert_file` / `tls_node_key_file` and
rejects mismatched identities. There is no `spedsql` cert subcommand
yet; provision with the `transport` package (the same calls the live
harness uses):

```go
ca, _ := transport.GenerateCA(365 * 24 * time.Hour)
certPEM, keyPEM, _ := ca.IssueNodeForHosts(nodeID, 90*24*time.Hour,
    []string{"db.example.com"}, nil)
```

Persist `ca.CertPEM` once per cluster and one cert/key pair per node. Node
certificates can also be API client credentials. Ensure each API server
certificate contains the DNS/IP SAN clients use to reach it. Rotate before
`NotAfter`; expired certificates are rejected.

### 3.4 Operations

- Health: `/healthz` (process), `/v1/status` (state, peers, watermarks,
  lag, queue depths), `/metrics` (Prometheus). `DBState` must be
  `ready` before writes are accepted.
- Backup: stop the node (or checkpoint online via `backup/`), copy the
  data directory. Restore/clone requires a **fresh** NodeID and
  certificate; opening restored data under the old identity is rejected
  (proven by `tests-live/backup-restore`).
- Peer churn: `remove_peer` retires and persists exclusion (survives
  restart, releases GC retention); `add_peer` readmits.
  Proven by `tests-live/churn-retirement`.
- Rolling schema change: additive migrations only, one node at a time;
  mixed-version meshes keep replicating. Proven by
  `tests-live/schema-evolution`.
- Upgrades: on-disk format is versioned (`format_version` = 2 with
  minimum reader/writer enforcement); replication handshakes negotiate
  capabilities and refuse unknown required bits, so mixed-version
  clusters fail closed instead of diverging.
- Loss window: default durability syncs Pebble before every write
  acknowledgement. `DurabilityAsync` returns without a disk sync and
  syncs about once per second plus on graceful close; use it only when
  the application accepts losing roughly the last second after a crash.

### 3.5 Verifying a deployment

```sh
bash tests-live/run.sh three-node-sync    # convergence smoke
bash tests-live/run.sh snapshot-resync    # stale-node snapshot path
bash tests-live/run.sh backup-restore     # restore identity rules
bash tests-live/run.sh all                # full live matrix (long)
SPEDSQL_TAGS=modernc bash tests-live/run.sh three-node-sync  # pure-Go backend
```

## 4. Verification record

SQLite backend refactor verification, 2026-09-28, linux/amd64:

- `go vet` and `go build` passed with both the default mattn tags and
  `CGO_ENABLED=0 -tags modernc`.
- Both `sqlengine` suites and all non-`tests-live` packages passed with
  `-short` under both drivers.
- The sustained concurrent-reader check passed at 1, 2, 4, 8, 16, and 32
  readers under both drivers.
- The live `three-node-sync` convergence scenario passed under both drivers.

2026-09-28, host linux/amd64 (Intel i5-6500), Go 1.26.x, HEAD
`b81e5c1` plus the §5 tree:

- `go build -tags "sqlite_preupdate_hook sqlite_fts5" ./...` — pass.
- `go build -tags "modernc" ./...` — pass.
- `GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -tags modernc ./...`
  and `GOOS=linux GOARCH=arm64 ...` — pass.
- `go vet` with both tag sets — pass (after repairing two breaks in the
  uncommitted `db_test.go`: missing `os` import, `db.Begin` → `db.BeginTx`).
- `go test -tags "sqlite_preupdate_hook sqlite_fts5" -short` on `.`,
  `./state/`, `./replication/`, `./bridge/`, `./transport/`,
  `./sqlengine/`, `./service/`, `./admin/`, `./metrics/`,
  `./objectstore/`, `./codec/`, `./crdt/`, `./schema/`, `./backup/`,
  `./overload/`, `./plumtree/`, `./filefetch/` — pass.
- New `replication/dialloop_stop_test.go` (detach-stops-dialer fix),
  including a `-race` run — pass; fails without the fix.
- All 28 live suites via `tests-live/run.sh` (repaired to pass
  `SPEDSQL_TAGS` to both the daemon build and `go test`, mirroring the
  harness fix): `addrpolicy`, `allow-nodes` (20 s development window),
  `backup-restore`, `benchmark`, `bridge-two-streams`, `chaos-load`
  (3/3 after the dialLoop fix), `churn-retirement`, `crash-recovery`,
  `crdt-contention`, `encryption`, `files-bridge`, `files-soak`,
  `highlow`, `large-payload`, `loadshare`, `long-running-five-node`
  (smoke), `overload-budgets`, `partial-mesh`, `partition` (after the
  poll-with-deadline repair), `plumtree-live`, `rekey`,
  `schema-evolution`, `snapshot-resync`, `soak-slo` (smoke),
  `subscribe`, `three-node-sync`, `views`, `write-priority` — all pass.

Not run for this release: full `-race` suites, multi-hour and
impaired-network soaks (pending acceptance, matrix #18; the 10-minute,
2-hour, and 1-hour soaks now run on a weekly CI schedule plus manual
dispatch via `bash tests-live/run.sh soak`; no scheduled run has completed
yet at the time of writing), the three-minute default
`allow-nodes` run, and the Windows/arm64 native test suites (CI-owned).

2026-09-28 follow-up (snapshot progress + live gate): `live-gate` CI job
runs `bash tests-live/run.sh gate` (smoke, encryption, backup/restore,
partitions, version skew, High/Low, files, scale mesh, upgrades,
snapshot resync, GC balance, rolling restart, crash recovery) against
the freshly built daemon on every push/PR, plus a pure-Go live smoke;
all gate suites pass locally (see TASKS_COMPLETED.md
SNAPSHOT-PROGRESS-001, LIVE-GATE-001, LIVE-GROUP-A-001). Do not run two
runner invocations against one checkout concurrently; isolate with
`SPEDSQL_LIVE_RUNTIME`.

2026-09-28 daemon API mTLS follow-up: the listener now serves HTTPS only,
verifies optional client certificates during the handshake against the
configured CA, and rejects missing certificates at every route except
`GET /healthz`. The full live release gate passed, including the new
`api-mtls` scenario; see TASKS_COMPLETED.md API-MTLS-001. This verification
applies to the current working tree and is not a release commit.

## 5. Release commit

No release commit exists yet: the tree is dirty. Review `git status --short`
and include the intended changes when preparing a release; the shared
workspace may contain unrelated in-progress work.

To cut it:

```sh
git status --short          # confirm the tree above (plus nothing unexpected)
git add -A
git commit -m "Release: reconciled status, verified matrix, deployment docs"
git rev-parse HEAD          # record this hash below and in the release notes
```

Then replace this paragraph with the recorded hash and re-run §4
against the clean checkout before publishing binaries.

Recorded release commit: _none yet_.
