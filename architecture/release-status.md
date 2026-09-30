# Release status and verification record

Implementation inventory, configured platform checks, deployment guidance, and
historical verification results. Code presence, test coverage, and successful
execution against a release revision are separate claims. Keep related topic
documents consistent with this inventory.

[Architecture index](README.md) · [Project README](../README.md) ·
[Versioning and release](versioning-and-release.md)

Documentation reconciled by static inspection on 2026-09-28, using HEAD
`4bdd2974766786865c6222bb4843cab79e41e5c5` and the working-tree move of the
standalone daemon into `tests-live/harness/testnode/`. Historical results in
[§4](#4-verification-record) do not certify this revision or subsequent edits.
Execution evidence for candidate `31bf1b5` is recorded in the 2026-09-29
§4 entry; no release tag has been cut ([§5](#5-release-commit)).

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

State is one of **Implemented** (a production code path exists),
**Partial** (some of the capability is missing), or **Pending**.
The evidence column identifies code and available tests or commands; it is not
a fresh pass result. The historical heading is retained for existing links.
Release-specific execution evidence belongs in [§4](#4-verification-record).

| # | Capability | State | Evidence |
|---|---|---|---|
| 1 | Embedded Go API, schema v1 (explicit `BLOB(16)` PK, additive `DB.Migrate`, replicated manifest, `AcceptRemoteSchema`) | Implemented | `db.go`, `schema/`, `schema_sync.go`; `go test -tags ... ./tests-live/schema-evolution` |
| 2 | Durable encrypted Pebble store (AES-256-GCM default, key registry, storage/data-key rotation, maintenance rewrite) | Implemented | `crypto/`, `state/`; `go test -tags ... ./tests-live/encryption ./tests-live/rekey` |
| 3 | In-memory SQLite materialization rebuilt from Pebble (default mattn/go-sqlite3; optional `modernc` driver) | Implemented | `sqlengine/`; both backend SQL engine suites. |
| 4 | Pre-update capture, transaction coalescing, per-cell LWW + row tombstones, offline writes, deterministic convergence | Implemented | `sqlengine/capture_*.go`, `crdt/`; `go test -tags ... ./tests-live/three-node-sync ./tests-live/crdt-contention` |
| 5 | QUIC + mTLS replication with configured or discovered peers: multi-origin forwarding, rotation, anti-entropy, partition healing | Implemented | `replication/`, `transport/`; `go test -tags ... ./tests-live/partial-mesh ./tests-live/partition ./tests-live/chaos-load ./tests-live/crash-recovery` |
| 6 | Bounded fanout/selection: selected targets, peer rotation, anti-entropy, shared pool caps (defaults: fanout 4, 32 sessions, 64 connections, 2 repairs) | Implemented | `replication/manager.go` (`reconcileSelectedLocked`), `transport/pool.go`, `config.go`; `go test -tags ... ./replication -run 'TestPeerScalingCapsAcrossChurn\|TestPeerSelectionBoundedByFanout\|TestPeerRotation'` |
| 7 | Optional Plumtree dissemination with required-capability negotiation (mixed modes refuse) | Implemented | `replication/plumtree.go`, `plumtree/`; `go test -tags ... ./tests-live/plumtree-live` |
| 8 | Transaction chunks (64 KiB, durable encrypted staging, restart resume, cross-peer repair) and paginated applied/observed progress | Implemented | `replication/transaction_chunks.go`, `replication/progress.go`, `replication/chunk_availability.go`, `state/transaction_stage.go`; `state/transaction_stage_test.go` covers restart/complete-batch staging; `replication/transaction_chunks_test.go` covers complete-only apply. `tests-live/large-payload` covers separate writes and large-value fidelity, not single bulk-transaction atomicity or interrupted cross-peer repair. |
| 9 | Overload budgets: global/per-peer queue byte/entry caps, 256 MiB durable staging cap, token buckets, retryable overload responses | Implemented | `overload/`, replication queue accounting; `go test -tags ... ./tests-live/overload-budgets` |
| 10 | Snapshot resync: consistent Pebble read cut, manifest v1, canonical digest, encrypted restart-safe staging, atomic (≤8 MiB) and chunked SSTable-ingest merge (≤512 MiB), atomic watermark publication, stall watchdog with bounded re-request, explicit busy deferral, per-peer transfer progress | Implemented | `state/snapshot*.go`, `replication/manager.go` (watchdog/deferral), `replication/snapshot_progress_test.go`; `go test -tags ... ./tests-live/snapshot-resync` (single + dual requester) |
| 11 | Snapshot source tail-history retention lease | Implemented | `state/snapshot.go`, `state/store.go`, `state/scan.go`; `TestSnapshotTailRetentionLease*` covers concurrent writes/GC, cancellation/retry, and expiry. In-memory lease releases when export returns or expires; protection through receiver publication and tail catch-up still needs live acceptance. |
| 12 | Log GC gated by persisted member admission/ack-progress retention deadlines; `RemovePeer` retirement persists across restart, `AddPeer` readmits | Implemented | `state/members.go`, `db.go`; `go test -tags ... ./tests-live/churn-retirement` |
| 13 | Backup/restore with fresh-writer identity, durable restore marker (same-identity rollback rejected), new-DBID reseed | Implemented | `backup/`, `state/restore.go`; `go test -tags ... ./tests-live/backup-restore` |
| 14 | SWIM/memberlist over QUIC | Implemented | `db.go` starts `replication.NewMembershipService` with configured bootstrap seeds; `replication/membership.go` calls `memberlist.Create`. Membership/transport tests exist. `TestDynamicBootstrapDiscovery` (partial-mesh) starts 3 nodes with no static peers and seed addresses only, requires SWIM membership of 3 on all nodes plus exact 20-row convergence with zero `AddPeer` calls (verified passing live; see §4 format for recording a candidate run). |
| 15 | High/Low bridge: domain roles, recipient-sealed bundles, durable outbox (encrypted payloads) / inbox journals, aggregate capacity with backpressure, atomic imports with stable source receipts + contiguous stream progress, waiting-schema holds, status/replay diagnostics, ownership/provenance with reorder convergence, sealed file-object transfer | Implemented | `bridge/`; `go test ./bridge/`; `go test -tags ... ./tests-live/highlow ./tests-live/files-bridge ./tests-live/bridge-two-streams` |
| 16 | Encrypted file objects: replicated metadata, streaming upload/read, search/list, delete tombstones, availability, grace collection, bounded mesh fetch from static peers, per-file key generations, object-inclusive vs metadata-only backup | Implemented | `objectstore/`, `files*.go`; `go test ./objectstore`; `go test -tags ... ./tests-live/files-soak`; File-fetch SWIM discovery remains pending independently of SQL membership (#14). |
| 17 | Writer scheduling (90/10 shares, idle borrowing, bounded debt, cancellation-aware admission), reactive subscriptions, IP/CIDR admission, `database/sql` driver, optional service/admin HTTP adapters, internal test-node HTTPS API with client-certificate authorization, `DB.Status`/`DB.Metrics`/Prometheus collectors | Implemented | `scheduler.go`, `subscriptions.go`, `transport/addrs.go`, `driver.go`, `service/`, `admin/`, `tests-live/harness/testnode/` TLS middleware (HTTPS health is exempt from client-certificate authorization); `go test -tags ... ./tests-live/api-mtls ./tests-live/loadshare ./tests-live/subscribe ./tests-live/addrpolicy`; `go test -tags ... ./service/ ./admin/ ./metrics/` |
| 18 | Hardening: decoder fuzz targets, crash-injection suite, adversarial-peer tests, cert-expiry rejection, disk-full fail-closed, short multi-process soaks | Implemented | `codec/`, `replication/fuzz_test.go`, `crash_test.go`, `diskfull_test.go`, `soak_test.go`; `go test -tags ... ./tests-live/soak-slo ./tests-live/long-running-five-node` (smoke durations; multi-hour/impaired-network acceptance remains pending) |
| 19 | Benchmark matrix (§59–§61) with recorded 10K/100K results, cipher/compression matrix, local/live writer throughput, transaction-size matrix, concurrent-reader suite | Implemented | `benchmark/`; [Benchmarks](benchmarks.md#62-running-the-matrix) |
| 20 | Automatic primary-key derivation (default `id` UUIDv4 / UUIDv5 from a flagged column) | Pending | Not implemented; v1 still requires the application to supply an explicit `BLOB(16)` PK. Not release-blocking. |

## 2. Supported platforms

The table separates configured [CI](../.github/workflows/ci.yml) checks from
historical local results. No current CI run was inspected for this reconciliation;
configuration alone is not evidence of a passing run.

| OS / arch | Backend (build tags) | CGO | Configured checks / historical local evidence |
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

Murmur-SQL is an embedded Go library. Applications link the package directly into their process:

### 3.1 Embedded library

```sh
go get github.com/marcgauthier/spedsql
```

Open a node as in the [project README quick start](../README.md#quick-start):
`Config{Path, NodeID, Schema, Pebble, Encryption, Replication}` with a
cluster-shared `DBID`, one CA, and per-node certificates carrying the
`replicateddb://node/<uuid>` URI SAN (see §3.2). Every replicated table
needs an explicit `BLOB(16)` primary key; see
[Schema rules](../README.md#schema-rules-v1).

### 3.2 Certificates & mTLS

Replication is mTLS-only. Each node certificate must carry its NodeID as
a URI SAN `replicateddb://node/<uuid>`; the engine loads certificates and
rejects mismatched identities. Provision with the `transport` package:

```go
ca, _ := transport.GenerateCA(365 * 24 * time.Hour)
certPEM, keyPEM, _ := ca.IssueNodeForHosts(nodeID, 90*24*time.Hour,
    []string{"db.example.com"}, nil)
```

Persist `ca.CertPEM` once per cluster and one cert/key pair per node. Rotate before
`NotAfter`; expired certificates are rejected.

### 3.3 Operations

- Health & Metrics: `DB.Status` (state, peers, watermarks, lag, queue depths), `DB.Metrics` (counters), and optional Prometheus collectors (`metrics` package). `DBState` must be `ready` before writes are accepted.
- Backup: stop the node (or checkpoint online via `backup/`), copy the
  data directory. Restore/clone requires a **fresh** NodeID and
  certificate; opening restored data under the old identity is rejected
  (proven by `tests-live/backup-restore`).
- Peer churn: `DB.RemovePeer` retires and persists exclusion (survives
  restart, releases GC retention); `DB.AddPeer` readmits with fresh obligations.
  Proven by `tests-live/churn-retirement`.
- Rolling schema change: additive migrations only (`DB.Migrate`), one node at a time;
  mixed-version meshes keep replicating. Proven by
  `tests-live/schema-evolution`.
- Upgrades: on-disk format is versioned (`format_version` = 2 with
  minimum reader/writer enforcement); replication handshakes negotiate
  capabilities and refuse unknown required bits, so mixed-version
  clusters fail closed instead of diverging. The compatible upgrade
  paths (rolling previous-to-current binaries, current binary opening
  a previous-release store, current restore of a previous-release
  backup) are proven by `tests-live/release-upgrade`.
- Loss window: default durability syncs Pebble before every write
  acknowledgement. `DurabilityAsync` returns without a disk sync and
  syncs about once per second plus on graceful close; use it only when
  the application accepts losing roughly the last second after a crash.

### 3.4 Verifying live multi-process scenarios

The live test suite validates multi-node cluster behavior across isolated processes using internal test fixtures:

```sh
bash tests-live/run.sh three-node-sync    # convergence smoke
bash tests-live/run.sh snapshot-resync    # stale-node snapshot path
bash tests-live/run.sh backup-restore     # restore identity rules
bash tests-live/run.sh all                # full live matrix (long)
SPEDSQL_TAGS=modernc bash tests-live/run.sh three-node-sync  # pure-Go backend
```

## 4. Verification record

The entries below preserve previously recorded results. Their source snapshots
were not fully identified by immutable commits, and their suite counts describe
the runner at the time. Use `tests-live/run.sh` for the current suite selection.
They must not be relabeled as successful runs against the current checkout.

SQLite backend refactor verification, 2026-09-28, linux/amd64:

- `go vet` and `go build` passed with both the default mattn tags and
  `CGO_ENABLED=0 -tags modernc`.
- Both `sqlengine` suites and all non-`tests-live` packages passed with
  `-short` under both drivers.
- The sustained concurrent-reader check passed at 1, 2, 4, 8, 16, and 32
  readers under both drivers.
- The live `three-node-sync` convergence scenario passed under both drivers.

2026-09-28, host linux/amd64 (Intel i5-6500), Go 1.26.x, HEAD
`b81e5c1` plus then-uncommitted changes (exact patch not recorded):

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

Not run in that recorded verification: full `-race` suites, multi-hour and
impaired-network soaks (pending acceptance, matrix #18; the 10-minute,
2-hour, and 1-hour soaks now run on a weekly CI schedule plus manual
dispatch via `bash tests-live/run.sh soak`; no scheduled run has completed
yet according to that historical entry), the three-minute default
`allow-nodes` run, and native Windows tests (configured in CI). Linux arm64
has cross-build coverage only; no native arm64 test job is configured.
A `go test -race` parent process does not instrument the separately built live
node: the current runner builds that executable without `-race`.

2026-09-28 follow-up (snapshot progress + live gate): `live-gate` CI job
runs `bash tests-live/run.sh gate` (smoke, encryption, backup/restore,
partitions, version skew, High/Low, files, scale mesh, upgrades,
snapshot resync, GC balance, rolling restart, crash recovery) against
the freshly built daemon on every push/PR, plus a pure-Go live smoke;
all gate suites pass locally (completion notes were in TASKS_COMPLETED.md
under SNAPSHOT-PROGRESS-001, LIVE-GATE-001, LIVE-GROUP-A-001; the file was
removed since, content survives in git history). Do not run two
runner invocations against one checkout concurrently; isolate with
`SPEDSQL_LIVE_RUNTIME`.

2026-09-28 daemon API mTLS follow-up: the listener now serves HTTPS only,
verifies optional client certificates during the handshake against the
configured CA, and rejects missing certificates at every route except
`GET /healthz`. The full live release gate passed, including the new
`api-mtls` scenario; see the 2026-09-28 20:58:47 UTC entry titled
“Require HTTPS with client-certificate verification for the daemon API” in
TASKS_COMPLETED.md (removed since; content survives in git history). This
is a historical report for the then-working tree, not verification of a
recorded release commit. The listener now lives in the internal test-node
fixture; the embedded package starts no HTTP listener.

2026-09-29 release and upgrade verification of candidate
`31bf1b582fd21b50b049d0c0b9a47d5d4589a3ac` (“Rename module to
github.com/marcgauthier/spedsql”), linux/amd64, Go 1.26.x, executed in
an isolated worktree checkout of that commit (clean tree, no patch):

- `go build` with both tag sets and `go vet` with both tag sets — pass.
- Cross-compiles: linux/arm64 and windows/amd64 (`modernc`) — pass.
  The previous release `4bdd297` also cross-builds for both targets.
- `go test -tags "sqlite_preupdate_hook sqlite_fts5" -count=1 ./...`
  — 54 ok; sole failure `tests-live/partial-mesh`
  TestDynamicBootstrapDiscovery (SWIM discovery flake, peer-owned WIP:
  fails intermittently, passes other runs).
- `go test -tags "modernc" -count=1 ./...` — 55 ok, fully green.
- `go test -race -tags "sqlite_preupdate_hook sqlite_fts5" -count=1
  ./...` — 54 ok with no data-race reports; sole failure
  `tests-live/compression` (peer's new suite, not wired into run.sh;
  exceeds the 10-minute package timeout under `-race`, passes
  non-race). Two genuine data races found during this cycle were fixed
  in this candidate: snapshot receive state shared across concurrent
  streams (now mutex-guarded) and WaitGroup Add racing Wait on attach
  during shutdown (now gated on closed under m.mu); both verified with
  targeted `-race` stress plus full root/replication `-race` runs.
- `bash tests-live/run.sh gate` — exit 0, 41 `RESULT: PASS`, zero FAIL,
  including the new `release-upgrade` scenario (3/3): rolling upgrade
  of previous-release binaries to the current build under continuous
  writes with a mixed-version replication proof mid-roll, current-binary
  open of a previous-release store (byte-identical digest), and current
  restore of a previous-release backup. Previous release for all three:
  `4bdd297` (old module path), so the suite also proves binary
  compatibility across the module rename. The suite additionally passes
  with `SPEDSQL_TAGS=modernc`.
- Load-flake hardening in this candidate: admission/fetch-completed
  counters polled instead of asserted immediately (an event counter
  trails its durable record across goroutines), gc-balance adaptive
  window plus post-stop drain, plumtree at-least-once delivery,
  crash-recovery/rolling-restart/partial-mesh convergence waits
  extended, crash waitForPeers diagnostics fixed, rolling-restart
  divergence forensics (per-node `/v1/status` dumps).

Exclusions and follow-ups (clear before production claims): the
compression suite under `-race` (not in any gate), scheduled
multi-hour soaks (never run), native Windows/arm64 execution
(CI-owned), and `gofmt` on 5 peer-owned files. TASKS_*.md trackers
were removed in this cycle, so older entries' pointers to
TASKS_COMPLETED.md are historical. No release tag has been cut; see
[§5](#5-release-commit).

The post-restart heal stall listed here in earlier revisions is FIXED
(uncommitted at this writing): root cause was a `peerSession`
close/send ABBA deadlock — `close` took `dataWriteMu` while running
under `peer.mu` (attach tie-break replacement, shutdown drain,
`RemovePeer`), while data senders held `dataWriteMu` across `peer.mu`
traffic accounting. After a restart the losing session's close met an
in-flight send and both wedged forever: frozen row gap, dead link, and
a hung `/v1/status`. Fix: the data stream handle is now atomic, so
`close` never takes `dataWriteMu` (replication/manager.go). Proof:
new in-process `TestRestartChurnHealsInProcess` hung 1/15 runs
pre-fix (stacks show the exact cycle) and passes 55/55 post-fix
(40 plain + 15 `-race`, zero race reports); new live
`TestRollingRestartChurn` plus the rolling-restart suite pass under
contention; full live gate green on the fixed build. Any release
candidate containing this fix needs re-verification per §5 step 3.

The SWIM discovery flake is likewise FIXED (uncommitted): every
replication session ran two AcceptStream consumers on one QUIC
connection (replication's plus membership's), which split incoming
streams at random — a data stream the membership loop won was dropped
as a header mismatch while the sender's writes kept succeeding into
the half-closed stream (captured live: 1210 batches sent, 0 streams
accepted, heal only on 30s+ recycle; ~30% of runs took 37-100s).
Fix: replication adoption yields the membership consumer
synchronously before starting its own loops, so exactly one consumer
accepts per connection (transport/memberlist.go,
replication/manager.go attach order). Proof: new
`TestReplAdoptionYieldsMembershipAcceptLoop` (fails without the
yield); `TestDynamicBootstrapDiscovery` 16/16 at ~1.1s post-fix;
peer's swim-discovery suite 3/3; full live gate green. Per user
decision 2026-09-29, discovery stays a v1.0 feature (stabilize, not
experimental); needs re-verification per §5 step 3 like the above.

Test-harness changes in the same uncommitted set: the live gate now
also runs `partial-mesh` (discovery), `migration-crash`,
`pause-resume`, and `graceful-shutdown` (mid-snapshot SIGTERM
intercept made deterministic via member-deadline sleep plus a polled
GC pass and a widened ~2MB snapshot payload; 4/4 including contention
and modernc); new scheduled `run.sh stress` repetition lane plus
`live-stress` CI job; shared `Cluster.DumpForensics` helper (used by
rolling-restart and partial-mesh); daemon straggler reaping in
harness cleanup plus a `run.sh` exit trap; CI `modernc`/`-race` live
lanes widened to three suites each.

## 5. Release commit

No release tag has been cut, so no verified release commit is recorded
here. Candidate `31bf1b582fd21b50b049d0c0b9a47d5d4589a3ac` was verified
in an isolated checkout per the procedure below; results and explicit
exclusions are attached in [§4](#4-verification-record) (2026-09-29
entry). Cutting a release from this candidate still requires clearing
those exclusions, re-verifying, and recording the tag.

Before publishing:

1. Review and commit the intended release changes; exclude unrelated work.
2. Record the full candidate hash and verify an isolated checkout of that hash.
3. Run the required builds, vet, both backend suites, live gate/full matrix,
   and production-duration/platform acceptance. Record exclusions explicitly.
4. Attach results to the candidate using this evidence format:

| Candidate commit | Command / suite and duration | Backend / build tags | OS / arch / Go | Result / log artifact |
|---|---|---|---|---|
| `31bf1b5` (full hash above) | builds, vet, CGO + modernc + race `./...`, live gate; see §4 2026-09-29 entry | CGO `sqlite_preupdate_hook sqlite_fts5` + `modernc` | linux/amd64, Go 1.26.x | Pass with exclusions (compression under race, soaks, native win/arm64, gofmt); heal stall + discovery flake fixed after this candidate, need re-verification; no release claim |

Record the resulting release tag and candidate hash in the release notes.
Documentation-only updates may reference the tested candidate; any runtime
change requires verification of the new candidate.
