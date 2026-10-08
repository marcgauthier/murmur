# Release status and verification record

Current implementation inventory, configured platform checks, and release
qualification evidence. Code presence, available tests, and successful execution
against a specific revision are separate claims.

[Architecture index](README.md) · [Project README](../README.md) ·
[Versioning and release](versioning-and-release.md) ·
[Migration plan](migration-plan.md)

The status below was reconciled against the current worktree on 2026-10-08.
No release tag or clean candidate revision is claimed.

## Contents

- [1. Verified feature matrix](#1-verified-feature-matrix)
- [2. Supported platforms](#2-supported-platforms)
- [3. Deployment](#3-deployment)
- [4. Verification record](#4-verification-record)
- [5. Release commit](#5-release-commit)

---

## 1. Verified feature matrix

“Implemented” means a production code path exists. It does not mean every
release acceptance gate has passed on the current worktree.

| Capability | State | Evidence and remaining qualification |
|---|---|---|
| Managed typed Go API and rich record schemas | Implemented | `records.go`, `internal/recordcodec/`; root and typed-record live suites. |
| RIME MVCC materializer and query indexes | Implemented | `rime/`; model-equivalence, transaction, index, race, and ownership suites. |
| Durable-first encrypted Spool commits and recovery | Implemented | `state/`, `spool/`, `internal/rimeadapter/`; continue broad storage fault and process-kill qualification. |
| Synchronous and asynchronous durability, typed group commit | Implemented | `group_commit.go`, `state/local_group.go`; continue shared-fsync and crash-boundary qualification. |
| Masterless QUIC/mTLS replication, CRDTs, snapshots and schema evolution | Implemented | `replication/`, `state/`, `schema/`; three-origin delivery permutations with duplicates, exact before/after manifest-store crash recovery on an isolated peer with two surviving peers, and same-record/disjoint-key local/remote commits pass; broader transport/interleaving stress remains. |
| Typed bridges, files, backup/restore, subscriptions, key rotation and maintenance | Implemented | `bridge/`, `files*.go`, `backup/`, `record_subscriptions.go`; full live release gate, one-hour five-node persistence soak, and two-hour three-node encrypted-write soak passed; extended chaos soaks remain. |
| SQLite/SQL runtime removal | Implemented in production | Root module has no SQL application API, SQLite engine, or SQLite driver. Format-5 stores and protocol-5 peers are rejected by the RIME cutover. Historical comparisons are isolated in nested benchmark modules. |
| Legacy live-suite cutover | Incomplete | The full sequential release gate passed, including transaction-chunk resume, crash, partition, hostile-peer/schema, corrupt-snapshot, ten-node scale, and previous-release format rejection. Remaining SQL calls are confined to previous-release compatibility fixtures and explicit removed-endpoint rejection tests; scheduled soak qualification remains. |
| Fixed-host rich-record performance acceptance | Pending | Complete reproducible workload cells and resource-limit reporting in `architecture/rime-performance-plan.md`. |

## 2. Supported platforms

GitHub Actions currently configures Linux amd64 with Go 1.26.x. Production
storage and the normal build do not require CGO; CI enables CGO for race
detection and live tooling. No Windows or non-amd64 support claim is made by
the configured workflow.

| OS / architecture | Build and test mode | Configured checks |
|---|---|---|
| Linux amd64 | Go 1.26.x; normal production build supports `CGO_ENABLED=0` | `go mod verify`, `go vet ./...`, `go test -race ./...`, benchmark-module tests, live gate, scheduled soaks and stress. |

See [CI configuration](../.github/workflows/ci.yml) and the
[live scenario guide](../tests-live/README.md). Workflow configuration alone
is not evidence that the latest worktree passed those jobs.

## 3. Deployment

Murmur is an embedded Go library. Applications open a fresh encrypted Spool
directory with stable typed definitions in `Config.Tables`, then access data
through Murmur's managed table and query API. Applications supply replicated
`RowID` primary keys. `README.md` contains the quick start and current API
example.

The SQL application API and SQLite runtime have been removed. Format-5 SQL-era
stores fail closed without being rewritten; use the previous release to export
legacy data and start this release with a fresh directory. Protocol-5 peers are
not compatible with protocol 6. See [the migration plan](migration-plan.md)
for the breaking changes and remaining qualification gates.

## 4. Verification record

Fresh worktree evidence (Linux amd64, 2026-10-08):

- `CGO_ENABLED=0 GOMAXPROCS=1 go test -p 1 . -count=1` passed the Murmur root
  package in 139.6s on the current worktree after the tombstone-resurrection
  correction.
- `CGO_ENABLED=0 GOMAXPROCS=1 MURMUR_LIVE_RUNTIME=/tmp/murmur-migration-maintenance-20261008 bash tests-live/run.sh maintenance-under-load` passed in 42s: three nodes observed log GC during active zero-error writers and converged.
- `CGO_ENABLED=0 GOMAXPROCS=1 MURMUR_LIVE_RUNTIME=/tmp/murmur-migration-delete-pruning-20261008 bash tests-live/run.sh delete-pruning` passed in 30.6s after the tombstone fix: three nodes converged through concurrent deletes, updates, and resurrections; 770 log batches were collected.
- One standard-tier `perf-matrix` mesh/reconnect run passed in 142.9s across 1/2/5-node mesh blasts and 1,000/10,000-row reconnect backlogs. An unrelated Go test was active on the host, so its measurements are exploratory and not fixed-host acceptance.
- `CGO_ENABLED=1 GOMAXPROCS=1 bash tests-live/run.sh gate` passed every
  sequential release-gate scenario in 936s. Typed scenarios used the runner's
  CGO-disabled builds; the previous-release compatibility fixture used its
  pinned SQLite-era build.
- `CGO_ENABLED=0 GOMAXPROCS=1 MURMUR_SLO_DURATION_SECONDS=7200 MURMUR_SLO_SETTLE_SECONDS=300 MURMUR_LIVE_RUNTIME=/tmp/murmur-migration-soak-bounded-20261008 bash tests-live/run.sh soak-slo` passed in 7,216.7s: three nodes, 597,169 operations over 3,000 bounded rows, write p95 107.4ms, maximum 2.47s, converged state digest, and runtime cleanup.
- `CGO_ENABLED=0 GOMAXPROCS=1 MURMUR_FIVE_NODE_DURATION_SECONDS=3600 MURMUR_FIVE_NODE_SETTLE_SECONDS=300 MURMUR_FIVE_NODE_INTERVAL_MS=200 MURMUR_LIVE_RUNTIME=/tmp/murmur-migration-five-node-rerun-20261008 bash tests-live/run.sh long-running-five-node` passed in 3,660.4s: five nodes, 72,925 acknowledged writes, two successful in-load data-directory restarts, converged digest, and runtime cleanup. It recorded 35 transient HTTP write errors.
- `CGO_ENABLED=0 GOMAXPROCS=1 MURMUR_ENDURANCE_NODES=5 MURMUR_ENDURANCE_DURATION_SECONDS=600 MURMUR_ENDURANCE_SETTLE_SECONDS=300 MURMUR_ENDURANCE_WRITE_INTERVAL_MS=100 MURMUR_LIVE_RUNTIME=/tmp/murmur-migration-endurance-fix-20261008 bash tests-live/run.sh endurance-chaos` passed in 630s after the tombstone-resurrection fix: five nodes converged after 20,913 acknowledged writes, five restarts, three key rotations, seven logical peer flaps, four disk-pressure holds, two snapshot resyncs, and GC on all nodes. `tc/netem` and libfaketime were unavailable, so packet impairment degraded to logical flaps and clock skew was skipped.
- `diskfull-live` and `impaired-network` were invoked on this worktree but skipped by their environment preflights: tmpfs mount was denied and `tc/netem` lacked `CAP_NET_ADMIN`. They are not counted as passing acceptance evidence.
- `CGO_ENABLED=0 GOMAXPROCS=1 go test -p 1 ./... -count=1` was stopped before
  completion after the RIME OCC live test failed to observe contention and the
  following default abuse fixture started ten nodes for its ten-minute run.
  The root package was rerun separately and passed. No all-package pass is
  claimed; scheduled soaks and fixed-host performance acceptance also remain.

The current live gate, root suite, one-hour five-node soak, two-hour
three-node soak, and bounded combined-fault run are observed results, not
inferred from workflow configuration. The all-package, extended capacity/chaos
soak, privileged disk-full and impaired-network, and broader fixed-host
performance gates remain incomplete.

## 5. Release commit

No release candidate or tag is recorded for the current worktree. Before
release, record the exact commit, API and storage/protocol break, complete test
and live-gate results, performance cells, supported platforms, and operator
migration instructions. Keep the prior release available for legacy export.
