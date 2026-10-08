# Testing and acceptance

Crash/convergence/network/encryption tests and alpha acceptance criteria.

[Architecture index](README.md) · [Project README](../README.md)

CI workflow configuration runs root tests, benchmark modules and live
scenarios. The current migration candidate has not passed the complete
repository or release-live gates. Current-version fixtures use managed typed
RIME operations; remaining SQL helpers are limited to negative API tests and
previous-release compatibility fixtures. See the
[migration plan](migration-plan.md#current-milestone-status-2026-10-08) for
the cutover and qualification status. A configured CI job is not evidence of
a passing run. For a supported live scenario, use
`MURMUR_RACE=1 bash tests-live/run.sh <scenario>` to instrument both the Go test
process and the separately built child node. Scheduled stress and fuzz lanes
complement, but do not replace, the deployment rehearsals in
[Operational rehearsals](operational-rehearsals.md).

## Contents

- [55. Crash-Recovery Tests](#55-crash-recovery-tests)
- [56. Convergence Property Testing](#56-convergence-property-testing)
- [57. Network Partition Tests](#57-network-partition-tests)
- [58. Encryption and Storage Tests](#58-encryption-and-storage-tests)
- [89. Initial Acceptance Criteria](#89-initial-acceptance-criteria)
- [Additional capability acceptance](#additional-capability-acceptance)

---

## 55. Crash-Recovery Tests

The `origin-signatures` live scenario is a release gate: an offline origin's transaction must forward successfully, while a trusted hostile relay cannot modify or fabricate that origin's transactions. See [origin signatures](origin-signatures.md) for the exact format and trust boundaries.

The `tests-live/open-progress/` scenario uses managed typed records and real
encrypted Spool in separate processes. It checks multi-table RIME reconstruction,
processed cell counts, unknown totals, committed rows, phases and terminal
events, callback ordering, cancellation, key-validation failure, and subsequent
successful reopens. Run `CGO_ENABLED=0 bash tests-live/run.sh open-progress`;
it is included in `all` and `gate`.
Inject crashes/failures after every important boundary.

Local write:

```text
before typed transaction staging
during RIME preparation
before Spool commit
during Spool commit
after Spool commit
before RIME publication
after RIME publication
before ACK to application
```

Remote apply:

```text
after network receive
after merge calculation
during Spool synchronized commit
after Spool commit
before ACK
after Spool commit and before RIME publication
after RIME publication
```

Restart must always converge to Spool authoritative state.

Also inject crashes during chunk staging/completion, grouped apply, snapshot candidate merge/validation, intent sync, watermark/generation publication (atomic commit, or chunked merge with durable resume plus final publication commit), keyring pinning, and post-publication RIME rebuild. Staging checks cover restart after a partial transfer, a pre-commit interruption with clean retry, and an over-budget transfer eviction; none may advance the applied watermark before complete apply. Before snapshot publication the old generation remains authoritative; afterward recovery completes from the published candidate. Never expose partial transaction effects or mixed-generation watermarks. Inject failures during schema manifest publication and backup restore identity rewriting; the persisted restore marker must prevent same-identity writable rollback on restart.

Single remote apply writes a synced complete-batch prepare record before final apply. The crash test interrupts after prepare and reopens the store; recovery must atomically install winners, log, receipt, receive watermark, HLC, and state generation, remove the prepare record, and make a repeated delivery idempotent. RIME materialization generation is reconciled from authoritative state during open/rebuild.

Typed remote materialization checks confirm updates/delete/resurrection, durable
receipt ordering, and restart reconstruction. Remote and local readers use RIME
snapshots; publication has no separate materialization delay.

---

## 56. Convergence Property Testing

Create a deterministic in-process simulator.

Generate random operations:

```text
insert
update one column
update multiple columns
delete
resurrect
offline node
network partition
reconnect
duplicate batch
delayed batch
out-of-order origin delivery
node restart
interrupted transaction chunks and cross-peer chunk duplicates
snapshot merge with unpropagated offline writes
compatible concurrent schema branches and repeated merges
```

Apply the same logical mutation set in different delivery orders.

Property-based convergence suites in `state/crdt_property_test.go` and `bridge/ownership_property_test.go` randomly permute delivery orders, inject duplicate retransmissions, and verify strong eventual consistency across all CRDT merge policies (`LWW`, `PN_COUNTER`, `OR_SET`, `MAX`, `MIN`) and High/Low bridge shadow ownership records. Invariants checked across all permutations include commutativity, associativity, idempotence, PN-counter algebraic sums, OR-Set add-wins set equivalence, extrema bounds, and High ownership precedence over Low stream updates.

The CRDT package also checks comparison reflexivity, antisymmetry, and
transitivity, folds a competing set of cell versions through several delivery
orders, and exercises equal-version and equal-HLC tombstone visibility
boundaries. These focused checks protect the merge ordering primitives used by
the larger replication convergence simulator.

Typed query tests verify point and indexed reads, stable snapshots, callback
ownership, and that closed read transactions release pinned RIME history.

Focused `state` tests cover origin-log scan pagination, making progress when a
single batch exceeds the requested byte budget, and preserving the scan cursor
when a callback fails. Replication protocol tests cover watermark-list round
trips and reject missing counts, truncated entries, and absurd entry counts.
Run them with `go test ./state ./replication`.

> Managed typed scenarios run without SQLite build tags and support
> `CGO_ENABLED=0`. Use `bash tests-live/run.sh <scenario>` to build the child
> fixture and run one live scenario at a time. Cluster scenarios use an
> isolated runtime and a separately built child process.

The native typed-record rehearsal at `tests-live/typed-records/` verifies
offline CRDT convergence, snapshot catch-up, restart reconstruction, older-peer
schema adoption, and two peers creating distinct additive schema branches
during a partition. Rejoin must converge both manifests and retain both branch
field values. It also uploads a file from a typed node, verifies metadata
replication, fetches and verifies the remote bytes, and checks replicated
deletion. Run it with `CGO_ENABLED=0 bash tests-live/run.sh typed-records`.

The `tests-live/typed-bridge/` rehearsal imports into a native RIME table,
verifies High ownership blocks a later Low update, releases the field through
the embedded API, then verifies the next Low update is visible after restart.
Run it with `CGO_ENABLED=0 bash tests-live/run.sh typed-bridge`.

The live integration port at `tests-live/crdt-contention/` runs three encrypted
QUIC nodes and verifies that concurrent updates to disjoint columns all
survive replication. Run it with `go test -count=1 ./tests-live/crdt-contention`.
It covers the disjoint-column invariant from GALVANIZE's CRDT contention
scenario; the multi-process daemon scenario below additionally covers
shared-cell contention, while delete pruning remains unported.

The managed-view integration at `tests-live/views/` uses an application Go
query over `Config.Tables` and managed RIME records; it checks replicated
filter/order results on three nodes and after one node restarts. It runs with
CGO disabled: `CGO_ENABLED=0 bash tests-live/run.sh views`.

The large-payload port at `tests-live/large-payload/` sends 250 separate row
inserts and a separate 1.5 MiB value insert across two encrypted QUIC nodes, then checks
the exact value digest and row count. Run it with
`go test -count=1 ./tests-live/large-payload`. It covers the direct mesh data
path and eventual fidelity, not atomicity of a single 251-row transaction,
interrupted-transaction restart, or cross-peer chunk repair. High/Low bundle
transport is covered separately by
`tests-live/highlow/` and `tests-live/files-bridge/`.

The allow-nodes port at `tests-live/allow-nodes/` uses valid NodeID-bound
certificates while restricting one peer to a loopback `/32`; it verifies the
allowed peer replicates and the disallowed address does not connect. Run it
with `go test -count=1 ./tests-live/allow-nodes`.

The rekey port at `tests-live/rekey/` rotates an application's wrapping key,
reopens with the new material, checks prior rows and a post-rotation write, and
rejects the old key. Run it with `go test -count=1 ./tests-live/rekey`.

The encryption port at `tests-live/encryption/` writes a unique plaintext
marker, verifies it is absent from durable storage, rejects a wrong key, and
reopens with the correct key to read the value. Run it with
`go test -count=1 ./tests-live/encryption`.

The `tests-live/three-node-sync/` smoke port concurrently commits 90 managed
typed records across three encrypted QUIC nodes and checks identical ordered
results on every replica. It runs with CGO disabled:
`CGO_ENABLED=0 bash tests-live/run.sh three-node-sync`.
It covers convergence mechanics from GALVANIZE's sustained-sync benchmark,
not its throughput or five-minute duration targets.

The `tests-live/open-progress/` subprocess rehearsal rebuilds typed RIME tables
from encrypted Spool, validates row/cell counts and callback ordering, and
checks restart, cancellation, failure, heartbeat, and status snapshots. Run it
with `CGO_ENABLED=0 bash tests-live/run.sh open-progress`.

The `tests-live/crash-recovery/` scenario runs three daemon processes with
concurrent managed typed writes, kills two nodes with `SIGKILL` while typed
requests are in flight, restarts their existing encrypted stores, and verifies
acknowledged rows and ordered digests converge. It then commits a new write.
Each write window defaults to 12 seconds and can be adjusted with
`MURMUR_CRASH_WRITE_SECONDS`; `MURMUR_CRASH_SETTLE_SECONDS` controls the final
convergence deadline. Run it with
`CGO_ENABLED=0 bash tests-live/run.sh crash-recovery`.

The `tests-live/crdt-contention/` scenario performs concurrent disjoint-column
updates followed by shared-cell contention from three daemon processes. It
requires the independent values to survive and all processes to expose the same
LWW cell winner and ordered logical-state digest. Run it with
`go test -count=1 ./tests-live/crdt-contention`.

The `tests-live/long-running-five-node/` scenario keeps five encrypted daemon
processes writing over a full mesh, restarts two node directories during load,
and verifies retained rows, common logical-state digests, materialized
generations, and persistent-store capacity. Its smoke run is 30 seconds. The
one-hour profile passed on 2026-10-08 with 72,925 acknowledged writes, two
successful persistence restarts, and a converged five-node digest. The test
harness gives a single unlock request up to 60 seconds because reopening an
existing store can outlast the ordinary live-request timeout. See the
[five-node soak record](../tests-live/long-running-five-node/README.md).

The four-node partition smoke test at `tests-live/partition/` splits a full
mesh into two pairs, verifies writes stay isolated, heals the mesh, compares
ordered state digests, and checks a later write replicates. Run it with
`go test -count=1 ./tests-live/partition`. It does not cover SWIM discovery,
multi-hop forwarding, churn, or scaling limits.

The local encrypted object-store tests stream multi-chunk content through
`objectstore.Put` and `objectstore.Read`, check digest/idempotency and absence
of plaintext at rest, and reject wrong keys, tampering, truncation, and
cancellation. They also verify inventory, reference/grace collection, explicit
reader pins, and collection racing with an active streaming read.
`TestPinConcurrentWithCollect` repeatedly races `Pin`, `Serve`, and authenticated
`Read` against zero-grace collection. Successful pins and serving handles must
keep the object readable until close, and all released objects must become
collectible again. Run them
with `go test ./objectstore`.

The `tests-live/chaos-load/` scenario keeps all three daemon processes writing
while node3 is partitioned from node1/node2, checks group row counts and
divergent digests, then re-admits peers while writes continue and verifies
convergence plus a post-heal write. The partition and healing windows default
to 12 seconds each and can be configured with
`MURMUR_CHAOS_PARTITION_SECONDS` and `MURMUR_CHAOS_HEAL_SECONDS`. Run it with
`go test -count=1 ./tests-live/chaos-load`. Abrupt process death is covered by
the separate crash-recovery test.

The `tests-live/files-soak/` scenario uploads files repeatedly over an
encrypted two-node QUIC mesh, checks metadata replication and search, fetches
each payload from its peer, validates SHA-256, and reports upload/fetch p95
latencies against broad smoke SLO bounds. Run the short form with
`go test -count=1 ./tests-live/files-soak`; set
`MURMUR_FILES_SOAK_DURATION_SECONDS=600` and
`MURMUR_FILES_SOAK_INTERVAL_SECONDS=60` with `-timeout=12m` for the
ten-minute acceptance run.

The `tests-live/files-bridge/` scenario covers two Low peers, a recipient-sealed
Low-to-High file import, and two High peers. High-2 learns metadata through its
mesh and fetches the verified object from High-1. Run it with
`go test -count=1 ./tests-live/files-bridge`.

`bridge.TestImportCompletionStorageFailureRecovery` injects real filesystem
write/sync failures between imported effects and completion, and between row
and file commits in a mixed bundle. After reopening encrypted storage it checks
that completion receipts and progress recover together, source row receipts
survive, and retry repairs completion and file metadata. It also interrupts
row-only completion before writing to prove replay repairs the missing bundle
receipt. `state.TestCompleteBridgeImportPreservesReceiptsAndProgress` checks
authenticated receipt preservation and monotonic replay progress;
`state.TestCompleteBridgeImportFailurePublishesNoReceipts` rejects a corrupt
progress read without publishing any staged receipts.
`bridge.TestDrainReturnsProgressReconciliationErrors` verifies that database
read and inbox journal-write failures during progress reconciliation are returned.

The `tests-live/snapshot-resync/` scenario stops one of three meshed daemons
running aggressive log retention, writes the survivors past retention and a
log-GC pass, then restarts the stale node: its needed ranges are gone, so it
must rejoin via snapshot without discarding its acknowledged pre-stop rows.
The snapshot path is proven by the receiver's `spedsql_repl_snapshots_received_total`
counter alongside digest convergence. A second test restarts two stale nodes
at once against one survivor so their snapshot requests overlap: the source
serves one transfer per peer with explicit busy deferrals, and both rejoins
converge with snapshot counters and progress/deferral series present.
Run them with `go test -count=1 ./tests-live/snapshot-resync`.

The `tests-live/backup-restore/` scenarios use managed typed records to stop a meshed daemon, back up
its durable directory offline, wipe it, restore under a fresh writer
identity with a reissued certificate, and restart into a reconverged mesh
with no lost rows; a second test proves opening restored data under a
stale identity is rejected. A direct typed backup round-trip also reconstructs
the RIME materializer from the restored Spool. Run them with
`bash tests-live/run.sh backup-restore` (CGO disabled).

The `tests-live/partial-mesh/` scenario peers three daemons as a chain
and requires writes to converge end to end via origin forwarding, with
provably no direct session between the unlinked ends (SWIM discovery is
not yet wired into the runtime). Run it with
`go test -count=1 ./tests-live/partial-mesh`.

The `tests-live/schema-evolution/` scenario validates three-node typed additive
schema adoption, writes from an older binding without erasing a newly added
field, late inserts, CRDT counters, and restart reconstruction. Run it with
`CGO_ENABLED=0 bash tests-live/run.sh schema-evolution`.

Typed schema migration, grouped local commits, and remote apply are tested in
their respective record, replication, and group-commit suites. They assert
durable receipts, schema epochs, remote watermarks, atomic RIME publication,
and reopening durability without a separate query-projection phase.

`TestTypedLocalAndRemoteCommitsInterleaveAndRecover` races 64 local typed
insert transactions and a same-record update against 65 remote batches on one
database. It verifies every row before and after reopen, computes the same-record
LWW winner from committed HLC/origin versions, and confirms remote batches never
echo to the local origin log. `state.TestConvergenceProperty` delivers three origins' batches in
different orders with duplicate replays and checks identical replica state.
The `tests-live/migration-crash/` scenario injects process exit immediately
before and after manifest storage on an isolated peer, then verifies restart
and convergence with two peers. Broader transport/interleaving stress remains
open.

The `tests-live/overload-budgets/` scenario runs two daemons with small
commit budgets and requires clean rejection of an oversize HTTP body and
an over-budget value (no partial rows, writer stays healthy), then
converges a concurrent 400-insert blast with bounded tail latency. Run it
with `go test -count=1 ./tests-live/overload-budgets`.

The `tests-live/churn-retirement/` scenario excludes a meshed peer via
the admin API and requires session drops, write isolation, GC retention
release, restart persistence of the exclusion, and healing on explicit
re-add. Run it with `go test -count=1 ./tests-live/churn-retirement`.

The `tests-live/plumtree-live/` scenarios converge writes across an
all-Plumtree three-daemon mesh, and prove required capability
negotiation by isolating a gossip-mode node meshed with Plumtree peers
(convergence on both sides plus recorded handshake refusals). Run them
with `go test -count=1 ./tests-live/plumtree-live`.

The `tests-live/bridge-two-streams/` scenarios import two independent
Low domains into one High cluster (one stream per High node): rows from
both domains converge with Low-owned provenance, a single-stream second
round leaves the other stream's progress untouched, and a cross-domain
row-identity collision fails closed with first-writer state intact. Run
them with `go test -count=1 ./tests-live/bridge-two-streams`.

The `tests-live/rolling-restart/` scenario restarts each node of a
three-daemon mesh in turn while writers hammer every node: no write to
a live node may fail, and the mesh converges to identical digests after
each restart. Run it with `go test -count=1 ./tests-live/rolling-restart`.

The `tests-live/version-skew/` scenario meshes two daemons with a third
that advertises handshake protocol version 99: the skewed node never
connects in either direction and exchanges no data, but stays alive and
keeps serving local reads/writes while the compatible pair converges.
Run it with `go test -count=1 ./tests-live/version-skew`.

The `tests-live/scale-mesh/` scenario converges 400 rows from two
origins across ten daemons at the default fanout of 4, with a 2s sampler
asserting no node ever selects more than 4 replication targets. The current
worktree passed the live runner in 7.3s with matching digests and runtime
cleanup. Run it with `bash tests-live/run.sh scale-mesh`.

The `tests-live/gc-balance/` scenario updates a fixed 200-key set with
eight parallel updaters under aggressive retention: after a 20s warmup,
GC must collect at least 90% of the origin-log batches a 40s window
produces (commits vs `spedsql_gc_log_collected_total`), proving the
retained set does not grow, with exact row counts and identical digests
at the end. Run it with `go test -count=1 ./tests-live/gc-balance`.

The `tests-live/release-upgrade/` scenario checks out the pinned previous
release into a scratch worktree and builds its daemon. It verifies the format
cutover by requiring the current binary to reject a previous-release store and
backup. Legacy rows are seeded through the previous binary; the test does not
convert them into the new RIME schema in place. Legacy and signed peers cannot
interoperate during this strict cutover. Run it with
`bash tests-live/run.sh release-upgrade` (or
`go test -count=1 ./tests-live/release-upgrade` after building the
current test-node binary); it needs a git checkout containing the
previous ref, so CI checks out full history (`fetch-depth: 0`).

The `tests-live/soak-slo/` scenario writes managed typed contention records
through three encrypted daemon processes, then gates logical digest convergence,
writer p95/max latency, service readiness, materialized generation, queue
depth, and disk size. The five-second smoke command is
`go test -count=1 ./tests-live/soak-slo`; the two-hour acceptance run is
`MURMUR_SLO_DURATION_SECONDS=7200 MURMUR_SLO_SETTLE_SECONDS=300 go test
-count=1 -timeout=3h ./tests-live/soak-slo`.

Recorded acceptance on 2026-09-27: the ten-minute file run completed 10
rounds and verified 655,360 fetched bytes (upload p95 8.51 ms; metadata,
fetch, and verification p95 35.13 ms). A 15-second three-node write run
committed 4,199 rows (1,410/1,361/1,428 per node), converged to digest
`6ed3f99c5abb450a6ffcc6e7507cd66c21d008d03aa9e8897c23a245ebe5ce68`, and
measured local-write p95 17.52 ms and maximum 322.72 ms. Queue, readiness,
materialized-generation, and disk-size gates passed.

The `tests-live/highlow/` scenarios move logical writes from a real Low
database to real High databases across a directory drop: disconnected
transfer with one-way role gating, gap delivery with duplicate/reordered
recovery, two-receiver convergence, forgery/corruption/misnaming rejection
without partial apply, outbox/inbox restart resumption, schema-hold
survival across restart with release after a local High migration, and
signer rotation plus outage catch-up, and High-owned-field reorder
convergence across two meshed High peers with opposite import/override
orders (plus release convergence). Run them with
`go test -count=1 ./tests-live/highlow`. Hostile-HLC policy/value
interleavings are pinned at the DB layer by
`TestBridgeShadowReorderConvergence`, single-peer override/release and
Low-delete protection by `bridge/ownership_test.go`, and file transfer by
`tests-live/files-bridge/`; staging-byte budgets remain unasserted.

The `tests-live/addrpolicy/` scenario launches three standalone daemon
processes in isolated node directories. A loopback CIDR admits node1's path to
unrestricted node3, while a TEST-NET policy prevents node2 from forming any
loopback session. Service API writes verify permitted replication and that
node2's write remains local. Run it with
`go test -count=1 ./tests-live/addrpolicy`.

The `tests-live/allow-nodes/` scenario launches three daemon processes with
valid certificates from one CA. Node2 allows only node1, while node1 and node3
accept all peers. Concurrent writes run for three minutes by default; the test
keeps node2's direct peer count at one, then checks row-count and ordered digest
convergence through node1. Set `MURMUR_ALLOW_NODES_WRITE_SECONDS` to adjust
the workload duration. Run it with `go test -count=1 ./tests-live/allow-nodes`.

Typed record subscription model coverage lives in `records_test.go`: it checks
initial snapshots, detached ownership, keyed additions/updates/removals, cursor
resume and expiry, materializer-generation resets, bounded-buffer resets, and
subscriber-limit admission/release, and subscription closure. The `tests-live/typed-records/` rehearsal holds a typed
subscription on one peer while a remote RIME write commits on the other, then
requires an update event with the matching row count. SQL query subscription
workers and their testnode API have been removed.

`TestTypedDurableCommitCrashBoundaries` in `records_test.go` injects failures
immediately before and after the typed Spool commit. The pre-commit failure
must leave the row absent; the post-commit lost acknowledgement must report an
uncertain transaction ID whose receipt and row are present after reopening.
This unit-level boundary test complements the separate-process recovery
rehearsals; it does not replace them.

The `tests-live/loadshare/` scenario saturates one node with 240 local
writes while its peer replicates 20 rows, then requires full
convergence, acquisitions in both scheduler classes, service debt
within the 1s bound, and reports the local-commit latency distribution.
Run it with `go test -count=1 ./tests-live/loadshare`.

The `tests-live/delete-pruning/` scenario shares 300 rows across three
daemons, then concurrently deletes, updates, and resurrects disjoint
thirds under aggressive log retention. A log-GC pass is forced
mid-flight and all nodes must converge on exact counts plus equal
PK-ordered digests. Run it with
`go test -count=1 ./tests-live/delete-pruning`.
Post-fix Murmur qualification passed on 2026-10-08 in 30.6s with 300 rows:
all three daemons converged after concurrent delete/update/resurrection, and
GC collected 770 log batches.

The `tests-live/tail-repair/` scenario partitions one node during a
peer write burst, proves the isolation (frozen count, zero snapshots
sent), then heals: the node must repair its tail from peer logs with
equal digests everywhere. Run it with
`go test -count=1 ./tests-live/tail-repair`.

The `tests-live/txchunk-resume/` scenario commits a large managed RIME
`InsertMany` transaction, SIGKILLs a peer mid-receipt (the kill only counts
when chunks were in flight), and requires the transfer to resume to exactly-once
delivery with atomic visibility and equal digests. Run it with
`CGO_ENABLED=0 bash tests-live/run.sh txchunk-resume`.

The `tests-live/clock-skew/` scenario runs nodes under libfaketime
with skewed clocks and requires concurrent writes to resolve to
identical LWW winners on every node. It skips when libfaketime is
absent. Run it with `go test -count=1 ./tests-live/clock-skew`.

The `tests-live/swim-discovery/` scenario starts nodes with seed
addresses only, requires a connected mesh to form, then kills the
seed and requires rediscovery plus reconvergence with equal digests.
Run it with `go test -count=1 ./tests-live/swim-discovery`.

The `tests-live/impaired-network/` scenario applies tc/netem latency,
loss, and a bandwidth-capped snapshot resync on loopback, each with a
p95-visibility bound. It skips without `CAP_NET_ADMIN`;
`MURMUR_IMPAIRED_NETWORK_FORCE=1` runs the workload unimpaired as a
smoke path. Run it with
`go test -count=1 ./tests-live/impaired-network`.

The `tests-live/flap-partition/` scenario flaps one node's links
through split/heal cycles under continuous writes from all nodes. The
first split carries an isolation proof (marker reaches the intact
peer, never the split node); no write to a live node may fail, and
the final heal must converge every written row. Run it with
`go test -count=1 ./tests-live/flap-partition`.

The `tests-live/three-way-heal/` scenario splits four daemons into
pairs, diverges them with disjoint inserts plus conflicting updates
to the same base rows, then heals link-by-link in two different
orders, waiting for real cross-side traffic at each step. Both orders
must reach the identical digest, including identical LWW conflict
winners. Run it with `go test -count=1 ./tests-live/three-way-heal`.

The `tests-live/pause-resume/` scenario SIGSTOP-freezes one node for
20 seconds while survivors write, asserts the freeze was detected
(alive-view dip or failed SWIM probes), then requires every
acknowledged write on all nodes with equal digests after SIGCONT.
Run it with `go test -count=1 ./tests-live/pause-resume`.

Typed schema migration is qualified by two complementary tests. The
`migration-concurrency` live gate checks concurrent additive publication,
mixed-epoch writes and peer convergence. The root package's
`TestTypedSchemaMigrationProcessCrashBoundary` starts child processes and
SIGKILLs immediately before and after Spool manifest durability, then verifies
the old or complete new epoch and every seeded row after reopen. Run it with
the subprocess test command below.

```sh
CGO_ENABLED=0 bash tests-live/run.sh migration-concurrency
CGO_ENABLED=0 go test -run '^TestTypedSchemaMigrationProcessCrashBoundary$' -count=1 .
```

The `tests-live/backup-under-fire/` scenario takes repeated online
backups via a suite-local agent while both mesh nodes absorb
sustained writes with zero failures. Every backup restores;
intermediate snapshots are non-empty, non-decreasing subsets, and the
final backup matches the survivors exactly. Run it with
`go test -count=1 ./tests-live/backup-under-fire`.

The `tests-live/tampered-backup/` scenario tampers backup artifacts
(byte flips, truncation, manifest edits) and requires every restore
attempt to fail closed, while an untampered backup restores exactly.
Run it with `go test -count=1 ./tests-live/tampered-backup`.

The `tests-live/downgrade-guard/` scenario builds the
previous-release daemon from git history, creates a store with it,
and requires the current build to refuse the combination loudly.
Run it with `go test -count=1 ./tests-live/downgrade-guard`.

The `tests-live/diskfull-live/` scenario runs a node on a small
tmpfs mount: writes past ENOSPC must fail closed with pre-full data
intact, and freeing space plus restart must recover to full
convergence. It skips without mount privilege. Run it with
`go test -count=1 ./tests-live/diskfull-live`.

`TestGroupCommitSharedFsyncFailureFailsEveryMember` queues typed transactions
using the group size cap, blocks their real
encrypted WAL synchronization, and fails that one shared sync with ENOSPC.
No waiter may return before the sync resolves, and every waiter must receive
the storage failure. It checks repair is attempted, reads and future writes
fail closed even after the filesystem recovers, and reopening recovers either
the complete group or none with matching receipts and local sequence.
`TestGroupCommitCrashBeforeSpoolCommit` abruptly exits a child process before
the Spool group commit; reopening must expose only the earlier durable baseline
and accept new writes. `TestGroupCommitCloseWhileGroupPending` uses barriers to
hold a typed group open during shutdown, then seals it and requires every member to finish
successfully and survive reopening. Run these with the other group tests using
`go test -race -tags '' -run '^TestGroupCommit' .`.

The `tests-live/plaintext-audit/` scenario writes random per-run markers
through the managed typed API, proves they round-trip, then scans both nodes'
entire directories plus a backup artifact: marker plaintext and key
material must be absent except in the harness-provisioned config.
Run it with `go test -count=1 ./tests-live/plaintext-audit`.

The `tests-live/file-permissions/` scenario requires every
product-owned secret path (key-registry files, backup/restore
artifacts) to grant nothing to group/other after fresh init and
after backup plus restore. Harness-minted material is audit-logged,
never asserted. Run it with
`go test -count=1 ./tests-live/file-permissions`.

The `tests-live/log-boundedness/` scenario sustains writes while
log-GC runs and requires retained-batch counts and on-disk log size
to stay bounded, with exact convergence at the end. Run it with
`go test -count=1 ./tests-live/log-boundedness`.

The `tests-live/hostile-peer/` scenario throws protocol attacks
(forged/oversized/malformed frames, handshake abuse) from a malicious
fixture peer at an honest mesh: every attack must be rejected
(metrics-observed), honest writes keep converging, and digests stay
equal. Run it with `go test -count=1 ./tests-live/hostile-peer`.

The `tests-live/hostile-schema/` scenario serves hostile schema
manifests from a malicious fixture peer: the mesh must quarantine
them without adopting or wedging, with the schema epoch unmoved and
digests equal. Run it with
`go test -count=1 ./tests-live/hostile-schema`.

The `tests-live/corrupt-snapshot/` scenario feeds corrupt snapshot
chunks to a stale node: every corrupt chunk must be discarded
(rejection observed), and the node must then converge via the honest
resync path. Run it with
`go test -count=1 ./tests-live/corrupt-snapshot`.

The `tests-live/sqli-api/` scenario sends classic SQLi payloads
through the test-node API's exec/query paths and requires every one to
be contained: no out-of-scope rows, honest data untouched, digests
equal. Run it with `go test -count=1 ./tests-live/sqli-api`.

The `tests-live/dos-client/` scenario layers connection abuse (TCP
half-open/partial, TLS idle, QUIC half-open, slow-loris trickle)
while requiring honest writes and replication to keep succeeding
within bounds, with exact convergence afterwards. Run it with
`go test -count=1 ./tests-live/dos-client`.

The `tests-live/unlock-abuse/` scenario requires certless admin
access to be refused, wrong-key and unknown-key-id unlock attempts
to return byte-identical generic 401s (no key/key-ID oracle), and
an honest unlock to succeed with data intact and the mesh
reconverged. Run it with `go test -count=1 ./tests-live/unlock-abuse`.

The `tests-live/files-crash/` scenario SIGKILLs the file uploader
mid-upload and the fetcher mid-fetch (kills only count when the op
was in flight), then requires byte-complete digest-verified
downloads, exact metadata convergence, and an attributable on-disk
inventory. Run it with `go test -count=1 ./tests-live/files-crash`.

The `tests-live/migration-concurrency/` scenario publishes disjoint
additive migrations concurrently from different nodes and requires
convergence on the union schema with all rows intact. Run it with
`go test -count=1 ./tests-live/migration-concurrency`.

The `tests-live/crash-loop/` scenario crash-loops one node while
peers write, then requires the node to rejoin and converge on every
row with equal digests. Run it with
`go test -count=1 ./tests-live/crash-loop`.

The `tests-live/rejoin-storm/` scenario kills two nodes at once
during outage writes, restarts both simultaneously, and requires the
full mesh to reconverge on every row with equal digests. Run it with
`go test -count=1 ./tests-live/rejoin-storm`.

The `tests-live/cert-lifecycle/` scenario swaps a node's certificate
for a short-expiry cert minted for the same NodeID, requires the mesh
to stay connected pre-expiry, then requires fresh handshakes to
reject the node after NotAfter (disconnected, markers never cross,
served cert proven expired at assert time), and requires rotation to
a fresh cert to fully reconverge the mesh. Run it with
`go test -count=1 ./tests-live/cert-lifecycle`.

The `tests-live/tls-floor/` scenario requires the HTTPS API to refuse
TLS 1.1/1.0 with a server-side rejection while TLS 1.2 succeeds with
a live query, and guards the product's QUIC TLS constructors at TLS
1.3 (a live sub-1.3 QUIC probe is infeasible; the gap is documented
in the suite). Run it with
`go test -count=1 ./tests-live/tls-floor`.

The `tests-live/dbid-isolation/` scenario cross-peers two clusters
with different DBIDs, identical schemas, and shared CA trust, and
requires every cross edge to stay disconnected with markers never
crossing, identity refusals advancing on all nodes, and both
clusters internally healthy. Run it with
`go test -count=1 ./tests-live/dbid-isolation`.

The `tests-live/files-corrupt-source/` scenario corrupts a published
object's bytes on its source node and requires a peer fetch to fail
with the exact invalid-object sentinel, leave no partial install,
fail identically on retry, and leave honest objects fetching. Run it
with `go test -count=1 ./tests-live/files-corrupt-source`.

The `tests-live/maintenance-under-load/` scenario sustains
concurrent writes with zero failures while tight retention forces
log-GC on every node (counter-observed, failures flat), then
requires exact convergence plus a post-maintenance write that
replicates everywhere. Run it with
`go test -count=1 ./tests-live/maintenance-under-load`.

The `tests-live/endurance-chaos/` scenario overlaps every fault at
once — continuous bounded writes plus rolling restarts, tc/netem
packet loss and latency (logical peer flaps when unprivileged), disk
pressure, a libfaketime clock-skew window (skipped loudly without the
library), online storage-key rotation, snapshot-forcing offline
windows, and sustained log GC — over one chaos window (default 10
minutes; 24–72 hours via `MURMUR_ENDURANCE_DURATION_SECONDS`). The
verdict is exact cross-node convergence with counter-proven GC on
every node, at least one observed snapshot resync, executed
restart/rotation/impairment/disk legs, a full peer mesh, and a
post-chaos write. Run it with
`go test -count=1 ./tests-live/endurance-chaos` (short runs fit the
default timeout; 24h/72h profiles need `-timeout=26h`/`-timeout=76h`).

Migration qualification on 2026-10-08: the fixed five-node, 10-minute
run passed with 20,913 acknowledged writes and exact convergence. It
observed five restarts, three key rotations, seven logical peer flaps,
four disk-pressure holds, two snapshot resyncs, and GC on every node.
The host lacked `CAP_NET_ADMIN` and libfaketime; packet shaping therefore
degraded to logical peer flaps and clock skew was skipped. A privileged
impaired-network run and 24–72 hour acceptance remain outstanding.

Invariant:

```text
all nodes eventually have identical current state
```

for every:

```text
table + row + column + tombstone
```

Then rebuild each RIME materializer and compare query-visible typed records.

This test is more important than basic happy-path integration tests.

---

## 57. Network Partition Tests

Minimum integration topology:

```text
A <-> B <-> C
```

Test:

```text
A cannot reach C directly.
A writes.
B forwards A-origin mutation.
C stores it under origin A.
A later connects directly to C.
watermarks prevent duplicate logical changes.
```

Then:

```text
partition all three
write conflicting cell values
reconnect in different orders
verify deterministic convergence
```

Also test full-mesh reachability and hub/spoke allow-list topologies while the replication scheduler retains bounded fanout. Full reachability does not authorize a permanent connection to every member.

### SWIM and QUIC integration tests

- Start nodes from overlapping partial seed lists and verify discovery without enumerating every peer. Reject an existing-cluster join without the shared DBID; unreachable seeds permit local operation and eventual retry.
- Exercise direct and indirect probes, reliable fallback, suspicion/refutation, packet loss, membership repair, process restart with stable identity, advertised-address changes, graceful leave, and partition recovery.
- Verify that probes/gossip use QUIC DATAGRAMs and membership exchanges use QUIC streams on the shared endpoint. Check `net.Conn` deadlines, source addresses, queue saturation, datagram negotiation/size failures, and shutdown without leaked goroutines.
- Reject wrong CA, unauthorized NodeID, certificate/claimed-identity mismatch, wrong DBID, incompatible protocol, malformed metadata/envelopes, and oversized messages. Accept legitimate relayed records only as hints until direct authentication.
- Force multi-hop forwarding and peer rotation with conflicting offline writes, duplicate delivery, gaps, and concurrent snapshots. Verify deterministic convergence without permanent all-peer links and preserve durable acknowledgement ordering.
- Verify `ForceSync`, inbound sessions, concurrent joins, and rejected/queued repairs obey the same budgets. Sustain bulk snapshot traffic while membership and acknowledgements continue making progress.
- Verify an unselected member with missing acknowledgements gates GC, SWIM failure does not release that obligation, unchanged acknowledgements do not extend its deadline, restart preserves deadlines, expiry requires snapshot recovery when logs are gone, and explicit local retirement survives rediscovery/restart.

### Scaling acceptance tests

Use an instrumented transport and deterministic scheduler simulation for clusters of 10, 100, and 1,000 members, plus real QUIC integration coverage on smaller clusters. Under the configured test bounds (fanout 3, 8 sessions, 32 connections), assert selected outbound replication targets never exceed three, total replication sessions never exceed eight, and total admitted connections plus handshakes never exceed 32. Production defaults are fanout 4, 32 sessions, and 64 connections (`config.go`). Include inbound bursts and transient repair work in peak accounting.

Measure convergence latency, bytes sent, dial/handshake churn, evictions, queue pressure, and probe failures across cluster sizes. Verify eventual convergence under finite loss/partitions and repeated successful peer selection. Distinguish fixed fanout and connection bounds from O(N) membership/watermark metadata and membership-size-dependent gossip retransmission traffic.

Implemented acceptance coverage: `replication.TestPeerScalingCapsAcrossChurn` simulates 10, 100, and 1,000 discovered members, checks selected fanout and configured pool bounds, and removes a quarter of the membership at each scale to verify selection refills. `replication.TestPlumtreeNegotiatedThreeNodeForwarding` exercises forwarding beyond the local selected target set. `transport.TestMemberlistQUICRemainsResponsiveWithReplicationSlotsSaturated` fills all eight replication slots, then establishes an authenticated QUIC membership session and delivers a SWIM datagram through the reserved connection capacity. These are bounded simulations and small-cluster transport checks; long-duration impaired-network soak measurements remain separate acceptance work.

### Recovery, dissemination, and overload acceptance

- `internal/rimeadapter.TestFailedSpoolAppendReturnsUncertainOutcome` injects a Murmur durable-append failure and checks the managed transaction returns an uncertain receipt, publishes nothing to the materializer, and fails subsequent managed writes closed.
- Supply the same origin out of order from several peers; retain missing ranges across restart, switch repair sources, and never acknowledge observed/staged heads as applied progress. Exercise unavailable retained history and snapshot fallback.
- Transfer a transaction larger than a frame using chunks; interrupt/restart, duplicate chunks across peers, inject conflicting digests/indexes, and verify durable contiguous acknowledgement followed by atomic RIME publication. Reject a local transaction above `MaxTransactionBytes` before success and enforce decompression/reassembly limits.
- Recover an offline node whose acknowledged insert/update/delete was never propagated. A peer snapshot must preserve its winning cells/tombstones and valid local sequence, while legitimately newer competing versions may win. Test source writes after the snapshot cut, candidate publication crashes, cancellation, and schema incompatibility.
- `internal/rimeadapter.TestLocalUpdateAfterConcurrentTombstoneKeepsPrimaryFieldForRebuild` places a remote tombstone in durable state while the local materializer still holds the row, performs a local resurrection update, closes and reopens the encrypted store, and rebuilds a fresh materializer. It verifies the immutable primary field is republished with the update so the resurrected row remains decodable after restart. The targeted Murmur test passed on 2026-10-08; see the [migration evidence](migration-plan.md#current-milestone-status-2026-10-08).
- Restore an older backup under its original identity and verify writable startup is rejected. Restore with a fresh identity/certificate and repair with the existing DBID; reseed a cluster with a new DBID and verify old nodes are isolated. Verify historical origin identities are preserved and source acknowledgement/retirement state is not inherited.
- Merge independent compatible schema additions with equal and unequal epochs in different orders; repeat exchanges without epoch churn. Reject same-column type/default/nullability conflicts and identity collisions, preserve local additions, and leave mutation watermarks unchanged under strict policy or conflict.
- The standalone `plumtree` state machine tests bounded eager/lazy selection, forwarding, duplicate pruning, GRAFT cache replies, conflicting identities, and cache/neighbor limits. After replication-manager integration, also test `IHAVE` delay scheduling, protected ring neighbors, payload-log fallback, partition repair, incompatible-mode rejection, and compare payload bytes/duplicates against default gossip without relaxing session/connection caps.
- Saturate byte and entry queues, staging storage, token buckets, and apply/repair workers. Verify bounded accounting of shared buffers, retryable overload, safe coalescing/shedding, eventual anti-entropy repair, and progress for membership/control plus bulk traffic. Grouped apply must keep each original transaction atomic and acknowledge only synchronized commits.

---

## 58. Encryption and Storage Tests

Required:

- Published vectors and round trips for all five ciphers.
- Default AES-256-GCM, alias normalization, key-length validation, direct/provider exclusivity, and wrong/missing-key rejection.
- No plaintext fallback or obvious application plaintext in any database content file.
- VFS conformance for sequential/random operations, partial writes, EOF/logical sizes, concurrency, preallocation, reuse, rename, links, checkpoints, and sync semantics.
- Header/index/chunk tampering, cross-file substitution, malformed lengths, and committed-content corruption fail authentication.
- Nonce uniqueness across reuse, failed writes, writable reopen, counter limits, and crashes.
- Fault injection before/after data sync, commit metadata, registry rename/directory sync, and maintenance file replacement; acknowledged writes survive every restart.
- Reopening large SSTables uses authenticated indexes without reading/decrypting the entire file.
- Lazy rotation with mixed old/new keys and all cipher combinations preserves reads, writes, rebuild, replication, and restart.
- Explicit rewrite resumes after interruption and does not mutate linked checkpoints.
- Rewrap leaves file ciphertext unchanged; new key opens and old key fails after completion; historical backup registry/key remains usable.
- Key retirement waits for live files, handles, checkpoints, and registered backups.
- Snapshot/replication never transmits at-rest keys; config, metrics, and logs redact key bytes.
- Existing Badger directories return `ErrUnsupportedStorageFormat` without modification.
- Default compression is enabled Deflate level 1 on every LSM level; none/snappy overrides work and unsupported levels fail validation.
- Compression is applied before encryption; compare compressible and incompressible data.
- Serialized state merges, atomic synchronized batches, and iterator/snapshot/value-closer lifecycle pass regressions and race checks.

---

## 89. Initial Acceptance Criteria

A first serious alpha should not be called successful until all of the following work:

- Embedded Go API with no standalone service.
- The managed RIME materializer reconstructs typed records from encrypted Spool and passes model-equivalence, durability, and rebuild tests.
- Base table writes captured through pre-update hook.
- Multi-statement transactions coalesce correctly.
- Spool is authoritative.
- Spool uses AES-256-GCM storage encryption by default.
- Keyring and storage-key rotation configured/tested.
- Startup rebuild recreates identical query-visible state.
- Per-column LWW convergence tested.
- Mutual TLS authenticates all peer-to-peer QUIC replication connections with valid CA-signed certificates and URI NodeID verification.
- Partial seed lists discover members through SWIM over authenticated QUIC without native UDP/TCP membership listeners.
- QUIC membership/datagram/stream adapters handle identity, DBID, size limits, deadlines, and graceful shutdown.
- Configured fanout/session/connection bounds (3/8/32 in the scaling test; production defaults 4/32/64) hold at 10/100/1,000 simulated members.
- Peer rotation and anti-entropy converge after missed forwarding, membership churn, and partitions.
- Offline conflicting writes converge.
- Replication logs garbage-collect safely.
- Unselected members retain GC obligations; SWIM failure cannot release them, and acknowledgement deadlines/retirements survive restart.
- Stale/new node can snapshot-resync.
- Snapshot source cuts are consistent; merged publication preserves winning offline writes/tombstones and recovers from crashes without partial state.
- `TestSnapshotManifestCellsAndTombstonesShareOneReadCut` commits a cell, tombstone, schema update, and generation change during export, then checks the manifest and all streamed cells remain from the original Spool cut. `TestSnapshotExportContextStopsAndReleasesSourceCut` checks cancellation stops export and permits later durable writes.
- Snapshot candidate tests reject over-budget/invalid manifests, manifest changes between chunks, and conflicting duplicate chunks without publishing cells or advancing watermarks. Existing import coverage also reopens after staging to verify resume, and rejects a corrupted final digest.
- Transaction-chunk codec tests cover reverse-order assembly, frame and transaction limits, malformed metadata, missing/duplicate chunks, and canonical digest verification. Durable encrypted staging and cross-peer atomic apply still require integration tests.
- Fresh-identity restore and new-DBID reseed prevent mutation identity reuse and old-cluster contamination.
- Cross-peer missing ranges and resumable transaction chunks preserve atomic visibility and contiguous durable acknowledgements.
- Compatible concurrent schema merges converge without repeated epoch bumps; incompatible definitions and strict refusal leave affected mutation watermarks unchanged.
- Optional Plumtree reduces duplicate payload traffic under measured workloads while retaining bounded eager/lazy targets and repair fallbacks.
- Queue byte/entry, bandwidth, cache, staging, and repair limits hold under overload; adaptive grouped apply preserves durability and control/bulk progress.
- The standalone `overload` package tests global/per-peer byte and entry admission, idempotent release, bounded peer state, token-bucket bursts, context cancellation, and peer-slot reuse. Replication queue/staging/cache integration and retryable protocol responses still need acceptance coverage.
- Key rotation procedure survives restart/failure testing.
- FTS/local indexes rebuild without replication.
- No application success acknowledgement before Spool durable commit.
- `go test -race` passes.
- Randomized convergence test passes repeatedly.
- Crash-injection suite passes.

---

---

## Additional capability acceptance

The existing core crash/fuzz/convergence suite does not cover High/Low domains,
file objects, writer priority, or address filtering.
Keep these extensions outside MVP completion claims until their subsystem criteria
and integration scenarios pass.

- Run High/Low fault and schema-hold scenarios from
  [the bridge acceptance criteria](high-low-replication.md#diagnostics-and-acceptance),
  including multiple High receivers and reordered ownership/value delivery.
- Run a two-Low/two-High encrypted-file scenario from
  [file acceptance](file-replication.md#retention-backup-and-acceptance), checking
  metadata convergence and payload digest/availability independently.
- Measure local write latency and writer-time share under sustained replication,
  then verify eventual convergence after draining the workload. Test idle borrowing,
  canceled admission, maintenance, and shutdown as specified in
  [writer scheduling](synchronization-and-overload.md#local-and-replication-write-scheduling).
- Exercise [address policy](membership-and-transport.md#ip-and-cidr-admission-policy)
  and [subscription continuity/reset](query-and-search.md#reactive-query-subscriptions)
  without weakening identity authorization or blocking durable writes.
- Interrupt a multi-chunk snapshot after its first chunk, restart the receiver,
  and resume the same transfer. Verify staged cells and watermarks stay hidden,
  reject a corrupt digest without publication, then confirm atomic publication
  and tail repair. State tests cover restart staging and digest rejection;
  end-to-end tail repair remains a live acceptance scenario.
- Commit a source write from inside a snapshot chunk callback and verify the
  manifest's watermarks and exported cells still describe the original Spool
  read cut; deliver the later write through normal tail replication.

Use isolated directories, node identities, and ports for live scenarios. Check
canonical logical state digests and file content digests rather than encrypted
container bytes. Retain failed scenario artifacts with secrets redacted. Document
which scenarios ran; available test files are not evidence of a passing run.

Current schema-level counter, set and extrema behavior, causal storage, signed wire formats, bridge ownership and upgrade requirements are specified in [merge policies](merge-policies.md). LWW remains the default.

Merge-policy correctness is covered by state and bridge property tests plus typed live partition/rejoin and restart scenarios. The kernel benchmark measures fixed causal cardinalities, not end-to-end throughput; see [benchmarks](benchmarks.md#crdt-join-cost).
