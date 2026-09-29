# Snapshots, backup, and restore

Log retention, safe snapshot merging/publication, backups, restore identities, and reseeding.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [36. Replication Log Garbage Collection](#36-replication-log-garbage-collection)
- [37. Snapshot / Full Seed](#37-snapshot--full-seed)
- [Current implementation and remaining gap](#current-implementation-and-remaining-gap)
- [38. Pebble Online Zero-Downtime Backup and Restore](#38-pebble-online-zero-downtime-backup-and-restore)

---

## 36. Replication Log Garbage Collection

Replication logs are temporary transport history.

Current state is permanent authoritative state.

Delete a log range only when policy says it is no longer required.

Use both:

1. Peer acknowledgement low-watermark.
2. Minimum retention window.

Example:

```text
gc_safe_sequence(origin) =
    min(durable ack of every admitted, non-retired member
        whose acknowledgement retention obligation has not expired)
```

Then retain an additional safety margin/time window.

GC membership is not the selected replication subset or the current connection pool. An authenticated admitted member continues to gate collection while unselected, disconnected, suspect, or dead until its retention obligation expires or it is explicitly retired. An unacknowledged origin contributes zero; an empty gating set still obeys minimum retention and minimum retained-batch policies. Discovery hints alone do not create durable obligations.

### Offline peers

Do not keep mutation history forever.

Define:

```go
MaxOfflineLogRetention time.Duration
```

Persist each admitted member's first-admission time, durable acknowledgement progress, and retention deadline. Initially the deadline is admission time plus `MaxOfflineLogRetention`; renew it when a successfully committed replication acknowledgement advances that member's durable progress. Membership probes, ordinary connection activity, and repeated unchanged acknowledgements do not renew it. Restart preserves the remaining obligation rather than granting a new window. GC cannot use volatile session `lastSeen` as its deadline source.

If an obligation expires, allow collection under the minimum-retention rules. This also bounds retention for a reachable member that stops acknowledging. When a returning peer requires already collected log entries, handshake returns:

```text
SNAPSHOT_REQUIRED
```

and the peer performs a full current-state sync.

Only validated durable acknowledgements may release progress-dependent log or transaction-receipt retention. Snapshot recovery must preserve current state, tombstones, and contiguous receive watermarks. Do not interpret SWIM liveness or another peer's gossip as proof that data was received.

### Peer retirement

Removing a node must be explicit.

A permanently removed peer must not block GC forever.

Store peer state:

```text
active
retired
first_admitted_at
last_durable_ack_progress_at
retention_deadline
excluded_from_discovery
```

`RemovePeer` persists local retirement/exclusion and closes its replication session; subsequent discovery cannot recreate that obligation. `AddPeer` clears the exclusion and starts a new admission obligation on successful authentication. A graceful remote leave or SWIM failure notification is temporary unavailability, not administrative retirement. Local retirement does not revoke that node's access to other cluster members.

Implementation status: admission records (`first_admitted_at`,
`last_durable_ack_progress_at`, `retention_deadline`, active/retired) persist
in Pebble (`state/members.go`) and are created on first successful
authenticated handshake. `AdvanceMemberAck` renews the deadline only when an
acknowledgement advances durable per-origin progress; repeated or stale acks
renew nothing. `RemovePeer` persists retirement plus exclusion and refuses
rejoining handshakes; `AddPeer` clears both for a fresh admission. Log GC
gates on the persisted live-obligation set only, never on session `lastSeen`,
and grants pre-upgrade durable ack progress one explicit window instead of
dropping it. Each pass drains every origin in bounded 4096-batch units
(`state.CollectUnitCap`), re-admitting the maintenance ticket per unit so
interactive writers interleave; units repeat until a short return proves
the origin caught up, so sustained write rates cannot outrun collection.
Retirement/exclusion surface per peer in `Status` and the
`metrics` collectors.

---

## 37. Snapshot / Full Seed

A snapshot is a logical copy of current replicated state, not a copy of the SQLite materialization.

### Current implementation and remaining gap

Export now captures cells, tombstones, schema identity, generation, HLC, and
watermarks from one Pebble read snapshot. The versioned manifest declares indexed
chunks, encoded size, and a canonical content digest. Receivers persist encrypted
chunks in a reserved Pebble keyspace, independently of applied state. After every
chunk is present and the complete digest validates, one synced Pebble batch merges
the snapshot with current local winners and advances covered watermarks. The SQL
materializer is gated and rebuilt after publication. Staging survives restart and
same-transfer retries; starting a new transfer removes abandoned staging.

`Replication.MaxSnapshotBytes` defaults to 512 MiB. That limit bounds staging;
snapshots above it are rejected. Snapshots at or below the atomic merge
threshold (`state.DefaultSnapshotAtomicMergeBytes`, 8 MiB) merge and publish
in one synced batch. Larger validated snapshots merge chunk by chunk. Each
chunk writes only CRDT-winning cells to an external SSTable through the same
encrypted Pebble VFS, ingests it, then records durable resume progress
(`nextChunk` plus the last merged key, so the canonical-order check continues
across chunks and restarts). The writer lock is released between chunks so
local commits and remote apply proceed. The receiver then publishes watermarks,
HLC, and the storage generation in one final atomic batch that also clears
staging. A crash after ingestion but before progress replays that chunk safely
through the same LWW comparison; orphan external tables are removed on reopen.
A crash
exposes merged state with unadvanced watermarks (resumable, never partially
published) or the complete merged state; a preempting newer transfer can
never publish the older partial merge. SQL stays gated on the pre-snapshot
materialization until publication completes, then rebuilds once from the
committed state. The source holds its consistent Pebble read cut for at most
`Replication.SnapshotTransferTimeout` (10 minutes by default).
`state.ExportSnapshotContext` acquires an in-memory source log-retention lease
at the manifest watermarks; log GC honors its floor. Expiry uses the context
deadline, or ten minutes when no deadline is supplied. The lease is released
when export returns, including cancellation/error, and is not persisted across
source restart. It does not explicitly wait for receiver publication or tail
catch-up before release.

`TestSnapshotTailRetentionLease*` covers concurrent writes/GC, cancellation and
retry, and expiry. Full live acceptance covering slow transfer with sustained
writes/aggressive GC through receiver publication, tail catch-up, and source/receiver
restarts is verified by `snapshot_recovery_test.go` (`TestSnapshotSlowTransferSustainedWritesAndAggressiveGC`,
`TestSnapshotTransferInterruptedBySourceAndReceiverRestarts`, `TestSnapshotRestartServesPublishedRows`)
and multi-process scenario `tests-live/snapshot-resync`. Progress diagnostics, busy deferral,
and the stall watchdog are implemented (`replication/snapshot_progress_test.go`). Publication tests cover
interrupted merge/restart, crash before publication, and SSTable ingestion before
progress persistence (`state/snapshot_chunked_test.go`).

Snapshot manifest:

```go
type SnapshotManifest struct {
    FormatVersion uint16
    SnapshotID    [16]byte
    DBID          [16]byte
    SchemaEpoch   uint64
    SchemaHash    [32]byte
    CreatedHLC    uint64
    StateGeneration uint64
    ChunkCount    uint64
    EncodedBytes  uint64
    ContentHash   [32]byte // canonical ordered state/chunks plus manifest metadata

    Watermarks []OriginWatermark
}
```

Snapshot includes:

- Current cell state.
- Row tombstones.
- Required schema metadata.
- Origin watermarks.
- Format version.

It does not include:

- SQLite indexes.
- FTS structures.
- Prepared statements.
- Pebble cache.
- peer-specific acknowledgement data from the source.

### Consistent source cut

Capture current cells, tombstones, schema, origin watermarks, and generation from one Pebble read snapshot. A manifest watermark certifies that the snapshot state represents the effects of the complete origin prefix it covers; it must not include unapplied staging or state committed after the snapshot cut. Record a canonical content digest over ordered state/chunks and manifest metadata, excluding the digest field itself, plus chunk count/size bounds. Validate the complete transfer rather than treating end-of-stream as proof of completion.

Allow source writes after the cut. Tail mutations remain normal replication traffic; retain the read snapshot/required tail history through a bounded transfer lease. If the lease expires, retry or take a new snapshot without publishing incomplete state.

### Receive, merge, and publish

For both empty and stale nodes within the configured size bound:

```text
1. Validate DBID, schema compatibility, format, manifest, and receiver budgets.
2. Receive into the encrypted Pebble staging keyspace; persist bounded chunks.
3. Verify all chunks and the complete content digest.
4. Pause local writes/remote apply for the final merge.
5. Merge incoming cells/tombstones against current local winners by version.
6. Preserve local identity, counters, receipts, logs, GC obligations, and configuration.
7. Commit cells, HLC, generation, and covered watermarks in one synced Pebble batch.
8. Gate SQL reads/writes and rebuild the materializer from the committed state.
9. Resume writes/apply and repair mutations after the snapshot cut.
```

An empty node has no local state to merge. A stale existing node must never replace acknowledged local data blindly: retain local winning cells and tombstones, and allow only a legitimately newer conflicting version to supersede them. Keep local origin sequence and HLC monotonic, raising the sequence to cover any validated local-origin prefix in the merged cut; observe the merged state before permitting new local writes. Preserve local origin logs/receipts under retention policy so peer dissemination remains possible. If history is absent, forward the preserved winning state through a subsequent logical snapshot rather than inventing an old mutation payload.

For an origin represented in both consistent cuts, the merged watermark may use the maximum of their contiguous covered prefixes only after all corresponding state/tombstones are merged. Never promote a staged/observed head to a watermark. Keep out-of-order pending transactions separately until gaps or snapshot-covered prefixes resolve them. Snapshot watermarks provide state coverage, not permission to fabricate missing historical log entries; track retained-log availability independently.

The final publication uses a single bounded Pebble batch rather than swapping database directories. Pebble's synced batch is the durable publication boundary: a crash exposes either the complete old state or the complete merged state, never a partial set of chunks or watermarks. Startup rebuilds SQL from that authoritative state. Cancellation, bad hashes, incompatible schema, or staging budget exhaustion leave applied cells and watermarks untouched. Larger snapshots merge chunk by chunk with durable resume progress (see above) and publish through the same single-batch boundary, so the configured bound no longer depends on fitting the whole merge in one batch. Repair admission follows [Section 32](synchronization-and-overload.md#32-mutation-batching) limits. Snapshot progress/deferral diagnostics are implemented: the receiver tracks per-peer transfer progress (`PeerStatus.SnapshotChunksReceived/Total`, Prometheus `spedsql_peer_snapshot_chunks_*`), a source with a transfer in flight answers concurrent requests with an explicit `ErrSnapshotBusy` deferral (counter `repl_snapshots_busy_deferred_total`), and a stall watchdog re-requests waits with no progress within `Replication.SnapshotRequestTimeout` (default 30 s; re-requests counted in `repl_snapshot_rerequested_total`).

---

## 38. Pebble Online Zero-Downtime Backup and Restore

Whole-database checkpoint backups do not implement one-way change replication or
independent file objects. Those additions have separate protocols and retention
requirements: [High/Low replication](high-low-replication.md) and
[encrypted file replication](file-replication.md).

For an online, zero-downtime backup of a 10–20+ GB dataset, executing a raw Key-Value snapshot loop is prohibited. At that scale, string formatting, decoding, and disk writing via standard iterators cause severe CPU starvation, GC thrashing, and multi-gigabyte heap inflation, degrading live query performance for tens of minutes. NOMADSQL leverages **Pebble's native Checkpoint primitive** (`db.Checkpoint(stagingDir)`), creating a point-in-time snapshot of the database on disk completely online.

### 38.1 Pebble Checkpoint Performance & Safety
- **Instant POSIX Hard Links:** Pebble flushes active memtables to disk and establishes filesystem hard links (`link()`) to existing, immutable SSTables in the staging directory.
- **Zero Heap & Disk Inflation:** A 20 GB database checkpoint completes in **<100 milliseconds**, consuming virtually zero CPU, zero memory overhead, and zero additional disk blocks at creation.
- **Concurrent Non-Blocking Safety:** Because SSTables are immutable, live concurrent reads and writes continue uninterrupted. Subsequent compactions or mutations in the live store produce new SSTables and unlink old ones; the checkpoint retains its hard links unaffected.
- **Same-Mount Guarantee:** Checkpoint staging directories must reside on the same filesystem mount as the database data directory (e.g. `filepath.Join(cfg.Path, ".backup-<timestamp>")`) so that hard links succeed instantly without falling back to cross-device byte copies.

### 38.2 Pure Ciphertext Pipeline (Zero Decryption)
- **At-Rest Container Preservation:** Because Pebble runs over NOMADSQL's Encrypted VFS (`crypto.EncryptedFS`), every SSTable (`*.sst`), WAL (`*.log`), and manifest (`MANIFEST-*`) on disk is already an authenticated ciphertext container (AES-256-GCM, ChaCha20-Poly1305, or AEGIS).
- **Sealed Key Registry Inclusion:** The checkpoint bundle packages the database's `KEYREGISTRY` file alongside the checkpointed Pebble SSTables. Because `KEYREGISTRY` is sealed under the master storage key (KEK), the entire backup bundle is **100% pure ciphertext**.
- **Zero Decryption & Zero Exposure:** **Zero bytes are decrypted during backup generation or offsite transmission.** The backup pipeline never accesses unencrypted data keys or plaintext records. Offsite storage (even untrusted S3 buckets or unencrypted FTP servers) receives only authenticated ciphertext.

### 38.3 Complete 4-Step Backup Pipeline
```text
[Live Pebble DB] 
       │ 
       ▼ (Step 1: <100ms)
[Hard-Link Checkpoint (.backup-tmp/)] + [reg.Pin()]
       │
       ▼ (Step 2: Streaming io.Pipe())
[tar.Writer + gzip.Writer] (Zero Disk/Heap Staging)
       │
       ▼ (Step 3: Ship Offsite)
┌──────────────────────┬──────────────────────┬──────────────────────┐
│  Local Destination   │  HTTPS / S3 Stream   │   FTP / FTPS Stream  │
└──────────────────────┴──────────────────────┴──────────────────────┘
       │
       ▼ (Step 4: Cleanup <10ms)
[reg.Unpin()] + [Unlink .backup-tmp/]
```

1. **Step 1 — Checkpoint & Key Pinning:**
   - Establish staging directory on the same filesystem.
   - Register a key pin via `db.keyReg.Pin(ctx, stagingDir, "checkpoint")` to prevent background key rotation or compaction cleanup from retiring data keys referenced by the checkpoint.
   - Execute `db.store.Checkpoint(filepath.Join(stagingDir, "data"))`.
   - Snapshot `keys/KEYREGISTRY` into the staging directory.
2. **Step 2 — Zero-Heap Streaming Archive:**
   - Connect `tar.Writer` and `gzip.Writer` to an `io.Pipe()`.
   - In a concurrent worker, stream `backup-metadata.json`, `keys/*`, and `data/*` directly into the pipe.
   - Peak memory usage is bounded by pipe buffers (<16 MiB), eliminating Out-Of-Memory (OOM) risks regardless of whether the database is 1 GB or 100 GB.
3. **Step 3 — Offsite Transfer:**
   - Stream the `io.PipeReader` directly into the configured destination:
     - **Local:** Stream to `filepath.Join(dir, name+".tmp")`, fsync, and atomically rename.
     - **HTTPS / S3:** Stream via HTTP `PUT` request body with `Content-Type: application/gzip` to generic HTTP endpoints or S3-compatible object storage (MinIO, AWS S3, Cloudflare R2).
     - **FTP / FTPS:** Stream directly through a passive data socket using the `STOR` command without intermediate local disk staging.
4. **Step 4 — Fast Cleanup:**
   - Remove key pin via `db.keyReg.Unpin(ctx, stagingDir)`.
   - Delete temporary staging directory via `os.RemoveAll(stagingDir)`. Unlinking hard links takes milliseconds and does not touch live SSTables.

### 38.4 Restore & Deterministic Rebuild
- **Extraction:** `replicateddb.Restore(ctx, cfg)` pulls the backup stream from the destination (Local, HTTPS, or FTP), verifies `backup-metadata.json`, and extracts `keys/` and `data/` into target directories.
- **Fail-Closed Verification:** Validates `NMC1` container magic and `NMKR` key registry magic. Restoring into a non-empty directory without `Overwrite=true` is rejected.
- **Autonomous Query Store Rebuild:** When `replicateddb.Open` is invoked with the master key:
  1. `crypto.OpenRegistry` authenticates and unwraps data keys.
  2. Pebble mounts the restored data directory via `crypto.EncryptedFS`.
  3. `sqlengine.Engine.Rebuild(store)` scans Pebble's authoritative state and reconstructs the in-memory SQLite query store.
  4. Validate the restore identity policy below, rebuild the query store, then admit writable QUIC replication. Catch-up depends on available retained history or snapshot merging; it is not unconditional.

#### Restart, restore/clone, and coordinated reseed

- **Ordinary restart:** Open the current authoritative directory using its existing NodeID, origin sequence, HLC, DBID, and certificate. Counter monotonicity follows synchronized state; a normal restart does not grant permission to replace it with an older backup.
- **Old-backup restore or clone:** Mark restored data as requiring a fresh writer identity. Require a new application-supplied NodeID, different from the backup's source identity and historical writer identities, with a matching certificate before writable replication. Preserve the original DBID when intentionally rejoining that cluster. Do not remap historical origin IDs, TxIDs, cell versions, or tombstones. Initialize the fresh origin sequence and observe restored HLC/state; retain historical state coverage and available logs for repair. Clear source-node membership, peer acknowledgement obligations, local retirement/exclusion records, and incomplete staging; admit peers under the new node's own policy.
- **Same-identity rollback:** Reject writable startup of an older restored backup under the original NodeID. For example, peers that already received A/101 would discard a new unrelated A/101 after restoring A's counter to 100. Do not try to infer safety from one peer's watermark or silently reuse that counter.
- **Coordinated cluster rollback/reseed:** Restore a chosen baseline on all intended nodes, give every node a fresh identity/certificate, and assign a new shared DBID before enabling replication. Reset node-specific replication obligations/staging and retain validated historical state coverage as the seed baseline. Older nodes with the previous DBID cannot join the reseeded cluster. Writes accepted after the baseline are absent unless explicitly preserved/replayed; never promise to recover them automatically.

Add restore-mode/identity metadata to backup metadata and persisted restore intent. Expose an explicit restore mode (`RestoreClone` by default, or `RestoreReseed`) plus the required fresh NodeID and, for reseed, new DBID through restore configuration. Validate the new identity, baseline, registry, and restore publication before networking; a crash cannot clear the fresh-identity requirement prematurely. An incompatible schema fails closed before catch-up. NodeID trust/certificates remain application-owned; restoring cannot mint authorization automatically.

Implemented (clone): `Restore` requires `FreshNodeID` (UUID, must differ from the backup source) and writes `restore-intent.json` into the target directory. `Open` refuses any NodeID except the intent's fresh identity, then `state` adoption atomically swaps the stored identity, resets the local origin sequence, records a durable restore marker (backup, retired source, fresh identity), and clears snapshot receive staging, peer-ack records, inherited member admission/retention records, and peer exclusion/retirement policies — while preserving cells, tombstones, origin IDs, TxIDs, versions, logs, receive watermarks, receipts, schema, HLC floor, generation, and DBID. The fresh identity must also be absent from historical origins. The intent file is removed after a successful rebuild, before networking; a crash replays adoption idempotently. Replication additionally fails fast when the TLS certificate's NodeID does not match configuration.

Implemented (reseed): `Restore` with `Mode: RestoreReseed` requires `NewDBID` (must differ from the backup's DBID) and records it in the intent; `Open` requires the same new DBID explicitly in config, first runs a crash-safe ciphertext rebind (`crypto.RebindStore`: every file container decrypted and re-sealed with new-DBID-bound headers, file keys, and record AADs via verified-temp plus atomic rename, then the key registry KEK re-derived and payload re-sealed under the new DBID with key material preserved), and state adoption moves the stored cluster identity in the same atomic batch (marker mode `reseed`). The rebind is idempotent with the registry persisted last, so a crash anywhere retries to convergence on the next Open; a missing registry is a no-op and a registry opening under neither identity fails closed. The replication handshake and snapshot manifest checks reject any peer whose DBID differs, proven end to end by `TestReseedFlowRejectsOldCluster`.

### 38.5 Automated Background Worker & Retention
`DB` includes an automated non-blocking backup worker (`backup.Worker`):
- Executes on a configurable interval (e.g. daily) or ticker.
- Automatically prunes expired backups on the destination based on `MaxBackups` (retaining the N latest archives) and `RetentionDays` (deleting backups older than a cutoff date).

---
