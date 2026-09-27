# Snapshots, backup, and restore

Log retention, safe snapshot merging/publication, backups, restore identities, and reseeding.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [36. Replication Log Garbage Collection](#36-replication-log-garbage-collection)
- [37. Snapshot / Full Seed](#37-snapshot--full-seed)
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

---

## 37. Snapshot / Full Seed

A snapshot is a logical copy of current replicated state, not a copy of LumoSQL.

Snapshot manifest:

```go
type SnapshotManifest struct {
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

- LumoSQL indexes.
- FTS structures.
- Prepared statements.
- Pebble cache.
- peer-specific acknowledgement data from the source.

### Consistent source cut

Capture current cells, tombstones, schema, origin watermarks, and generation from one Pebble read snapshot. A manifest watermark certifies that the snapshot state represents the effects of the complete origin prefix it covers; it must not include unapplied staging or state committed after the snapshot cut. Record a canonical content digest over ordered state/chunks and manifest metadata, excluding the digest field itself, plus chunk count/size bounds. Validate the complete transfer rather than treating end-of-stream as proof of completion.

Allow source writes after the cut. Tail mutations remain normal replication traffic; retain the read snapshot/required tail history through a bounded transfer lease. If the lease expires, retry or take a new snapshot without publishing incomplete state.

### Receive, merge, and publish

For both empty and stale nodes:

```text
1. Validate DBID, schema compatibility, format, manifest, and receiver budgets.
2. Receive into encrypted staging storage; persist bounded transfer progress.
3. Verify all chunks and the complete content digest.
4. Pause local writes/remote apply and drain the state writer/GC coordinator.
5. Merge authoritative local cells/tombstones into the staged candidate by version.
6. Preserve local identity/counters, receipts, logs, GC obligations, and configuration.
7. Persist/validate the complete candidate and a recoverable publication intent.
8. Atomically publish the active storage generation and rebuild LumoSQL.
9. Resume writes/apply and repair mutations after the merged snapshot cut.
```

An empty node has no local state to merge. A stale existing node must never replace acknowledged local data blindly: retain local winning cells and tombstones, and allow only a legitimately newer conflicting version to supersede them. Keep local origin sequence and HLC monotonic, raising the sequence to cover any validated local-origin prefix in the merged cut; observe the merged state before permitting new local writes. Preserve local origin logs/receipts under retention policy so peer dissemination remains possible. If history is absent, forward the preserved winning state through a subsequent logical snapshot rather than inventing an old mutation payload.

For an origin represented in both consistent cuts, the merged watermark may use the maximum of their contiguous covered prefixes only after all corresponding state/tombstones are merged. Never promote a staged/observed head to a watermark. Keep out-of-order pending transactions separately until gaps or snapshot-covered prefixes resolve them. Snapshot watermarks provide state coverage, not permission to fabricate missing historical log entries; track retained-log availability independently.

Receive/download while the previous generation remains active. The final merge/publication pauses durable writes and remote apply; do not promise zero-downtime snapshot installation. Preserve the old generation until the new generation, registry references, and publication marker are synced. Use a package-owned durable generation pointer/intent with directory sync, not an in-place clear/reload or a non-atomic series of per-chunk authoritative writes. Recovery selects the previously committed generation before publication, or completes recovery/rebuild from the newly committed generation afterward. Never expose partial state, mixed generations, or candidate watermarks before publication.

Stage encrypted files with retained key references; only retire the previous generation after publication recovery and readers have drained. SQL remains gated during final publication/rebuild under the existing lifecycle rules. Cancellation, bad hashes, incompatible schema, or staging budget exhaustion leave the active state untouched. Use [Section 32](synchronization-and-overload.md#32-mutation-batching)'s repair admission limits and report snapshot progress/deferrals explicitly.

---

## 38. Pebble Online Zero-Downtime Backup and Restore

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
  3. `sqlengine.Engine.Rebuild(store)` scans Pebble's authoritative state and reconstructs the LumoSQL LMDB MVCC query store.
  4. Validate the restore identity policy below, rebuild the query store, then admit writable QUIC replication. Catch-up depends on available retained history or snapshot merging; it is not unconditional.

#### Restart, restore/clone, and coordinated reseed

- **Ordinary restart:** Open the current authoritative directory using its existing NodeID, origin sequence, HLC, DBID, and certificate. Counter monotonicity follows synchronized state; a normal restart does not grant permission to replace it with an older backup.
- **Old-backup restore or clone:** Mark restored data as requiring a fresh writer identity. Require a new application-supplied NodeID, different from the backup's source identity and historical writer identities, with a matching certificate before writable replication. Preserve the original DBID when intentionally rejoining that cluster. Do not remap historical origin IDs, TxIDs, cell versions, or tombstones. Initialize the fresh origin sequence and observe restored HLC/state; retain historical state coverage and available logs for repair. Clear source-node membership, peer acknowledgement obligations, local retirement/exclusion records, and incomplete staging; admit peers under the new node's own policy.
- **Same-identity rollback:** Reject writable startup of an older restored backup under the original NodeID. For example, peers that already received A/101 would discard a new unrelated A/101 after restoring A's counter to 100. Do not try to infer safety from one peer's watermark or silently reuse that counter.
- **Coordinated cluster rollback/reseed:** Restore a chosen baseline on all intended nodes, give every node a fresh identity/certificate, and assign a new shared DBID before enabling replication. Reset node-specific replication obligations/staging and retain validated historical state coverage as the seed baseline. Older nodes with the previous DBID cannot join the reseeded cluster. Writes accepted after the baseline are absent unless explicitly preserved/replayed; never promise to recover them automatically.

Add restore-mode/identity metadata to backup metadata and persisted restore intent. Expose an explicit restore mode (`RestoreClone` by default, or `RestoreReseed`) plus the required fresh NodeID and, for reseed, new DBID through restore configuration. Validate the new identity, baseline, registry, and restore publication before networking; a crash cannot clear the fresh-identity requirement prematurely. An incompatible schema fails closed before catch-up. NodeID trust/certificates remain application-owned; restoring cannot mint authorization automatically.

### 38.5 Automated Background Worker & Retention
`DB` includes an automated non-blocking backup worker (`backup.Worker`):
- Executes on a configurable interval (e.g. daily) or ticker.
- Automatically prunes expired backups on the destination based on `MaxBackups` (retaining the N latest archives) and `RetentionDays` (deleting backups older than a cutoff date).

---

