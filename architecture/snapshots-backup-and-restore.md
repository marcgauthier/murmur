# Snapshots, backup, and restore

Log retention, safe snapshot merging/publication, backups, restore identities, and reseeding.

[Architecture index](README.md) · [Project README](../README.md)

## Contents

- [36. Replication Log Pruning & Retention](#36-replication-log-pruning--retention)
- [37. Snapshot / Full Seed](#37-snapshot--full-seed)
- [38. Spool Online Zero-Downtime Backup and Restore](#38-spool-online-zero-downtime-backup-and-restore)

---

## 36. Replication Log Pruning & Retention

The replication log must not grow unbounded.

Maintain:

```text
min_log_sequence_per_node
```

Prune when:

- Sequence <= min across all peers, OR
- Sequence exceeds maximum age, OR
- Sequence exceeds maximum log size.

If a peer falls behind the low watermark, it cannot use normal delta sync; it must use a snapshot.

### 36.1 Floor Calculation Rules

All admitted, unretired members with unexpired obligations contribute to the floor calculation. A suspect or unreachable peer continues to contribute to the floor until its retention deadline expires.

---

## 37. Snapshot / Full Seed

Merged snapshots require `Replication.TrustedSnapshotSources`; empty disables remote snapshot ingestion. Same-DBID clones preserve origin proofs, while new-DBID reseeds establish a trusted baseline and discard old replication logs. See [origin signatures](origin-signatures.md) for the exact format and trust boundaries.

A snapshot is a logical copy of current replicated state, not a copy of the RIME materialization.

### Implementation and guarantees

Export captures cells, tombstones, schema identity, generation, HLC, and
watermarks from one Spool/Radix read snapshot. The versioned manifest declares indexed
chunks, encoded size, and a canonical content digest. Receivers persist encrypted
chunks in a reserved staging keyspace, independently of applied state. After every
chunk is present and the complete digest validates, synced Spool commits merge
the snapshot with current local winners and advance covered watermarks. The
the query materializer is rebuilt after publication. Before the final snapshot chunk,
callback-based typed writes drain and new callbacks wait, so publication cannot
invalidate a staged write's materializer generation. The writer coordinator is
released between ordinary chunks, allowing local commits and remote apply to
proceed. Staging survives restart and same-transfer retries; starting a new
transfer removes abandoned staging.

`Replication.MaxSnapshotBytes` defaults to 512 MiB. That limit bounds staging;
snapshots above it are rejected. Snapshots at or below the atomic merge
threshold (`state.DefaultSnapshotAtomicMergeBytes`, 8 MiB) merge and publish
in one synced Spool commit. Larger validated snapshots merge chunk by chunk. Each
chunk writes only CRDT-winning cells and records durable resume progress
(`nextChunk` plus the last merged key, so the canonical-order check continues
across chunks and restarts). The writer coordinator is released between chunks so
local commits and remote apply proceed. The receiver then publishes watermarks,
HLC, and the storage generation in one final atomic Spool commit that also clears
staging, quiesces typed callback writes, and installs the corresponding RIME
generation before releasing those writers.

---

## 38. Spool Online Zero-Downtime Backup and Restore

Whole-database checkpoint backups do not implement one-way change replication or
independent file objects. Those additions have separate protocols and retention
requirements: [High/Low replication](high-low-replication.md) and
[encrypted file replication](file-replication.md).

Typed persistent node-local tables are part of the whole-database Spool
checkpoint, even though their cells are excluded from replication snapshots and
logs. Restoring a backup with the application's node-local table definitions
rebuilds those rows under the restored database; restore still requires a fresh
node identity.

For an online, zero-downtime backup of a large dataset, executing a raw Key-Value snapshot loop is prohibited. Murmur leverages **Spool's native Checkpoint primitive** (`spool.Checkpoint(stagingDir)`), creating a point-in-time snapshot of the database on disk completely online.

### 38.1 Spool Checkpoint Performance & Safety
- **Instant POSIX Hard Links:** Spool flushes active buffers to disk and establishes filesystem hard links (`link()`) to existing immutable segments, `manifest.enc`, and `keys.enc` in the staging directory.
- **Zero Heap & Disk Inflation:** A large database checkpoint completes in **<100 milliseconds**, consuming virtually zero CPU, zero memory overhead, and zero additional disk blocks at creation.
- **Concurrent Non-Blocking Safety:** Because completed segments are immutable, live concurrent reads and writes continue uninterrupted. Subsequent compactions in the live store produce new segments and unlink old ones; the checkpoint retains its hard links unaffected.
- **Same-Mount Guarantee:** Checkpoint staging directories reside on the same filesystem mount as the database data directory (e.g. `filepath.Join(cfg.Path, ".backup-<timestamp>")`) so that hard links succeed instantly without falling back to cross-device byte copies.

### 38.2 Pure Ciphertext Pipeline (Zero Decryption)
- **At-Rest Container Preservation:** Every segment (`*.seg`), keyring (`keys.enc`), and manifest (`manifest.enc`) on disk is already an authenticated ciphertext container (AES-256-GCM).
- **Sealed Key Registry Inclusion:** The checkpoint bundle packages `keys.enc` sealed under the master storage key (KEK); the entire backup bundle is **100% pure ciphertext**.
- **Zero Decryption & Zero Exposure:** **Zero bytes are decrypted during backup generation or offsite transmission.** The backup pipeline never accesses unencrypted data keys or plaintext records. Offsite storage (even untrusted S3 buckets or unencrypted FTP servers) receives only authenticated ciphertext.

### 38.3 Complete 4-Step Backup Pipeline
```text
[Live Spool DB] 
       │ 
       ▼ (Step 1: <100ms)
[Hard-Link Checkpoint (.backup-tmp/)]
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
[Release Handle] + [Unlink .backup-tmp/]
```

1. **Step 1 — Checkpoint & State Cut:**
   - Establish staging directory on the same filesystem.
   - Hold commits briefly to capture metadata and create checkpoint handle.
   - Execute Spool Checkpoint into staging directory (`manifest.enc`, `keys.enc`, `*.seg`).
2. **Step 2 — Zero-Heap Streaming Archive:**
   - Connect `tar.Writer` and `gzip.Writer` to an `io.Pipe()`.
   - In a concurrent worker, stream `backup-metadata.json` and `data/*` directly into the pipe.
   - Peak memory usage is bounded by pipe buffers (<16 MiB), eliminating Out-Of-Memory (OOM) risks regardless of dataset size.
3. **Step 3 — Offsite Transfer:**
   - Stream the `io.PipeReader` directly into the configured destination (Local, HTTPS/S3, FTP/FTPS).
4. **Step 4 — Fast Cleanup:**
   - Release the checkpoint handle and delete temporary staging directory via `os.RemoveAll(stagingDir)`.

### 38.4 Restore & Deterministic Rebuild
- **Extraction:** `murmur.Restore(ctx, cfg)` pulls the backup stream from the destination (Local, HTTPS, or FTP), verifies `backup-metadata.json`, and extracts `data/` into target directories.
- **Fail-Closed Verification:** Validates Spool container headers and encrypted keys format. Restoring into a non-empty directory without `Overwrite=true` is rejected.
- **Autonomous Query Store Rebuild:** When `murmur.Open` is invoked with the master key:
  1. Spool authenticates and unwraps data keys from `keys.enc`.
  2. Spool loads validated state records into the in-memory Radix tree.
  3. The managed RIME adapter scans authoritative state and reconstructs the in-memory typed query store.
  4. Validate the restore identity policy below, rebuild the query store, then admit writable QUIC replication.

#### Restart, restore/clone, and coordinated reseed

- **Ordinary restart:** Open the current authoritative directory using its existing NodeID, origin sequence, HLC, DBID, and certificate.
- **Old-backup restore or clone:** Mark restored data as requiring a fresh writer identity (`FreshNodeID`).
- **Same-identity rollback:** Reject writable startup of an older restored backup under the original NodeID.
- **Coordinated cluster rollback/reseed:** Restore a chosen baseline on all intended nodes, give every node a fresh identity/certificate, and assign a new shared DBID before enabling replication.

### 38.5 Automated Background Worker & Retention
`DB` includes an automated non-blocking backup worker (`backup.Worker`):
- Executes on a configurable interval (e.g. daily) or ticker.
- Automatically prunes expired backups on the destination based on `MaxBackups` and `RetentionDays`.

---

Current schema-level counter, set and extrema behavior, causal storage, signed wire formats, bridge ownership and upgrade requirements are specified in [merge policies](merge-policies.md). LWW remains the default.
