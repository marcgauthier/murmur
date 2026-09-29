# Operational rehearsals

Repeatable operational drills for backup recovery, key custody, certificates,
storage exhaustion, and recovery-time objectives. These procedures do not claim
that a drill has been performed; record each execution in the team's incident
or release record.

[Architecture index](README.md) · [Testing and acceptance](testing.md) ·
[Backup and restore](snapshots-backup-and-restore.md) ·
[Encryption and key management](encryption.md) ·
[Runtime and diagnostics](runtime-and-diagnostics.md)

## Cadence and evidence

Run the automated backup/restore and storage-failure acceptance checks for each
release. Perform the environment-dependent certificate-renewal and key-custody
drills at least quarterly and after changing the PKI or key-provider workflow.
Measure recovery time during each restore drill. Keep the date, software
revision, backup age, dataset size, operator, elapsed times, row/state digest,
and deviations in the operational record. Never put key bytes, private keys, or
bearer credentials in that record.

Set explicit service RPO and RTO values before production. The repository does
not prescribe numeric targets. Measure from the declared incident start to
service restoration, and report separately the data-loss window (RPO) and time
until reads and writes are healthy (RTO). A drill without an agreed target is a
measurement, not a pass.

## Backup restore drill

1. Select a recent encrypted backup and record its creation time, size, source
   DBID/NodeID, and the key-provider key ID needed to open it.
2. Restore into a new isolated directory and a fresh writer NodeID. Never test
   by overwriting the only backup or a production data directory.
3. Start the restored node with the matching application key and a certificate
   whose identity matches the fresh NodeID. Verify startup rebuild, expected
   schema, row counts, and a deterministic logical-state digest.
4. Connect only to a staging peer set, verify replication catch-up, then perform
   and read back a new write. Record elapsed restore, rebuild, and catch-up
   times and compare data loss and recovery time with the approved RPO/RTO.
5. Run the existing automated path with
   `bash tests-live/run.sh backup-restore`; it checks fresh-identity restore and
   stale-identity rejection. It does not replace a restore using the actual
   production backup destination and key provider.

## Lost-key recovery drill

1. In an isolated environment, simulate loss of the node's local key-cache or
   credential mount while preserving access to the authoritative key provider
   or escrow. Restore access using the documented identity-verification and
   dual-control process; do not copy secrets into shell history or CI logs.
2. Open a disposable backup with the recovered key source and verify its
   registry, data, and logical-state digest. Exercise the provider outage and
   missing-key error path as well as the successful recovery path.
3. If the only wrapping key is permanently lost and no escrow or provider copy
   exists, encrypted backups cannot be decrypted. Treat that as unrecoverable
   data loss; key rotation and replication do not reconstruct the lost key.
4. Record who authorized recovery, which key ID was recovered, time to restore
   access, and backup age. Do not record the key material itself.

## Certificate renewal drill

1. In staging, issue a replacement node certificate and private key for the
   same configured NodeID, with the expected URI identity and CA chain.
2. Install the pair using the deployment's atomic secret/config rollout. Check
   validity dates, chain, URI identity, and file permissions before restarting
   or reloading the node according to the deployment procedure.
3. Verify authenticated QUIC connections, peer convergence, and rejection of
   the expired prior certificate. If the deployment PKI supports revocation,
   separately verify its revocation enforcement. Confirm renewal alerts fire
   before expiry and record rollout plus convergence time.
4. The repository acceptance suite includes certificate identity and expiry
   rejection coverage; the live backup-restore scenario also uses a reissued
   certificate. Neither provisions certificates in an external production PKI.

## Disk-full drill

Run only against a disposable filesystem or a bounded loopback test volume.
Never fill a host or shared runner filesystem. Generate sustained writes until
the volume reports `ENOSPC`, then verify the first storage error is surfaced,
the node enters failed state, subsequent operations fail closed, and no failed
write is reported as acknowledged. Free capacity, restart, and verify recovery
from the last durable WAL prefix and the logical-state digest. Record the
failure and restart recovery times. Existing `diskfull_test.go` checks injected
storage failure; repeat the filesystem-level drill on the target deployment
filesystem because its behavior depends on the volume and mount configuration.

## Recovery-time target rehearsal

For each release candidate, restore a representative production-sized backup
in an isolated environment and measure backup retrieval, decryption/open,
Pebble recovery, SQL materialization rebuild, and replication catch-up
separately. Include a node restart after a simulated abrupt stop. Verify
acknowledged writes and compare the resulting digest with the recovery oracle.
Record p50/p95 across repeated runs when practical; fail the operational target
if any required recovery path exceeds the agreed RTO or loses data beyond the
agreed RPO. Keep the dataset size and hardware profile with the measurements so
future comparisons are meaningful.
