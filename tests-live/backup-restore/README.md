# Backup, disaster restore, and mesh rejoin (multi-process)

Run with `bash tests-live/run.sh backup-restore`. The fixture uses the managed
typed API and runs with CGO disabled.

`TestBackupRestoreRejoinMesh` meshes three Murmur daemons, stops
node3, takes an offline backup of its durable directory through the
library, wipes the directory to simulate total loss, restores the backup
under a fresh writer identity (TLS certificate reissued for the new
NodeID), and restarts the daemon on the restored data. The mesh must
reconverge with no lost rows, and the node's `/metrics` identity must be
the fresh one.

`TestRestoreRejectsStaleIdentity` proves rollback safety: opening
restored data under any NodeID other than the restore intent's fresh
identity is rejected, while the fresh identity opens cleanly.

`TestTypedBackupRestoreRoundTripLive` writes through the native record API in
a test-node process, backs up its encrypted Spool directory, restores it under
a fresh writer identity, and reopens it with the same typed schema. The
restored RIME materializer must return the original typed record.
