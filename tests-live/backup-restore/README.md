# Backup, disaster restore, and mesh rejoin (multi-process)

Run with `go test -count=1 ./tests-live/backup-restore`.

`TestBackupRestoreRejoinMesh` meshes three `spedsql` daemons, stops
node3, takes an offline backup of its durable directory through the
library, wipes the directory to simulate total loss, restores the backup
under a fresh writer identity (TLS certificate reissued for the new
NodeID), and restarts the daemon on the restored data. The mesh must
reconverge with no lost rows, and the node's `/metrics` identity must be
the fresh one.

`TestRestoreRejectsStaleIdentity` proves rollback safety: opening
restored data under any NodeID other than the restore intent's fresh
identity is rejected, while the fresh identity opens cleanly.
