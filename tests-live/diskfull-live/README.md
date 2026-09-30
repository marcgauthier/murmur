# Disk-full fails closed and recovers

Run with `go test -count=1 ./tests-live/diskfull-live`. A node runs on
a small tmpfs mount (`SPEDSQL_DISKFULL_TMPFS_MB`,
`SPEDSQL_DISKFULL_FILL_ROWS`); writes past ENOSPC must fail closed
with the pre-full data intact, and freeing space plus restart must
recover to full convergence. Requires mount privilege; skips with a
message otherwise.
