# Online backup under sustained writes

Run with `CGO_ENABLED=0 go test -count=1 ./tests-live/backup-under-fire` or
`bash tests-live/run.sh backup-under-fire`. A suite-local backup agent compiled
without SQLite tags joins the native typed-record mesh and takes repeated online backups while both mesh nodes absorb
sustained writes for `MURMUR_BACKUP_UNDER_FIRE_WRITE_SECONDS` with
zero failed writes. Every completed backup restores; intermediate
snapshots are non-empty, non-decreasing subsets, and the final backup
matches the survivors exactly (count plus a canonical digest of unique record
names). Completed archives restore and are inspected through typed RIME
queries.
