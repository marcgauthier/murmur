# Online backup under sustained writes

Run with `go test -count=1 ./tests-live/backup-under-fire`. A
suite-local backup agent (compiled with the same `MURMUR_TAGS` as the
daemon) takes repeated online backups while both mesh nodes absorb
sustained writes for `MURMUR_BACKUP_UNDER_FIRE_WRITE_SECONDS` with
zero failed writes. Every completed backup restores; intermediate
snapshots are non-empty, non-decreasing subsets, and the final backup
matches the survivors exactly (count plus digest).
