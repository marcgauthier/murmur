# Tampered backups fail closed

Run with `go test -count=1 ./tests-live/tampered-backup`. Backup
artifacts are tampered (byte flips, truncation, manifest edits) and
every restore attempt must fail closed with no partial state; an
untampered backup restores exactly (count plus an offline digest
mirror of `ComputeTableDigest`).
