# Tampered backups fail closed

Run with `CGO_ENABLED=0 go test -count=1 ./tests-live/tampered-backup` or
`bash tests-live/run.sh tampered-backup`. The gate uses managed typed records.
Backup
artifacts are tampered (byte flips, truncation, manifest edits) and
every restore attempt must fail closed with no partial state; an
untampered backup restores exactly through RIME queries (count plus a
canonical digest of unique record names).
