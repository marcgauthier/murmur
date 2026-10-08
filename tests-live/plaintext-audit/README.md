# No plaintext markers or key material on disk

Run with `bash tests-live/run.sh plaintext-audit`. Random per-run markers
are written through typed RIME, replicated, and crash-cycled; every
marker round-trips through the typed API (positive control), then both nodes'
entire directories (spool, segments, keys, tmp, logs) and a backup
artifact are scanned: marker plaintext and key material must be
absent everywhere except the harness-provisioned config file.
