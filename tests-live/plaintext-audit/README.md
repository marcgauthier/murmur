# No plaintext markers or key material on disk

Run with `go test -count=1 ./tests-live/plaintext-audit`. Random
per-run markers are written, replicated, and crash-cycled; every
marker round-trips through SQL (positive control), then both nodes'
entire directories (pebble, WAL, keys, tmp, logs) and a backup
artifact are scanned: marker plaintext and key material must be
absent everywhere except the harness-provisioned config file.
