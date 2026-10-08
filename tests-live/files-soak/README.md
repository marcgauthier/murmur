# Encrypted file transfer soak

Run the smoke version with `CGO_ENABLED=0 bash tests-live/run.sh files-soak`.
The scenario uses managed typed records without SQLite or CGO. Two
Murmur-SQL daemon processes in discrete `node1`/`node2` directories form an
encrypted mesh; the test drives them over HTTP, uploading 64 KiB files
repeatedly to one node, waiting for replicated metadata, fetching the
payloads from the peer, checking each SHA-256 digest, and searching the
replicated names. It reports p95 upload and metadata-to-verified-fetch
latencies and gates them at 5 seconds and 20 seconds respectively, and
asserts the fetch counters show every file completed with zero failures.

The default run lasts five seconds with one second between uploads. To run the
longer file soak, set the duration and interval explicitly:

```sh
MURMUR_FILES_SOAK_DURATION_SECONDS=600 \
MURMUR_FILES_SOAK_INTERVAL_SECONDS=60 \
go test -timeout=12m -count=1 ./tests-live/files-soak
```

This is a Low-to-Low mesh transfer soak; cross-domain transfer is covered
by the multi-process `tests-live/files-bridge` scenario.
