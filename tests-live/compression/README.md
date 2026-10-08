# Compression live scenario

This on-disk Spool workload compares `none` and standard-library `deflate`
using managed RIME records. It covers narrow OLTP values, text-heavy records,
JSON documents, and incompressible random blobs. Each mode/profile cell opens
a fresh encrypted database, writes seeded batches of 500 records, updates 10%
of rows where applicable, closes cleanly, and measures the Spool data
directory. No SQL materializer or replication processes participate.

## Gate

Every mode must preserve the expected row count and seeded ID checksum.
Deflate must reduce text-heavy storage, while random blobs must remain within
10% of the uncompressed size. Every cell must contain segment bytes.

## Run

```sh
CGO_ENABLED=0 go test ./tests-live/compression/ -run TestSpoolCompressionSizes -v -count=1
```

`MURMUR_LIVE_COMPRESSION_SCALE` multiplies every profile's row count (default
1). A scale-1 run on 2026-10-08 produced these Spool sizes:

```text
mode     | oltp-small | text-heavy | json-docs | blobs-random
none     | 27.1MB     | 32.1MB     | 14.9MB    | 18.3MB
deflate  | 5.0MB      | 8.6MB      | 2.4MB     | 17.1MB
ratio    | 18%        | 27%        | 16%       | 94%
```

These are workload and machine measurements, not size guarantees. The typed
fixture uses one stable record shape across profiles, so its results should be
compared with future RIME runs rather than historical SQL materializer cells.
