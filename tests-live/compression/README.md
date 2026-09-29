# Compression live scenario

Pebble on-disk size benchmark across compression modes (`none`, `zstd-3`,
`zstd-9`, `zstd-12`) for four realistic traffic shapes: narrow OLTP rows,
text-heavy articles, JSON event documents, and incompressible random blobs.
Each (mode, traffic) cell opens a real database on real disk, writes a
seeded deterministic dataset through the full SQL stack in 500-row
transactions plus a 10% churn pass, shuts down cleanly, then walks the
Pebble data directory for total, SST, and WAL bytes.

In-process by design: a disk-size comparison needs production engine
bytes, not replication processes, and sixteen HTTPS-driven daemon runs
would measure HTTP overhead instead of compression. Small (256 KiB)
memtables force real flushes and compactions so every cell measures SST
bytes. Encryption stays on (production-realistic, and required by
`Config`); the key registry is a constant offset identical across modes.

## Gate

Identical row counts and matching dataset checksums across modes per
traffic shape; compressible text must shrink under every zstd mode;
random blobs must stay within 10% of uncompressed; every cell must
contain SST bytes (guards against measuring memtable residency).

## Run

```sh
go test ./tests-live/compression/ -run TestPebbleCompressionSizes -v -count=1
```

`SPEDSQL_LIVE_COMPRESSION_SCALE` multiplies every profile's row count
(default 1). Per-cell lines plus a summary table render in the test log:

```text
text-heavy   zstd-3   rows=8000 total=5.3MB sst=5.2MB (3 files) wal=103.9KB write=9s
zstd-3   | 3.6MB (28%) | 5.2MB (29%) | 1.8MB (21%) | 16.4MB (99%)
```

## Sample results

On 2026-09-29 (scale 1), SST bytes per traffic shape:

```text
mode     | oltp-small    | text-heavy    | json-docs    | blobs-random
none     | 12.5MB (100%) | 18.1MB (100%) | 8.4MB (100%) | 16.7MB (100%)
zstd-3   | 3.6MB (28%)   | 5.2MB (29%)   | 1.8MB (21%)  | 16.4MB (99%)
zstd-9   | 3.5MB (28%)   | 5.1MB (28%)   | 1.7MB (20%)  | 16.4MB (99%)
zstd-12  | 3.4MB (27%)   | 4.9MB (27%)   | 1.7MB (20%)  | 16.4MB (99%)
```

Compressible traffic shrinks to roughly a quarter with any zstd level;
levels 9 and 12 buy only ~1 extra point over level 3 on these shapes,
while random blobs stay flat as expected. These are measurements of this
machine and workload, not size guarantees.
