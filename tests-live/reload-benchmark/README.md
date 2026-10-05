# Live reload benchmark

Create realistic random logs in ten tables through Murmur's SQL transaction
API, close the encrypted Pebble store, and measure reconstruction into fresh
in-memory SQLite. The default target is **10,000,000,000 bytes (10 GB)** of
SQLite pages, including indexes, measured by `page_count × page_size`.
Generation stops after the first committed batch reaching that size, so a
small overshoot is expected. This is not a target for compressed disk bytes.

```sh
# Full 10 GB run; six-hour timeout.
bash tests-live/run.sh reload-benchmark

# Development run: 32 MiB, same schema and validation.
MURMUR_RELOAD_TARGET_BYTES=33554432 bash tests-live/run.sh reload-benchmark

# Reuse a completed full-size dataset without generating it again.
MURMUR_RELOAD_REUSE=/media/marc/2TB/TEST/reload-benchmark/<run-id> \
  bash tests-live/run.sh reload-benchmark

# Record embedded startup progress (uses the existing rebuild scan).
MURMUR_RELOAD_PROGRESS=1 MURMUR_RELOAD_REUSE=/home/marc/TEST/reload-benchmark/<run-id> \
  bash tests-live/run.sh reload-benchmark

# Optional pure-Go backend.
CGO_ENABLED=0 MURMUR_TAGS=modernc bash tests-live/run.sh reload-benchmark
```

The benchmark is explicit: routine `all`, `gate`, and unconfigured
`go test ./tests-live/...` do not run it. Direct Go invocation requires:

```sh
MURMUR_RELOAD_BENCH=1 go test -tags 'sqlite_preupdate_hook sqlite_fts5' \
  -v -count=1 -timeout=6h ./tests-live/reload-benchmark
```

## Data and storage

The tables are application, HTTP access, authentication, audit, database,
network, job, payment, email, and security logs. Each has common diagnostic
fields plus event-specific fields, a timestamp index, and a service/severity
index. Seeded gofakeit v7 supplies identities, hosts, requests, messages, and
variable-sized diagnostic event bundles. Rows include integer, real, text,
BLOB, NULL, Unicode, and multiline values. There is no FTS in this baseline.
Primary keys are chronological UUIDv7 values with seeded random suffixes,
matching append-oriented logs and reducing setup's random-key lookups. The
rebuild still scans the same cell storage and inserts through the existing
materializer. Population batches are capped at 5,000 rows or approximately
8 MiB of encoded data, whichever comes first.

The destination must exist. The default is `/media/marc/2TB/TEST`; there is no
fallback to the system temporary directory. Each new dataset occupies a unique
`reload-benchmark/<timestamp>-<random>/` directory containing:

- `db/`: authoritative encrypted Pebble data and the encryption registry.
- `manifest.json`: completed dataset identity, configuration, generation,
  SQLite size, disk size, per-table counts and content hashes.
- `measurements/<timestamp>-<random>/`: worker logs, request metadata,
  individual measurements, and aggregate `results.json`.

The fixture uses a public test-only encryption key. Do not store real data in
this dataset. Generation uses default AES-256-GCM encryption, Zstd level 3,
and synchronous durability. Replication is disabled. The main SQLite database
is memory resident; its backing-file path is checked after each load. Temporary
work uses the existing engine defaults. Counts and hashes in metadata describe
synthetic data.

Artifacts are preserved on success and failure. An interrupted population
has no completed manifest and cannot be reused. Reuse requires matching seed,
target, dataset version, and configuration; pass the same size override when
reusing a development dataset. Each reuse gets a new measurement directory.
Delete a chosen run directory manually when its dataset is no longer needed.

## Measurements and interpretation

Population, full reopen, and direct rebuild each run in a separate process,
one at a time. The default performs one measurement of each reload path:

- **Full open:** wall time inside production `DB.Open` until query-ready.
- **Direct rebuild:** registry open, Pebble open, and engine initialization
  timed separately; the existing `Engine.Rebuild` call timed directly. It
  includes scanning, decoding, row assembly, inserts, and secondary indexes.

The direct path uses the stored schema manifest and the same encrypted VFS,
Pebble budgets/compression, and SQLite engine as the production path. This
dataset has no bridge state. Table progress timings combine scanning and
insertion; they do not isolate either operation or index creation.

After the timed operation, run the first count query and record its latency.
Then verify all counts, indexes, the absence of SQLite backing files, and
full SHA-256 content digests. Digests scan explicit columns in primary-key
order and encode SQLite values with Murmur's typed binary codec. Validation
time is reported separately. Logical payload bytes count text/BLOB bytes and
eight bytes per integer/real, excluding NULLs and storage overhead.

When progress is enabled, the full-open measurement records callback snapshots
in `open_progress` and verifies the final cell count and committed row count.
The direct-rebuild measurement does not enable startup reporting.

JSON records seconds, rows/second, SQLite page bytes, Pebble file lengths
(not filesystem allocated blocks), and Linux peak RSS measured before full
validation. RSS includes native SQLite allocations; it is omitted where
unavailable. Go version, platform, and SQLite version are recorded.

These are **fresh-process measurements with uncontrolled OS filesystem
cache**. Generation and earlier reloads can warm the cache. The full-open
path runs before the direct path; neither is claimed to measure cold disk.
No cache-dropping command or performance threshold is used.
The difference between these independent measurements cannot be attributed
solely to startup overhead, because cache state and background work differ.

Allow memory for at least the 10 GB SQLite image plus SQLite allocation
overhead, Go, Pebble, and encryption buffers. Pebble retains mutation logs,
and compaction requires disk headroom; disk use may exceed the SQLite target.
Generation/validation buffers are bounded, and no full copy of the rows is
kept in Go. The six-hour timeout includes generation, hashing, and reloads.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `MURMUR_RELOAD_ROOT` | `/media/marc/2TB/TEST` | Existing writable destination for new runs. |
| `MURMUR_RELOAD_TARGET_BYTES` | `10000000000` | Positive SQLite page-byte target. |
| `MURMUR_RELOAD_SEED` | `42` | Positive deterministic generator seed. |
| `MURMUR_RELOAD_REPETITIONS` | `1` | Measurements of each reload path. |
| `MURMUR_RELOAD_REUSE` | unset | Completed run directory to measure again. |
| `MURMUR_RELOAD_PROGRESS` | unset | Set to `1` to record embedded progress during full open, without an extra counting pass. |

## Recorded 10 GB baseline

On 2026-10-02 UTC, Linux amd64, Go 1.26.0, bundled SQLite 3.50.4:

- 1,371,480 rows: 137,148 in each of the ten tables, with 20 secondary indexes.
- Initial SQLite pages: 10,000,498,688 bytes; logical values: 8,863,498,804 bytes.
- Encrypted Pebble file lengths after population: 5,664,469,882 bytes.
- Population: 3,266.94 seconds; baseline validation and close are additional.

| Path | Timed load | First query | Peak RSS before validation | Validation |
| --- | ---: | ---: | ---: | ---: |
| Production `DB.Open` | 362.369 s | 0.261 ms | 11.787 GB | 42.651 s |
| Direct `Engine.Rebuild` | 153.671 s | 0.201 ms | 11.893 GB | 42.817 s |

Direct Pebble open took 3.157 seconds. Rebuilt SQLite pages occupied
9,993,945,088 bytes; page packing can differ after reconstruction. Both
paths matched every table's count, typed content digest, and index checks.
The main SQLite database had no backing-file path. These are individual
observations with uncontrolled OS cache, not a cold-disk comparison.

Dataset directory:
`/media/marc/2TB/TEST/reload-benchmark/20261002T012436Z-879366731`.
The complete JSON report is under
`measurements/20261002T012436Z-3214447337/results.json` within that directory.

## Startup progress without a counting pass

The revised reporter was measured on 2026-10-02 using the same 10 GB dataset
on the SSD under `/home/marc/TEST`. Production startup took 67.542 seconds
without reporting and 70.322 seconds with reporting: an observed difference of
2.780 seconds (4.12%). These are one fresh process per mode with uncontrolled
OS cache, so the difference is not a precise isolated reporting overhead.
No counting phase occurred; total, percentage, and ETA remained unknown.
The final snapshot reported 16,457,760 processed cells and 1,371,480 committed
rows. Both runs passed all table counts, typed content hashes, 20 secondary
indexes, and the absence of a SQLite backing file. Content validation is
measured separately from startup.

Artifacts are in the dataset's
`measurements/work-progress-20261002T110113Z-dy4bpgb0/` directory:
`control.json`, `progress.json` (all snapshots), logs, and `comparison.json`.

## Historical startup progress measurement (superseded)

The original counting-pass implementation, since removed, was measured in a
separate 2026-10-02 run that reused the same dataset on the SSD under
`/home/marc/TEST`. Full production startup took 74.481 seconds without reporting
and 97.139 seconds with `OnOpenProgress`. The exact counting pass took
25.549 seconds, data loading 69.431 seconds, and index creation 2.043 seconds.
The reporter produced 394 snapshots, including live ETA and current tables,
and finished with 16,457,760 cells and 1,371,480 committed rows. All content
hashes and 20 indexes passed. These are single fresh-process observations with
uncontrolled OS cache; their difference is not a precise isolated overhead.

Artifacts are in the dataset's
`measurements/open-progress-20261002T103233Z-iur9r5ll/` directory:
`control.json`, `progress.json` (all snapshots), logs, and `comparison.json`.

See [startup benchmarks](../../architecture/benchmarks.md#61-startup-benchmarks)
and the [live-test index](../README.md).
