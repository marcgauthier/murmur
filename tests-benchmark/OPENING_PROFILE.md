# Database opening profile — 2026-10-15

Measured a private copy of the OVERWATCH durable database using the installed
Go native archive, an optimized build of the current Rust source, and the loading
changes in this revision. The live database was not opened or modified.

The fixture contains 31,673,556 bytes in its encrypted segment, 320,779,923
uncompressed block bytes, 5,287 physical blocks, and 113,800 historical record
visits. Recovered SQLite occupies 37,462,016 bytes: **80,433 rows, 116 tables,
194 indexes**. Compressed storage size alone understates startup work because it
includes history and conflict metadata as well as application values.

## Measurements

| Build and entry point | Database open | Peak process RSS |
| --- | ---: | ---: |
| Previously installed debug native archive, Go client | 568.271 s | 545 MiB |
| Current Rust source before these loading changes, release with CPU sampling | 12.482 s | 525 MiB |
| Rust with these loading changes, release with CPU sampling | 7.113 s | 156 MiB |
| Fresh release native archive with these changes, Go client | **6.812 s** | **150 MiB** |

These are individual runs with filesystem pages already cached. The old Go run
overlapped build activity and received 69% of a CPU on average; the final Go run
received 97%. Total user plus system CPU, including verification and close, was
392.44 s for the old Go run and 6.84 s for the final Go run. The observed wall-time
ratio includes both replacing the stale debug archive and the source changes;
it is not a claim that these source changes alone provide that speedup.

| Startup phase | Rust before, sampled | Rust after, sampled | Final Go release |
| --- | ---: | ---: | ---: |
| Spool opening and ordered delivery | 4.285 s | 3.104 s | 2.937 s |
| SQLite schema, metadata reads, row insertion | 7.453 s | 3.491 s | 3.390 s |
| Indexes and integrity check | 0.742 s | 0.516 s | 0.484 s |

The old Go archive spent 152.704 s opening Spool, then did not begin the first
table rebuild until 560.571 s. In the Rust baseline, the first table appeared at
10.899 s. Streaming row recovery now reports table/row progress while metadata
is read instead of first materializing a complete `ReplicaState`.

## CPU findings and changes

Native gperftools sampling of the Rust release baseline collected 1,245 samples
at 100 Hz. Mutually exclusive stack groups attributed 32.4% to Spool startup,
49.9% to metadata materialization, 12.7% to the remaining SQLite recovery work,
and 5.0% to other startup work. The optimized run collected 709 samples: 41.7%
Spool startup, 32.6% recovery point reads, 21.6% remaining SQLite recovery work,
and 4.1% other startup work. Percentages are approximate sampled CPU shares;
the smaller total changes their interpretation.

- Ordered loading previously retained only the last plaintext block and parsed
  its body again for every delivered record. Hash-sorted row keys repeatedly
  returned to blocks. It now reuses decoded bodies through Spool's existing
  byte-bounded cache, retaining that cache in the opened store. One current
  block is available even when caching is disabled. Committed groups are still
  authenticated before winner delivery.
- Recovery previously read every row into a complete logical state, then read
  every row again through `metadata::export` to recover versions it already had.
  It also materialized tombstones that SQLite reconstruction does not use.
  Recovery now reads each winning row directly into SQLite, validates its key,
  and holds one insert statement per table. Tombstones remain in Spool.
- The zstd codec now decodes into the bounded destination supplied by the
  authenticated block header, avoiding repeated streaming-buffer growth.
  Existing frames, concatenated frames, bounds, and damaged input are checked.

SQLite still uses exactly one in-memory database and one writer connection.
These changes do not introduce a second metadata store, modify the on-disk
format, or parallelize SQLite writes. Background workers retain their existing
responsibilities.

## OVERWATCH build diagnosis

OVERWATCH's Linux launcher selects the local `../MURMUR/clients/go` module through
`go build -mod=mod`. That client links a prebuilt `libmurmur_go.a`; Go does not
recompile Rust. The installed 573 MiB archive was dated October 14 and matched
the debug archive byte for byte, while OVERWATCH had been rebuilt October 15.
Rebuilding only OVERWATCH therefore kept linking stale, unoptimized Murmur code.

The Linux launcher and Windows packaging script now build the module-selected
native library with `--release` before linking OVERWATCH. The native script
refreshes Go's library fingerprint; its fingerprint generator executes on the
build host even when `GOOS` selects another target. The fresh release archive is
80 MiB. OVERWATCH was rebuilt against it without starting the backend or frontend.
Windows packaging was syntax checked but not executed.

There is also a conditional preliminary database open to discover a missing node
ID when file storage is enabled. That branch is disabled in this fixture's
configuration and did not explain the measured stall. Auxiliary application
initialization after `Database::open` is outside these measurements.

## Validation and reproduction

`openbench` checks every SQL value in primary-key order after the timed window.
The before/after SQL digests and all row/table/index counts match. See the
[benchmark instructions](README.md#database-opening-profile) for the command and
optional CPU sampler. Use a closed database copy or consistent backup and a
private raw 32-byte key file. No application data or keys belong in this report.

Checks passed: all 108 Spool checks, 33 targeted Rust persistence/backup/progress/
table-setting checks, bounded zstd tests, Go core integration tests and recovery
examples, `go vet`, selected recovery tests with `GOEXPERIMENT=cgocheck2`, the
808-symbol API audit, shell syntax checks, and host fingerprint generation under
a Windows cross-build environment.

The broader Rust suite has two snapshot receipt assertion failures:
`snapshot_waits_for_local_capture_preserves_local_only_tables_and_concurrent_writes`
and `streamed_snapshot_resumes_incremental_installation_after_restart`. Both also
fail with the original loading implementation restored in a temporary source
copy. Its test fixtures additionally needed missing adapter field initializers
to compile. The Go snapshot example smoke test fails with `stream skipped
incremental checkpoint`; that scenario uses a map backend without Spool or
SQLite. These failures remain outside this loading change.
