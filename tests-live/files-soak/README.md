# files-soak (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-soak/`
(`files_soak_test.go` + `README.md`).

## What it proves

A two-node encrypted mesh (file object key `0x51..`, 4 MiB file cap)
soaks 64 KiB file uploads for `MURMUR_FILES_SOAK_DURATION_SECONDS`
(default 5 s), one upload every `MURMUR_FILES_SOAK_INTERVAL_SECONDS`
(default 1 s). One `#[test]` (`encrypted_file_fetch_soak`, from
`TestEncryptedFileFetchSoak`) proves the Go pass criteria in Go order:
each round uploads `soak/file-NNNN.bin` (payload
`data[i] = (round*31 + i*17) % 251`) to node 1, waits for replicated
metadata on node 2, fetches, downloads, verifies the digest, and
searches the replicated names; afterwards the fetch gate shows every
file completed with zero failures and the first file reads
existing + available.

Pass criteria: at least one upload round; every uploaded file is
locally available on the peer with its upload digest and the fetcher
recorded no error; `soak/file-0000.bin` exists and is available;
per-round search for `"file-"` returns exactly the files uploaded so
far; upload p95 stays under 5 s and metadata-to-verified-fetch p95
under 20 s.

## Env knobs

- `MURMUR_FILES_SOAK_DURATION_SECONDS` (default 5): soak wall-clock
  budget. Long soak: `MURMUR_FILES_SOAK_DURATION_SECONDS=600
  MURMUR_FILES_SOAK_INTERVAL_SECONDS=60`.
- `MURMUR_FILES_SOAK_INTERVAL_SECONDS` (default 1): sleep between
  rounds (skipped after the last round).
- Standard harness knobs (`MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).

Convergence waits are 20 s with 50 ms polls, matching the Go
`WaitFileAvailable` bound; peer fetch runs on a 100 ms
`fetch_interval` (the `files-bridge` pattern).

## Deviations from the Go original

- No HTTP: uploads, downloads, metadata, and search use the direct
  `Database` file API (`upload_file`/`get_file`/`file_metadata`/
  `search_files`); there is no HTTP server in the Rust product.
  File names, payload bytes, payload pattern, object key, and the 4
  MiB file cap keep the Go values.
- No explicit fetch call exists in the Rust product: peer fetch is
  automatic on `fetch_interval`, so the Go `FetchFile` steps become
  `file_available` waits (the `files-bridge` pattern). The Go
  negative fetch interval (disabled background worker) has no Rust
  equivalent (`fetch_interval` must be non-zero); 100 ms keeps the
  steps deterministic while converging fast.
- The Go `FetchStats` gate (completed count plus zero failures) maps
  to per-file `file_available` + digest checks over every uploaded
  file plus `last_fetch_error()` returning `None`: the Rust product
  exposes no fetch counters.
- The Go `StatFile` exists + available gate maps to
  `file_metadata("soak/file-0000.bin").is_some()` plus
  `file_available`.
- The SHA-256 gate is digest consistency plus full byte equality:
  the upload digest (product-computed SHA-256) must match the peer
  metadata digest and the download must be byte-identical. No
  independent hash recomputation and no new registry dep (the
  `files-bridge` pattern).
- The Go `t.Logf` soak summary (rounds, p95s, fetched bytes) maps to
  an `eprintln!` line without the fetched-bytes counter (no Rust
  equivalent); the latency SLO gates are unchanged.
