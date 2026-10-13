# files-corrupt-source (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/files-corrupt-source/`
(`files_corrupt_source_test.go` + `README.md`).

## What it proves

A two-node encrypted mesh (file object key `0x42..`, 4 MiB file cap)
proves that fetching from a corrupt source fails verification. One
`#[test]` (`corrupt_source_fetch_fails_verification`, from
`TestCorruptSourceFetchFailsVerification`) follows the Go phases in Go
order: a positive control (upload/replicate/fetch/verify), honest
publication of a victim object, corruption of its stored container
bytes on the source node while stopped (same length, 16 flipped bytes
in the middle of the container, i.e. inside chunk ciphertext), restart
and re-mesh, then the failing fetch legs.

Pass criteria: the peer fetch fails with the Rust verification
sentinel (`object authentication`, the `Error::Auth` equivalent of
Go's `objectstore: invalid encrypted object`); the fetch-error signal
advances from none to that error; the fetching node reports the
victim unavailable, refuses its download, and holds neither an
object file for the unverified digest nor any staging residue;
victim metadata stays intact (original digest/size, not deleted) on
both nodes; the corruption is persistent (still failing the same
verification after more background retry cycles); and honest objects
still fetch afterwards.

## Env knobs

- `MURMUR_FILES_CORRUPT_VICTIM_KB` (default 256): victim payload size
  in KiB (Go: same name and default).
- Standard harness knobs (`MURMUR_LIVE_RUNTIME`,
  `MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
  `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`).

Convergence waits are 30 s with 50 ms polls, matching the Go bounds;
peer fetch runs on a 100 ms `fetch_interval` (the `files-soak`
pattern). The staging-quiescence wait is 5 s with 10 ms polls, and
the retry-persistence window is 500 ms (~5 background fetch cycles).

## Deviations from the Go original

- No HTTP: uploads, downloads, metadata, and search use the direct
  `Database` file API (`upload_file`/`get_file`/`file_metadata`/
  `list_files`); there is no HTTP server in the Rust product. File
  names, sizes, object key, and the 4 MiB file cap keep the Go
  values.
- No explicit fetch call exists in the Rust product: peer fetch is
  automatic on `fetch_interval`, so the Go `FetchFile` steps become
  `file_available` waits (the `files-soak` pattern). The Go
  negative fetch interval (disabled background worker) has no Rust
  equivalent (`fetch_interval` must be non-zero); instead the
  fetcher is stopped while the victim is published and corrupted,
  then restarted, so no fetch can start before the corruption lands.
  This is deterministic where stopping the source after metadata
  arrival would race the first fetch tick. Node restart is
  close/reopen plus re-mesh (fresh ephemeral ports); there is no
  separate unlock/readiness step (the open watchdog runs inside
  `Cluster::start_node`).
- The Go `objectstore: invalid encrypted object` sentinel becomes
  `Error::Auth("object authentication")` (verified in
  `replication/src/files.rs`: flipped chunk ciphertext fails AES-GCM
  decryption during `publish_fetched` verification), observed via
  `last_fetch_error`, which records the `replication
  authentication: ...` display string. The Go fetch-failed counter
  has no Rust equivalent; the test asserts the error signal
  advances from none (clean-control baseline) to that sentinel.
- The Go second explicit `FetchFile` becomes a retry-persistence
  observation: after ~5 more background fetch cycles the recorded
  error must still be the auth sentinel and the victim must still
  be unavailable, proving the corruption is persistent rather than
  transient.
- Staging-residue checks observe a quiescent moment between 100 ms
  retry ticks (failed Rust fetches synchronously discard the
  `.part` plus its `.identity` sidecar in `publish_staged`,
  verified in `replication/src/filefetch.rs`), then require no
  `.part` anywhere under the fetching node dir. Any `.part` counts
  as residue (successful Rust fetches rename the `.part` into
  place), which is stronger than Go's digest-prefixed match. The
  32-byte `.identity` resume sidecars have no Go equivalent and are
  removed on the same failure path.
- Digests are compared via replicated-metadata equality and
  byte-exact download round-trips: the payload pattern is
  name-keyed (`(len*31 + name[i%len] + i*17) % 251`, the
  `files-crash` pattern, no hash dependency) instead of Go's
  SHA-256-seeded bytes.
