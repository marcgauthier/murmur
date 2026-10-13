# compression (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/compression/`
(`README.md`, `compression_test.go`, `TestSpoolCompressionSizes`).

## What it proves

Each (`none` / `deflate`) x (`oltp-small` / `text-heavy` / `json-docs` /
`blobs-random`) cell opens a fresh encrypted store, writes a seeded
deterministic dataset in batches of 500 rows, updates every 10th row where
applicable, closes cleanly, and measures the on-disk store directory.
In-process by design: no replication or SQL materializer participates.

## Env knobs

- `MURMUR_LIVE_COMPRESSION_SCALE` (default 1): multiplies every profile's row
  count.
- `MURMUR_LIVE_RUNTIME` / `MURMUR_LIVE_FAILURES`: runtime and failure-artifact
  roots (harness convention).

## Pass criteria (Go parity)

- Every cell holds the expected row count with an identical seeded-ID
  checksum across modes.
- `text-heavy` under `deflate` is strictly smaller than under `none`.
- `blobs-random` under `deflate` stays within 10% of `none`.
- Every cell contains segment bytes (non-empty store directory).

## Deviations from Go

- Cells write equivalent seeded KV traffic directly to `spool::Store` instead
  of managed RIME records on a full database: the Go engine exposes a
  per-database `Spool.Compression` knob, while the Rust `Database` hardcodes
  its Spool block codec to a custom zstd codec (`src/storage.rs`,
  `spool_options`, codec id 128) with only a level knob, so `none` vs
  `deflate` cannot be selected through `Database::open`. Same 64 KiB target
  blocks, same AES-256-GCM encryption, same traffic shapes and row counts.
- No `Cluster`: single-store cells use `artifacts::{prepare_runtime_dir,
  finish}` directly for the runtime/failures convention. On failure the cell
  stores are retained as-is (no per-node forensics handle applies).
- ID checksum is FNV-1a/64 instead of SHA-256 (only cross-mode equality
  within one process is asserted).
- The Go suite's absolute size table is a workload measurement, not a gate;
  only the relational gates above are asserted.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/compression/Cargo.toml
```
