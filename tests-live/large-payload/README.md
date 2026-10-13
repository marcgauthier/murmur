# large-payload (Rust port)

Go source: `/media/marc/2TB/Archives/MURMUR/tests-live/large-payload/`
(`README.md` + `large_payload_test.go`:
`TestBulkTransactionWithLargeValueReplicatesExactly`).

## What it proves

Two encrypted, mutually authenticated nodes form a mesh. Node 1 commits
250 small rows plus one row carrying a 1.5 MiB text value (each in its own
transaction); node 2 must replicate all 251 rows with the payload's exact
length and content.

## Pass criteria

- `Cluster::new` opens 2 meshed nodes under `tests-live/runtime/large-payload/`.
- Node 1 inserts `bulk-0000`..`bulk-0249` plus the `large-payload` row with
  a 1,572,864-byte value (`"murmur-large-payload-"` repeated, `'x'`-padded).
- Within 60 s (50 ms poll), node 2 holds 251 rows and the large row's value
  has exactly 1,572,864 bytes identical to the committed payload.

## Env knobs

None scenario-specific. Harness knobs apply: `MURMUR_LIVE_RUNTIME`,
`MURMUR_LIVE_FAILURES`, `MURMUR_LIVE_UNLOCK_STALL_SECONDS`,
`MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS`.

## Deviations from the Go original

- In-process multi-`Database` cluster (QUIC mesh, ephemeral `127.0.0.1:0`)
  instead of `testnode` daemon processes over HTTPS+mTLS (per design).
- Plain SQL (`records(id TEXT PRIMARY KEY, name TEXT, phone TEXT)`)
  instead of the managed typed-records HTTP API (`TypedContentionInsert` /
  `TypedContentionRows` / `TypedContentionRead`); the bulk-row score travels
  in the `phone` column as text.
- Fixed row ids (`bulk-%04d`, `large-payload-row`) instead of random row
  ids; equivalent because the cluster starts empty.
- Exact byte-equality assertion on the replicated payload instead of the
  SHA-256 digest comparison; strictly stronger (equal bytes imply equal
  digests) and needs no extra dependency.

## Compile check (only gate; never run here)

```sh
cargo test --no-run --manifest-path /home/marc/MURMUR/tests-live/large-payload/Cargo.toml
```
