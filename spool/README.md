<img src="logo.png" alt="murmur-spool logo" width="400">

# murmur-spool

A Rust port of Murmur's `spool` embedded persistence engine
(`github.com/marcgauthier/murmur/spool`), byte-compatible on disk:
block v3, manifest v3, keys.enc v3, and the maintenance intent v1
formats are identical, so stores written by either implementation
open in the other.

The crate name on disk is `murmur-spool`; the library imports as
`spool`, matching the Go package name.

## Design

Spool durably journals every mutation, rebuilds state at startup through load
callbacks, and supports point reads from its key/location index. Writes are sharded in memory, sealed
into blocks, compressed and encrypted in parallel, and appended
sequentially to segment files.

Prefix searches use fixed 1,024-bit namespace hints per index shard (32 KiB for
256 shards) to skip shards that cannot contain a matching path namespace. The
first three `/` separators identify a namespace; shorter or binary non-path
prefixes use the full ordered-index traversal. Binary keys remain supported.
Hints are rebuilt during loading and never cleared on deletion, so collisions
or deleted namespaces can cause extra searches. Every returned key comes from
the ordered index. This index overhead is independent of `read_cache_bytes`.

- Concurrent `put` / `delete` / batches plus atomic multi-key
  `commit` groups with per-call durability (`Async`, `Flush`, `Sync`).
- AES-256-GCM block encryption with rotating data keys, Argon2id
  passphrase support, and database-context binding.
- Deflate block compression with pluggable user codecs (ids 128-255).
- Online compaction, tombstone collection, backup checkpoints, key
  rewrite/rebind maintenance, and fault-injection hooks.

## Use

```rust
use spool::{default_options, open_and_load};

let mut opts = default_options("./data");
opts.master_key = vec![7u8; 32];
let store = open_and_load(&opts, &mut |records| {
    // Populate application memory; each key arrives once with its
    // current version (tombstone if deleted).
    Ok(())
})?;
store.put(b"k", b"v")?;
store.delete(b"old")?;
store.flush()?; // explicit durability point
store.close()?;
# Ok::<(), spool::Error>(())
```

API documentation is on the items themselves (`cargo doc`); the
[Go spool README](../spool/README.md) describes the shared on-disk
format and operational model in depth.

## Go API differences

The port follows the Go shape, adapted to Rust idioms:

- Names are snake_case (`open`, `put_batch`, `rotate_key`,
  `key_inventory`, `rewrite_data_keys`); `Open`/`OpenAndLoad` are
  free functions.
- Errors are `spool::Error` with a machine-readable
  `ErrorKind` (one variant per Go sentinel); match on
  `err.kind()`, not strings.
- `Store::checkpoint` takes `Option<&AtomicBool>` for cancellation
  instead of a `context.Context`.
- `Options::reclaim_interval` and `data_key_max_age` are
  `Option<Duration>` (`None` disables); `Options::compression` is
  `Option<Compression>` (`None` selects the default).
- `FaultHooks` is shared as `Arc<FaultHooks>`; arm/disarm hooks at
  runtime by capturing an `AtomicBool` in the hook closure (see
  `tests/faults.rs`). Hook failures are `String` messages.

## Layout

```text
spoolrust/
  src/            library (one module per Go source file)
  tests/          ported behavior suites + Go interop tests
  tests/data/     Go-written fixture stores (encrypted + plain)
  interop/go/     Go helpers (gowrite/goread) for live interop
  examples/       writestore/readstore (compat key 32 x 0x42)
```

## Testing

```sh
cargo test            # full suite incl. live Go interop (needs `go`)
cargo test --test api # one suite
```

`tests/interop.rs` proves byte-compatibility live in both directions
(Go writes / Rust reads and back, encrypted and plain, including
cross-implementation appends). It shells out to the Go toolchain; when
`go` is not on `PATH` the live legs print a warning and pass, while
the committed `tests/data/gostore-*` fixtures always verify that
Go-written stores load.

Behavior suites mirror the Go tests file by file:

| Rust suite     | Go source                          |
| -------------- | ---------------------------------- |
| `api.rs`       | `spool_api_test.go`, `spool_diag_test.go` |
| `commit.rs`    | `spool_commit_test.go`, `spool_batch_test.go` |
| `recovery.rs`  | `spool_recovery_test.go`, `torn_tail_test.go` |
| `reclaim.rs`   | `spool_compact_test.go`, `spool_tomb_test.go` |
| `checkpoint.rs`| `spool_ckpt_test.go` (core cases)  |
| `keymgmt.rs`   | `spool_keymgmt_test.go`, `spool_rotation_test.go` |
| `maintenance.rs`| `spool_maint_test.go`             |
| `concurrent.rs`| `spool_concurrent_test.go`         |
| `faults.rs`    | `spool_fault_test.go` (core cases) |
| `smoke.rs`     | open/write/reopen round-trips      |

## License

MIT. See [LICENSE-MIT](LICENSE-MIT).

## Point reads and block cache

The default `target_block_bytes` is 64 KiB. Atomic groups span independently
compressed/encrypted blocks. Oversized individual records get dedicated blocks.
`read_cache_bytes` defaults to 32 MiB; zero disables caching. Blocks are admitted
on a second read and evicted by least recent access. The byte budget includes
compressed encrypted frames, decoded buffers and record descriptors. Hot reads
skip disk reads, decryption, decompression and body parsing. Decoded buffers are
wiped when released. Existing larger blocks remain compatible.

Ordered startup delivery uses this same bounded cache and parses each decoded
body once per cache miss. Cache entries survive into the opened store. One
current block remains available during delivery even with caching disabled;
all committed groups are still authenticated before winner delivery begins.

`Store::keys_page(prefix, after, limit)` returns a bounded, sorted page from the
existing location index, exclusively after the supplied key. It reads no values.
Use an immutable checkpoint for a stable boundary across pages; live pages can
reflect concurrent commits. Prefix lookups use ordered index ranges. The managed
SQLite layer handles oversized logical values with multiple physical records;
Spool's default 16 MiB value limit and 64 KiB block target remain unchanged.
