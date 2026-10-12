//! Spool: an embedded persistence engine optimized for very fast
//! concurrent writes.
//!
//! Rust port of the Go `spool` package (`github.com/marcgauthier/murmur/spool`),
//! byte-compatible on disk: block v3, manifest v3, keys.enc v3, and the
//! maintenance intent v1 formats are identical, so stores written by
//! either implementation open in the other.
//!
//! Indexed `get` reads use a bounded hot block cache. Spool durably journals
//! every mutation and rebuilds state at startup through load callbacks.
//! Writes are sharded in memory,
//! sealed into blocks, compressed and encrypted in parallel, and
//! appended sequentially to segment files.
//!
//! Basic use:
//!
//! ```no_run
//! use spool::{default_options, open_and_load};
//!
//! let mut opts = default_options("./data");
//! opts.master_key = vec![7u8; 32];
//! let store = open_and_load(&opts, &mut |records| {
//!     // populate application memory; each key arrives once
//!     // with its current version (tombstone if deleted)
//!     Ok(())
//! })?;
//! store.put(b"k", b"v")?;
//! store.delete(b"old")?;
//! store.flush()?; // explicit durability point
//! store.close()?;
//! # Ok::<(), spool::Error>(())
//! ```

mod read_cache;
pub mod block;
pub mod checkpoint;
pub mod compaction;
pub mod compression;
pub mod crypto;
pub mod error;
pub mod filestats;
pub mod index;
pub mod keymgmt;
pub mod keyring;
pub mod loader;
pub mod lock;
pub mod maintenance;
pub mod manifest;
pub mod options;
pub mod record;
pub mod segment;
pub mod stats;
pub mod store;

pub use checkpoint::Checkpoint;
pub use compression::Codec;
pub use error::{Error, ErrorKind, Result};
pub use filestats::{FileStats, SegmentState};
pub use index::Location;
pub use keymgmt::{DataKeyInfo, KeyInventory, KeyReference};
pub use keyring::{read_key_hint, validate_wrapping_key, DataKey, KeyHint, KeyStatus};
pub use loader::{load, load_with_options, LoadFunc, LoadOptions, ReplayProgress};
pub use options::{
    default_options, Compression, Durability, Encryption, FaultHook, FaultHooks, FlushPolicy,
    Options,
};
pub use record::{Mutation, Record, KV};
pub use stats::{FileStat, Stats, StoreInfo};
pub use store::{open, open_and_load, open_and_load_ordered, Store};
