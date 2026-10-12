//! Store directory locking.
//!
//! Rust port of `spool/filelock_unix.go` / `spool/filelock_windows.go`:
//! an exclusive, non-blocking lock on `dir/LOCK` for the store's
//! lifetime.

use std::fs::{self, File};
use std::path::Path;

use fs2::FileExt;

use crate::error::{err, ErrorKind, Result};

pub(crate) const LOCK_FILE_NAME: &str = "LOCK";

/// Held lock guard. Dropping closes the file, releasing the lock.
pub(crate) struct StoreLock {
    _file: File,
}

/// Take the store lock. Double opens fail with [`ErrorKind::Locked`].
pub(crate) fn lock_store(dir: &Path) -> Result<StoreLock> {
    let f = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(dir.join(LOCK_FILE_NAME))
        .map_err(|e| err(ErrorKind::Io, format!("lock file: {e}")))?;
    f.try_lock_exclusive()
        .map_err(|_| err(ErrorKind::Locked, "store is locked by another process"))?;
    Ok(StoreLock { _file: f })
}
