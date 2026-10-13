//! Open-with-key plus `on_open_progress` watchdog.
//!
//! Rust equivalent of the Go await-unlock + `/v1/admin/open-progress`
//! polling: same env knobs (`MURMUR_LIVE_UNLOCK_STALL_SECONDS=120`,
//! `MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS=1800`), same advancing-vs-frozen
//! semantics. `Database::open` runs on a worker thread while the caller
//! watches progress snapshots; a stall (no advancement for the stall window)
//! or an overall timeout fails fast.
//!
//! Deviation: the Rust product has no open cancellation, so a timed-out open
//! keeps running detached in the background. Do not reuse that directory
//! afterwards (use a fresh node dir or abort the scenario).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use sqlite_session::{Database, OpenProgress, Options};

/// Comparable progress snapshot (clocks and error text excluded).
#[derive(Debug, Clone, PartialEq)]
struct Snapshot {
    phase: sqlite_session::OpenPhase,
    processed_items: u64,
    rows_inserted: u64,
    rows_skipped: u64,
    current_table: Option<String>,
}

impl From<&OpenProgress> for Snapshot {
    fn from(p: &OpenProgress) -> Self {
        Self {
            phase: p.phase,
            processed_items: p.processed_items,
            rows_inserted: p.rows_inserted,
            rows_skipped: p.rows_skipped,
            current_table: p.current_table.clone(),
        }
    }
}

/// Open a database under the stall/overall watchdog. Any caller-provided
/// `on_open_progress` callback is preserved (it runs after the recorder).
pub fn open_with_watchdog(
    path: impl AsRef<Path>,
    key: impl AsRef<[u8]>,
    mut opts: Options,
) -> crate::Result<Database> {
    let stall = crate::env_seconds("MURMUR_LIVE_UNLOCK_STALL_SECONDS", 120);
    let overall = crate::env_seconds("MURMUR_LIVE_UNLOCK_TIMEOUT_SECONDS", 1800);
    let path_buf: PathBuf = path.as_ref().to_path_buf();
    let key_vec: Vec<u8> = key.as_ref().to_vec();

    let seen: Arc<Mutex<Option<Snapshot>>> = Arc::new(Mutex::new(None));
    let seen_cb = seen.clone();
    let user_cb = opts.on_open_progress.clone();
    opts.on_open_progress = Some(Arc::new(move |p: OpenProgress| {
        *seen_cb.lock().unwrap() = Some(Snapshot::from(&p));
        if let Some(cb) = &user_cb {
            cb(p);
        }
    }));

    let (tx, rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("murmur-live-open".into())
        .spawn(move || {
            let _ = tx.send(Database::open(&path_buf, &key_vec, opts));
        })
        .map_err(|e| crate::Error::Setup(format!("spawn open thread: {e}")))?;

    let start = Instant::now();
    let mut last_advance = start;
    let mut last_snapshot: Option<Snapshot> = None;
    let mut last_log = start;
    loop {
        match rx.recv_timeout(Duration::from_millis(250)) {
            Ok(result) => return result.map_err(crate::Error::from),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(crate::Error::Setup("open worker thread died".into()));
            }
        }
        let current = seen.lock().unwrap().clone();
        if current != last_snapshot {
            last_advance = Instant::now();
            last_snapshot = current.clone();
        }
        let elapsed = start.elapsed();
        if last_advance.elapsed() >= stall {
            return Err(crate::Error::UnlockTimeout(
                elapsed,
                "no open-progress advancement",
            ));
        }
        if elapsed >= overall {
            return Err(crate::Error::UnlockTimeout(elapsed, "overall open timeout"));
        }
        if last_log.elapsed() >= Duration::from_secs(30) {
            last_log = Instant::now();
            eprintln!("open still advancing after {elapsed:?}: {current:?}");
        }
    }
}
