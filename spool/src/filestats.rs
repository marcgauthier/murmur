//! Per-segment counters.
//!
//! Rust port of `spool/filestats.go`.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::SystemTime;

/// A segment file's lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentState {
    /// Accepts new appends.
    Active,
    /// Immutable and readable.
    Sealed,
    /// Being rewritten; reclamation must skip it.
    Compacting,
    /// Fully reclaimed and awaiting unlink.
    Obsolete,
}

impl std::fmt::Display for SegmentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SegmentState::Active => write!(f, "active"),
            SegmentState::Sealed => write!(f, "sealed"),
            SegmentState::Compacting => write!(f, "compacting"),
            SegmentState::Obsolete => write!(f, "obsolete"),
        }
    }
}

/// A segment's record and byte counters.
#[derive(Debug, Clone)]
pub struct FileStats {
    /// Segment file id.
    pub file_id: u64,
    /// Total records ever attributed.
    pub total_records: u64,
    /// Live value records.
    pub live_records: u64,
    /// Superseded, stale and dropped records.
    pub dead_records: u64,
    /// Total record bytes.
    pub total_bytes: u64,
    /// Live value record bytes.
    pub live_bytes: u64,
    /// Segment creation time.
    pub created_at: SystemTime,
}

#[derive(Debug, Clone)]
pub(crate) struct FileInfo {
    pub stats: FileStats,
    pub state: SegmentState,
    /// Tombstone records still winning their keys.
    pub live_tombs: u64,
}

/// Guarded per-segment counters.
pub(crate) struct FileStatsRegistry {
    lock: RwLock<HashMap<u64, FileInfo>>,
}

impl FileStatsRegistry {
    pub fn new() -> Self {
        FileStatsRegistry {
            lock: RwLock::new(HashMap::new()),
        }
    }

    /// Add a file id if absent.
    pub fn register(&self, id: u64, created: SystemTime) {
        let mut m = self.lock.write().unwrap();
        m.entry(id).or_insert(FileInfo {
            stats: FileStats {
                file_id: id,
                total_records: 0,
                live_records: 0,
                dead_records: 0,
                total_bytes: 0,
                live_bytes: 0,
                created_at: created,
            },
            state: SegmentState::Active,
            live_tombs: 0,
        });
    }

    /// Update a file's lifecycle state.
    pub fn set_state(&self, id: u64, st: SegmentState) {
        let mut m = self.lock.write().unwrap();
        if let Some(f) = m.get_mut(&id) {
            f.state = st;
        }
    }

    fn with(&self, id: u64, f: impl FnOnce(&mut FileInfo)) {
        let mut m = self.lock.write().unwrap();
        let e = m.entry(id).or_insert_with(|| FileInfo {
            stats: FileStats {
                file_id: id,
                total_records: 0,
                live_records: 0,
                dead_records: 0,
                total_bytes: 0,
                live_bytes: 0,
                created_at: SystemTime::now(),
            },
            state: SegmentState::Active,
            live_tombs: 0,
        });
        f(e);
    }

    /// Account a newly visible value record.
    pub fn value_installed(&self, id: u64, size: u32) {
        self.with(id, |f| {
            f.stats.total_records += 1;
            f.stats.total_bytes += size as u64;
            f.stats.live_records += 1;
            f.stats.live_bytes += size as u64;
        });
    }

    /// Account a superseded value record.
    pub fn value_evicted(&self, id: u64, size: u32) {
        let mut m = self.lock.write().unwrap();
        if let Some(f) = m.get_mut(&id) {
            f.stats.live_records = f.stats.live_records.saturating_sub(1);
            f.stats.live_bytes = f.stats.live_bytes.saturating_sub(size as u64);
            f.stats.dead_records += 1;
        }
    }

    /// Account a newly winning tombstone.
    pub fn tomb_installed(&self, id: u64, size: u32) {
        self.with(id, |f| {
            f.stats.total_records += 1;
            f.stats.total_bytes += size as u64;
            f.live_tombs += 1;
        });
    }

    /// Account a superseded tombstone.
    pub fn tomb_evicted(&self, id: u64) {
        let mut m = self.lock.write().unwrap();
        if let Some(f) = m.get_mut(&id) {
            f.live_tombs = f.live_tombs.saturating_sub(1);
            f.stats.dead_records += 1;
        }
    }

    /// Account a reclaimed tombstone: no longer live but still
    /// occupying space until its file unlinks.
    pub fn tomb_dropped(&self, id: u64) {
        let mut m = self.lock.write().unwrap();
        if let Some(f) = m.get_mut(&id) {
            f.live_tombs = f.live_tombs.saturating_sub(1);
            f.stats.dead_records += 1;
        }
    }

    /// Account a stillborn record (stale on arrival).
    pub fn record_dead(&self, id: u64, size: u32) {
        self.with(id, |f| {
            f.stats.total_records += 1;
            f.stats.total_bytes += size as u64;
            f.stats.dead_records += 1;
        });
    }

    /// Copy one file's info.
    pub fn snapshot(&self, id: u64) -> Option<FileInfo> {
        self.lock.read().unwrap().get(&id).cloned()
    }

    /// All tracked file ids.
    pub fn ids(&self) -> Vec<u64> {
        self.lock.read().unwrap().keys().copied().collect()
    }

    /// Forget a file after unlinking.
    pub fn remove(&self, id: u64) {
        self.lock.write().unwrap().remove(&id);
    }

    /// Sum counters for stats: (segments, total, live, dead, total_bytes, live_bytes).
    pub fn aggregate(&self) -> (u64, u64, u64, u64, u64, u64) {
        let m = self.lock.read().unwrap();
        let mut out = (m.len() as u64, 0, 0, 0, 0, 0);
        for f in m.values() {
            out.1 += f.stats.total_records;
            out.2 += f.stats.live_records;
            out.3 += f.stats.dead_records;
            out.4 += f.stats.total_bytes;
            out.5 += f.stats.live_bytes;
        }
        out
    }
}

impl Default for FileStatsRegistry {
    fn default() -> Self {
        Self::new()
    }
}
