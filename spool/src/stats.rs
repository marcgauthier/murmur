//! Store statistics and diagnostics.
//!
//! Rust port of `spool/stats.go`.

use std::sync::atomic::Ordering;

use crate::filestats::SegmentState;
use crate::options::Encryption;
use crate::store::Store;

/// Store counters. All values are cheap snapshots.
#[derive(Debug, Clone)]
pub struct Stats {
    /// Live (non-deleted) key count.
    pub keys: u64,
    /// Tracked segment file count.
    pub segments: u64,
    /// Value records the index points at.
    pub live_records: u64,
    /// Superseded, stale and dropped records.
    pub dead_records: u64,
    /// Tracked segment content bytes.
    pub disk_bytes: u64,
    /// Live value record bytes.
    pub live_bytes: u64,
    /// Buffered plus in-flight records.
    pub pending_records: u64,
    /// Buffered plus in-flight bytes.
    pub pending_bytes: u64,
    /// Committed blocks (flushes and compactions).
    pub blocks_written: u64,
    /// Committed frame bytes on disk.
    pub bytes_written: u64,
    /// Uncompressed over compressed block bytes (1.0 when unmeasured).
    pub compression_ratio: f64,
    /// Completed compaction rewrites.
    pub compactions: u64,
    /// Bytes rewritten by compaction.
    pub compaction_bytes: u64,
    /// Torn tails found during loads.
    pub truncated_tails: u64,
    /// Failed background flushes (retried).
    pub flush_errors: u64,
    /// Failed background reclamation passes.
    pub reclaim_errors: u64,
    /// Admitted commit groups.
    pub groups_accepted: u64,
    /// Groups appended and committed.
    pub groups_written: u64,
    /// Groups committed at Sync durability.
    pub groups_synced: u64,
    /// Incomplete tail groups discarded in loads.
    pub groups_discarded: u64,
    /// Admitted groups awaiting commit.
    pub pending_groups: u64,
    /// Commit calls admitted (async and sync).
    pub commits: u64,
    /// Completed Sync/Flush barriers.
    pub syncs: u64,
    /// Terminal storage failures (0 or 1).
    pub storage_failures: u64,
    /// Completed data-key rotations (manual and age-triggered).
    pub rotations: u64,
    /// Registered backup checkpoints.
    pub checkpoints: u64,
    /// Cumulative Commit call time (divide by commits for the mean).
    pub commit_nanos: u64,
    /// Cumulative Sync/Flush barrier time (divide by syncs).
    pub sync_nanos: u64,
    /// flushInternal admissions (background and explicit).
    pub flushes: u64,
    /// Cumulative flushInternal time.
    pub flush_nanos: u64,
    /// Completed Reclaim passes.
    pub reclaims: u64,
    /// Cumulative Reclaim time.
    pub reclaim_nanos: u64,
    /// Cumulative file-rewrite time (divide by compactions).
    pub compaction_nanos: u64,
    /// Approximate index memory.
    pub index_bytes_estimate: u64,
    /// Worker-held buffer bound: Workers blocks built at once, each
    /// transiently holding plaintext, compressed, and sealed copies.
    pub worker_bytes_max: u64,
}

impl Store {
    /// Snapshot the store's counters.
    pub fn stats(&self) -> Stats {
        let (segments, _, live, dead, total_bytes, live_bytes) = self.inner.fstats.aggregate();
        let mut st = Stats {
            keys: self.inner.idx.key_count(),
            segments,
            live_records: live,
            dead_records: dead,
            disk_bytes: total_bytes,
            live_bytes,
            pending_records: 0,
            pending_bytes: 0,
            blocks_written: self.inner.blocks_written.load(Ordering::Relaxed),
            bytes_written: self.inner.bytes_written.load(Ordering::Relaxed),
            compression_ratio: 1.0,
            compactions: self.inner.compactions.load(Ordering::Relaxed),
            compaction_bytes: self.inner.compaction_bytes.load(Ordering::Relaxed),
            truncated_tails: self.inner.truncated_tails.load(Ordering::Relaxed),
            flush_errors: self.inner.flush_errors.load(Ordering::Relaxed),
            reclaim_errors: self.inner.reclaim_errors.load(Ordering::Relaxed),
            groups_accepted: self.inner.groups_accepted.load(Ordering::Relaxed),
            groups_written: self.inner.groups_written.load(Ordering::Relaxed),
            groups_synced: self.inner.groups_synced.load(Ordering::Relaxed),
            groups_discarded: self.inner.groups_discarded.load(Ordering::Relaxed),
            pending_groups: 0,
            commits: self.inner.commits.load(Ordering::Relaxed),
            syncs: self.inner.syncs.load(Ordering::Relaxed),
            storage_failures: self.inner.storage_failures.load(Ordering::Relaxed),
            rotations: self.inner.rotations.load(Ordering::Relaxed),
            checkpoints: self.inner.ckpt.lock().unwrap().len() as u64,
            commit_nanos: self.inner.commit_nanos.load(Ordering::Relaxed),
            sync_nanos: self.inner.sync_nanos.load(Ordering::Relaxed),
            flushes: self.inner.flushes.load(Ordering::Relaxed),
            flush_nanos: self.inner.flush_nanos.load(Ordering::Relaxed),
            reclaims: self.inner.reclaims.load(Ordering::Relaxed),
            reclaim_nanos: self.inner.reclaim_nanos.load(Ordering::Relaxed),
            compaction_nanos: self.inner.compaction_nanos.load(Ordering::Relaxed),
            index_bytes_estimate: self.inner.idx.memory_estimate(),
            worker_bytes_max: self.inner.workers as u64
                * self.inner.opts.max_block_bytes as u64
                * 3,
        };
        st.pending_records = self.inner.pending_records.load(Ordering::SeqCst).max(0) as u64;
        st.pending_bytes = self.inner.pending_bytes.load(Ordering::SeqCst).max(0) as u64;
        st.pending_groups = self.inner.pending_groups.load(Ordering::SeqCst).max(0) as u64;
        let (comp, uncomp) = (
            self.inner.bytes_written.load(Ordering::Relaxed),
            self.inner.bytes_uncompressed.load(Ordering::Relaxed),
        );
        if comp > 0 {
            st.compression_ratio = uncomp as f64 / comp as f64;
        }
        st
    }
}

/// Store identity, effective algorithms, and positions.
#[derive(Debug, Clone)]
pub struct StoreInfo {
    /// Immutable random store identity.
    pub store_id: [u8; 16],
    /// Whether blocks are sealed.
    pub encrypted: bool,
    /// Effective algorithms.
    pub encryption: Encryption,
    /// Effective compression (wire id).
    pub compression: u8,
    /// Authenticated wrapping id ("" when plain).
    pub wrapping_key_id: String,
    /// Authoritative manifest generation.
    pub generation: u64,
    /// Sequence epoch of this open.
    pub epoch: u64,
    /// Newest admitted group (0 when none).
    pub last_group_id: u64,
    /// Segment writer positions.
    pub next_file_id: u64,
    /// Segment writer positions.
    pub active_file_id: u64,
}

impl Store {
    /// Snapshot store identity and positions. Touches memory only.
    pub fn info(&self) -> StoreInfo {
        let m = self.inner.manifest.lock().unwrap();
        StoreInfo {
            store_id: m.store_id,
            encrypted: self.inner.ring.is_some(),
            encryption: self.inner.opts.encryption,
            compression: self
                .inner
                .opts
                .compression
                .map(|c| c.0)
                .unwrap_or(crate::compression::ID_DEFLATE),
            wrapping_key_id: self.inner.wrap_id.read().unwrap().clone(),
            generation: m.generation,
            epoch: self.inner.seq_base >> crate::store::EPOCH_SHIFT,
            last_group_id: self.inner.last_group.load(Ordering::SeqCst),
            next_file_id: m.next_file_id,
            active_file_id: m.active_file_id,
        }
    }
}

/// Per-file diagnostic snapshot.
#[derive(Debug, Clone)]
pub struct FileStat {
    /// Segment file id.
    pub file_id: u64,
    /// Lifecycle state.
    pub state: SegmentState,
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
    /// Live tombstone winners.
    pub live_tombs: u64,
}

impl Store {
    /// Snapshot per-file statistics ascending by file id.
    pub fn file_stats(&self) -> Vec<FileStat> {
        let mut ids = self.inner.fstats.ids();
        ids.sort_unstable();
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            let Some(snap) = self.inner.fstats.snapshot(id) else {
                continue;
            };
            out.push(FileStat {
                file_id: id,
                state: snap.state,
                total_records: snap.stats.total_records,
                live_records: snap.stats.live_records,
                dead_records: snap.stats.dead_records,
                total_bytes: snap.stats.total_bytes,
                live_bytes: snap.stats.live_bytes,
                live_tombs: snap.live_tombs,
            });
        }
        out
    }
}
