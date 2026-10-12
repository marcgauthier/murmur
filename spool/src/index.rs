//! Sharded key→location index.
//!
//! Rust port of `spool/index.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::RwLock;

/// One record version on disk. Identity for liveness checks is the
/// full tuple; `sequence` orders versions of a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
    /// Segment file id.
    pub file_id: u64,
    /// Block sequence.
    pub block_id: u64,
    /// Block's starting offset in the file.
    pub block_offset: i64,
    /// Record index within the block.
    pub record_index: u32,
    /// Record sequence.
    pub sequence: u64,
    /// Key+value bytes plus framing.
    pub size: u32,
}

/// In-memory winner for a deleted key.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TombMark {
    pub seq: u64,
    pub file: u64,
    /// Full winning location for exact delivery matching.
    pub loc: Location,
}

struct IndexShard {
    lock: RwLock<ShardMaps>,
}

struct ShardMaps {
    values: HashMap<Vec<u8>, Location>,
    tombs: HashMap<Vec<u8>, TombMark>,
}

/// Sharded key→location map plus tombstone winners.
pub(crate) struct Index {
    shards: Vec<IndexShard>,
    mask: u64,
    keys: AtomicI64,
}

impl Index {
    pub fn new(shard_count: usize) -> Self {
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(IndexShard {
                lock: RwLock::new(ShardMaps {
                    values: HashMap::new(),
                    tombs: HashMap::new(),
                }),
            });
        }
        Index {
            shards,
            mask: (shard_count - 1) as u64,
            keys: AtomicI64::new(0),
        }
    }

    fn shard_idx(&self, key: &[u8]) -> usize {
        shard_for_key(key, self.mask) as usize
    }

    /// Install a record as the current version of `key` when newer
    /// than anything known. Shared by flush, compaction and load.
    pub fn commit_record(
        &self,
        fs: &crate::filestats::FileStatsRegistry,
        key: &[u8],
        seq: u64,
        tomb: bool,
        mut loc: Location,
        size: u32,
    ) {
        let sh = &self.shards[self.shard_idx(key)];
        let mut maps = sh.lock.write().unwrap();
        let cur = current_locked(&maps, key);
        if let Some((cur_seq, _, _, _, _)) = cur {
            if seq <= cur_seq {
                // Stale: a newer version already won.
                fs.record_dead(loc.file_id, size);
                return;
            }
        }
        if let Some((_, cur_tomb, cur_file, cur_size, _)) = cur {
            if cur_tomb {
                maps.tombs.remove(key);
                fs.tomb_evicted(cur_file);
            } else {
                maps.values.remove(key);
                fs.value_evicted(cur_file, cur_size);
                self.keys.fetch_add(-1, Ordering::Relaxed);
            }
        }
        if tomb {
            loc.sequence = seq;
            loc.size = size;
            maps.tombs.insert(
                key.to_vec(),
                TombMark {
                    seq,
                    file: loc.file_id,
                    loc,
                },
            );
            fs.tomb_installed(loc.file_id, size);
            return;
        }
        loc.sequence = seq;
        loc.size = size;
        maps.values.insert(key.to_vec(), loc);
        fs.value_installed(loc.file_id, size);
        self.keys.fetch_add(1, Ordering::Relaxed);
    }

    /// True when the index still resolves `key` to exactly `want`.
    pub fn points_at(&self, key: &[u8], want: Location) -> bool {
        let sh = &self.shards[self.shard_idx(key)];
        let maps = sh.lock.read().unwrap();
        matches!(maps.values.get(key), Some(got) if *got == want)
    }

    /// True when the tombstone (key, seq, file) still wins.
    pub fn tomb_live(&self, key: &[u8], seq: u64, file: u64) -> bool {
        let sh = &self.shards[self.shard_idx(key)];
        let maps = sh.lock.read().unwrap();
        matches!(maps.tombs.get(key), Some(tm) if tm.seq == seq && tm.file == file)
    }

    /// Remove the tombstone winner for `key` when it still matches
    /// (seq, file). Returns the dropped mark.
    pub fn drop_tomb(&self, key: &[u8], seq: u64, file: u64) -> Option<TombMark> {
        let sh = &self.shards[self.shard_idx(key)];
        let mut maps = sh.lock.write().unwrap();
        let tm = maps.tombs.get(key)?;
        if tm.seq != seq || tm.file != file {
            return None;
        }
        maps.tombs.remove(key)
    }

    /// Copy every winning version for second-pass load delivery.
    pub fn snapshot_winners(&self) -> HashMap<Vec<u8>, WinnerLoc> {
        let mut out: HashMap<Vec<u8>, WinnerLoc> = HashMap::new();
        for sh in &self.shards {
            let maps = sh.lock.read().unwrap();
            for (k, loc) in &maps.values {
                match out.get(k) {
                    Some(w) if w.seq >= loc.sequence => {}
                    _ => {
                        out.insert(
                            k.clone(),
                            WinnerLoc {
                                seq: loc.sequence,
                                tomb: false,
                                loc: *loc,
                            },
                        );
                    }
                }
            }
            for (k, tm) in &maps.tombs {
                match out.get(k) {
                    Some(w) if w.seq >= tm.seq => {}
                    _ => {
                        out.insert(
                            k.clone(),
                            WinnerLoc {
                                seq: tm.seq,
                                tomb: true,
                                loc: tm.loc,
                            },
                        );
                    }
                }
            }
        }
        out
    }

    pub fn location(&self, key: &[u8]) -> Option<Location> {
        self.shards[self.shard_idx(key)]
            .lock
            .read()
            .unwrap()
            .values
            .get(key)
            .copied()
    }
    pub fn keys_with_prefix(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for shard in &self.shards {
            keys.extend(
                shard
                    .lock
                    .read()
                    .unwrap()
                    .values
                    .keys()
                    .filter(|k| k.starts_with(prefix))
                    .cloned(),
            );
        }
        keys.sort();
        keys
    }

    /// Number of live (non-deleted) keys.
    pub fn key_count(&self) -> u64 {
        self.keys.load(Ordering::Relaxed).max(0) as u64
    }

    /// Approximate index memory: key bytes plus a per-entry
    /// allowance. A diagnostic estimate, not an allocator measure.
    pub fn memory_estimate(&self) -> u64 {
        const PER_ENTRY: u64 = 128;
        let mut total: u64 = 0;
        for sh in &self.shards {
            let maps = sh.lock.read().unwrap();
            for k in maps.values.keys() {
                total += k.len() as u64 + PER_ENTRY;
            }
            for k in maps.tombs.keys() {
                total += k.len() as u64 + PER_ENTRY;
            }
        }
        total
    }

    /// Rewrite every entry's block offset from a maintenance re-seal
    /// map. Returns the first unmapped block sequence, if any.
    pub fn rebase_blocks(&self, offsets: &HashMap<u64, i64>) -> Result<(), u64> {
        for sh in &self.shards {
            let mut maps = sh.lock.write().unwrap();
            for loc in maps.values.values_mut() {
                match offsets.get(&loc.block_id) {
                    Some(off) => loc.block_offset = *off,
                    None => return Err(loc.block_id),
                }
            }
            for tm in maps.tombs.values_mut() {
                match offsets.get(&tm.loc.block_id) {
                    Some(off) => tm.loc.block_offset = *off,
                    None => return Err(tm.loc.block_id),
                }
            }
        }
        Ok(())
    }

    /// Call `f` for every tombstone winner. The entry may change
    /// concurrently; callers must re-verify identity.
    pub fn scan_tombs(&self, mut f: impl FnMut(&[u8], TombMark)) {
        for sh in &self.shards {
            let batch: Vec<(Vec<u8>, TombMark)> = {
                let maps = sh.lock.read().unwrap();
                maps.tombs.iter().map(|(k, tm)| (k.clone(), *tm)).collect()
            };
            for (k, tm) in batch {
                f(&k, tm);
            }
        }
    }

    /// Install a rewritten record only when the index still resolves
    /// the key to exactly the expected old location.
    pub fn commit_rewrite(
        &self,
        fs: &crate::filestats::FileStatsRegistry,
        key: &[u8],
        expected: Location,
        tomb: bool,
        loc: Location,
        size: u32,
    ) -> bool {
        let sh = &self.shards[self.shard_idx(key)];
        let mut maps = sh.lock.write().unwrap();
        if tomb {
            match maps.tombs.get(key) {
                Some(tm) if tm.seq == expected.sequence && tm.file == expected.file_id => {}
                _ => {
                    fs.record_dead(loc.file_id, size);
                    return false;
                }
            }
            maps.tombs.remove(key);
            fs.tomb_evicted(expected.file_id);
            maps.tombs.insert(
                key.to_vec(),
                TombMark {
                    seq: loc.sequence,
                    file: loc.file_id,
                    loc,
                },
            );
            fs.tomb_installed(loc.file_id, size);
            return true;
        }
        match maps.values.get(key) {
            Some(got) if *got == expected => {}
            _ => {
                fs.record_dead(loc.file_id, size);
                return false;
            }
        }
        let old_size = maps.values.get(key).map(|l| l.size).unwrap_or(0);
        maps.values.remove(key);
        fs.value_evicted(expected.file_id, old_size);
        maps.values.insert(key.to_vec(), loc);
        fs.value_installed(loc.file_id, size);
        true
    }
}

/// One key's winning version for second-pass delivery.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WinnerLoc {
    pub seq: u64,
    #[allow(dead_code)]
    pub tomb: bool,
    pub loc: Location,
}

/// Current winning version of `key`, if any:
/// (seq, is_tomb, file, size).
fn current_locked(maps: &ShardMaps, key: &[u8]) -> Option<(u64, bool, u64, u32, bool)> {
    let mut out: Option<(u64, bool, u64, u32, bool)> = None;
    if let Some(loc) = maps.values.get(key) {
        out = Some((loc.sequence, false, loc.file_id, loc.size, true));
    }
    if let Some(tm) = maps.tombs.get(key) {
        let take = match &out {
            None => true,
            Some((seq, _, _, _, _)) => tm.seq > *seq,
        };
        if take {
            out = Some((tm.seq, true, tm.file, 0, true));
        }
    }
    out
}

const FNV_OFFSET64: u64 = 14695981039346656037;
const FNV_PRIME64: u64 = 1099511628211;

/// FNV-1a shard selection (matches Go's `shardForKey`).
pub(crate) fn shard_for_key(key: &[u8], mask: u64) -> u64 {
    let mut h = FNV_OFFSET64;
    for b in key {
        h ^= *b as u64;
        h = h.wrapping_mul(FNV_PRIME64);
    }
    h & mask
}
