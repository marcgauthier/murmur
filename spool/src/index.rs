//! Sharded key→location index.
//!
//! Rust port of `spool/index.go`.

use std::collections::{HashMap, BTreeMap};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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
    // Disposable namespace membership hints, never record locations/versions.
    // Bits are only added: deletion/collisions can cause extra searches, never
    // missing keys. Published before inserting a live key under the shard lock.
    namespaces: [AtomicU64; 16],
}

impl IndexShard {
    fn may_match(&self, bits: Option<[usize; 3]>) -> bool {
        bits.map_or(true, |bits| bits.into_iter().all(|bit|
            self.namespaces[bit / 64].load(Ordering::SeqCst) & (1 << (bit % 64)) != 0))
    }
    fn remember(&self, key: &[u8]) {
        if let Some(bits) = namespace_bits(key) {
            for bit in bits {
                let word = &self.namespaces[bit / 64]; let mask = 1 << (bit % 64);
                if word.load(Ordering::Relaxed) & mask == 0 {
                    word.fetch_or(mask, Ordering::SeqCst);
                }
            }
        }
    }
}

// Any key matching a prefix with three '/' separators has the same namespace.
// Short/non-path prefixes simply use the original full-shard traversal.
fn namespace_bits(key: &[u8]) -> Option<[usize; 3]> {
    let mut separators = 0;
    for (index, byte) in key.iter().enumerate() {
        if *byte != b'/' { continue; }
        separators += 1;
        if separators == 3 {
            let hash = shard_for_key(&key[..=index], u64::MAX);
            let step = (hash >> 32) | 1;
            return Some(std::array::from_fn(|i|
                hash.wrapping_add(step.wrapping_mul(i as u64)) as usize & 1023));
        }
    }
    None
}

struct ShardMaps {
    values: BTreeMap<Vec<u8>, Location>,
    tombs: HashMap<Vec<u8>, TombMark>,
}

/// Sharded key→location map plus tombstone winners.
pub(crate) struct Index {
    shards: Vec<IndexShard>,
    mask: u64,
    keys: AtomicI64,
    memory_bytes: AtomicU64,
}

const PER_ENTRY_BYTES: u64 = 128;

impl Index {
    pub fn new(shard_count: usize) -> Self {
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            shards.push(IndexShard {
                namespaces: std::array::from_fn(|_| AtomicU64::new(0)),
                lock: RwLock::new(ShardMaps {
                    values: BTreeMap::new(),
                    tombs: HashMap::new(),
                }),
            });
        }
        Index {
            shards,
            mask: (shard_count - 1) as u64,
            keys: AtomicI64::new(0),
            memory_bytes: AtomicU64::new(0),
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
        } else {
            // Replacing a value/tombstone keeps the same key charge. Loading
            // and inserting a new winner add it once under the shard lock.
            self.memory_bytes.fetch_add(key.len() as u64 + PER_ENTRY_BYTES, Ordering::Relaxed);
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
        if cur.map_or(true, |(_, was_tomb, _, _, _)| was_tomb) {
            sh.remember(key);
        }
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
        let removed = maps.tombs.remove(key);
        if removed.is_some() {
            self.memory_bytes.fetch_sub(key.len() as u64 + PER_ENTRY_BYTES, Ordering::Relaxed);
        }
        removed
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
        let bits = namespace_bits(prefix);
        for shard in &self.shards {
            if !shard.may_match(bits) { continue; }
            keys.extend(
                shard
                    .lock
                    .read()
                    .unwrap()
                    .values
                    .range::<[u8], _>((std::ops::Bound::Included(prefix), std::ops::Bound::Unbounded))
                    .take_while(|(k,_)| k.starts_with(prefix))
                    .map(|(k,_)| k.clone()),
            );
        }
        keys.sort();
        keys
    }

    /// Bounded sorted traversal of the existing location index.
    pub fn keys_page(&self,prefix:&[u8],after:Option<&[u8]>,limit:usize)->Vec<Vec<u8>> {
        let mut keys=std::collections::BTreeSet::new();
        if limit==0{return Vec::new();}
        let bits = namespace_bits(prefix);
        for shard in &self.shards {
            if !shard.may_match(bits) { continue; }
            let maps=shard.lock.read().unwrap();
            let start=after.filter(|after|*after>prefix).unwrap_or(prefix);
            for (key,_) in maps.values.range::<[u8], _>((std::ops::Bound::Included(start), std::ops::Bound::Unbounded)).take_while(|(key,_)|key.starts_with(prefix)) {
                if after.is_some_and(|after|key.as_slice()<=after){continue;}
                keys.insert(key.clone());
                if keys.len()>limit {keys.pop_last();}
                if keys.len()==limit && keys.last().is_some_and(|last|key>=last){break;}
            }
        }
        keys.into_iter().collect()
    }

    /// Number of live (non-deleted) keys.
    pub fn key_count(&self) -> u64 {
        self.keys.load(Ordering::Relaxed).max(0) as u64
    }

    /// Approximate index memory: key bytes plus a per-entry
    /// allowance. A diagnostic estimate, not an allocator measure.
    pub fn memory_estimate(&self) -> u64 {
        self.memory_bytes.load(Ordering::Relaxed)
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

#[cfg(test)]
mod memory_tests {
    use super::*;
    use crate::filestats::FileStatsRegistry;

    fn location(seq: u64) -> Location {
        Location { file_id: 1, block_id: seq, block_offset: 0,
            record_index: 0, sequence: seq, size: 16 }
    }
    fn scanned(index: &Index) -> u64 {
        index.shards.iter().map(|shard| {
            let maps = shard.lock.read().unwrap();
            maps.values.keys().chain(maps.tombs.keys())
                .map(|key| key.len() as u64 + PER_ENTRY_BYTES).sum::<u64>()
        }).sum()
    }
    fn scanned_keys(index: &Index, prefix: &[u8]) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for shard in &index.shards {
            keys.extend(shard.lock.read().unwrap().values.range(prefix.to_vec()..)
                .take_while(|(key,_)| key.starts_with(prefix)).map(|(key,_)| key.clone()));
        }
        keys.sort(); keys
    }
    #[test]
    fn prefix_hints_preserve_binary_keys_pages_and_winner_transitions() {
        let index = Index::new(256); let fs = FileStatsRegistry::new();
        for seq in 1..=500 {
            let key = format!("a/b/{}/{}", seq % 7, seq % 53).into_bytes();
            index.commit_record(&fs,&key,seq,seq % 3 == 0,location(seq),16);
        }
        for (offset,key) in [b"\xff/\0/\xfe/one".as_slice(),b"a//b/",b"a/b/",b"plain",b""].into_iter().enumerate() {
            let seq = 1000 + offset as u64;
            index.commit_record(&fs,key,seq,false,location(seq),16);
        }
        for prefix in [b"".as_slice(),b"a",b"a/",b"a/b",b"a/b/",b"a/b/1/",b"a/b/missing/",b"a//b/",b"\xff/\0/\xfe/",b"plain/"] {
            let expected = scanned_keys(&index,prefix);
            assert_eq!(index.keys_with_prefix(prefix),expected);
            let mut actual = Vec::new(); let mut after: Option<Vec<u8>> = None;
            loop {
                let page = index.keys_page(prefix,after.as_deref(),5);
                if page.is_empty() { break; }
                after = page.last().cloned(); actual.extend(page);
            }
            assert_eq!(actual,expected);
        }
        // Saturation/collisions must degrade to searching, not incorrect results.
        for shard in &index.shards {for word in &shard.namespaces {word.store(u64::MAX,Ordering::SeqCst);}}
        assert_eq!(index.keys_with_prefix(b"a/b/1/"),scanned_keys(&index,b"a/b/1/"));
        assert!(index.keys_with_prefix(b"a/b/missing/").is_empty());
    }
    #[test]
    fn completed_concurrent_insertions_are_never_skipped() {
        let index = Index::new(256); let fs = FileStatsRegistry::new();
        std::thread::scope(|scope| {
            for thread in 0..8 {
                let index = &index; let fs = &fs;
                scope.spawn(move || {
                    for seq in 1..=200 {
                        let key = format!("a/b/{thread}/{seq}");
                        index.commit_record(fs,key.as_bytes(),seq,false,location(seq),16);
                        assert!(index.keys_with_prefix(format!("a/b/{thread}/").as_bytes()).contains(&key.as_bytes().to_vec()));
                    }
                });
            }
        });
        assert_eq!(index.keys_with_prefix(b"a/b/"),scanned_keys(&index,b"a/b/"));
    }
    #[test]
    #[ignore = "component prefix-search performance experiment"]
    fn prefix_search_benchmark() {
        let index = Index::new(256); let fs = FileStatsRegistry::new();
        for seq in 1..=100_000 {
            let key = format!("05/meta/row/{seq:08}");
            index.commit_record(&fs,key.as_bytes(),seq,false,location(seq),16);
        }
        for origins in [10,500] {
            for origin in 0..origins {
                let key = format!("07/replication/applied/{origin:032x}");
                index.commit_record(&fs,key.as_bytes(),origin + 1,false,location(origin + 1),16);
            }
            let prefix = b"07/replication/applied/";
            let expected = scanned_keys(&index,prefix);
            assert_eq!(index.keys_with_prefix(prefix),expected);
            let candidates = index.shards.iter().filter(|shard|shard.may_match(namespace_bits(prefix))).count();
            let start = std::time::Instant::now();
            for _ in 0..2000 {std::hint::black_box(scanned_keys(&index,prefix));}
            let before_ns = start.elapsed().as_nanos();
            let start = std::time::Instant::now();
            for _ in 0..2000 {std::hint::black_box(index.keys_with_prefix(prefix));}
            let after_ns = start.elapsed().as_nanos();
            println!("PREFIX_SEARCH origins={origins} queries=2000 shards=256 candidates={candidates} before_ns={before_ns} after_ns={after_ns}");
        }
    }
    #[test]
    fn accounting_matches_winners_through_delete_rewrite_and_reinsertion() {
        let index = Index::new(16);
        let fs = FileStatsRegistry::new();
        for seq in 1..=500 {
            let key = format!("key/{:04}/{}", seq % 37, "x".repeat((seq % 37) as usize));
            index.commit_record(&fs, key.as_bytes(), seq, seq % 3 == 0, location(seq), 16);
            // Older delivery must not alter accounting.
            index.commit_record(&fs, key.as_bytes(), seq - 1, seq % 3 != 0, location(seq - 1), 16);
            assert_eq!(index.memory_estimate(), scanned(&index));
            if let Some(old) = index.location(key.as_bytes()) {
                let mut rewritten = old; rewritten.file_id = 2;
                assert!(index.commit_rewrite(&fs, key.as_bytes(), old, false, rewritten, 16));
                assert!(!index.commit_rewrite(&fs, key.as_bytes(), old, false, rewritten, 16));
            } else {
                let old = index.shards[index.shard_idx(key.as_bytes())].lock.read().unwrap()
                    .tombs.get(key.as_bytes()).unwrap().loc;
                let mut rewritten = old; rewritten.file_id = 2;
                assert!(index.commit_rewrite(&fs, key.as_bytes(), old, true, rewritten, 16));
                assert!(index.drop_tomb(key.as_bytes(), seq, 1).is_none());
                assert!(index.drop_tomb(key.as_bytes(), seq, 2).is_some());
                assert!(index.drop_tomb(key.as_bytes(), seq, 2).is_none());
            }
            assert_eq!(index.memory_estimate(), scanned(&index));
        }
    }
    #[test]
    fn concurrent_accounting_matches_final_index() {
        let index = Index::new(16);
        let fs = FileStatsRegistry::new();
        std::thread::scope(|scope| {
            for thread in 0..8 {
                let index = &index; let fs = &fs;
                scope.spawn(move || {
                    for seq in 1..=1000 {
                        let key = format!("{thread}/{}", seq % 83);
                        index.commit_record(fs, key.as_bytes(), seq, seq % 2 == 0, location(seq), 16);
                        if seq % 2 == 0 { index.drop_tomb(key.as_bytes(), seq, 1); }
                    }
                });
            }
        });
        assert_eq!(index.memory_estimate(), scanned(&index));
    }
    #[test]
    #[ignore = "component performance experiment"]
    fn memory_accounting_benchmark() {
        for count in [1000, 10_000, 100_000] {
            let index = Index::new(16); let fs = FileStatsRegistry::new();
            for seq in 1..=count {
                let key = format!("metadata/key/{seq:08}");
                index.commit_record(&fs, key.as_bytes(), seq, seq % 3 == 0, location(seq), 16);
            }
            assert_eq!(index.memory_estimate(), scanned(&index));
            let start = std::time::Instant::now();
            for _ in 0..500 { std::hint::black_box(scanned(&index)); }
            let scan_ns = start.elapsed().as_nanos();
            let start = std::time::Instant::now();
            for _ in 0..500 { std::hint::black_box(index.memory_estimate()); }
            let accounted_ns = start.elapsed().as_nanos();
            println!("INDEX_MEMORY keys={count} iterations=500 scan_ns={scan_ns} accounted_ns={accounted_ns}");
        }
    }
}

/// FNV-1a shard selection (matches Go's `shardForKey`).
pub(crate) fn shard_for_key(key: &[u8], mask: u64) -> u64 {
    let mut h = FNV_OFFSET64;
    for b in key {
        h ^= *b as u64;
        h = h.wrapping_mul(FNV_PRIME64);
    }
    h & mask
}
