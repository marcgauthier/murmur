//! Byte-bounded hot block cache. A bounded probation list keeps sequential
//! scans from replacing repeatedly accessed blocks. No application value index.
use crate::block::DecodedBody;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;

pub(crate) type Key = (u64, i64);
pub(crate) struct Block {
    // Keep the original compressed/encrypted frame alongside the decoded body.
    pub frame: Vec<u8>,
    pub plain: Vec<u8>,
    pub body: DecodedBody,
}
impl Block {
    fn bytes(&self) -> usize {
        self.frame.capacity()
            + self.plain.capacity()
            + self.body.records.capacity() * std::mem::size_of::<crate::block::DecodedRecord>()
            + std::mem::size_of::<Self>()
            + 128
    }
}
impl Drop for Block {
    fn drop(&mut self) {
        crate::crypto::wipe(&mut self.plain);
    }
}
pub(crate) struct Cache {
    limit: usize,
    bytes: usize,
    clock: u128,
    entries: HashMap<Key, (Arc<Block>, u128)>,
    order: BTreeMap<u128, Key>,
    seen: HashSet<Key>,
    probation: VecDeque<Key>,
}
impl Cache {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            bytes: 0,
            clock: 0,
            entries: HashMap::new(),
            order: BTreeMap::new(),
            seen: HashSet::new(),
            probation: VecDeque::new(),
        }
    }
    pub fn get(&mut self, key: Key) -> Option<Arc<Block>> {
        let (block, age) = self.entries.get_mut(&key)?;
        self.order.remove(age);
        self.clock += 1;
        *age = self.clock;
        self.order.insert(*age, key);
        Some(block.clone())
    }
    pub fn insert(&mut self, key: Key, block: Arc<Block>) {
        let size = block.bytes();
        if self.limit == 0 || size > self.limit || self.entries.contains_key(&key) {
            return;
        }
        if !self.seen.contains(&key) {
            if self.probation.len() == 4096 {
                if let Some(old) = self.probation.pop_front() {
                    self.seen.remove(&old);
                }
            }
            self.seen.insert(key);
            self.probation.push_back(key);
            return;
        }
        while size > self.limit - self.bytes {
            let Some((_, old)) = self.order.pop_first() else {
                break;
            };
            if let Some((entry, _)) = self.entries.remove(&old) {
                self.bytes -= entry.bytes();
            }
        }
        self.clock += 1;
        self.bytes += size;
        self.order.insert(self.clock, key);
        self.entries.insert(key, (block, self.clock));
    }
    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
        self.seen.clear();
        self.probation.clear();
        self.bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn block(size: usize) -> Arc<Block> {
        Arc::new(Block {
            frame: vec![1; 32],
            plain: vec![7; size],
            body: DecodedBody {
                group_id: 1,
                block_index: 0,
                block_count: 1,
                records: vec![],
            },
        })
    }
    fn admit(cache: &mut Cache, key: Key, b: &Arc<Block>) {
        cache.insert(key, b.clone());
        cache.insert(key, b.clone());
    }
    #[test]
    fn disabled_and_oversized_blocks_are_never_cached() {
        let b = block(100);
        for capacity in [0, b.bytes() - 1] {
            let mut cache = Cache::new(capacity);
            admit(&mut cache, (1, 0), &b);
            assert!(cache.get((1, 0)).is_none());
            assert_eq!(cache.bytes, 0);
        }
    }
    #[test]
    fn eviction_is_byte_bounded_and_uses_recent_access() {
        let b = block(100);
        let mut cache = Cache::new(b.bytes() * 2);
        admit(&mut cache, (1, 0), &b);
        admit(&mut cache, (2, 0), &b);
        assert!(cache.get((1, 0)).is_some());
        admit(&mut cache, (3, 0), &b);
        assert!(cache.get((2, 0)).is_none());
        assert!(cache.get((1, 0)).is_some());
        assert!(cache.get((3, 0)).is_some());
        assert_eq!(cache.bytes, b.bytes() * 2);
    }
    #[test]
    fn sequential_scan_cannot_displace_hot_blocks_or_grow_probation_unbounded() {
        let b = block(100);
        let mut cache = Cache::new(b.bytes());
        admit(&mut cache, (1, 0), &b);
        for seq in 2..10000 {
            cache.insert((seq, 0), b.clone());
            assert!(cache.bytes <= cache.limit);
        }
        assert!(cache.get((1, 0)).is_some());
        assert_eq!(cache.probation.len(), 4096);
        assert_eq!(cache.seen.len(), 4096);
    }
    #[test]
    fn clear_releases_buffers_without_invalidating_active_readers() {
        let b = block(100);
        let weak = Arc::downgrade(&b);
        let mut cache = Cache::new(b.bytes());
        admit(&mut cache, (1, 0), &b);
        drop(b);
        let reader = cache.get((1, 0)).unwrap();
        cache.clear();
        assert_eq!(reader.plain, vec![7; 100]);
        assert_eq!(cache.bytes, 0);
        assert!(cache.entries.is_empty());
        assert!(cache.seen.is_empty());
        drop(reader);
        assert!(weak.upgrade().is_none());
    }
    #[test]
    fn duplicate_insert_preserves_accounting() {
        let b = block(100);
        let mut cache = Cache::new(b.bytes());
        admit(&mut cache, (1, 0), &b);
        for _ in 0..100 {
            cache.insert((1, 0), b.clone());
        }
        assert_eq!(cache.bytes, b.bytes());
        assert_eq!(cache.entries.len(), 1);
    }
}
