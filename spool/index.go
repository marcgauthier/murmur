package spool

import (
	"sync"
	"sync/atomic"
)

// Location identifies one record version on disk. Identity for
// liveness checks is the full tuple; Sequence orders versions of a
// key.
type Location struct {
	FileID      uint64
	BlockID     uint64
	BlockOffset int64
	RecordIndex uint32
	Sequence    uint64
	// Size is the record's key+value bytes plus framing. It keeps
	// LiveBytes exact on eviction and fits the struct's padding.
	Size uint32
}

// tombMark is the in-memory winner for a deleted key.
type tombMark struct {
	seq  uint64
	file uint64
	loc  Location // full winning location for exact delivery matching
}

// indexShard holds a slice of the key space with its own lock.
type indexShard struct {
	mu     sync.RWMutex
	values map[string]Location
	tombs  map[string]tombMark
}

// index is the sharded key→location map plus tombstone winners. Lock
// order: index shard -> fileStats. Never acquire a shard lock while
// holding the stats lock.
type index struct {
	shards []indexShard
	mask   uint64
	keys   atomic.Int64
}

func newIndex(shardCount int) *index {
	ix := &index{shards: make([]indexShard, shardCount), mask: uint64(shardCount - 1)}
	for i := range ix.shards {
		ix.shards[i].values = make(map[string]Location)
		ix.shards[i].tombs = make(map[string]tombMark)
	}
	return ix
}

const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

func shardForKey(key []byte, mask uint64) uint64 {
	h := uint64(fnvOffset64)
	for _, b := range key {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	return h & mask
}

func (ix *index) shardFor(key []byte) *indexShard {
	return &ix.shards[shardForKey(key, ix.mask)]
}

// commitRecord installs rec as the current version of key when it is
// newer than anything known, and attributes file statistics. Stale
// records are stillborn: they occupy space (counted dead) but never
// become visible. loc describes the record's committed home; size is
// its key+value bytes plus framing.
//
// It is shared by the flush path, the compaction path and the load
// rebuild so all three agree on last-write-wins.
func (ix *index) commitRecord(fs *fileStats, key string, seq uint64, tomb bool, loc Location, size uint32) {
	sh := ix.shardFor([]byte(key))
	sh.mu.Lock()
	defer sh.mu.Unlock()

	curSeq, curTomb, curFile, curSize, has := ix.currentLocked(sh, key)
	if has && seq <= curSeq {
		// Stale: a newer version already won.
		fs.recordDead(loc.FileID, size)
		return
	}
	if has {
		if curTomb {
			delete(sh.tombs, key)
			fs.tombEvicted(curFile)
		} else {
			delete(sh.values, key)
			fs.valueEvicted(curFile, curSize)
			ix.keys.Add(-1)
		}
	}
	if tomb {
		loc.Sequence = seq
		loc.Size = size
		sh.tombs[key] = tombMark{seq: seq, file: loc.FileID, loc: loc}
		fs.tombInstalled(loc.FileID, size)
		return
	}
	loc.Sequence = seq
	loc.Size = size
	sh.values[key] = loc
	fs.valueInstalled(loc.FileID, size)
	ix.keys.Add(1)
}

// currentLocked reports the winning version of key. The caller must
// hold at least a read lock.
func (ix *index) currentLocked(sh *indexShard, key string) (seq uint64, isTomb bool, file uint64, size uint32, has bool) {
	if loc, ok := sh.values[key]; ok {
		seq, file, size, has = loc.Sequence, loc.FileID, loc.Size, true
	}
	if tm, ok := sh.tombs[key]; ok && (!has || tm.seq > seq) {
		seq, isTomb, file, size, has = tm.seq, true, tm.file, 0, true
	}
	return seq, isTomb, file, size, has
}

// pointsAt reports whether the index still resolves key to exactly
// loc (full identity match). Used by compaction's liveness recheck.
func (ix *index) pointsAt(key string, want Location) bool {
	sh := ix.shardFor([]byte(key))
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	got, ok := sh.values[key]
	return ok && got == want
}

// tombLive reports whether the tombstone (key, seq, file) is still
// the current winner for key.
func (ix *index) tombLive(key string, seq, file uint64) bool {
	sh := ix.shardFor([]byte(key))
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	tm, ok := sh.tombs[key]
	return ok && tm.seq == seq && tm.file == file
}

// dropTomb removes the tombstone winner for key when it still matches
// (seq, file). It returns the dropped mark for stats accounting.
func (ix *index) dropTomb(key string, seq, file uint64) (tombMark, bool) {
	sh := ix.shardFor([]byte(key))
	sh.mu.Lock()
	defer sh.mu.Unlock()
	tm, ok := sh.tombs[key]
	if !ok || tm.seq != seq || tm.file != file {
		return tombMark{}, false
	}
	delete(sh.tombs, key)
	return tm, true
}

// snapshotWinners copies every winning version for second-pass
// load delivery. The caller must quiesce writers (rebuild holds no
// competition) or accept a point-in-time view.
func (ix *index) snapshotWinners() map[string]winnerLoc {
	out := make(map[string]winnerLoc)
	for i := range ix.shards {
		sh := &ix.shards[i]
		sh.mu.RLock()
		for k, loc := range sh.values {
			if w, ok := out[k]; !ok || loc.Sequence > w.seq {
				out[k] = winnerLoc{seq: loc.Sequence, loc: loc}
			}
		}
		for k, tm := range sh.tombs {
			if w, ok := out[k]; !ok || tm.seq > w.seq {
				out[k] = winnerLoc{seq: tm.seq, tomb: true, loc: tm.loc}
			}
		}
		sh.mu.RUnlock()
	}
	return out
}

// keyCount returns the number of live (non-deleted) keys.
func (ix *index) keyCount() uint64 {
	if n := ix.keys.Load(); n > 0 {
		return uint64(n)
	}
	return 0
}

// memoryEstimate approximates index memory: key bytes plus a
// per-entry allowance covering locations, tomb marks, and map
// overhead. It is a diagnostic estimate, not an allocator measure.
func (ix *index) memoryEstimate() uint64 {
	const perEntry = 128
	var total uint64
	for i := range ix.shards {
		sh := &ix.shards[i]
		sh.mu.RLock()
		for k := range sh.values {
			total += uint64(len(k)) + perEntry
		}
		for k := range sh.tombs {
			total += uint64(len(k)) + perEntry
		}
		sh.mu.RUnlock()
	}
	return total
}

// rebaseBlocks rewrites every entry's block offset from a maintenance
// re-seal map (block sequence to new file offset). Block sequences
// are store-wide unique, so one flat map covers all rewritten files.
// It returns the first block sequence an entry references without a
// mapping, if any; callers treat that as a fatal inconsistency
// (reopen rebuilds the index from disk).
func (ix *index) rebaseBlocks(offsets map[uint64]int64) (uint64, bool) {
	for i := range ix.shards {
		sh := &ix.shards[i]
		sh.mu.Lock()
		for k, loc := range sh.values {
			off, ok := offsets[loc.BlockID]
			if !ok {
				sh.mu.Unlock()
				return loc.BlockID, false
			}
			loc.BlockOffset = off
			sh.values[k] = loc
		}
		for k, tm := range sh.tombs {
			off, ok := offsets[tm.loc.BlockID]
			if !ok {
				sh.mu.Unlock()
				return tm.loc.BlockID, false
			}
			tm.loc.BlockOffset = off
			sh.tombs[k] = tm
		}
		sh.mu.Unlock()
	}
	return 0, true
}

// scanTombs calls fn for every tombstone winner. fn runs without
// locks held; the entry may change concurrently, so callers must
// re-verify identity (see dropTomb) before acting on it.
func (ix *index) scanTombs(fn func(key string, tm tombMark)) {
	type entry struct {
		key string
		tm  tombMark
	}
	for i := range ix.shards {
		sh := &ix.shards[i]
		sh.mu.RLock()
		batch := make([]entry, 0, len(sh.tombs))
		for k, tm := range sh.tombs {
			batch = append(batch, entry{key: k, tm: tm})
		}
		sh.mu.RUnlock()
		for _, e := range batch {
			fn(e.key, e.tm)
		}
	}
}
