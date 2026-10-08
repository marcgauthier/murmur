package rime

// TxID is a monotonically increasing transaction/commit identifier. Reads pin
// a snapshot TxID; every committed write advances the global counter (an
// atomic.Uint64 on DB).
type TxID uint64

// version is one committed record version. val==nil && tomb means deleted.
type version[T any] struct {
	commit TxID
	val    *T
	tomb   bool
}

// vchunkCap bounds the versions held in one chain chunk. Publishing a version
// copies at most vchunkCap-1 retained versions, so per-write work stays
// constant no matter how long the retained history grows.
const vchunkCap = 32

// chain is a persistent newest-first list of version chunks for one primary
// key. Each chunk holds ascending commits and the head chunk holds the newest
// versions. Chains are immutable once published: writes install a new head
// chunk that shares older chunks with its predecessor, so readers holding a
// chain may use it without locks.
type chain[T any] struct {
	vers  []version[T] // ascending commits; non-empty once published
	total int          // versions in this chunk and all prev chunks
	prev  *chain[T]    // older chunks, nil at the tail
}

// visible returns the newest version with commit <= snap.
// ok=false means no version is visible (never existed or not yet created).
func (c *chain[T]) visible(snap TxID) (v version[T], ok bool) {
	for ch := c; ch != nil; ch = ch.prev {
		n := len(ch.vers)
		if n == 0 {
			continue
		}
		if ch.vers[0].commit > snap {
			continue // the whole chunk is newer than the snapshot
		}
		// Every newer chunk holds only versions above snap, so the
		// newest version at or below snap lives in this chunk.
		if ch.vers[n-1].commit <= snap {
			return ch.vers[n-1], true
		}
		lo, hi := 0, n-1
		for lo < hi {
			mid := (lo + hi + 1) / 2
			if ch.vers[mid].commit <= snap {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		return ch.vers[lo], true
	}
	return version[T]{}, false
}

// latest returns the newest committed version, or ok=false when empty.
func (c *chain[T]) latest() (v version[T], ok bool) {
	if len(c.vers) == 0 {
		return version[T]{}, false
	}
	return c.vers[len(c.vers)-1], true
}

// appendVersion returns a new head with v appended. It copies at most
// vchunkCap-1 retained versions and shares every older chunk with the old
// head, so both allocation count and copied bytes stay bounded.
func (c *chain[T]) appendVersion(v version[T]) *chain[T] {
	if len(c.vers) < vchunkCap {
		nv := make([]version[T], len(c.vers)+1)
		copy(nv, c.vers)
		nv[len(c.vers)] = v
		return &chain[T]{vers: nv, total: c.total + 1, prev: c.prev}
	}
	return &chain[T]{vers: []version[T]{v}, total: c.total + 1, prev: c}
}
