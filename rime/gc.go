package rime

import "context"

// GC reclaims versions that no active snapshot can observe, plus fully
// deleted rows, and returns what it reclaimed. It operates shard by shard
// with short lock holds to avoid latency spikes.
func (db *DB) GC() GCResult {
	return db.GCContext(context.Background())
}

// GCContext is GC with cancellation.
func (db *DB) GCContext(ctx context.Context) GCResult {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return GCResult{}
	}
	// Match the commit lock order. Snapshot registration is excluded until
	// every table has been reclaimed against the same active snapshot set.
	db.commitMu.Lock()
	defer db.commitMu.Unlock()
	db.viewMu.Lock()
	defer db.viewMu.Unlock()
	oldest := db.oldestActive()
	if oldest > db.retainedFloor {
		db.retainedFloor = oldest
	}
	db.mu.Lock()
	tables := make([]innerTable, 0, len(db.tables))
	for _, t := range db.tables {
		tables = append(tables, t)
	}
	db.mu.Unlock()
	res := GCResult{Tables: len(tables)}
	for _, t := range tables {
		if ctx.Err() != nil {
			break
		}
		res.Reclaimed += t.gcOldest(ctx, oldest)
	}
	db.gcReclaimed.Add(uint64(res.Reclaimed))
	db.gcRuns.Add(1)
	return res
}

// gcOldest prunes each chain to the newest version <= oldest plus all newer
// versions. Rows whose head is a tombstone at commit <= oldest vanish
// entirely. Cancellation leaves already-pruned chains valid. It backs innerTable.
func (t *Table[T]) gcOldest(ctx context.Context, oldest TxID) (reclaimed int) {
	checked := 0
	for _, s := range t.shards {
		if ctx.Err() != nil {
			return reclaimed
		}
		s.mu.Lock()
		for key, c := range s.rows {
			if checked%64 == 0 && ctx.Err() != nil {
				s.mu.Unlock()
				return reclaimed
			}
			checked++
			if h, ok := c.latest(); ok && h.tomb && h.commit <= oldest {
				// Deleted before every active snapshot: drop the row.
				delete(s.rows, key)
				reclaimed += c.total
				continue
			}
			// Collect the spine of chunks entirely newer than oldest;
			// the first chunk at or below oldest holds the boundary.
			var newer []*chain[T]
			ch := c
			for ch != nil && (len(ch.vers) == 0 || ch.vers[0].commit > oldest) {
				newer = append(newer, ch)
				ch = ch.prev
			}
			if ch == nil {
				// All versions newer than any active snapshot: keep all.
				continue
			}
			// Newest index within the boundary chunk at or below oldest.
			keep, hi := 0, len(ch.vers)-1
			for keep < hi {
				mid := (keep + hi + 1) / 2
				if ch.vers[mid].commit <= oldest {
					keep = mid
				} else {
					hi = mid - 1
				}
			}
			if keep == 0 && ch.prev == nil {
				// Nothing below the boundary: the whole chain stays.
				continue
			}
			kept := append([]version[T](nil), ch.vers[keep:]...)
			nb := &chain[T]{vers: kept, total: len(kept)}
			for i := len(newer) - 1; i >= 0; i-- {
				nb = &chain[T]{vers: newer[i].vers, total: nb.total + len(newer[i].vers), prev: nb}
			}
			reclaimed += c.total - nb.total
			s.rows[key] = nb
		}
		s.mu.Unlock()
	}
	return reclaimed
}
