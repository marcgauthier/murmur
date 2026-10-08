package rime

import "sync"

// candMapPool recycles candidate key-set maps for hot repetitive paths
// (indexed joins, foreign-key checks). Planner candidate sets are owned by
// the plan cache and are never pooled.
//
// Reuse discipline: maps leave the pool empty (cleared on release) and are
// returned exactly once via tmpScope.release. Pooled maps must never escape
// the scope (no storing in caches, no returning to callers).
var candMapPool sync.Pool // map[any]struct{}

// tmpScope hands out pooled temporaries and returns them all at once.
// Acquire with db.newScope and always defer release.
type tmpScope struct {
	db   *DB
	maps []map[any]struct{}
}

func (db *DB) newScope() *tmpScope { return &tmpScope{db: db} }

// cmap returns an empty candidate map, pooled when available.
func (s *tmpScope) cmap() map[any]struct{} {
	s.db.poolGets.Add(1)
	if v := candMapPool.Get(); v != nil {
		s.db.poolHits.Add(1)
		m := v.(map[any]struct{})
		s.maps = append(s.maps, m)
		return m
	}
	m := make(map[any]struct{})
	s.maps = append(s.maps, m)
	return m
}

// release clears every handed-out map and returns it to the pool.
func (s *tmpScope) release() {
	for _, m := range s.maps {
		clear(m)
		candMapPool.Put(m)
	}
	s.maps = nil
}
