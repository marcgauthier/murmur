package rime

// IndexStat describes one index.
type IndexStat struct {
	Name        string
	Kind        string // unique, hash, ordered, prefix, compound
	Entries     int64
	Cardinality int64
	// Selectivity is the distinct fraction (Cardinality/Entries); the
	// planner prefers higher selectivity for equality lookups.
	Selectivity float64
}

// TableStats describes one table.
type TableStats struct {
	Table        string
	Records      int64 // live heads (excludes tombstoned heads)
	Versions     int64 // total committed versions retained
	Tombstones   int64 // heads that are tombstones
	IndexEntries int64
	Indexes      map[string]IndexStat
	PlanHits     uint64
	PlanMisses   uint64
}

// DBStats describes the whole database.
type DBStats struct {
	Tables         int
	Records        int64
	Versions       int64
	Tombstones     int64
	ActiveTxns     int
	OldestSnapshot TxID
	LatestCommit   TxID
	IndexEntries   int64
	GCReclaimed    uint64
	GCRuns         uint64
	TempPoolGets   uint64
	TempPoolHits   uint64
	TablesDetail   map[string]TableStats
}

// GCResult reports one GC pass.
type GCResult struct {
	Tables    int
	Reclaimed int
}

// tableStats computes statistics at the latest committed state.
func (t *Table[T]) tableStats(snap TxID) TableStats {
	st := TableStats{Table: t.name}
	for _, s := range t.shards {
		s.mu.RLock()
		for _, c := range s.rows {
			st.Versions += int64(c.total)
			if h, ok := c.latest(); ok {
				if h.tomb {
					st.Tombstones++
				} else {
					if v, ok := c.visible(snap); ok && !v.tomb {
						st.Records++
					}
				}
			}
		}
		s.mu.RUnlock()
	}
	entries, per := t.idx.indexStats()
	st.IndexEntries = entries
	st.Indexes = per
	st.PlanHits = t.planHits.Load()
	st.PlanMisses = t.planMisses.Load()
	return st
}

// Stats returns database-wide statistics.
func (db *DB) Stats() DBStats {
	db.viewMu.RLock()
	defer db.viewMu.RUnlock()
	detail := map[string]TableStats{}
	out := DBStats{
		LatestCommit:   db.latest(),
		OldestSnapshot: db.oldestActive(),
		ActiveTxns:     db.activeCount(),
		GCReclaimed:    db.gcReclaimed.Load(),
		GCRuns:         db.gcRuns.Load(),
		TempPoolGets:   db.poolGets.Load(),
		TempPoolHits:   db.poolHits.Load(),
		TablesDetail:   detail,
	}
	db.mu.Lock()
	tables := make([]innerTable, 0, len(db.tables))
	for _, t := range db.tables {
		tables = append(tables, t)
	}
	db.mu.Unlock()
	snap := db.latest()
	for _, t := range tables {
		ts := t.tableStats(snap)
		detail[t.tableName()] = ts
		out.Records += ts.Records
		out.Versions += ts.Versions
		out.Tombstones += ts.Tombstones
		out.IndexEntries += ts.IndexEntries
	}
	out.Tables = len(tables)
	return out
}

// TableStats returns statistics for one table.
func (t *Table[T]) TableStats() TableStats {
	t.db.viewMu.RLock()
	defer t.db.viewMu.RUnlock()
	return t.tableStats(t.db.latest())
}
