package rime

import (
	"context"
)

// collect runs match + optional sort/paginate. materialize=false counts only.
func (q *Query[T]) collect(ctx context.Context, tx *Tx, materialize bool) ([]*T, int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if tx == nil {
		tx = q.tbl.db.ReadTxContext(ctx)
		defer tx.Close()
	}
	if err := q.tbl.checkTx(tx); err != nil {
		return nil, 0, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	t := q.tbl
	snap := t.snapOf(tx)
	p := t.cachedOrPlan(q, snap)
	if err := tx.mustOpen(); err != nil {
		return nil, 0, err
	}
	overlay := (tx.write || tx.preview) && len(tx.pending) > 0
	if p.empty && !overlay {
		return nil, 0, nil
	}
	if materialize && q.limit == 0 {
		return nil, 0, nil
	}
	maxScan := t.db.cfg.maxScan
	maxResults := t.db.cfg.maxResults
	var rows []*T
	collectRows := materialize || overlay
	// examine tests one visible record; a match counts toward the Limit
	// early exit.
	examine := func(rec *T) bool {
		for _, e := range q.exprs {
			if !e.test(rec) {
				return false
			}
		}
		if collectRows {
			rows = append(rows, rec)
		}
		return true
	}
	// need stops scans once Limit+Offset rows match.
	need := -1
	if materialize && !overlay && q.limit >= 0 {
		need = q.limit + q.offset
	}
	var scanned, matched int
	var err error
	if !p.empty {
		scanned, matched, err = q.scanEach(ctx, snap, p, need, maxScan, examine)
	}
	if err != nil {
		return nil, 0, err
	}
	if err := tx.mustOpen(); err != nil {
		return nil, 0, err
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if maxScan > 0 && scanned > maxScan {
		return nil, 0, ErrLimitExceeded
	}
	if overlay {
		final := make(map[any]*pendingWrite, len(tx.pending))
		for _, w := range tx.pending {
			if w.table == t && !w.superseded {
				final[w.key] = w
			}
		}
		if maxScan > 0 && scanned+len(final) > maxScan {
			return nil, 0, ErrLimitExceeded
		}
		for key, w := range final {
			for i := 0; i < len(rows); i++ {
				if t.pkGet(rows[i]) == key {
					rows = append(rows[:i], rows[i+1:]...)
					i--
				}
			}
			if !w.del {
				rec := w.val.(*T)
				match := true
				for _, e := range q.exprs {
					if !e.test(rec) {
						match = false
						break
					}
				}
				if match {
					rows = append(rows, rec)
				}
			}
		}
		matched = len(rows)
	}
	if !materialize {
		return nil, matched, nil
	}
	if !p.ordered || overlay {
		q.sortRows(rows)
	}
	// Offset/Limit.
	if q.offset > 0 {
		if q.offset >= len(rows) {
			rows = nil
		} else {
			rows = rows[q.offset:]
		}
	}
	if q.limit >= 0 && len(rows) > q.limit {
		rows = rows[:q.limit]
	}
	if maxResults > 0 && len(rows) > maxResults {
		return nil, 0, ErrLimitExceeded
	}
	return rows, matched, nil
}

// scanEach walks every candidate record for plan p at snap, calling examine
// for each visible record. It returns the examined and matched counts.
// need >= 0 stops the scan once need matches accumulate (Limit+Offset early
// exit); maxScan > 0 stops once scanned exceeds it, leaving the limit error
// to the caller so scans and folds share one walk.
func (q *Query[T]) scanEach(ctx context.Context, snap TxID, p *plan[T], need, maxScan int, examine func(rec *T) bool) (scanned, matched int, err error) {
	t := q.tbl
	switch {
	case p.orderedKeys != nil:
		for i, k := range p.orderedKeys {
			if i%64 == 0 && ctx.Err() != nil {
				return scanned, matched, ctx.Err()
			}
			rec := t.visibleKey(k, snap)
			if rec == nil {
				continue
			}
			scanned++
			if maxScan > 0 && scanned > maxScan {
				break
			}
			if examine(rec) {
				matched++
			}
			if need >= 0 && matched >= need {
				break
			}
		}
	case p.keys != nil:
		i := 0
		for k := range p.keys {
			if i%64 == 0 && ctx.Err() != nil {
				return scanned, matched, ctx.Err()
			}
			i++
			// Without ORDER BY any Limit matches do; stop scanning early.
			if need >= 0 && matched >= need && len(q.orders) == 0 {
				break
			}
			rec := t.visibleKey(k, snap)
			if rec == nil {
				continue
			}
			scanned++
			if maxScan > 0 && scanned > maxScan {
				break
			}
			if examine(rec) {
				matched++
			}
		}
	default:
		aborted := false
		for _, s := range t.shards {
			for _, c := range s.chains() {
				v, ok := c.visible(snap)
				if !ok || v.tomb {
					continue
				}
				if scanned%64 == 0 && ctx.Err() != nil {
					return scanned, matched, ctx.Err()
				}
				scanned++
				if maxScan > 0 && scanned > maxScan {
					aborted = true
					break
				}
				if examine(v.val) {
					matched++
				}
				if need >= 0 && matched >= need && len(q.orders) == 0 {
					aborted = true
					break
				}
			}
			if aborted {
				break
			}
		}
	}
	return scanned, matched, nil
}

// visibleKey fetches the record for key visible at snap, or nil.
func (t *Table[T]) visibleKey(key any, snap TxID) *T {
	s := t.shardFor(key)
	s.mu.RLock()
	c, ok := s.rows[key]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	v, ok := c.visible(snap)
	if !ok || v.tomb {
		return nil
	}
	return v.val
}
