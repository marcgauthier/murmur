package rime

import "context"

// JoinRow is one join output pair. Right is nil for unmatched LEFT JOIN rows.
type JoinRow[A, B any] struct {
	Left  *A
	Right *B
}

func joinBindings[A, B any](left BoundTable[A], right BoundTable[B]) (*Tx, context.Context, func(), error) {
	if left.table == nil || right.table == nil || left.table.db != right.table.db {
		return nil, nil, func() {}, ErrTxDatabase
	}
	if left.tx != nil && right.tx != nil && left.tx != right.tx {
		return nil, nil, func() {}, ErrTxDatabase
	}
	// Synchronous pre-check: AfterFunc fires in its own goroutine even for
	// an already-canceled parent, so without this the join body races the
	// derived context's cancellation and may return success.
	if err := left.context().Err(); err != nil {
		return nil, nil, func() {}, err
	}
	if err := right.context().Err(); err != nil {
		return nil, nil, func() {}, err
	}
	tx := left.tx
	if tx == nil {
		tx = right.tx
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopLeft := context.AfterFunc(left.context(), cancel)
	stopRight := context.AfterFunc(right.context(), cancel)
	done := func() { stopLeft(); stopRight(); cancel() }
	return tx, ctx, done, nil
}

// InnerJoin hash-joins two bound table snapshots using key functions.
func InnerJoin[A, B any, K comparable](left BoundTable[A], right BoundTable[B], lk func(*A) K, rk func(*B) K, filters ...func(*A, *B) bool) ([]JoinRow[A, B], error) {
	tx, ctx, done, err := joinBindings(left, right)
	if err != nil {
		return nil, err
	}
	defer done()
	return innerJoinContext(ctx, tx, left.table, right.table, lk, rk, filters...)
}

func innerJoinContext[A, B any, K comparable](ctx context.Context, tx *Tx, left *Table[A], right *Table[B], lk func(*A) K, rk func(*B) K, filters ...func(*A, *B) bool) ([]JoinRow[A, B], error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if left.db != right.db {
		return nil, ErrTxDatabase
	}
	if tx == nil {
		tx = left.db.ReadTxContext(ctx)
		defer tx.Close()
	}
	if err := left.checkTx(tx); err != nil {
		return nil, err
	}
	if err := right.checkTx(tx); err != nil {
		return nil, err
	}
	lrows, rrows, err := snapshots(ctx, tx, left, right)
	if err != nil {
		return nil, err
	}
	probe := make(map[K][]*B, len(rrows))
	for _, r := range rrows {
		k := rk(r)
		probe[k] = append(probe[k], r)
	}
	out := make([]JoinRow[A, B], 0, len(lrows))
	for i, l := range lrows {
		if i%64 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		for _, r := range probe[lk(l)] {
			if matchJoin(filters, l, r) {
				out = append(out, JoinRow[A, B]{Left: l, Right: r})
			}
		}
		if max := left.db.cfg.maxResults; max > 0 && len(out) > max {
			return nil, ErrLimitExceeded
		}
	}
	if err := tx.mustOpen(); err != nil {
		return nil, err
	}
	return out, nil
}

// InnerJoinOn joins bound tables on typed fields.
func InnerJoinOn[A, B any, K comparable](left BoundTable[A], lfield KeyField[A, K], right BoundTable[B], rfield KeyField[B, K], filters ...func(*A, *B) bool) ([]JoinRow[A, B], error) {
	tx, ctx, done, err := joinBindings(left, right)
	if err != nil {
		return nil, err
	}
	defer done()
	return innerJoinOnContext(ctx, tx, left.table, lfield, right.table, rfield, filters...)
}

func innerJoinOnContext[A, B any, K comparable](ctx context.Context, tx *Tx, left *Table[A], lfield KeyField[A, K], right *Table[B], rfield KeyField[B, K], filters ...func(*A, *B) bool) ([]JoinRow[A, B], error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if left.db != right.db {
		return nil, ErrTxDatabase
	}
	if tx == nil {
		tx = left.db.ReadTxContext(ctx)
		defer tx.Close()
	}
	if err := left.checkTx(tx); err != nil {
		return nil, err
	}
	if err := right.checkTx(tx); err != nil {
		return nil, err
	}
	if try, ok := indexedProbeTx(tx, right, rfield.keyName()); ok {
		return indexedJoin(ctx, tx, left, lfield.keyOf, right, rfield.keyName(), try, filters, false)
	}
	return innerJoinContext(ctx, tx, left, right, lfield.keyOf, rfield.keyOf, filters...)
}

// LeftJoinOn keeps every left row, with a nil Right when nothing matches.
func LeftJoinOn[A, B any, K comparable](left BoundTable[A], lfield KeyField[A, K], right BoundTable[B], rfield KeyField[B, K], filters ...func(*A, *B) bool) ([]JoinRow[A, B], error) {
	tx, ctx, done, err := joinBindings(left, right)
	if err != nil {
		return nil, err
	}
	defer done()
	return leftJoinOnContext(ctx, tx, left.table, lfield, right.table, rfield, filters...)
}

func leftJoinOnContext[A, B any, K comparable](ctx context.Context, tx *Tx, left *Table[A], lfield KeyField[A, K], right *Table[B], rfield KeyField[B, K], filters ...func(*A, *B) bool) ([]JoinRow[A, B], error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if left.db != right.db {
		return nil, ErrTxDatabase
	}
	if tx == nil {
		tx = left.db.ReadTxContext(ctx)
		defer tx.Close()
	}
	if err := left.checkTx(tx); err != nil {
		return nil, err
	}
	if err := right.checkTx(tx); err != nil {
		return nil, err
	}
	if try, ok := indexedProbeTx(tx, right, rfield.keyName()); ok {
		return indexedJoin(ctx, tx, left, lfield.keyOf, right, rfield.keyName(), try, filters, true)
	}
	lrows, rrows, err := snapshots(ctx, tx, left, right)
	if err != nil {
		return nil, err
	}
	probe := make(map[K][]*B, len(rrows))
	for _, r := range rrows {
		k := rfield.keyOf(r)
		probe[k] = append(probe[k], r)
	}
	out := make([]JoinRow[A, B], 0, len(lrows))
	for i, l := range lrows {
		if i%64 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		hit := false
		for _, r := range probe[lfield.keyOf(l)] {
			if matchJoin(filters, l, r) {
				out = append(out, JoinRow[A, B]{Left: l, Right: r})
				hit = true
			}
		}
		if !hit {
			out = append(out, JoinRow[A, B]{Left: l})
		}
		if max := left.db.cfg.maxResults; max > 0 && len(out) > max {
			return nil, ErrLimitExceeded
		}
	}
	if err := tx.mustOpen(); err != nil {
		return nil, err
	}
	return out, nil
}

// JoinPairs hash-joins two in-memory row slices (e.g. prior query results).
func JoinPairs[A, B any, K comparable](left []*A, right []*B, lk func(*A) K, rk func(*B) K, filters ...func(*A, *B) bool) []JoinRow[A, B] {
	probe := make(map[K][]*B, len(right))
	for _, r := range right {
		k := rk(r)
		probe[k] = append(probe[k], r)
	}
	out := make([]JoinRow[A, B], 0, len(left))
	for _, l := range left {
		for _, r := range probe[lk(l)] {
			if matchJoin(filters, l, r) {
				out = append(out, JoinRow[A, B]{Left: l, Right: r})
			}
		}
	}
	return out
}

func matchJoin[A, B any](filters []func(*A, *B) bool, l *A, r *B) bool {
	for _, f := range filters {
		if !f(l, r) {
			return false
		}
	}
	return true
}

// snapshots materializes both sides at one snapshot.
func snapshots[A, B any](ctx context.Context, tx *Tx, left *Table[A], right *Table[B]) ([]*A, []*B, error) {
	if tx != nil && (tx.write || tx.preview) && len(tx.pending) > 0 {
		lrows, err := scanJoinOverlay(ctx, tx, left)
		if err != nil {
			return nil, nil, err
		}
		rrows, err := scanJoinOverlay(ctx, tx, right)
		return lrows, rrows, err
	}
	if err := tx.mustOpen(); err != nil {
		return nil, nil, err
	}
	snap := left.snapOf(tx)
	lrows, err := scanSnap(ctx, left, snap, nil)
	if err != nil {
		return nil, nil, err
	}
	rrows, err := scanSnap(ctx, right, snap, nil)
	if err != nil {
		return nil, nil, err
	}
	return lrows, rrows, nil
}

// Join inputs are bounded by maxScan, not maxResults (which bounds join
// output). Preserve that contract when including transaction-local records.
func scanJoinOverlay[T any](ctx context.Context, tx *Tx, table *Table[T]) ([]*T, error) {
	rows, err := scanSnap(ctx, table, table.snapOf(tx), nil)
	if err != nil {
		return nil, err
	}
	final := make(map[any]*pendingWrite)
	for _, p := range tx.pending {
		if p.table == table && !p.superseded {
			final[p.key] = p
		}
	}
	if max := table.db.cfg.maxScan; max > 0 && len(rows)+len(final) > max {
		return nil, ErrLimitExceeded
	}
	n := 0
	for _, row := range rows {
		if _, changed := final[table.pkGet(row)]; !changed {
			rows[n] = row
			n++
		}
	}
	clear(rows[n:])
	rows = rows[:n]
	for _, p := range final {
		if !p.del {
			rows = append(rows, p.val.(*T))
		}
	}
	if err := tx.mustOpen(); err != nil {
		return nil, err
	}
	return rows, nil
}

// scanSnap collects all records visible at snap, optionally pre-filtered.
func scanSnap[T any](ctx context.Context, t *Table[T], snap TxID, keep func(*T) bool) ([]*T, error) {
	var out []*T
	n := 0
	for _, s := range t.shards {
		for _, c := range s.chains() {
			v, ok := c.visible(snap)
			if !ok || v.tomb {
				continue
			}
			if n%64 == 0 && ctx.Err() != nil {
				return nil, ctx.Err()
			}
			n++
			if max := t.db.cfg.maxScan; max > 0 && n > max {
				return nil, ErrLimitExceeded
			}
			if keep == nil || keep(v.val) {
				out = append(out, v.val)
			}
		}
	}
	return out, nil
}

// eqOp selects one indexed-join probe shape. The scratch op keeps the generic
// candidatesInto path; the typed ops skip boxing and the scratch map.
type eqOp uint8

const (
	eqScratch eqOp = iota
	eqUniqueStr
	eqHashStr
	eqUniqueI64
	eqHashI64
	eqUniqueU64
	eqHashU64
	eqUniqueF64
	eqHashF64
	eqUniqueBool
	eqHashBool
)

// resolveEqOp picks the probe shape for one join: the key type K must map
// exactly to the field's index kind, else the scratch path handles norms,
// cross-kind keys, and named types exactly as before.
func resolveEqOp[K comparable](ix *indexSet, field string) eqOp {
	var zero K
	var kk keyKind
	switch any(zero).(type) {
	case string:
		kk = kString
	case bool:
		kk = kBool
	case int, int8, int16, int32, int64:
		kk = kInt64
	case uint, uint8, uint16, uint32, uint64:
		kk = kUint64
	case float32, float64:
		kk = kFloat64
	default:
		return eqScratch
	}
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if uk, ok := ix.uniqueKinds[field]; ok {
		if uk != kk {
			return eqScratch
		}
		switch kk {
		case kString:
			return eqUniqueStr
		case kInt64:
			return eqUniqueI64
		case kUint64:
			return eqUniqueU64
		case kFloat64:
			return eqUniqueF64
		case kBool:
			return eqUniqueBool
		}
		return eqScratch
	}
	if hk, ok := ix.hashKinds[field]; ok {
		if hk != kk {
			return eqScratch
		}
		switch kk {
		case kString:
			return eqHashStr
		case kInt64:
			return eqHashI64
		case kUint64:
			return eqHashU64
		case kFloat64:
			return eqHashF64
		case kBool:
			return eqHashBool
		}
	}
	return eqScratch
}

// probeEqInto resolves visible matches for one join key through the typed
// index, appending to dst. It holds the index lock only across the bucket
// read; visibility resolves after release to avoid nesting index and shard
// locks. ok=false selects the scratch path.
func probeEqInto[B any, K comparable](op eqOp, ix *indexSet, field string, kk K, t *Table[B], snap TxID, dst []*B) ([]*B, bool) {
	var one any
	var many []any
	found := false
	assertOK := true
	ix.mu.RLock()
	switch op {
	case eqUniqueStr:
		k, ok := any(kk).(string)
		if !ok {
			assertOK = false
			break
		}
		one, found = ix.uniqueStr[field][k]
	case eqHashStr:
		k, ok := any(kk).(string)
		if !ok {
			assertOK = false
			break
		}
		b, ok := ix.hashStr[field][k]
		if ok {
			if b.set != nil {
				many = make([]any, 0, len(b.set))
				for key := range b.set {
					many = append(many, key)
				}
			} else {
				one = b.single
			}
			found = true
		}
	case eqUniqueI64:
		k, ok := normKIntOK(any(kk))
		if !ok {
			assertOK = false
			break
		}
		one, found = ix.uniqueI64[field][k]
	case eqHashI64:
		k, ok := normKIntOK(any(kk))
		if !ok {
			assertOK = false
			break
		}
		b, ok := ix.hashI64[field][k]
		if ok {
			if b.set != nil {
				many = make([]any, 0, len(b.set))
				for key := range b.set {
					many = append(many, key)
				}
			} else {
				one = b.single
			}
			found = true
		}
	case eqUniqueU64:
		k, ok := normKUintOK(any(kk))
		if !ok {
			assertOK = false
			break
		}
		one, found = ix.uniqueU64[field][k]
	case eqHashU64:
		k, ok := normKUintOK(any(kk))
		if !ok {
			assertOK = false
			break
		}
		b, ok := ix.hashU64[field][k]
		if ok {
			if b.set != nil {
				many = make([]any, 0, len(b.set))
				for key := range b.set {
					many = append(many, key)
				}
			} else {
				one = b.single
			}
			found = true
		}
	case eqUniqueF64:
		k, ok := normKFloatOK(any(kk))
		if !ok {
			assertOK = false
			break
		}
		one, found = ix.uniqueF64[field][k]
	case eqHashF64:
		k, ok := normKFloatOK(any(kk))
		if !ok {
			assertOK = false
			break
		}
		b, ok := ix.hashF64[field][k]
		if ok {
			if b.set != nil {
				many = make([]any, 0, len(b.set))
				for key := range b.set {
					many = append(many, key)
				}
			} else {
				one = b.single
			}
			found = true
		}
	case eqUniqueBool:
		k, ok := any(kk).(bool)
		if !ok {
			assertOK = false
			break
		}
		one, found = ix.uniqueBool[field][k]
	case eqHashBool:
		k, ok := any(kk).(bool)
		if !ok {
			assertOK = false
			break
		}
		b, ok := ix.hashBool[field][k]
		if ok {
			if b.set != nil {
				many = make([]any, 0, len(b.set))
				for key := range b.set {
					many = append(many, key)
				}
			} else {
				one = b.single
			}
			found = true
		}
	default:
		ix.mu.RUnlock()
		return dst, false
	}
	ix.mu.RUnlock()
	// A failed assertion means the resolver admitted a key it cannot probe;
	// decline to the scratch path rather than mis-resolving.
	if !assertOK {
		return dst, false
	}
	if !found {
		return dst, true
	}
	if many != nil {
		for _, k := range many {
			if r := t.visibleKey(k, snap); r != nil {
				dst = append(dst, r)
			}
		}
		return dst, true
	}
	if r := t.visibleKey(one, snap); r != nil {
		dst = append(dst, r)
	}
	return dst, true
}

// normKIntOK widens any signed key to the int64 index domain.
func normKIntOK(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	}
	return 0, false
}

// normKUintOK widens any unsigned key to the uint64 index domain.
func normKUintOK(v any) (uint64, bool) {
	switch n := v.(type) {
	case uint:
		return uint64(n), true
	case uint8:
		return uint64(n), true
	case uint16:
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case uint64:
		return n, true
	}
	return 0, false
}

// normKFloatOK widens any float key to the float64 index domain.
func normKFloatOK(v any) (float64, bool) {
	switch n := v.(type) {
	case float32:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// indexedProbeTx reports whether an equality index on field is usable at tx.
func indexedProbeTx[T any](tx *Tx, t *Table[T], field string) (TxID, bool) {
	if tx != nil && (tx.write || tx.preview) && len(tx.pending) > 0 {
		return 0, false
	}
	t.db.viewMu.RLock()
	defer t.db.viewMu.RUnlock()
	var snap TxID
	if tx == nil {
		snap = t.db.latest()
	} else {
		if err := tx.mustOpen(); err != nil {
			return 0, false
		}
		snap = tx.snap
	}
	if !t.idx.hasEquality(field) {
		return 0, false
	}
	if fresh := TxID(0); true {
		_ = fresh
	}
	if snap < TxID(tableChangeOf(t)) {
		return 0, false
	}
	return snap, true
}

func tableChangeOf[T any](t *Table[T]) uint64 { return t.lastChange.Load() }

// indexedJoin probes the right equality index per left row.
func indexedJoin[A, B any, K comparable](ctx context.Context, tx *Tx, left *Table[A], lk func(*A) K, right *Table[B], rfield string, snap TxID, filters []func(*A, *B) bool, leftOuter bool) ([]JoinRow[A, B], error) {
	lrows, err := scanSnap(ctx, left, left.snapOf(tx), nil)
	if err != nil {
		return nil, err
	}
	scope := left.db.newScope()
	defer scope.release()
	probe := scope.cmap()
	out := make([]JoinRow[A, B], 0, len(lrows))
	eqop := resolveEqOp[K](right.idx, rfield)
	var historical map[any][]*B
	// lastKey caches the previous row's match set: matches depend only on
	// (key, snap) and the snapshot is fixed, so repeated keys skip the
	// index probe, visibility resolution, and per-row allocations.
	var lastKey K
	var lastMatches []*B
	haveLast := false
	for i, l := range lrows {
		if i%64 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		hit := false
		kk := lk(l)
		var matches []*B
		if haveLast && kk == lastKey {
			matches = lastMatches
		} else if historical != nil {
			matches = historical[any(kk)]
			lastKey, lastMatches, haveLast = kk, matches, true
		} else {
			right.db.viewMu.RLock()
			if snap < TxID(right.lastChange.Load()) {
				rows, scanErr := scanSnap(ctx, right, snap, nil)
				if scanErr != nil {
					right.db.viewMu.RUnlock()
					return nil, scanErr
				}
				historical = make(map[any][]*B)
				for _, r := range rows {
					key := right.gets[rfield](r)
					historical[key] = append(historical[key], r)
				}
				matches = historical[any(kk)]
			} else {
				var ok bool
				matches, ok = probeEqInto(eqop, right.idx, rfield, kk, right, snap, matches)
				if !ok {
					clear(probe)
					for k := range right.idx.candidatesInto(probe, rfield, any(kk)) {
						if r := right.visibleKey(k, snap); r != nil {
							matches = append(matches, r)
						}
					}
				}
			}
			right.db.viewMu.RUnlock()
			lastKey, lastMatches, haveLast = kk, matches, true
		}
		for _, r := range matches {
			if matchJoin(filters, l, r) {
				out = append(out, JoinRow[A, B]{Left: l, Right: r})
				hit = true
			}
		}
		if leftOuter && !hit {
			out = append(out, JoinRow[A, B]{Left: l})
		}
		if max := left.db.cfg.maxResults; max > 0 && len(out) > max {
			return nil, ErrLimitExceeded
		}
	}
	if err := tx.mustOpen(); err != nil {
		return nil, err
	}
	return out, nil
}
