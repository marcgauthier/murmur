package rime

import (
	"math"
	"sort"
	"strings"
	"sync"
)

// plan is one compiled execution strategy.
type plan[T any] struct {
	table         string
	rows          int
	strategy      string
	index         string
	predicates    []string
	estCandidates int
	filters       []string
	ordered       bool
	empty         bool
	stale         bool
	keys          map[any]struct{}
	orderedKeys   []any
	limit         int
	hasLimit      bool
	orderDesc     []string
}

// flattenConjuncts splits top-level ANDs (including the implicit AND across
// Query.exprs) into conjuncts.
func flattenConjuncts[T any](exprs []Expr[T]) []Expr[T] {
	var out []Expr[T]
	for _, e := range exprs {
		if e.kind() == kAnd {
			out = append(out, flattenConjuncts(e.kids())...)
		} else {
			out = append(out, e)
		}
	}
	return out
}

// choosePlan selects the cheapest index path for q at snapshot snap.
func choosePlan[T any](q *Query[T], snap TxID) *plan[T] {
	t := q.tbl
	p := &plan[T]{table: t.name, rows: t.headCount(), limit: q.limit, hasLimit: q.limit >= 0}
	for _, o := range q.orders {
		arrow := "ASC"
		if o.desc {
			arrow = "DESC"
		}
		p.orderDesc = append(p.orderDesc, o.f.orderName()+" "+arrow)
	}
	// Index seeks require a fresh view: the table must be unchanged since the
	// snapshot. Otherwise a full scan with predicate re-evaluation is the
	// only correct path.
	if fresh := TxID(t.lastChange.Load()); snap < fresh {
		p.strategy = "FULL SCAN"
		p.stale = true
		p.estCandidates = p.rows
		for _, e := range flattenConjuncts(q.exprs) {
			p.filters = append(p.filters, e.describe())
		}
		return p
	}
	conj := flattenConjuncts(q.exprs)
	eq := map[string]any{}
	var eqOrder []string
	used := map[Expr[T]]bool{}
	for _, e := range conj {
		switch e.kind() {
		case kFalse:
			p.empty = true
			p.strategy = "EMPTY (contradiction)"
			return p
		case kTrue:
			used[e] = true
		case kPred:
			pi := e.pred()
			if pi.op == opEq && len(pi.paramSeq) == 0 || pi.op == opEq && allLiteral(pi) {
				if prev, ok := eq[pi.field]; ok && prev != pi.vals[0] {
					p.empty = true
					p.strategy = "EMPTY (contradiction)"
					return p
				}
				if _, ok := eq[pi.field]; !ok {
					eqOrder = append(eqOrder, pi.field)
				}
				eq[pi.field] = pi.vals[0]
				used[e] = true
			}
		}
	}
	describeUnused := func() []string {
		var f []string
		for _, e := range conj {
			if !used[e] {
				f = append(f, e.describe())
			}
		}
		return f
	}
	// 1. Compound index with full equality coverage (most fields wins).
	if choices := t.idx.compoundChoices(eq); len(choices) > 0 {
		name := choices[0]
		fields := t.idx.comp[name].fields
		if keys, ok := t.idx.compoundLookup(name, eq); ok {
			p.strategy = "INDEX SEEK"
			p.index = name
			for _, f := range fields {
				p.predicates = append(p.predicates, f+" = "+quote(eq[f]))
				for _, e := range conj {
					if e.kind() == kPred && e.pred().op == opEq && e.pred().field == f {
						used[e] = true
					}
				}
			}
			p.keys = keys
			p.estCandidates = len(keys)
			p.filters = describeUnused()
			return finishPlan(q, p)
		}
	}
	// 2. Equality on the most selective index. Cardinality counters pick
	// the winner before probing, so only one candidate set is built.
	bestField, bestEst := "", math.MaxFloat64
	for _, f := range eqOrder {
		est, ok := t.idx.eqEstimate(f)
		if !ok {
			continue
		}
		if est < bestEst {
			bestEst, bestField = est, f
		}
	}
	var bestSet map[any]struct{}
	if bestField != "" {
		bestSet = t.idx.candidates(bestField, eq[bestField])
	}
	if bestSet != nil {
		p.strategy = "INDEX SEEK"
		p.index = bestField
		p.predicates = append(p.predicates, bestField+" = "+quote(eq[bestField]))
		for _, e := range conj {
			if e.kind() == kPred && e.pred().op == opEq && e.pred().field == bestField {
				used[e] = true
			}
		}
		p.keys = bestSet
		p.estCandidates = len(bestSet)
		p.filters = describeUnused()
		return finishPlan(q, p)
	}
	// 3. UNION of OR branches that are all equalities on one indexed field.
	if u := unionPlan(q, conj, used); u != nil {
		return u
	}
	// 4. IN list on an indexed field.
	for _, e := range conj {
		if e.kind() != kPred || e.pred().op != opIn || !allLiteral(e.pred()) {
			continue
		}
		f := e.pred().field
		if !t.idx.hasEquality(f) {
			continue
		}
		union := map[any]struct{}{}
		for _, v := range e.pred().vals {
			for k := range t.idx.candidates(f, v) {
				union[k] = struct{}{}
			}
		}
		used[e] = true
		p.strategy = "INDEX SEEK"
		p.index = f
		p.predicates = append(p.predicates, e.describe())
		p.keys = union
		p.estCandidates = len(union)
		p.filters = describeUnused()
		return finishPlan(q, p)
	}
	// 5. Ordered range / BETWEEN on an ordered index.
	if r := rangePlan(q, conj, used); r != nil {
		return r
	}
	// 6. Prefix search on a prefix index (StartsWith, or LIKE "literal%").
	for _, e := range conj {
		if e.kind() != kPred || !allLiteral(e.pred()) {
			continue
		}
		var pre string
		switch e.pred().op {
		case opStartsWith:
			pre = e.pred().vals[0].(string)
		case opLike:
			var ok bool
			pre, ok = likePrefix(e.pred().vals[0].(string))
			if !ok {
				continue
			}
		default:
			continue
		}
		f := e.pred().field
		if keys := t.idx.prefixLookup(f, pre); keys != nil {
			used[e] = true
			p.strategy = "INDEX SEEK"
			p.index = f + " (prefix)"
			p.predicates = append(p.predicates, e.describe())
			p.keys = keys
			p.estCandidates = len(keys)
			p.filters = describeUnused()
			return finishPlan(q, p)
		}
	}
	// 7. Ordered scan when ORDER BY matches one ordered index prefix.
	if o := orderPlan(q); o != nil {
		o.filters = describeUnused()
		return o
	}
	// 8. Full scan.
	p.strategy = "FULL SCAN"
	p.estCandidates = p.rows
	p.filters = describeUnused()
	return finishPlan(q, p)
}

func allLiteral(pi *predInfo) bool {
	for _, s := range pi.paramSeq {
		if s != 0 {
			return false
		}
	}
	return true
}

// finishPlan marks index-ordered output when the single ORDER BY key matches
// an ordered index traversal direction (handled by callers via orderedKeys).
func finishPlan[T any](q *Query[T], p *plan[T]) *plan[T] { return p }

// unionPlan handles top-level ORs whose branches are all equalities/IN on one
// indexed field.
func unionPlan[T any](q *Query[T], conj []Expr[T], used map[Expr[T]]bool) *plan[T] {
	t := q.tbl
	for _, e := range conj {
		if e.kind() != kOr {
			continue
		}
		field := ""
		vals := map[any]bool{}
		ok := true
		for _, b := range e.kids() {
			if b.kind() != kPred || !allLiteral(b.pred()) {
				ok = false
				break
			}
			pi := b.pred()
			if field == "" {
				field = pi.field
			}
			if pi.field != field {
				ok = false
				break
			}
			switch pi.op {
			case opEq:
				vals[pi.vals[0]] = true
			case opIn:
				for _, v := range pi.vals {
					vals[v] = true
				}
			default:
				ok = false
			}
		}
		if !ok || field == "" || !t.idx.hasEquality(field) {
			continue
		}
		union := map[any]struct{}{}
		for v := range vals {
			for k := range t.idx.candidates(field, v) {
				union[k] = struct{}{}
			}
		}
		used[e] = true
		var f []string
		for _, c := range conj {
			if !used[c] {
				f = append(f, c.describe())
			}
		}
		return &plan[T]{table: t.name, rows: t.headCount(), strategy: "INDEX UNION", index: field,
			predicates: []string{e.describe()}, estCandidates: len(union), filters: f, keys: union,
			limit: q.limit, hasLimit: q.limit >= 0, orderDesc: orderDescOf(q)}
	}
	return nil
}

func orderDescOf[T any](q *Query[T]) []string {
	var out []string
	for _, o := range q.orders {
		arrow := "ASC"
		if o.desc {
			arrow = "DESC"
		}
		out = append(out, o.f.orderName()+" "+arrow)
	}
	return out
}

type bound struct {
	val  any
	open bool
}

// rangePlan intersects range predicates per field and uses the tightest
// ordered index available.
func rangePlan[T any](q *Query[T], conj []Expr[T], used map[Expr[T]]bool) *plan[T] {
	t := q.tbl
	los := map[string]*bound{}
	his := map[string]*bound{}
	var order []string
	consume := func(e Expr[T], f string, lo, hi *bound) {
		if lo != nil {
			if prev, ok := los[f]; !ok || compareOrdValues(lo.val, prev.val) > 0 ||
				(compareOrdValues(lo.val, prev.val) == 0 && lo.open && !prev.open) {
				los[f] = lo
			}
		}
		if hi != nil {
			if prev, ok := his[f]; !ok || compareOrdValues(hi.val, prev.val) < 0 ||
				(compareOrdValues(hi.val, prev.val) == 0 && hi.open && !prev.open) {
				his[f] = hi
			}
		}
		used[e] = true
	}
	for _, e := range conj {
		if e.kind() != kPred || !allLiteral(e.pred()) {
			continue
		}
		pi := e.pred()
		f := pi.field
		if _, ok := t.idx.ordered[f]; !ok {
			continue
		}
		switch pi.op {
		case opGt:
			consume(e, f, &bound{pi.vals[0], true}, nil)
		case opGe:
			consume(e, f, &bound{pi.vals[0], false}, nil)
		case opLt:
			consume(e, f, nil, &bound{pi.vals[0], true})
		case opLe:
			consume(e, f, nil, &bound{pi.vals[0], false})
		case opBetween:
			consume(e, f, &bound{pi.lo, pi.loOpen}, &bound{pi.hi, pi.hiOpen})
		}
	}
	for f := range los {
		order = append(order, f)
	}
	for f := range his {
		if _, ok := los[f]; !ok {
			order = append(order, f)
		}
	}
	if len(order) == 0 {
		return nil
	}
	// Feasibility + tightness: prefer the field with a bounded two-sided range.
	sort.Strings(order)
	best := ""
	for _, f := range order {
		lo, hlo := los[f], his[f]
		_ = hlo
		if lo != nil {
			if hi, ok := his[f]; ok {
				c := compareOrdValues(lo.val, hi.val)
				if c > 0 || (c == 0 && (lo.open || hi.open)) {
					p := &plan[T]{table: t.name, rows: t.headCount(), strategy: "EMPTY (contradiction)",
						estCandidates: 0, orderDesc: orderDescOf(q)}
					p.empty = true
					return p
				}
				best = f
				break
			}
		}
	}
	if best == "" {
		best = order[0]
	}
	var lo, hi any
	var loOpen, hiOpen bool
	if b, ok := los[best]; ok {
		lo, loOpen = b.val, b.open
	}
	if b, ok := his[best]; ok {
		hi, hiOpen = b.val, b.open
	}
	desc := len(q.orders) == 1 && q.orders[0].desc && q.orders[0].f.orderName() == best
	keys := t.idx.orderedRange(best, lo, hi, loOpen, hiOpen, desc, -1)
	var f []string
	for _, c := range conj {
		if !used[c] {
			f = append(f, c.describe())
		}
	}
	var preds []string
	if lb, ok := los[best]; ok {
		preds = append(preds, boundDesc(best, lb.val, lb.open, true))
	}
	if hb, ok := his[best]; ok {
		preds = append(preds, boundDesc(best, hb.val, hb.open, false))
	}
	ordered := desc || (len(q.orders) == 1 && !q.orders[0].desc && q.orders[0].f.orderName() == best)
	return &plan[T]{table: t.name, rows: t.headCount(), strategy: "ORDERED RANGE", index: best,
		predicates: preds, estCandidates: len(keys), filters: f, ordered: ordered,
		orderedKeys: keys, limit: q.limit, hasLimit: q.limit >= 0, orderDesc: orderDescOf(q)}
}

func boundDesc(f string, val any, open, isLo bool) string {
	op := map[bool]string{true: ">", false: ">="}[!open]
	if !isLo {
		op = map[bool]string{true: "<", false: "<="}[!open]
	}
	return f + " " + op + " " + quote(val)
}

// orderPlan uses an ordered index purely for ORDER BY delivery.
func orderPlan[T any](q *Query[T]) *plan[T] {
	t := q.tbl
	if len(q.orders) != 1 {
		return nil
	}
	o := q.orders[0]
	if _, ok := t.idx.ordered[o.f.orderName()]; !ok {
		return nil
	}
	keys := t.idx.orderedRange(o.f.orderName(), nil, nil, false, false, o.desc, -1)
	return &plan[T]{table: t.name, rows: t.headCount(), strategy: "ORDERED SCAN", index: o.f.orderName(),
		estCandidates: len(keys), ordered: true, orderedKeys: keys,
		limit: q.limit, hasLimit: q.limit >= 0, orderDesc: orderDescOf(q)}
}

// hasEquality reports whether field has a hash or unique index.
func (ix *indexSet) hasEquality(field string) bool {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if _, ok := ix.uniqueKinds[field]; ok {
		return true
	}
	_, ok := ix.hashKinds[field]
	return ok
}

// headCount counts live map entries (heads, including tombstones).
func headCountOf[T any](t *Table[T]) int {
	n := 0
	for _, s := range t.shards {
		s.mu.RLock()
		n += len(s.rows)
		s.mu.RUnlock()
	}
	return n
}

func (t *Table[T]) headCount() int { return headCountOf(t) }

var _ = strings.Contains

// planCacheMax bounds cached plans per table (FIFO eviction).
const planCacheMax = 128

// cachedPlan is a reusable execution strategy. Candidate maps are owned by
// the cache entry and never mutated, so hits share them safely. Entries are
// valid only while the table is unchanged (gen == table lastChange).
type cachedPlan struct {
	gen           TxID
	strategy      string
	index         string
	predicates    []string
	filters       []string
	ordered       bool
	empty         bool
	keys          map[any]struct{}
	orderedKeys   []any
	estCandidates int
}

type planCache struct {
	mu    sync.Mutex
	order []string
	items map[string]*cachedPlan
}

func fingerprint[T any](q *Query[T]) string {
	var sb strings.Builder
	for _, e := range q.exprs {
		sb.WriteString(e.describe())
		sb.WriteByte(0x1e)
	}
	sb.WriteByte(0x1f)
	for _, o := range q.orders {
		sb.WriteString(o.f.orderName())
		if o.desc {
			sb.WriteByte('-')
		} else {
			sb.WriteByte('+')
		}
	}
	return sb.String()
}

// cachedOrPlan returns a cached plan on hit or plans, stores, and returns on
// miss. Stale snapshots (table changed since) bypass the cache entirely.
func (t *Table[T]) cachedOrPlan(q *Query[T], snap TxID) *plan[T] {
	t.db.viewMu.RLock()
	defer t.db.viewMu.RUnlock()
	gen := TxID(t.lastChange.Load())
	if snap < gen {
		return choosePlan(q, snap)
	}
	fp := fingerprint(q)
	t.pc.mu.Lock()
	cp, ok := t.pc.items[fp]
	t.pc.mu.Unlock()
	if ok && cp.gen == gen {
		t.planHits.Add(1)
		return &plan[T]{table: t.name, rows: t.headCount(), strategy: cp.strategy,
			index: cp.index, predicates: cp.predicates, estCandidates: cp.estCandidates,
			filters: cp.filters, ordered: cp.ordered, empty: cp.empty,
			keys: cp.keys, orderedKeys: cp.orderedKeys,
			limit: q.limit, hasLimit: q.limit >= 0, orderDesc: orderDescOf(q)}
	}
	p := choosePlan(q, snap)
	t.planMisses.Add(1)
	t.pc.mu.Lock()
	if existing, dup := t.pc.items[fp]; !dup || existing.gen != gen {
		if !dup && len(t.pc.order) >= planCacheMax {
			old := t.pc.order[0]
			t.pc.order = append(t.pc.order[:0], t.pc.order[1:]...)
			delete(t.pc.items, old)
		}
		if !dup {
			t.pc.order = append(t.pc.order, fp)
		}
		t.pc.items[fp] = &cachedPlan{gen: gen, strategy: p.strategy, index: p.index,
			predicates: p.predicates, filters: p.filters, ordered: p.ordered,
			empty: p.empty, keys: p.keys, orderedKeys: p.orderedKeys,
			estCandidates: p.estCandidates}
	}
	t.pc.mu.Unlock()
	return p
}
