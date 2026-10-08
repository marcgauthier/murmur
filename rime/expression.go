package rime

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
)

// ordered constrains ordered column types.
type ordered interface {
	~string | ~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

func cmpFor[V ordered]() func(a, b V) int { return cmp.Compare[V] }

func lessAny[T comparable](a, b T) bool {
	switch av := any(a).(type) {
	case string:
		return av < any(b).(string)
	case int:
		return av < any(b).(int)
	case int64:
		return av < any(b).(int64)
	case uint64:
		return av < any(b).(uint64)
	case float64:
		return av < any(b).(float64)
	case UUID:
		bv := any(b).(UUID)
		return bytes.Compare(av[:], bv[:]) < 0
	}
	return fmt.Sprintf("%v", a) < fmt.Sprintf("%v", b)
}

func errNoField(table, name string) error {
	return fmt.Errorf("%w: table %s has no field %q", ErrBadSchema, table, name)
}

// predOp identifies a leaf predicate operator.
type predOp int

const (
	opEq predOp = iota
	opNe
	opGt
	opGe
	opLt
	opLe
	opBetween
	opIn
	opNotIn
	opIsNull
	opIsNotNull
	opStartsWith
	opEndsWith
	opContains
	opLike
)

func (o predOp) String() string {
	switch o {
	case opEq:
		return "="
	case opNe:
		return "!="
	case opGt:
		return ">"
	case opGe:
		return ">="
	case opLt:
		return "<"
	case opLe:
		return "<="
	case opBetween:
		return "BETWEEN"
	case opIn:
		return "IN"
	case opNotIn:
		return "NOT IN"
	case opIsNull:
		return "IS NULL"
	case opIsNotNull:
		return "IS NOT NULL"
	case opStartsWith:
		return "STARTS WITH"
	case opEndsWith:
		return "ENDS WITH"
	case opContains:
		return "CONTAINS"
	case opLike:
		return "LIKE"
	}
	return "?"
}

// Expr is a typed query predicate over records of type T.
type Expr[T any] interface {
	test(*T) bool
	describe() string
	kind() exprKind
	pred() *predInfo
	kids() []Expr[T]
}

type exprKind int

const (
	kTrue exprKind = iota
	kFalse
	kPred
	kAnd
	kOr
	kNot
)

// predInfo is the planner-visible form of a leaf predicate.
type predInfo struct {
	field    string
	op       predOp
	vals     []any   // literal values; param slots substituted at bind
	paramSeq []int64 // parallel to vals; 0 means literal
	paramTyp reflect.Type
	lo, hi   any
	loOpen   bool
	hiOpen   bool
	fetch    func(rec any) any // untyped field extractor for rebound predicates
}

type expr[T any] struct {
	k    exprKind
	fn   func(*T) bool
	desc string
	p    *predInfo
	kid  []Expr[T]
}

func (e *expr[T]) test(rec *T) bool { return e.fn(rec) }
func (e *expr[T]) describe() string { return e.desc }
func (e *expr[T]) kind() exprKind   { return e.k }
func (e *expr[T]) pred() *predInfo  { return e.p }
func (e *expr[T]) kids() []Expr[T]  { return e.kid }

// And combines predicates conjunctively.
func And[T any](exprs ...Expr[T]) Expr[T] {
	parts := make([]string, len(exprs))
	fn := func(rec *T) bool {
		for _, e := range exprs {
			if !e.test(rec) {
				return false
			}
		}
		return true
	}
	for i, e := range exprs {
		parts[i] = e.describe()
	}
	return &expr[T]{k: kAnd, fn: fn, desc: "(" + strings.Join(parts, " AND ") + ")", kid: exprs}
}

// Or combines predicates disjunctively.
func Or[T any](exprs ...Expr[T]) Expr[T] {
	parts := make([]string, len(exprs))
	fn := func(rec *T) bool {
		for _, e := range exprs {
			if e.test(rec) {
				return true
			}
		}
		return false
	}
	for i, e := range exprs {
		parts[i] = e.describe()
	}
	return &expr[T]{k: kOr, fn: fn, desc: "(" + strings.Join(parts, " OR ") + ")", kid: exprs}
}

// Not negates a predicate.
func Not[T any](e Expr[T]) Expr[T] {
	return &expr[T]{k: kNot, fn: func(rec *T) bool { return !e.test(rec) }, desc: "NOT (" + e.describe() + ")", kid: []Expr[T]{e}}
}

// True matches every record; False matches none.
func True[T any]() Expr[T] {
	return &expr[T]{k: kTrue, fn: func(*T) bool { return true }, desc: "TRUE"}
}

// False matches no record.
func False[T any]() Expr[T] {
	return &expr[T]{k: kFalse, fn: func(*T) bool { return false }, desc: "FALSE"}
}

func quote(v any) string {
	if s, ok := v.(string); ok {
		return fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf("%v", v)
}

func newPred[T any, V comparable](f Field[T, V], op predOp, v V) Expr[T] {
	val := any(v)
	seq := paramSeqFor(val)
	pi := &predInfo{field: f.name, op: op, vals: []any{val}, paramSeq: []int64{seq},
		fetch: func(rec any) any { return any(f.get(rec.(*T))) }}
	if seq != 0 {
		pi.paramTyp = reflect.TypeOf(v)
	}
	var fn func(*T) bool
	switch op {
	case opEq:
		fn = func(rec *T) bool { return f.get(rec) == v }
	case opNe:
		fn = func(rec *T) bool { return f.get(rec) != v }
	case opGt:
		fn = func(rec *T) bool { return lessAny(v, f.get(rec)) }
	case opGe:
		fn = func(rec *T) bool { return v == f.get(rec) || lessAny(v, f.get(rec)) }
	case opLt:
		fn = func(rec *T) bool { return lessAny(f.get(rec), v) }
	case opLe:
		fn = func(rec *T) bool { return v == f.get(rec) || lessAny(f.get(rec), v) }
	case opStartsWith:
		fn = func(rec *T) bool { return strings.HasPrefix(any(f.get(rec)).(string), any(val).(string)) }
	case opEndsWith:
		fn = func(rec *T) bool { return strings.HasSuffix(any(f.get(rec)).(string), any(val).(string)) }
	case opContains:
		fn = func(rec *T) bool { return strings.Contains(any(f.get(rec)).(string), any(val).(string)) }
	}
	return &expr[T]{k: kPred, fn: fn, desc: f.name + " " + op.String() + " " + quote(v), p: pi}
}

func newPredMulti[T any, V comparable](f Field[T, V], op predOp, vs []V) Expr[T] {
	vals := make([]any, len(vs))
	seqs := make([]int64, len(vs))
	set := make(map[V]struct{}, len(vs))
	var typ reflect.Type
	for i, v := range vs {
		vals[i] = any(v)
		seqs[i] = paramSeqFor(any(v))
		if seqs[i] != 0 {
			typ = reflect.TypeOf(v)
		}
		set[v] = struct{}{}
	}
	names := make([]string, len(vs))
	for i, v := range vs {
		names[i] = quote(v)
	}
	var fn func(*T) bool
	if op == opIn {
		fn = func(rec *T) bool {
			_, ok := set[f.get(rec)]
			return ok
		}
	} else {
		fn = func(rec *T) bool {
			_, ok := set[f.get(rec)]
			return !ok
		}
	}
	return &expr[T]{k: kPred, fn: fn, desc: f.name + " " + op.String() + " (" + strings.Join(names, ", ") + ")",
		p: &predInfo{field: f.name, op: op, vals: vals, paramSeq: seqs, paramTyp: typ,
			fetch: func(rec any) any { return any(f.get(rec.(*T))) }}}
}

func newNullPred[T any, V comparable](f Field[T, V], wantNull bool) Expr[T] {
	var fn func(*T) bool
	var desc string
	if wantNull {
		fn = func(rec *T) bool { return isNullField(any(f.get(rec)), f.presence, f.nullable) }
		desc = f.name + " IS NULL"
	} else {
		fn = func(rec *T) bool { return !isNullField(any(f.get(rec)), f.presence, f.nullable) }
		desc = f.name + " IS NOT NULL"
	}
	op := opIsNull
	if !wantNull {
		op = opIsNotNull
	}
	return &expr[T]{k: kPred, fn: fn, desc: desc, p: &predInfo{field: f.name, op: op,
		fetch: func(rec any) any { return any(f.get(rec.(*T))) }}}
}

func newRangePred[T any, V comparable](f Field[T, V], op predOp, lo, hi any, loOpen, hiOpen bool) Expr[T] {
	if paramSeqFor(lo) != 0 || paramSeqFor(hi) != 0 {
		panic("rime: Param() is not supported in Between; combine Ge/Le with Param instead")
	}
	fn := func(rec *T) bool {
		v := any(f.get(rec))
		if lessAny(v, lo) || lessAny(hi, v) {
			return false
		}
		if loOpen && v == lo {
			return false
		}
		if hiOpen && v == hi {
			return false
		}
		return true
	}
	desc := f.name + " BETWEEN " + quote(lo) + " AND " + quote(hi)
	return &expr[T]{k: kPred, fn: fn, desc: desc,
		p: &predInfo{field: f.name, op: op, lo: lo, hi: hi, loOpen: loOpen, hiOpen: hiOpen,
			fetch: func(rec any) any { return any(f.get(rec.(*T))) }}}
}

// --- Compiled-query parameters ---

// Param returns a placeholder for compiled queries. Supported types: string,
// int, int64, uint, uint64, float64. The returned sentinel must only flow
// into predicate constructors consumed by Compile; it must never be stored.
func Param[V comparable]() V {
	var zero V
	s, ok := paramSentinel(any(zero))
	if !ok {
		panic(fmt.Sprintf("rime: Param[%T] unsupported; use string, int, int64, uint, uint64, or float64", zero))
	}
	paramReg.Lock()
	paramReg.next++
	seq := paramReg.next
	paramReg.vals[paramKey{typ: reflect.TypeOf(zero), val: s}] = seq
	paramReg.Unlock()
	return s.(V)
}

type paramKey struct {
	typ reflect.Type
	val any
}

var paramReg = struct {
	sync.Mutex
	next int64
	vals map[paramKey]int64
}{vals: map[paramKey]int64{}}

var paramSeqGen atomic.Int64

func paramSentinel(zero any) (any, bool) {
	n := paramSeqGen.Add(1)
	switch zero.(type) {
	case string:
		return fmt.Sprintf("\x00rime:param:%d\x00", n), true
	case int:
		return int(-(1 << 62) + n), true
	case int64:
		return int64(-(1 << 62) + n), true
	case uint:
		return uint((1 << 63) + uint(n)), true
	case uint64:
		return uint64((1 << 63) + uint64(n)), true
	case float64:
		return float64(-1e300) + float64(n), true
	}
	return nil, false
}

// paramSeqFor returns the registration sequence when v is a Param sentinel.
func paramSeqFor(v any) int64 {
	if v == nil {
		return 0
	}
	paramReg.Lock()
	defer paramReg.Unlock()
	return paramReg.vals[paramKey{typ: reflect.TypeOf(v), val: v}]
}

// Compiled is a pre-planned query with positional parameters.
type Compiled[T any] struct {
	tbl    *Table[T]
	tx     *Tx
	ctx    context.Context
	tmpl   []Expr[T]
	seqs   []int64 // parameter slots in Param() creation order
	typs   []reflect.Type
	limit  int
	offset int
	orders []orderSpec[T]
}

// Compile plans exprs once; bind values per execution via Find.
func (t *Table[T]) Compile(exprs ...Expr[T]) *Compiled[T] {
	set := map[int64]reflect.Type{}
	var walk func(e Expr[T])
	walk = func(e Expr[T]) {
		if e.kind() == kPred {
			p := e.pred()
			for i, s := range p.paramSeq {
				if s != 0 {
					ty := p.paramTyp
					if ty == nil && i < len(p.vals) && p.vals[i] != nil {
						ty = reflect.TypeOf(p.vals[i])
					}
					set[s] = ty
				}
			}
		}
		for _, k := range e.kids() {
			walk(k)
		}
	}
	for _, e := range exprs {
		walk(e)
	}
	seqs := make([]int64, 0, len(set))
	for s := range set {
		seqs = append(seqs, s)
	}
	// Deterministic slot order = Param() creation order = seq ascending.
	for i := 0; i < len(seqs); i++ {
		for j := i + 1; j < len(seqs); j++ {
			if seqs[j] < seqs[i] {
				seqs[i], seqs[j] = seqs[j], seqs[i]
			}
		}
	}
	typs := make([]reflect.Type, len(seqs))
	for i, s := range seqs {
		typs[i] = set[s]
	}
	return &Compiled[T]{tbl: t, tmpl: append([]Expr[T](nil), exprs...), seqs: seqs, typs: typs, limit: -1, ctx: context.Background()}
}

// Limit caps compiled results; Offset skips leading rows.
func (c *Compiled[T]) clone() *Compiled[T] {
	n := *c
	n.tmpl = append([]Expr[T](nil), c.tmpl...)
	n.orders = append([]orderSpec[T](nil), c.orders...)
	return &n
}
func (c *Compiled[T]) In(tx *Tx) *Compiled[T] { n := c.clone(); n.tx = tx; return n }
func (c *Compiled[T]) WithContext(ctx context.Context) *Compiled[T] {
	n := c.clone()
	if ctx == nil {
		ctx = context.Background()
	}
	n.ctx = ctx
	return n
}
func (c *Compiled[T]) Limit(n int) *Compiled[T]  { v := c.clone(); v.limit = n; return v }
func (c *Compiled[T]) Offset(n int) *Compiled[T] { v := c.clone(); v.offset = n; return v }

// OrderByAsc appends an ascending ordering; OrderByDesc a descending one.
func (c *Compiled[T]) OrderByAsc(f OrderField[T]) *Compiled[T] {
	v := c.clone()
	v.orders = append(v.orders, orderSpec[T]{f: f})
	return v
}
func (c *Compiled[T]) OrderByDesc(f OrderField[T]) *Compiled[T] {
	v := c.clone()
	v.orders = append(v.orders, orderSpec[T]{f: f, desc: true})
	return v
}

// bind substitutes args for Param slots positionally (Param creation order).
func (c *Compiled[T]) bind(args []any) ([]Expr[T], error) {
	if len(args) != len(c.seqs) {
		return nil, fmt.Errorf("rime: compiled query wants %d args, got %d", len(c.seqs), len(args))
	}
	pos := map[int64]int{}
	for i, s := range c.seqs {
		pos[s] = i
	}
	var subVals func(vals []any, seqs []int64, typ reflect.Type) ([]any, error)
	subVals = func(vals []any, seqs []int64, typ reflect.Type) ([]any, error) {
		out := make([]any, len(vals))
		for i, v := range vals {
			if seqs != nil && seqs[i] != 0 {
				a := args[pos[seqs[i]]]
				if typ != nil && reflect.TypeOf(a) != typ {
					return nil, fmt.Errorf("rime: param %d wants %s, got %T", pos[seqs[i]], typ, a)
				}
				out[i] = a
			} else {
				out[i] = v
			}
		}
		return out, nil
	}
	var walk func(e Expr[T]) (Expr[T], error)
	walk = func(e Expr[T]) (Expr[T], error) {
		if e.kind() != kPred {
			kids := e.kids()
			if len(kids) == 0 {
				return e, nil
			}
			nk := make([]Expr[T], len(kids))
			for i, k := range kids {
				n, err := walk(k)
				if err != nil {
					return nil, err
				}
				nk[i] = n
			}
			switch e.kind() {
			case kAnd:
				return And[T](nk...), nil
			case kOr:
				return Or[T](nk...), nil
			default:
				return Not[T](nk[0]), nil
			}
		}
		p := e.pred()
		hasParam := false
		for _, s := range p.paramSeq {
			if s != 0 {
				hasParam = true
			}
		}
		if !hasParam {
			return e, nil
		}
		vals, err := subVals(p.vals, p.paramSeq, p.paramTyp)
		if err != nil {
			return nil, err
		}
		return rebuildPred(e, vals), nil
	}
	out := make([]Expr[T], len(c.tmpl))
	for i, e := range c.tmpl {
		n, err := walk(e)
		if err != nil {
			return nil, err
		}
		out[i] = n
	}
	return out, nil
}

// rebuildPred replays a param leaf test against bound values.
func rebuildPred[T any](e Expr[T], vals []any) Expr[T] {
	inner := e.(*expr[T])
	p := inner.p
	fetch := p.fetch
	switch p.op {
	case opEq, opNe, opGt, opGe, opLt, opLe, opStartsWith, opEndsWith, opContains:
		v := vals[0]
		return &expr[T]{k: kPred, p: &predInfo{field: p.field, op: p.op, vals: vals, fetch: fetch},
			desc: p.field + " " + p.op.String() + " " + quote(v),
			fn:   func(rec *T) bool { return comparePred(p.op, fetch(rec), v) }}
	case opIn, opNotIn:
		set := make(map[any]struct{}, len(vals))
		for _, v := range vals {
			set[v] = struct{}{}
		}
		neg := p.op == opNotIn
		names := make([]string, len(vals))
		for i, v := range vals {
			names[i] = quote(v)
		}
		return &expr[T]{k: kPred, p: &predInfo{field: p.field, op: p.op, vals: vals, fetch: fetch},
			desc: p.field + " " + p.op.String() + " (" + strings.Join(names, ", ") + ")",
			fn: func(rec *T) bool {
				_, ok := set[fetch(rec)]
				return ok != neg
			}}
	}
	return e
}

// comparePred evaluates one leaf operator over type-erased values.
func comparePred(op predOp, fv, v any) bool {
	switch op {
	case opEq:
		return fv == v
	case opNe:
		return fv != v
	case opGt:
		return compareOrdValues(fv, v) > 0
	case opGe:
		return compareOrdValues(fv, v) >= 0
	case opLt:
		return compareOrdValues(fv, v) < 0
	case opLe:
		return compareOrdValues(fv, v) <= 0
	case opStartsWith:
		fs, ok1 := fv.(string)
		ps, ok2 := v.(string)
		return ok1 && ok2 && strings.HasPrefix(fs, ps)
	case opEndsWith:
		fs, ok1 := fv.(string)
		ps, ok2 := v.(string)
		return ok1 && ok2 && strings.HasSuffix(fs, ps)
	case opContains:
		fs, ok1 := fv.(string)
		ps, ok2 := v.(string)
		return ok1 && ok2 && strings.Contains(fs, ps)
	case opLike:
		fs, ok1 := fv.(string)
		ps, ok2 := v.(string)
		return ok1 && ok2 && likeMatch(fs, ps)
	}
	return false
}

// compareOrdValues orders scalar values across numeric kinds, strings,
// and UUIDs. Non-scalar or mismatched values fall back to formatted text,
// which is deterministic but not semantically meaningful.
func compareOrdValues(a, b any) int {
	if ar, aok := asFloat(a); aok {
		if br, bok := asFloat(b); bok {
			switch {
			case ar < br:
				return -1
			case ar > br:
				return 1
			}
			return 0
		}
	}
	if as, aok := a.(string); aok {
		if bs, bok := b.(string); bok {
			return strings.Compare(as, bs)
		}
	}
	if au, aok := a.(UUID); aok {
		if bu, bok := b.(UUID); bok {
			return bytes.Compare(au[:], bu[:])
		}
	}
	if ab, aok := a.([16]byte); aok {
		if bb, bok := b.([16]byte); bok {
			return bytes.Compare(ab[:], bb[:])
		}
	}
	return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
}

// newLikePred builds a LIKE predicate with % (any run) and _ (one char)
// wildcards. Byte-oriented; suited to ASCII patterns.
func newLikePred[T any](f Field[T, string], pat string) Expr[T] {
	pi := &predInfo{field: f.name, op: opLike, vals: []any{pat}, paramSeq: []int64{paramSeqFor(pat)},
		fetch: func(rec any) any { return any(f.get(rec.(*T))) }}
	if pi.paramSeq[0] != 0 {
		pi.paramTyp = stringType
	}
	return &expr[T]{k: kPred, p: pi, desc: f.name + " LIKE " + quote(pat),
		fn: func(rec *T) bool { return likeMatch(f.get(rec), pat) }}
}

var stringType = reflect.TypeOf("")

// likePrefix extracts a usable prefix index key from patterns of the form
// "literal%". It reports false for any other wildcard shape.
func likePrefix(pat string) (string, bool) {
	if !strings.HasSuffix(pat, "%") || strings.ContainsAny(pat[:len(pat)-1], "%_") {
		return "", false
	}
	return pat[:len(pat)-1], true
}

// likeMatch matches s against pat (% any run incl. empty, _ exactly one byte).
func likeMatch(s, pat string) bool {
	px, sx := 0, 0
	star, ss := -1, 0
	for sx < len(s) {
		if px < len(pat) && (pat[px] == '_' || pat[px] == s[sx]) {
			px++
			sx++
		} else if px < len(pat) && pat[px] == '%' {
			star, ss = px, sx
			px++
		} else if star != -1 {
			px = star + 1
			ss++
			sx = ss
		} else {
			return false
		}
	}
	for px < len(pat) && pat[px] == '%' {
		px++
	}
	return px == len(pat)
}

// asFloat promotes any numeric value to float64 for cross-kind ordering.
func asFloat(v any) (float64, bool) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), true
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true
	}
	return 0, false
}
