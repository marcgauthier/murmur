package rime

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"unsafe"
)

// Number constrains numeric aggregation inputs.
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

type aggKind int

const (
	aggCount aggKind = iota
	aggSum
	aggAvg
	aggMin
	aggMax
)

// Agg is one aggregation over a row set, built with Count, SumOf, AvgOf,
// MinOf, or MaxOf. Aggregations operate directly on native values.
type Agg[T any] struct {
	name  string
	kind  aggKind
	num   func(*T) float64
	ordLt func(a, b *T) bool
	ordV  func(*T) any
	field string // ordered-index fast path for Min/Max
	off   uintptr
	nk    numKind // numNone falls back to num
}

// numKind selects the direct-load width for SUM/AVG extraction.
type numKind uint8

const (
	numNone numKind = iota
	numInt8
	numInt16
	numInt32
	numInt64
	numUint8
	numUint16
	numUint32
	numUint64
	numFloat32
	numFloat64
)

// numKindFor maps a numeric Go type to its load width. Width derives from
// kind and size so named types and platform-sized int/uint resolve to the
// correct identical-layout load.
func numKindFor[V Number]() numKind {
	var zero V
	t := reflect.TypeOf(zero)
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch t.Size() {
		case 1:
			return numInt8
		case 2:
			return numInt16
		case 4:
			return numInt32
		default:
			return numInt64
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch t.Size() {
		case 1:
			return numUint8
		case 2:
			return numUint16
		case 4:
			return numUint32
		default:
			return numUint64
		}
	case reflect.Float32:
		return numFloat32
	case reflect.Float64:
		return numFloat64
	}
	return numNone
}

// numAt reads the aggregation input as float64 through one direct offset
// load, bypassing the num/get closure chain on the per-row path.
func (a Agg[T]) numAt(rec *T) float64 {
	if a.nk == numNone {
		return a.num(rec)
	}
	p := unsafe.Pointer(uintptr(unsafe.Pointer(rec)) + a.off)
	switch a.nk {
	case numInt8:
		return float64(*(*int8)(p))
	case numInt16:
		return float64(*(*int16)(p))
	case numInt32:
		return float64(*(*int32)(p))
	case numInt64:
		return float64(*(*int64)(p))
	case numUint8:
		return float64(*(*uint8)(p))
	case numUint16:
		return float64(*(*uint16)(p))
	case numUint32:
		return float64(*(*uint32)(p))
	case numUint64:
		return float64(*(*uint64)(p))
	case numFloat32:
		return float64(*(*float32)(p))
	case numFloat64:
		return *(*float64)(p)
	}
	return a.num(rec)
}

// Count tallies rows.
func Count[T any]() Agg[T] { return Agg[T]{name: "COUNT", kind: aggCount} }

// SumOf totals a numeric field.
func SumOf[T any, V Number](f OrderedField[T, V]) Agg[T] {
	a := Agg[T]{name: "SUM(" + f.name + ")", kind: aggSum,
		num: func(rec *T) float64 { return float64(f.get(rec)) }}
	if off, ok := fieldOffset[T, V](f.tbl.sch, f.name); ok {
		a.off, a.nk = off, numKindFor[V]()
	}
	return a
}

// AvgOf averages a numeric field.
func AvgOf[T any, V Number](f OrderedField[T, V]) Agg[T] {
	a := Agg[T]{name: "AVG(" + f.name + ")", kind: aggAvg,
		num: func(rec *T) float64 { return float64(f.get(rec)) }}
	if off, ok := fieldOffset[T, V](f.tbl.sch, f.name); ok {
		a.off, a.nk = off, numKindFor[V]()
	}
	return a
}

// MinOf takes the minimum of an ordered field.
func MinOf[T any, V ordered](f OrderedField[T, V]) Agg[T] {
	return Agg[T]{name: "MIN(" + f.name + ")", kind: aggMin, field: f.name,
		ordLt: func(a, b *T) bool { return f.ord(f.get(a), f.get(b)) < 0 },
		ordV:  func(rec *T) any { return any(f.get(rec)) }}
}

// MaxOf takes the maximum of an ordered field.
func MaxOf[T any, V ordered](f OrderedField[T, V]) Agg[T] {
	return Agg[T]{name: "MAX(" + f.name + ")", kind: aggMax, field: f.name,
		ordLt: func(a, b *T) bool { return f.ord(f.get(a), f.get(b)) < 0 },
		ordV:  func(rec *T) any { return any(f.get(rec)) }}
}

func (a Agg[T]) run(rows []*T) any {
	switch a.kind {
	case aggCount:
		return len(rows)
	case aggSum:
		s := 0.0
		for _, r := range rows {
			s += a.numAt(r)
		}
		return s
	case aggAvg:
		if len(rows) == 0 {
			return 0.0
		}
		s := 0.0
		for _, r := range rows {
			s += a.numAt(r)
		}
		return s / float64(len(rows))
	case aggMin, aggMax:
		if len(rows) == 0 {
			return nil
		}
		best := rows[0]
		for _, r := range rows[1:] {
			if a.kind == aggMin {
				if a.ordLt(r, best) {
					best = r
				}
			} else if a.ordLt(best, r) {
				best = r
			}
		}
		return a.ordV(best)
	}
	return nil
}

// Aggregate runs aggregations over the query matches.
func (q *Query[T]) Aggregate(aggs ...Agg[T]) ([]any, error) {
	return q.aggregateContext(q.context(), q.tx, aggs...)
}

// AggregateContext is Aggregate with cancellation.
func (q *Query[T]) aggregateContext(ctx context.Context, tx *Tx, aggs ...Agg[T]) ([]any, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tx == nil {
		tx = q.tbl.db.ReadTxContext(ctx)
		defer tx.Close()
	}
	if err := q.tbl.checkTx(tx); err != nil {
		return nil, err
	}
	// Fast path: unfiltered MIN/MAX straight off the ordered index.
	if len(q.exprs) == 0 && q.limit < 0 && q.offset == 0 && len(aggs) == 1 && (aggs[0].kind == aggMin || aggs[0].kind == aggMax) {
		if v, ok := q.tbl.orderedExtreme(tx, aggs[0]); ok {
			if err := tx.mustOpen(); err != nil {
				return nil, err
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return []any{v}, nil
		}
	}
	rows, err := q.findContext(ctx, tx)
	if err != nil {
		return nil, err
	}
	out := make([]any, len(aggs))
	for i, a := range aggs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		out[i] = a.run(rows)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Sum returns the selected rows' sum for a SumOf field descriptor.
func (q *Query[T]) Sum(a Agg[T]) (float64, error) {
	if a.kind != aggSum {
		return 0, fmt.Errorf("rime: Sum requires SumOf field")
	}
	out, err := q.Aggregate(a)
	if err != nil {
		return 0, err
	}
	return out[0].(float64), nil
}

// Avg returns the selected rows' average for an AvgOf field descriptor.
func (q *Query[T]) Avg(a Agg[T]) (float64, error) {
	if a.kind != aggAvg {
		return 0, fmt.Errorf("rime: Avg requires AvgOf field")
	}
	out, err := q.Aggregate(a)
	if err != nil {
		return 0, err
	}
	return out[0].(float64), nil
}

// orderedExtreme resolves MIN/MAX from the ordered index, skipping versions
// invisible at the snapshot. ok=false falls back to a full scan.
func (t *Table[T]) orderedExtreme(tx *Tx, a Agg[T]) (any, bool) {
	if tx != nil && (tx.write || tx.preview) && len(tx.pending) > 0 {
		return nil, false
	}
	if err := tx.mustOpen(); err != nil {
		return nil, false
	}
	t.db.viewMu.RLock()
	defer t.db.viewMu.RUnlock()
	snap := t.snapOf(tx)
	if snap < TxID(t.lastChange.Load()) {
		return nil, false
	}
	t.idx.mu.RLock()
	o, ok := t.idx.ordered[a.field]
	if !ok {
		t.idx.mu.RUnlock()
		return nil, false
	}
	entry, exists := o.extreme(a.kind == aggMax)
	t.idx.mu.RUnlock()
	if !exists {
		return nil, true
	}
	if rec := t.visibleKey(entry.key, snap); rec != nil {
		return a.ordV(rec), true
	}
	return nil, false
}

// GroupedQuery is a query with GROUP BY keys.
type GroupedQuery[T any] struct {
	q      *Query[T]
	fields []GroupField[T]
}

// GroupBy groups matches by the given fields before aggregation.
func (q *Query[T]) GroupBy(fields ...GroupField[T]) *GroupedQuery[T] {
	return &GroupedQuery[T]{q: q.clone(), fields: append([]GroupField[T](nil), fields...)}
}

// groupID encodes one group key for map identity.
func groupID(keys []any) string {
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(0x1f)
		}
		part := fmt.Sprintf("%T\x00%v", k, k)
		fmt.Fprintf(&sb, "%d:%s", len(part), part)
	}
	return sb.String()
}

// GroupRow is one group: its key values plus one value per aggregation.
type GroupRow struct {
	Names  []string
	Keys   []any
	Values []any
}

// Aggregate groups matches and runs each aggregation per group.
func (g *GroupedQuery[T]) In(tx *Tx) *GroupedQuery[T] {
	return &GroupedQuery[T]{q: g.q.In(tx), fields: append([]GroupField[T](nil), g.fields...)}
}
func (g *GroupedQuery[T]) WithContext(ctx context.Context) *GroupedQuery[T] {
	return &GroupedQuery[T]{q: g.q.WithContext(ctx), fields: append([]GroupField[T](nil), g.fields...)}
}

func (g *GroupedQuery[T]) Aggregate(aggs ...Agg[T]) ([]GroupRow, error) {
	return g.aggregateContext(g.q.context(), g.q.tx, aggs...)
}

// AggregateContext is GroupBy Aggregate with cancellation.
func (g *GroupedQuery[T]) aggregateContext(ctx context.Context, tx *Tx, aggs ...Agg[T]) ([]GroupRow, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := g.q.findContext(ctx, tx)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(g.fields))
	for i, f := range g.fields {
		names[i] = f.groupName()
	}
	groups := map[string]int{}
	var out []GroupRow
	var members [][]*T
	for i, r := range rows {
		if i%64 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		keys := make([]any, len(g.fields))
		for i, f := range g.fields {
			keys[i] = f.groupKey(r)
		}
		id := groupID(keys)
		gi, ok := groups[id]
		if !ok {
			gi = len(out)
			groups[id] = gi
			out = append(out, GroupRow{Names: names, Keys: keys})
			members = append(members, nil)
		}
		members[gi] = append(members[gi], r)
	}
	for i := range out {
		out[i].Values = make([]any, len(aggs))
		for j, a := range aggs {
			out[i].Values[j] = a.run(members[i])
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
