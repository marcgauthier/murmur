package murmur

import (
	"fmt"
	"reflect"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/rimeadapter"
	"github.com/marcgauthier/murmur/q"
	"github.com/marcgauthier/murmur/rime"
)

// This file implements item-level aggregates and group-by. Descriptors in q
// translate into typed RIME aggregations, so the engine behavior matches the
// typed API exactly.

// ItemGroupRow is one group: its key values plus one value per aggregation,
// in the order the aggregates were requested.
type ItemGroupRow struct {
	Names  []string
	Keys   []any
	Values []any
}

// Aggregate runs read-only aggregates over the query matches, returning one
// value per descriptor. Sum and Avg return float64, Count returns int, and
// Min/Max return native field values (nil when nothing matches).
func (iq *ItemQuery) Aggregate(aggs ...q.Aggregate) ([]any, error) {
	if err := iq.check(); err != nil {
		return nil, err
	}
	return iq.binding.ops.aggregate(iq.db, iq.spec(), aggs)
}

// ItemGroupedQuery groups query matches by struct fields before aggregation.
// It preserves the query's filters, ordering, pagination, and bindings.
type ItemGroupedQuery struct {
	db     *DB
	spec   *itemQuerySpec
	fields []string
	err    error
}

// GroupBy groups query matches by the named struct fields. Group keys must
// be comparable built-in scalar, identity, or timestamp fields.
func (iq *ItemQuery) GroupBy(fields ...string) *ItemGroupedQuery {
	if iq == nil {
		return &ItemGroupedQuery{err: rime.ErrBadView}
	}
	gq := &ItemGroupedQuery{db: iq.db}
	if err := iq.check(); err != nil {
		gq.err = err
		return gq
	}
	for _, field := range fields {
		if err := iq.binding.validateGroupField(field); err != nil {
			gq.err = err
			return gq
		}
	}
	gq.spec = iq.spec()
	gq.fields = append([]string(nil), fields...)
	return gq
}

// Aggregate runs aggregates within each group.
func (gq *ItemGroupedQuery) Aggregate(aggs ...q.Aggregate) ([]ItemGroupRow, error) {
	if gq == nil {
		return nil, rime.ErrBadView
	}
	if gq.err != nil {
		return nil, gq.err
	}
	if gq.db == nil || gq.spec == nil {
		return nil, rime.ErrBadView
	}
	return gq.spec.binding.ops.grouped(gq.db, gq.spec, gq.fields, aggs)
}

// validateGroupField checks group-key names eagerly so builders fail fast.
func (b *itemBinding) validateGroupField(field string) error {
	fieldType, ok := b.fields[field]
	if !ok {
		return fmt.Errorf("murmur: unknown field %q for %s", field, b.goType)
	}
	if !itemGroupableType(fieldType) {
		return fmt.Errorf("murmur: field %q has type %s, which item queries cannot group by", field, fieldType)
	}
	return nil
}

// itemGroupableType reports whether a field type supports group-by keys:
// comparable built-in scalars, 16-byte identities, UUIDs, and timestamps.
func itemGroupableType(typ reflect.Type) bool {
	switch typ {
	case itemStringType, itemBoolType,
		reflect.TypeFor[int](), reflect.TypeFor[int8](), reflect.TypeFor[int16](),
		reflect.TypeFor[int32](), reflect.TypeFor[int64](),
		reflect.TypeFor[uint](), reflect.TypeFor[uint8](), reflect.TypeFor[uint16](),
		reflect.TypeFor[uint32](), reflect.TypeFor[uint64](),
		reflect.TypeFor[float32](), reflect.TypeFor[float64](),
		itemRowIDType, itemBytesType, itemUUIDType,
		reflect.TypeFor[time.Time]():
		return true
	}
	return false
}

// itemAggregateNumericType reports whether a field type supports Sum and Avg.
func itemAggregateNumericType(typ reflect.Type) bool {
	switch typ {
	case reflect.TypeFor[int](), reflect.TypeFor[int8](), reflect.TypeFor[int16](),
		reflect.TypeFor[int32](), reflect.TypeFor[int64](),
		reflect.TypeFor[uint](), reflect.TypeFor[uint8](), reflect.TypeFor[uint16](),
		reflect.TypeFor[uint32](), reflect.TypeFor[uint64](),
		reflect.TypeFor[float32](), reflect.TypeFor[float64]():
		return true
	}
	return false
}

func aggregateItemQuery[T any](db *DB, name string, spec *itemQuerySpec, aggs []q.Aggregate) ([]any, error) {
	desc, err := translateItemAggs[T](db, name, spec, aggs)
	if err != nil {
		return nil, err
	}
	query, err := buildItemQuery[T](db, name, spec)
	if err != nil {
		return nil, err
	}
	return query.Aggregate(desc...)
}

func aggregateItemGroups[T any](db *DB, name string, spec *itemQuerySpec, fields []string, aggs []q.Aggregate) ([]ItemGroupRow, error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.recordMu.RLock()
	inner, err := resolveItemTable[T](db, name, spec)
	if err != nil {
		db.recordMu.RUnlock()
		return nil, err
	}
	keys, err := translateItemGroupKeys[T](inner, spec.binding, fields)
	if err != nil {
		db.recordMu.RUnlock()
		return nil, err
	}
	desc, err := translateItemAggsLocked[T](inner, spec.binding, aggs)
	db.recordMu.RUnlock()
	if err != nil {
		return nil, err
	}
	query, err := buildItemQuery[T](db, name, spec)
	if err != nil {
		return nil, err
	}
	rows, err := query.GroupBy(keys...).Aggregate(desc...)
	if err != nil {
		return nil, err
	}
	out := make([]ItemGroupRow, len(rows))
	for i, row := range rows {
		out[i] = ItemGroupRow{Names: append([]string(nil), fields...), Keys: row.Keys, Values: row.Values}
	}
	return out, nil
}

// translateItemAggs resolves the live table and converts descriptors.
func translateItemAggs[T any](db *DB, name string, spec *itemQuerySpec, aggs []q.Aggregate) ([]rime.Agg[T], error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.recordMu.RLock()
	defer db.recordMu.RUnlock()
	inner, err := resolveItemTable[T](db, name, spec)
	if err != nil {
		return nil, err
	}
	return translateItemAggsLocked[T](inner, spec.binding, aggs)
}

func translateItemAggsLocked[T any](inner *rimeadapter.Table[T], binding *itemBinding, aggs []q.Aggregate) ([]rime.Agg[T], error) {
	out := make([]rime.Agg[T], 0, len(aggs))
	for _, agg := range aggs {
		switch agg.Op {
		case q.OpCount:
			out = append(out, rime.Count[T]())
		case q.OpSum, q.OpAvg:
			desc, err := translateItemNumericAgg[T](inner, binding, agg)
			if err != nil {
				return nil, err
			}
			out = append(out, desc)
		case q.OpMin, q.OpMax:
			desc, err := translateItemOrderedAgg[T](inner, binding, agg)
			if err != nil {
				return nil, err
			}
			out = append(out, desc)
		default:
			return nil, fmt.Errorf("murmur: unsupported aggregate %d", agg.Op)
		}
	}
	return out, nil
}

func translateItemNumericAgg[T any](inner *rimeadapter.Table[T], binding *itemBinding, agg q.Aggregate) (rime.Agg[T], error) {
	fieldType, ok := binding.fields[agg.Field]
	if !ok {
		return rime.Agg[T]{}, fmt.Errorf("murmur: unknown field %q for %s", agg.Field, binding.goType)
	}
	if !itemAggregateNumericType(fieldType) {
		return rime.Agg[T]{}, fmt.Errorf("murmur: aggregate %s requires a built-in numeric field, %q has type %s", agg.Op, agg.Field, fieldType)
	}
	logical := binding.logical[agg.Field]
	switch fieldType {
	case reflect.TypeFor[int]():
		return itemNumAgg[T, int](inner, logical, agg.Op), nil
	case reflect.TypeFor[int8]():
		return itemNumAgg[T, int8](inner, logical, agg.Op), nil
	case reflect.TypeFor[int16]():
		return itemNumAgg[T, int16](inner, logical, agg.Op), nil
	case reflect.TypeFor[int32]():
		return itemNumAgg[T, int32](inner, logical, agg.Op), nil
	case reflect.TypeFor[int64]():
		return itemNumAgg[T, int64](inner, logical, agg.Op), nil
	case reflect.TypeFor[uint]():
		return itemNumAgg[T, uint](inner, logical, agg.Op), nil
	case reflect.TypeFor[uint8]():
		return itemNumAgg[T, uint8](inner, logical, agg.Op), nil
	case reflect.TypeFor[uint16]():
		return itemNumAgg[T, uint16](inner, logical, agg.Op), nil
	case reflect.TypeFor[uint32]():
		return itemNumAgg[T, uint32](inner, logical, agg.Op), nil
	case reflect.TypeFor[uint64]():
		return itemNumAgg[T, uint64](inner, logical, agg.Op), nil
	case reflect.TypeFor[float32]():
		return itemNumAgg[T, float32](inner, logical, agg.Op), nil
	case reflect.TypeFor[float64]():
		return itemNumAgg[T, float64](inner, logical, agg.Op), nil
	}
	return rime.Agg[T]{}, fmt.Errorf("murmur: aggregate %s requires a built-in numeric field, %q has type %s", agg.Op, agg.Field, fieldType)
}

func itemNumAgg[T any, V rime.Number](inner *rimeadapter.Table[T], logical string, op q.AggOp) rime.Agg[T] {
	field := rimeadapter.NumericFieldOf[T, V](inner, logical)
	if op == q.OpSum {
		return rime.SumOf(field)
	}
	return rime.AvgOf(field)
}

func translateItemOrderedAgg[T any](inner *rimeadapter.Table[T], binding *itemBinding, agg q.Aggregate) (rime.Agg[T], error) {
	fieldType, ok := binding.fields[agg.Field]
	if !ok {
		return rime.Agg[T]{}, fmt.Errorf("murmur: unknown field %q for %s", agg.Field, binding.goType)
	}
	logical := binding.logical[agg.Field]
	switch fieldType {
	case itemStringType:
		return itemOrdAgg[T, string](inner, logical, agg.Op), nil
	case reflect.TypeFor[int]():
		return itemOrdAgg[T, int](inner, logical, agg.Op), nil
	case reflect.TypeFor[int8]():
		return itemOrdAgg[T, int8](inner, logical, agg.Op), nil
	case reflect.TypeFor[int16]():
		return itemOrdAgg[T, int16](inner, logical, agg.Op), nil
	case reflect.TypeFor[int32]():
		return itemOrdAgg[T, int32](inner, logical, agg.Op), nil
	case reflect.TypeFor[int64]():
		return itemOrdAgg[T, int64](inner, logical, agg.Op), nil
	case reflect.TypeFor[uint]():
		return itemOrdAgg[T, uint](inner, logical, agg.Op), nil
	case reflect.TypeFor[uint8]():
		return itemOrdAgg[T, uint8](inner, logical, agg.Op), nil
	case reflect.TypeFor[uint16]():
		return itemOrdAgg[T, uint16](inner, logical, agg.Op), nil
	case reflect.TypeFor[uint32]():
		return itemOrdAgg[T, uint32](inner, logical, agg.Op), nil
	case reflect.TypeFor[uint64]():
		return itemOrdAgg[T, uint64](inner, logical, agg.Op), nil
	case reflect.TypeFor[float32]():
		return itemOrdAgg[T, float32](inner, logical, agg.Op), nil
	case reflect.TypeFor[float64]():
		return itemOrdAgg[T, float64](inner, logical, agg.Op), nil
	}
	return rime.Agg[T]{}, fmt.Errorf("murmur: aggregate %s requires a string or built-in numeric field, %q has type %s", agg.Op, agg.Field, fieldType)
}

func itemOrdAgg[T any, V rimeadapter.Ordered](inner *rimeadapter.Table[T], logical string, op q.AggOp) rime.Agg[T] {
	field := rimeadapter.OrderedFieldOf[T, V](inner, logical)
	if op == q.OpMin {
		return rime.MinOf(field)
	}
	return rime.MaxOf(field)
}

func translateItemGroupKeys[T any](inner *rimeadapter.Table[T], binding *itemBinding, fields []string) ([]rime.GroupField[T], error) {
	keys := make([]rime.GroupField[T], 0, len(fields))
	for _, field := range fields {
		fieldType, ok := binding.fields[field]
		if !ok {
			return nil, fmt.Errorf("murmur: unknown field %q for %s", field, binding.goType)
		}
		logical := binding.logical[field]
		switch fieldType {
		case itemStringType:
			keys = append(keys, rimeadapter.FieldOf[T, string](inner, logical))
		case itemBoolType:
			keys = append(keys, rimeadapter.FieldOf[T, bool](inner, logical))
		case reflect.TypeFor[int]():
			keys = append(keys, rimeadapter.FieldOf[T, int](inner, logical))
		case reflect.TypeFor[int8]():
			keys = append(keys, rimeadapter.FieldOf[T, int8](inner, logical))
		case reflect.TypeFor[int16]():
			keys = append(keys, rimeadapter.FieldOf[T, int16](inner, logical))
		case reflect.TypeFor[int32]():
			keys = append(keys, rimeadapter.FieldOf[T, int32](inner, logical))
		case reflect.TypeFor[int64]():
			keys = append(keys, rimeadapter.FieldOf[T, int64](inner, logical))
		case reflect.TypeFor[uint]():
			keys = append(keys, rimeadapter.FieldOf[T, uint](inner, logical))
		case reflect.TypeFor[uint8]():
			keys = append(keys, rimeadapter.FieldOf[T, uint8](inner, logical))
		case reflect.TypeFor[uint16]():
			keys = append(keys, rimeadapter.FieldOf[T, uint16](inner, logical))
		case reflect.TypeFor[uint32]():
			keys = append(keys, rimeadapter.FieldOf[T, uint32](inner, logical))
		case reflect.TypeFor[uint64]():
			keys = append(keys, rimeadapter.FieldOf[T, uint64](inner, logical))
		case reflect.TypeFor[float32]():
			keys = append(keys, rimeadapter.FieldOf[T, float32](inner, logical))
		case reflect.TypeFor[float64]():
			keys = append(keys, rimeadapter.FieldOf[T, float64](inner, logical))
		case itemRowIDType:
			keys = append(keys, rimeadapter.FieldOf[T, ids.RowID](inner, logical))
		case itemBytesType:
			keys = append(keys, rimeadapter.FieldOf[T, [16]byte](inner, logical))
		case itemUUIDType:
			keys = append(keys, rimeadapter.FieldOf[T, rime.UUID](inner, logical))
		case reflect.TypeFor[time.Time]():
			keys = append(keys, rimeadapter.FieldOf[T, time.Time](inner, logical))
		default:
			return nil, fmt.Errorf("murmur: field %q has type %s, which item queries cannot group by", field, fieldType)
		}
	}
	return keys, nil
}
