package murmur

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/rimeadapter"
	"github.com/marcgauthier/murmur/q"
	"github.com/marcgauthier/murmur/rime"
	"github.com/marcgauthier/murmur/schema"
)

// This file implements Murmur's generic Storm-style item API. define[T]
// captures type-erased operation bindings per table; the registry below maps
// registered struct types to those bindings so one function serves every
// collection. Queries translate q.Matcher expressions into typed RIME
// predicates, preserving index use for equality and range filters.

// itemOps holds type-erased bindings to recordTable[T] methods, built when
// define[T] registers a table. Every closure resolves the live typed table by
// name on each call, so bindings stay valid across materializer rebuilds.
type itemOps struct {
	insert     func(db *DB, tx *Tx, item any) error
	fetch      func(db *DB, tx *Tx, read *recordReadTx, key ids.RowID) (any, error)
	save       func(db *DB, tx *Tx, item any) error
	update     func(db *DB, tx *Tx, key ids.RowID, apply func(record any) error) error
	remove     func(db *DB, tx *Tx, key ids.RowID) error
	counterAdd func(db *DB, tx *Tx, key ids.RowID, field string, delta int64) error
	setAdd     func(db *DB, tx *Tx, key ids.RowID, field, value string) error
	setRemove  func(db *DB, tx *Tx, key ids.RowID, field, value string) error
	extrema    func(db *DB, tx *Tx, key ids.RowID, field string, value any, max bool) error
	find       func(db *DB, spec *itemQuerySpec) ([]any, error)
	first      func(db *DB, spec *itemQuerySpec) (any, error)
	count      func(db *DB, spec *itemQuerySpec) (int, error)
	exists     func(db *DB, spec *itemQuerySpec) (bool, error)
	explain    func(db *DB, spec *itemQuerySpec) (string, error)
	aggregate  func(db *DB, spec *itemQuerySpec, aggs []q.Aggregate) ([]any, error)
	grouped    func(db *DB, spec *itemQuerySpec, fields []string, aggs []q.Aggregate) ([]ItemGroupRow, error)
	rowEqual   func(db *DB, name string, a, b any) (bool, error)
	rowClone   func(db *DB, name string, row any) (any, error)
}

// newItemOps builds type-erased bindings for the table registered as name.
func newItemOps[T any](name string) *itemOps {
	return &itemOps{
		explain: func(db *DB, spec *itemQuerySpec) (string, error) {
			query, err := buildItemQuery[T](db, name, spec)
			if err != nil {
				return "", err
			}
			if err := query.check(); err != nil {
				return "", err
			}
			db.recordMu.RLock()
			defer db.recordMu.RUnlock()
			if err := db.requireRead(); err != nil {
				return "", err
			}
			return query.inner.Explain(), nil
		},
		insert: func(db *DB, tx *Tx, item any) error {
			table, err := tableOf[T](db, name)
			if err != nil {
				return err
			}
			record, err := itemAs[T](item)
			if err != nil {
				return err
			}
			return table.Insert(tx, record)
		},
		fetch: func(db *DB, tx *Tx, read *recordReadTx, key ids.RowID) (any, error) {
			if read != nil {
				return (&recordTable[T]{db: db, name: strings.ToLower(name)}).GetRead(read, key)
			}
			table, err := tableOf[T](db, name)
			if err != nil {
				return nil, err
			}
			switch {
			case tx != nil:
				return table.GetTx(tx, key)
			case read != nil:
				return table.GetRead(read, key)
			default:
				return table.Get(key)
			}
		},
		save: func(db *DB, tx *Tx, item any) error {
			table, err := tableOf[T](db, name)
			if err != nil {
				return err
			}
			record, err := itemAs[T](item)
			if err != nil {
				return err
			}
			return table.Save(tx, record)
		},
		update: func(db *DB, tx *Tx, key ids.RowID, apply func(record any) error) error {
			table, err := tableOf[T](db, name)
			if err != nil {
				return err
			}
			return table.Update(tx, key, func(record *T) error { return apply(record) })
		},
		remove: func(db *DB, tx *Tx, key ids.RowID) error {
			table, err := tableOf[T](db, name)
			if err != nil {
				return err
			}
			return table.Delete(tx, key)
		},
		counterAdd: func(db *DB, tx *Tx, key ids.RowID, field string, delta int64) error {
			table, err := tableOf[T](db, name)
			if err != nil {
				return err
			}
			return recordCounterAdd(tx, table, key, field, delta)
		},
		setAdd: func(db *DB, tx *Tx, key ids.RowID, field, value string) error {
			table, err := tableOf[T](db, name)
			if err != nil {
				return err
			}
			return recordSetAdd(tx, table, key, field, value)
		},
		setRemove: func(db *DB, tx *Tx, key ids.RowID, field, value string) error {
			table, err := tableOf[T](db, name)
			if err != nil {
				return err
			}
			return recordSetRemove(tx, table, key, field, value)
		},
		extrema: func(db *DB, tx *Tx, key ids.RowID, field string, value any, max bool) error {
			table, err := tableOf[T](db, name)
			if err != nil {
				return err
			}
			if max {
				return recordMax(tx, table, key, field, value)
			}
			return recordMin(tx, table, key, field, value)
		},
		find: func(db *DB, spec *itemQuerySpec) ([]any, error) {
			query, err := buildItemQuery[T](db, name, spec)
			if err != nil {
				return nil, err
			}
			rows, err := query.Find()
			if err != nil {
				return nil, err
			}
			out := make([]any, len(rows))
			for i, row := range rows {
				out[i] = row
			}
			return out, nil
		},
		first: func(db *DB, spec *itemQuerySpec) (any, error) {
			query, err := buildItemQuery[T](db, name, spec)
			if err != nil {
				return nil, err
			}
			return query.First()
		},
		count: func(db *DB, spec *itemQuerySpec) (int, error) {
			query, err := buildItemQuery[T](db, name, spec)
			if err != nil {
				return 0, err
			}
			return query.Count()
		},
		exists: func(db *DB, spec *itemQuerySpec) (bool, error) {
			query, err := buildItemQuery[T](db, name, spec)
			if err != nil {
				return false, err
			}
			return query.Exists()
		},
		aggregate: func(db *DB, spec *itemQuerySpec, aggs []q.Aggregate) ([]any, error) {
			return aggregateItemQuery[T](db, name, spec, aggs)
		},
		grouped: func(db *DB, spec *itemQuerySpec, fields []string, aggs []q.Aggregate) ([]ItemGroupRow, error) {
			return aggregateItemGroups[T](db, name, spec, fields, aggs)
		},
		rowEqual: func(db *DB, name string, a, b any) (bool, error) {
			table, err := tableOf[T](db, name)
			if err != nil {
				return false, err
			}
			inner, unlock, err := table.lockInner()
			if err != nil {
				return false, err
			}
			defer unlock()
			if reflect.TypeFor[T]() == reflect.TypeFor[any]() {
				anyTable, ok := any(inner).(*rimeadapter.Table[any])
				if !ok {
					return false, fmt.Errorf("murmur: model binding changed: %w", ErrUnsupportedSchema)
				}
				return anyTable.Equal(&a, &b)
			}
			ra, err := itemAs[T](a)
			if err != nil {
				return false, err
			}
			rb, err := itemAs[T](b)
			if err != nil {
				return false, err
			}
			return inner.Equal(ra, rb)
		},
		rowClone: func(db *DB, name string, row any) (any, error) {
			table, err := tableOf[T](db, name)
			if err != nil {
				return nil, err
			}
			inner, unlock, err := table.lockInner()
			if err != nil {
				return nil, err
			}
			defer unlock()
			if reflect.TypeFor[T]() == reflect.TypeFor[any]() {
				anyTable, ok := any(inner).(*rimeadapter.Table[any])
				if !ok {
					return nil, fmt.Errorf("murmur: model binding changed: %w", ErrUnsupportedSchema)
				}
				cloned, err := anyTable.Clone(&row)
				if err != nil {
					return nil, err
				}
				native, ok := any(cloned).(*any)
				if !ok {
					return nil, fmt.Errorf("murmur: clone returned %T, want a record", cloned)
				}
				return *native, nil
			}
			record, err := itemAs[T](row)
			if err != nil {
				return nil, err
			}
			return inner.Clone(record)
		},
	}
}

// itemAs converts an item known to match T into a usable record pointer,
// accepting both T values and *T pointers.
func itemAs[T any](item any) (*T, error) {
	switch record := item.(type) {
	case *T:
		if record == nil {
			return nil, fmt.Errorf("murmur: item is a nil pointer")
		}
		return record, nil
	case T:
		return &record, nil
	default:
		var zero T
		return nil, fmt.Errorf("murmur: expected %T, got %T", &zero, item)
	}
}

// itemBinding resolves one registered struct type to its table and cached
// reflection metadata.
type itemBinding struct {
	table        string
	goType       reflect.Type
	ptrType      reflect.Type
	primaryField string
	primaryIndex []int
	// fields maps Go field names to their types for exported, non-ignored
	// top-level fields. logical maps Go names to RIME column names.
	fields  map[string]reflect.Type
	logical map[string]string
	// nonLWW marks fields with counter, set, or extrema merge policies,
	// which partial updates must not touch.
	nonLWW map[string]bool
	ops    *itemOps
	model  *modelIdentity
}

func newItemBinding(definition TableDefinition) (*itemBinding, error) {
	if definition.goType == nil || definition.itemOps == nil {
		return nil, fmt.Errorf("murmur: table %q has no item bindings", definition.name)
	}
	goType := definition.goType
	if goType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("murmur: table %q record type %s is not a struct", definition.name, goType)
	}
	binding := &itemBinding{
		table:   definition.name,
		goType:  goType,
		ptrType: reflect.PointerTo(goType),
		ops:     definition.itemOps,
		model:   definition.model,
		fields:  make(map[string]reflect.Type),
		logical: make(map[string]string),
		nonLWW:  make(map[string]bool),
	}
	primary, ok := goType.FieldByName(definition.primaryField)
	if !ok {
		return nil, fmt.Errorf("murmur: table %q primary field %q is absent", definition.name, definition.primaryField)
	}
	binding.primaryField = primary.Name
	binding.primaryIndex = primary.Index
	for i := 0; i < goType.NumField(); i++ {
		field := goType.Field(i)
		if field.PkgPath != "" {
			continue // unexported, ignored by RIME
		}
		skip, logical := itemRimeTag(field.Tag.Get("rime"))
		if skip {
			continue
		}
		if logical == "" {
			logical = field.Name
		}
		binding.fields[field.Name] = field.Type
		binding.logical[field.Name] = logical
	}
	for _, column := range definition.table.Columns {
		if column.MergePolicy != schema.LWW {
			for goName, logical := range binding.logical {
				if logical == column.Name {
					binding.nonLWW[goName] = true
				}
			}
		}
	}
	return binding, nil
}

// itemRimeTag reports whether a field is skipped and its logical column name.
func itemRimeTag(tag string) (skip bool, logical string) {
	if tag == "" {
		return false, ""
	}
	for _, directive := range strings.Split(tag, ",") {
		directive = strings.TrimSpace(directive)
		if directive == "-" {
			return true, ""
		}
		if name, ok := strings.CutPrefix(directive, "name="); ok {
			logical = strings.TrimSpace(name)
		}
	}
	return false, logical
}

// registerItemBindings rebuilds the struct-type registry from the current
// table definitions. A struct registered for more than one table is marked
// ambiguous: the typed API keeps working, but the item API refuses the type
// until the ambiguity is resolved.
func (db *DB) registerItemBindings() error {
	db.recordMu.RLock()
	definitions := append([]TableDefinition(nil), db.recordDefinitions...)
	db.recordMu.RUnlock()
	bindings, ambiguous, err := compileItemBindings(definitions)
	if err != nil {
		return err
	}
	db.itemMu.Lock()
	db.itemBindings = bindings
	db.itemAmbiguous = ambiguous
	db.itemMu.Unlock()
	return nil
}

func compileItemBindings(definitions []TableDefinition) (map[reflect.Type]*itemBinding, map[reflect.Type][]string, error) {
	bindings := make(map[reflect.Type]*itemBinding, len(definitions))
	ambiguous := make(map[reflect.Type][]string)
	for _, definition := range definitions {
		if definition.goType == nil || definition.itemOps == nil {
			continue
		}
		binding, err := newItemBinding(definition)
		if err != nil {
			return nil, nil, err
		}
		if tables, ok := ambiguous[binding.goType]; ok {
			ambiguous[binding.goType] = append(tables, definition.name)
			continue
		}
		if prev, ok := bindings[binding.goType]; ok {
			delete(bindings, binding.goType)
			ambiguous[binding.goType] = []string{prev.table, definition.name}
			continue
		}
		bindings[binding.goType] = binding
	}
	return bindings, ambiguous, nil
}

// resolveItemBinding maps a struct value or pointer to its registered table.
func (db *DB) resolveItemBinding(model any) (*itemBinding, error) {
	db.itemMu.RLock()
	defer db.itemMu.RUnlock()
	return resolveItemBindingFrom(model, db.itemBindings, db.itemAmbiguous)
}

func resolveItemBindingFrom(model any, bindings map[reflect.Type]*itemBinding, ambiguous map[reflect.Type][]string) (*itemBinding, error) {
	if model == nil {
		return nil, fmt.Errorf("murmur: item requires a registered struct value: %w", ErrUnsupportedSchema)
	}
	typ := reflect.TypeOf(model)
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("murmur: item requires a registered struct value, got %T: %w", model, ErrUnsupportedSchema)
	}
	if tables, ok := ambiguous[typ]; ok {
		return nil, fmt.Errorf("murmur: type %s is registered for tables %q; register each table under a distinct Model name to disambiguate: %w", typ, tables, ErrUnsupportedSchema)
	}
	binding, ok := bindings[typ]
	if !ok {
		return nil, fmt.Errorf("murmur: no table registered for type %s: %w", typ, ErrUnsupportedSchema)
	}
	return binding, nil
}

// rowID extracts the replicated primary key from a struct value or pointer.
func (b *itemBinding) rowID(item any) (ids.RowID, error) {
	if b.model != nil {
		if err := b.model.ensure(item, false); err != nil {
			return ids.RowID{}, err
		}
	}
	value := reflect.ValueOf(item)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return ids.RowID{}, fmt.Errorf("murmur: item is a nil pointer")
		}
		value = value.Elem()
	}
	if !value.IsValid() || value.Type() != b.goType {
		return ids.RowID{}, fmt.Errorf("murmur: expected %s, got %T", b.goType, item)
	}
	return itemRowID(value.FieldByIndex(b.primaryIndex))
}

func itemRowID(value reflect.Value) (ids.RowID, error) {
	if value.Kind() != reflect.Array || value.Len() != len(ids.RowID{}) || value.Type().Elem().Kind() != reflect.Uint8 {
		return ids.RowID{}, fmt.Errorf("murmur: primary key must be a 16-byte array, got %s", value.Type())
	}
	var id ids.RowID
	for i := range id {
		id[i] = byte(value.Index(i).Uint())
	}
	return id, nil
}

// validateFields checks partial-update field names, merge policies, and value
// types without touching the database.
func (b *itemBinding) validateFields(fields map[string]any) error {
	for name, value := range fields {
		fieldType, ok := b.fields[name]
		if !ok {
			return fmt.Errorf("murmur: unknown field %q for %s", name, b.goType)
		}
		if name == b.primaryField || (b.model != nil && name == b.model.business) {
			return fmt.Errorf("murmur: primary field %q is immutable", name)
		}
		if b.nonLWW[name] {
			return fmt.Errorf("murmur: field %q uses a merge policy; use CounterAdd, SetAdd, SetRemove, Max, or Min", name)
		}
		if _, err := coerceItemValue(fieldType, value); err != nil {
			return fmt.Errorf("murmur: field %q: %w", name, err)
		}
	}
	return nil
}

// applyFields validates and assigns explicitly supplied fields, including
// zero values. Map presence selects fields; absent fields are untouched.
func (b *itemBinding) applyFields(item any, fields map[string]any) error {
	if err := b.validateFields(fields); err != nil {
		return err
	}
	value := reflect.ValueOf(item)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("murmur: item must be a non-nil pointer")
	}
	value = value.Elem()
	if value.Type() != b.goType {
		return fmt.Errorf("murmur: expected *%s, got %T", b.goType, item)
	}
	for name, field := range fields {
		coerced, err := coerceItemValue(b.fields[name], field)
		if err != nil {
			return fmt.Errorf("murmur: field %q: %w", name, err)
		}
		value.FieldByName(name).Set(coerced)
	}
	return nil
}

var (
	itemStringType = reflect.TypeFor[string]()
	itemBoolType   = reflect.TypeFor[bool]()
	itemRowIDType  = reflect.TypeFor[ids.RowID]()
	itemBytesType  = reflect.TypeFor[[16]byte]()
	itemUUIDType   = reflect.TypeFor[rime.UUID]()
)

// itemComparableType reports whether a field type supports item equality,
// membership, and ordering expressions.
func itemComparableType(typ reflect.Type) bool {
	return itemOrderedType(typ) || typ.Kind() == reflect.Bool || isItemRowIDType(typ)
}

// itemOrderedType includes named scalar types and exact time.Time timestamps.
func itemOrderedType(typ reflect.Type) bool {
	return typ == reflect.TypeFor[time.Time]() || typ.Kind() == reflect.String || isItemNumericKind(typ.Kind())
}

func isItemNumericKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

func isItemRowIDType(typ reflect.Type) bool {
	return typ.Kind() == reflect.Array && typ.Len() == len(ids.RowID{}) && typ.Elem().Kind() == reflect.Uint8
}

// coerceItemValue converts an untyped value to a field's exact Go type. A nil
// value selects the field's zero value. Numeric widths convert with range
// checks; strings, bools, and 16-byte identities convert across named types.
func coerceItemValue(fieldType reflect.Type, value any) (reflect.Value, error) {
	if value == nil {
		return reflect.Zero(fieldType), nil
	}
	actual := reflect.ValueOf(value)
	if actual.Type() == fieldType {
		return actual, nil
	}
	if isItemRowIDType(fieldType) && isItemRowIDType(actual.Type()) {
		out := reflect.New(fieldType).Elem()
		for i := 0; i < len(ids.RowID{}); i++ {
			out.Index(i).SetUint(actual.Index(i).Uint())
		}
		return out, nil
	}
	if fieldType.Kind() == reflect.String && actual.Kind() == reflect.String {
		return actual.Convert(fieldType), nil
	}
	if fieldType.Kind() == reflect.Bool && actual.Kind() == reflect.Bool {
		return actual.Convert(fieldType), nil
	}
	if isItemNumericKind(fieldType.Kind()) && isItemNumericKind(actual.Kind()) {
		return coerceItemNumeric(fieldType, actual)
	}
	if actual.Type().AssignableTo(fieldType) {
		return actual, nil
	}
	return reflect.Value{}, fmt.Errorf("cannot use %s as %s", actual.Type(), fieldType)
}

func coerceItemNumeric(fieldType reflect.Type, actual reflect.Value) (reflect.Value, error) {
	out := reflect.New(fieldType).Elem()
	switch fieldType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := itemInt64(actual)
		if err != nil {
			return reflect.Value{}, err
		}
		if out.OverflowInt(n) {
			return reflect.Value{}, fmt.Errorf("value %v overflows %s", actual.Interface(), fieldType)
		}
		out.SetInt(n)
		return out, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := itemUint64(actual)
		if err != nil {
			return reflect.Value{}, err
		}
		if out.OverflowUint(n) {
			return reflect.Value{}, fmt.Errorf("value %v overflows %s", actual.Interface(), fieldType)
		}
		out.SetUint(n)
		return out, nil
	case reflect.Float32, reflect.Float64:
		var n float64
		switch actual.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			n = float64(actual.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			n = float64(actual.Uint())
		case reflect.Float32, reflect.Float64:
			n = actual.Convert(reflect.TypeFor[float64]()).Float()
		}
		if fieldType.Kind() == reflect.Float32 && (math.IsNaN(n) || math.Abs(n) > math.MaxFloat32) {
			if !math.IsNaN(n) {
				return reflect.Value{}, fmt.Errorf("value %v overflows %s", actual.Interface(), fieldType)
			}
		}
		if out.OverflowFloat(n) {
			return reflect.Value{}, fmt.Errorf("value %v overflows %s", actual.Interface(), fieldType)
		}
		out.SetFloat(n)
		return out, nil
	}
	return reflect.Value{}, fmt.Errorf("cannot use %s as %s", actual.Type(), fieldType)
}

func itemInt64(actual reflect.Value) (int64, error) {
	switch actual.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return actual.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n := actual.Uint()
		if n > math.MaxInt64 {
			return 0, fmt.Errorf("value %v overflows int64", actual.Interface())
		}
		return int64(n), nil
	case reflect.Float32, reflect.Float64:
		n := actual.Convert(reflect.TypeFor[float64]()).Float()
		if math.IsNaN(n) || math.IsInf(n, 0) || n != math.Trunc(n) || n < math.MinInt64 || n > math.MaxInt64 {
			return 0, fmt.Errorf("value %v is not an integer", actual.Interface())
		}
		return int64(n), nil
	}
	return 0, fmt.Errorf("cannot use %s as an integer", actual.Type())
}

func itemUint64(actual reflect.Value) (uint64, error) {
	switch actual.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := actual.Int()
		if n < 0 {
			return 0, fmt.Errorf("value %v is negative", actual.Interface())
		}
		return uint64(n), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return actual.Uint(), nil
	case reflect.Float32, reflect.Float64:
		n := actual.Convert(reflect.TypeFor[float64]()).Float()
		if math.IsNaN(n) || math.IsInf(n, 0) || n != math.Trunc(n) || n < 0 || n > math.MaxUint64 {
			return 0, fmt.Errorf("value %v is not an unsigned integer", actual.Interface())
		}
		return uint64(n), nil
	}
	return 0, fmt.Errorf("cannot use %s as an unsigned integer", actual.Type())
}

// InsertItem inserts a new record for a registered struct type. The item must
// be a T value or *T pointer carrying its primary key.
func (db *DB) InsertItem(ctx context.Context, item any) error {
	if db == nil {
		return ErrClosed
	}
	binding, err := db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if binding.model != nil {
		if err := binding.model.ensure(item, true); err != nil {
			return err
		}
	}
	if _, err := binding.rowID(item); err != nil {
		return err
	}
	return db.WriteTxContext(ctx, func(tx *Tx) error {
		return binding.ops.insert(db, tx, item)
	})
}

// GetItem populates the supplied struct pointer with the current record. The
// item must be a non-nil pointer carrying the primary key to read.
func (db *DB) GetItem(ctx context.Context, item any) error {
	if db == nil {
		return ErrClosed
	}
	binding, err := db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := requireItemPointer(binding, item); err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	got, err := binding.ops.fetch(db, nil, nil, key)
	if err != nil {
		return err
	}
	reflect.ValueOf(item).Elem().Set(reflect.ValueOf(got).Elem())
	return nil
}

// SaveItem inserts or replaces a complete record for a registered struct type.
func (db *DB) SaveItem(ctx context.Context, item any) error {
	if db == nil {
		return ErrClosed
	}
	binding, err := db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if binding.model != nil {
		if err := binding.model.ensure(item, true); err != nil {
			return err
		}
	}
	if _, err := binding.rowID(item); err != nil {
		return err
	}
	return db.WriteTxContext(ctx, func(tx *Tx) error {
		return binding.ops.save(db, tx, item)
	})
}

// UpdateFields updates only the specified top-level fields, including zero
// values. Absent fields are untouched. On success the supplied pointer, when
// addressable, reflects the new values.
func (db *DB) UpdateFields(ctx context.Context, item any, fields map[string]any) error {
	if db == nil {
		return ErrClosed
	}
	binding, err := db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if len(fields) == 0 {
		return nil
	}
	if err := binding.validateFields(fields); err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	err = db.WriteTxContext(ctx, func(tx *Tx) error {
		return binding.ops.update(db, tx, key, func(record any) error {
			return binding.applyFields(record, fields)
		})
	})
	if err != nil {
		return err
	}
	if value := reflect.ValueOf(item); value.Kind() == reflect.Pointer && !value.IsNil() {
		return binding.applyFields(item, fields)
	}
	return nil
}

// DeleteItem deletes the record identified by the item's primary key.
func (db *DB) DeleteItem(ctx context.Context, item any) error {
	if db == nil {
		return ErrClosed
	}
	binding, err := db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	return db.WriteTxContext(ctx, func(tx *Tx) error {
		return binding.ops.remove(db, tx, key)
	})
}

// InsertItem stages a new record in the transaction.
func (tx *Tx) InsertItem(item any) error {
	if tx == nil || tx.db == nil {
		return rime.ErrTxClosed
	}
	binding, err := tx.db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if binding.model != nil {
		if err := binding.model.ensure(item, true); err != nil {
			return err
		}
	}
	if _, err := binding.rowID(item); err != nil {
		return err
	}
	return binding.ops.insert(tx.db, tx, item)
}

// GetItem populates the supplied struct pointer through the transaction's
// snapshot and staged overlay.
func (tx *Tx) GetItem(item any) error {
	if tx == nil || tx.db == nil {
		return rime.ErrTxClosed
	}
	binding, err := tx.db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if err := requireItemPointer(binding, item); err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	got, err := binding.ops.fetch(tx.db, tx, nil, key)
	if err != nil {
		return err
	}
	reflect.ValueOf(item).Elem().Set(reflect.ValueOf(got).Elem())
	return nil
}

// SaveItem stages a full-record upsert in the transaction.
func (tx *Tx) SaveItem(item any) error {
	if tx == nil || tx.db == nil {
		return rime.ErrTxClosed
	}
	binding, err := tx.db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if binding.model != nil {
		if err := binding.model.ensure(item, true); err != nil {
			return err
		}
	}
	if _, err := binding.rowID(item); err != nil {
		return err
	}
	return binding.ops.save(tx.db, tx, item)
}

// UpdateFields stages a partial update in the transaction. On success the
// supplied pointer, when addressable, reflects the new values.
func (tx *Tx) UpdateFields(item any, fields map[string]any) error {
	if tx == nil || tx.db == nil {
		return rime.ErrTxClosed
	}
	binding, err := tx.db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	if len(fields) == 0 {
		return nil
	}
	if err := binding.validateFields(fields); err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	if err := binding.ops.update(tx.db, tx, key, func(record any) error {
		return binding.applyFields(record, fields)
	}); err != nil {
		return err
	}
	if value := reflect.ValueOf(item); value.Kind() == reflect.Pointer && !value.IsNil() {
		return binding.applyFields(item, fields)
	}
	return nil
}

// DeleteItem stages a tombstone for the item's primary key.
func (tx *Tx) DeleteItem(item any) error {
	if tx == nil || tx.db == nil {
		return rime.ErrTxClosed
	}
	binding, err := tx.db.resolveItemBinding(item)
	if err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	return binding.ops.remove(tx.db, tx, key)
}

// GetItem populates the supplied struct pointer from a pinned read snapshot.
func (tx *recordReadTx) GetItem(item any) error {
	if tx == nil || tx.db == nil || tx.inner == nil || tx.done {
		return rime.ErrTxClosed
	}
	binding, err := resolveItemBindingFrom(item, tx.itemBindings, tx.itemAmbiguous)
	if err != nil {
		return err
	}
	if err := requireItemPointer(binding, item); err != nil {
		return err
	}
	key, err := binding.rowID(item)
	if err != nil {
		return err
	}
	got, err := binding.ops.fetch(tx.db, nil, tx, key)
	if err != nil {
		return err
	}
	reflect.ValueOf(item).Elem().Set(reflect.ValueOf(got).Elem())
	return nil
}

func requireItemPointer(binding *itemBinding, item any) error {
	value := reflect.ValueOf(item)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("murmur: item must be a non-nil *%s", binding.goType)
	}
	if value.Type() != binding.ptrType {
		return fmt.Errorf("murmur: expected *%s, got %T", binding.goType, item)
	}
	return nil
}

// itemOrder is one ORDER BY key over a struct field name.
type itemOrder struct {
	field string
	desc  bool
}

// itemQuerySpec carries a fully built item query to the typed executor.
type itemQuerySpec struct {
	binding  *itemBinding
	matchers []q.Matcher
	orders   []itemOrder
	limit    int
	offset   int
	tx       *Tx
	read     *recordReadTx
	ctx      context.Context
}

// ItemQuery is a read-only generic query over one registered struct type.
// Builders return independent copies; terminal methods report sticky build
// errors before touching the database.
type ItemQuery struct {
	db       *DB
	binding  *itemBinding
	matchers []q.Matcher
	orders   []itemOrder
	limit    int
	offset   int
	tx       *Tx
	read     *recordReadTx
	ctx      context.Context
	err      error
}

// Query starts a generic query over the table registered for model's struct
// type. The model may be a T value or *T pointer; filters combine
// conjunctively.
func (db *DB) Query(model any, filters ...q.Matcher) *ItemQuery {
	iq := &ItemQuery{db: db, limit: -1}
	if db == nil {
		iq.err = ErrClosed
		return iq
	}
	binding, err := db.resolveItemBinding(model)
	if err != nil {
		iq.err = err
		return iq
	}
	iq.binding = binding
	for _, matcher := range filters {
		if err := binding.validateMatcher(matcher); err != nil {
			iq.err = err
			return iq
		}
	}
	iq.matchers = append(iq.matchers, filters...)
	return iq
}

func (iq *ItemQuery) clone() *ItemQuery {
	if iq == nil {
		return &ItemQuery{limit: -1}
	}
	out := *iq
	out.matchers = append([]q.Matcher(nil), iq.matchers...)
	out.orders = append([]itemOrder(nil), iq.orders...)
	return &out
}

// Where appends conjunctive filters.
func (iq *ItemQuery) Where(filters ...q.Matcher) *ItemQuery {
	out := iq.clone()
	if out.err != nil || out.binding == nil {
		return out
	}
	for _, matcher := range filters {
		if err := out.binding.validateMatcher(matcher); err != nil {
			out.err = err
			return out
		}
	}
	out.matchers = append(out.matchers, filters...)
	return out
}

// OrderBy appends an ascending sort key over a struct field name.
func (iq *ItemQuery) OrderBy(field string) *ItemQuery { return iq.order(field, false) }

// OrderByDescending appends a descending sort key over a struct field name.
func (iq *ItemQuery) OrderByDescending(field string) *ItemQuery {
	return iq.order(field, true)
}

func (iq *ItemQuery) order(field string, desc bool) *ItemQuery {
	out := iq.clone()
	if out.err != nil || out.binding == nil {
		return out
	}
	if err := out.binding.validateOrderField(field); err != nil {
		out.err = err
		return out
	}
	out.orders = append(out.orders, itemOrder{field: field, desc: desc})
	return out
}

// Limit caps returned records; a negative value clears the limit.
func (iq *ItemQuery) Limit(n int) *ItemQuery {
	out := iq.clone()
	if n < 0 {
		out.limit = -1
	} else {
		out.limit = n
	}
	return out
}

// Offset skips leading rows; negative values clamp to zero.
func (iq *ItemQuery) Offset(n int) *ItemQuery {
	out := iq.clone()
	if n < 0 {
		n = 0
	}
	out.offset = n
	return out
}

// In binds the query to a write transaction's snapshot and staged overlay,
// replacing any read-transaction binding.
func (iq *ItemQuery) In(tx *Tx) *ItemQuery {
	out := iq.clone()
	out.tx = tx
	out.read = nil
	return out
}

// inRead binds the query to a pinned read snapshot, replacing any write
// transaction binding.
func (iq *ItemQuery) inRead(tx *recordReadTx) *ItemQuery {
	out := iq.clone()
	out.read = tx
	out.tx = nil
	return out
}

// WithContext binds cancellation to the query.
func (iq *ItemQuery) WithContext(ctx context.Context) *ItemQuery {
	out := iq.clone()
	if ctx != nil {
		out.ctx = ctx
	}
	return out
}

func (iq *ItemQuery) check() error {
	if iq == nil {
		return rime.ErrBadView
	}
	if iq.err != nil {
		return iq.err
	}
	if iq.db == nil || iq.binding == nil {
		return rime.ErrBadView
	}
	return nil
}

func (iq *ItemQuery) spec() *itemQuerySpec {
	return &itemQuerySpec{
		binding:  iq.binding,
		matchers: iq.matchers,
		orders:   iq.orders,
		limit:    iq.limit,
		offset:   iq.offset,
		tx:       iq.tx,
		read:     iq.read,
		ctx:      iq.ctx,
	}
}

// FindInto executes the query and assigns detached records to dest, which
// must be a *[]T or *[]*T for the queried struct type.
func (iq *ItemQuery) FindInto(dest any) error {
	if err := iq.check(); err != nil {
		return err
	}
	target := reflect.ValueOf(dest)
	if !target.IsValid() || target.Kind() != reflect.Pointer || target.IsNil() {
		return fmt.Errorf("murmur: FindInto requires a non-nil slice pointer")
	}
	slice := target.Elem()
	if slice.Kind() != reflect.Slice {
		return fmt.Errorf("murmur: FindInto requires a *[]%s or *[]*%s, got %T", iq.binding.goType, iq.binding.goType, dest)
	}
	var wantPtr bool
	switch slice.Type().Elem() {
	case iq.binding.goType:
	case iq.binding.ptrType:
		wantPtr = true
	default:
		return fmt.Errorf("murmur: FindInto requires a *[]%s or *[]*%s, got %T", iq.binding.goType, iq.binding.goType, dest)
	}
	rows, err := iq.binding.ops.find(iq.db, iq.spec())
	if err != nil {
		return err
	}
	out := reflect.MakeSlice(slice.Type(), len(rows), len(rows))
	for i, row := range rows {
		record := reflect.ValueOf(row)
		if wantPtr {
			out.Index(i).Set(record)
		} else {
			out.Index(i).Set(record.Elem())
		}
	}
	slice.Set(out)
	return nil
}

// FirstInto executes the query and assigns the first match to dest, which
// must be a *T for the queried struct type. It returns rime.ErrNotFound when
// no record matches.
func (iq *ItemQuery) FirstInto(dest any) error {
	if err := iq.check(); err != nil {
		return err
	}
	if err := requireItemPointer(iq.binding, dest); err != nil {
		return err
	}
	got, err := iq.binding.ops.first(iq.db, iq.spec())
	if err != nil {
		return err
	}
	reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(got).Elem())
	return nil
}

// Count returns the number of matching records.
func (iq *ItemQuery) Count() (int, error) {
	if err := iq.check(); err != nil {
		return 0, err
	}
	return iq.binding.ops.count(iq.db, iq.spec())
}

// Exists reports whether at least one record matches.
func (iq *ItemQuery) Exists() (bool, error) {
	if err := iq.check(); err != nil {
		return false, err
	}
	return iq.binding.ops.exists(iq.db, iq.spec())
}

// validateOrderField checks that a struct field supports ORDER BY.
func (b *itemBinding) validateOrderField(field string) error {
	fieldType, ok := b.fields[field]
	if !ok {
		return fmt.Errorf("murmur: unknown field %q for %s", field, b.goType)
	}
	if !itemComparableType(fieldType) {
		return fmt.Errorf("murmur: field %q has type %s, which item queries cannot order", field, fieldType)
	}
	return nil
}

// validateMatcher checks field names, operators, and value types eagerly so
// query builders fail fast on typos.
func (b *itemBinding) validateMatcher(matcher q.Matcher) error {
	matcher, err := derefItemMatcher(matcher)
	if err != nil {
		return err
	}
	switch m := matcher.(type) {
	case q.StringMatcher:
		typ, ok := b.fields[m.Field]
		if !ok {
			return fmt.Errorf("murmur: unknown field %q for %s", m.Field, b.goType)
		}
		if typ.Kind() != reflect.String {
			return fmt.Errorf("murmur: field %q is not a string", m.Field)
		}
		if m.Op > q.OpLike {
			return fmt.Errorf("murmur: invalid string operator for field %q", m.Field)
		}
		return nil
	case q.Comparison:
		fieldType, ok := b.fields[m.Field]
		if !ok {
			return fmt.Errorf("murmur: unknown field %q for %s", m.Field, b.goType)
		}
		switch m.Op {
		case q.OpEq, q.OpNe:
			if !itemComparableType(fieldType) {
				return fmt.Errorf("murmur: field %q has type %s, which item queries cannot compare", m.Field, fieldType)
			}
		case q.OpGt, q.OpGte, q.OpLt, q.OpLte:
			if !itemOrderedType(fieldType) {
				return fmt.Errorf("murmur: field %q has type %s, which item queries cannot range over", m.Field, fieldType)
			}
		default:
			return fmt.Errorf("murmur: unsupported operator for field %q", m.Field)
		}
		if _, ok := m.Value.(q.Placeholder); !ok {
			if _, err := coerceItemValue(fieldType, m.Value); err != nil {
				return fmt.Errorf("murmur: field %q: %w", m.Field, err)
			}
		}
		return nil
	case q.InMatcher:
		fieldType, ok := b.fields[m.Field]
		if !ok {
			return fmt.Errorf("murmur: unknown field %q for %s", m.Field, b.goType)
		}
		if !itemComparableType(fieldType) {
			return fmt.Errorf("murmur: field %q has type %s, which item queries cannot compare", m.Field, fieldType)
		}
		for _, value := range m.Values {
			if _, ok := value.(q.Placeholder); ok {
				continue
			}
			if _, err := coerceItemValue(fieldType, value); err != nil {
				return fmt.Errorf("murmur: field %q: %w", m.Field, err)
			}
		}
		return nil
	case q.AndMatcher:
		for _, child := range m.Matchers {
			if err := b.validateMatcher(child); err != nil {
				return err
			}
		}
		return nil
	case q.OrMatcher:
		for _, child := range m.Matchers {
			if err := b.validateMatcher(child); err != nil {
				return err
			}
		}
		return nil
	case q.NotMatcher:
		if m.Matcher == nil {
			return fmt.Errorf("murmur: Not requires a matcher")
		}
		return b.validateMatcher(m.Matcher)
	default:
		return fmt.Errorf("murmur: unsupported matcher %T", matcher)
	}
}

// derefItemMatcher unwraps pointer indirections around matchers.
func derefItemMatcher(matcher q.Matcher) (q.Matcher, error) {
	if matcher == nil {
		return nil, fmt.Errorf("murmur: nil query matcher")
	}
	value := reflect.ValueOf(matcher)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, fmt.Errorf("murmur: nil query matcher")
		}
		value = value.Elem()
	}
	deref, ok := value.Interface().(q.Matcher)
	if !ok {
		return nil, fmt.Errorf("murmur: unsupported matcher %T", matcher)
	}
	return deref, nil
}

// resolveItemTable looks up the live adapter table for an item spec.
// Callers must hold recordMu (read or write).
func resolveItemTable[T any](db *DB, name string, spec *itemQuerySpec) (*rimeadapter.Table[T], error) {
	if db == nil {
		return nil, ErrClosed
	}
	name = strings.ToLower(name)
	if spec.tx != nil {
		if spec.tx.db != db || spec.tx.inner == nil {
			return nil, rime.ErrTxClosed
		}
	}
	if spec.read != nil {
		if spec.read.db != db || spec.read.inner == nil || spec.read.done {
			return nil, rime.ErrTxClosed
		}
	}
	handles := db.recordTables
	if spec.read != nil {
		handles = spec.read.tables
	}
	inner, ok := handles[name].(*rimeadapter.Table[T])
	if !ok || inner == nil || inner.RecordType() != spec.binding.goType {
		return nil, fmt.Errorf("murmur: model %s is unavailable in this generation: %w", spec.binding.goType, ErrUnsupportedSchema)
	}
	return inner, nil
}

// buildItemQuery translates an item spec into a typed read-only RIME query.
func buildItemQuery[T any](db *DB, name string, spec *itemQuerySpec) (*recordQuery[T], error) {
	if db == nil {
		return nil, ErrClosed
	}
	db.recordMu.RLock()
	defer db.recordMu.RUnlock()
	inner, err := resolveItemTable[T](db, name, spec)
	if err != nil {
		return nil, err
	}
	exprs, orders, err := translateItemSpec[T](inner, spec)
	if err != nil {
		return nil, err
	}
	query := &recordQuery[T]{db: db, inner: inner.Where(exprs...), clone: inner.Clone}
	switch {
	case spec.tx != nil:
		query.inner, err = inner.WhereTx(spec.tx.inner, exprs...)
		if err != nil {
			return nil, err
		}
		query.inner = query.inner.WithContext(spec.tx.ctx)
	case spec.read != nil:
		query.inner = query.inner.In(spec.read.inner).WithContext(spec.read.ctx)
	}
	for i, order := range orders {
		if spec.orders[i].desc {
			query = query.OrderByDesc(order)
		} else {
			query = query.OrderByAsc(order)
		}
	}
	if spec.limit >= 0 {
		query = query.Limit(spec.limit)
	}
	if spec.offset > 0 {
		query = query.Offset(spec.offset)
	}
	if spec.ctx != nil {
		query = query.WithContext(spec.ctx)
	}
	return query, nil
}

// translateItemSpec converts matchers and sort keys into typed RIME
// expressions while holding one read lock on the table binding.
func translateItemSpec[T any](inner *rimeadapter.Table[T], spec *itemQuerySpec) ([]rime.Expr[T], []rime.OrderField[T], error) {
	exprs := make([]rime.Expr[T], 0, len(spec.matchers))
	for _, matcher := range spec.matchers {
		expr, err := translateItemMatcher[T](inner, spec.binding, matcher)
		if err != nil {
			return nil, nil, err
		}
		if expr != nil {
			exprs = append(exprs, expr)
		}
	}
	orders := make([]rime.OrderField[T], 0, len(spec.orders))
	for _, order := range spec.orders {
		field, err := translateItemOrder[T](inner, spec.binding, order.field)
		if err != nil {
			return nil, nil, err
		}
		orders = append(orders, field)
	}
	return exprs, orders, nil
}

// translateItemMatcher converts one matcher into a typed predicate. A nil
// expression means match-all and contributes no predicate.
func translateItemMatcher[T any](inner *rimeadapter.Table[T], binding *itemBinding, matcher q.Matcher) (rime.Expr[T], error) {
	matcher, err := derefItemMatcher(matcher)
	if err != nil {
		return nil, err
	}
	switch m := matcher.(type) {
	case q.StringMatcher:
		field, err := rimeadapter.DynamicFieldOf(inner, binding.logical[m.Field])
		if err != nil {
			return nil, err
		}
		names := []string{"STARTS WITH", "ENDS WITH", "CONTAINS", "LIKE"}
		if m.Op > q.OpLike {
			return nil, fmt.Errorf("murmur: invalid string operator")
		}
		return field.StringMatch(names[m.Op], m.Pattern)
	case q.Comparison:
		return translateItemComparison[T](inner, binding, m)
	case q.InMatcher:
		return translateItemIn[T](inner, binding, m)
	case q.AndMatcher:
		kids := make([]rime.Expr[T], 0, len(m.Matchers))
		for _, child := range m.Matchers {
			expr, err := translateItemMatcher[T](inner, binding, child)
			if err != nil {
				return nil, err
			}
			if expr != nil {
				kids = append(kids, expr)
			}
		}
		switch len(kids) {
		case 0:
			return nil, nil
		case 1:
			return kids[0], nil
		default:
			return rime.And[T](kids...), nil
		}
	case q.OrMatcher:
		if len(m.Matchers) == 0 {
			return rime.False[T](), nil
		}
		kids := make([]rime.Expr[T], 0, len(m.Matchers))
		for _, child := range m.Matchers {
			expr, err := translateItemMatcher[T](inner, binding, child)
			if err != nil {
				return nil, err
			}
			if expr == nil {
				return rime.True[T](), nil
			}
			kids = append(kids, expr)
		}
		if len(kids) == 1 {
			return kids[0], nil
		}
		return rime.Or[T](kids...), nil
	case q.NotMatcher:
		if m.Matcher == nil {
			return nil, fmt.Errorf("murmur: Not requires a matcher")
		}
		expr, err := translateItemMatcher[T](inner, binding, m.Matcher)
		if err != nil {
			return nil, err
		}
		if expr == nil {
			return rime.False[T](), nil
		}
		return rime.Not[T](expr), nil
	default:
		return nil, fmt.Errorf("murmur: unsupported matcher %T", matcher)
	}
}

func translateItemComparison[T any](inner *rimeadapter.Table[T], binding *itemBinding, comparison q.Comparison) (rime.Expr[T], error) {
	fieldType, ok := binding.fields[comparison.Field]
	if !ok {
		return nil, fmt.Errorf("murmur: unknown field %q for %s", comparison.Field, binding.goType)
	}
	if _, ok := comparison.Value.(q.Placeholder); ok {
		return nil, fmt.Errorf("murmur: field %q has an unbound parameter; compile the query and bind a value", comparison.Field)
	}
	logical := binding.logical[comparison.Field]
	switch comparison.Op {
	case q.OpEq, q.OpNe:
		return translateItemEquality[T](inner, logical, comparison.Field, fieldType, comparison.Op == q.OpEq, comparison.Value)
	case q.OpGt, q.OpGte, q.OpLt, q.OpLte:
		return translateItemRange[T](inner, logical, comparison.Field, fieldType, comparison.Op, comparison.Value)
	default:
		return nil, fmt.Errorf("murmur: unsupported operator for field %q", comparison.Field)
	}
}

func translateItemEquality[T any](inner *rimeadapter.Table[T], logical, field string, fieldType reflect.Type, eq bool, value any) (rime.Expr[T], error) {
	coerced, err := coerceItemValue(fieldType, value)
	if err != nil {
		return nil, fmt.Errorf("murmur: field %q: %w", field, err)
	}
	switch fieldType {
	case itemStringType:
		return itemEq[T, string](inner, logical, eq, coerced.Interface().(string)), nil
	case itemBoolType:
		return itemEq[T, bool](inner, logical, eq, coerced.Interface().(bool)), nil
	case reflect.TypeFor[int]():
		return itemEq[T, int](inner, logical, eq, coerced.Interface().(int)), nil
	case reflect.TypeFor[int8]():
		return itemEq[T, int8](inner, logical, eq, coerced.Interface().(int8)), nil
	case reflect.TypeFor[int16]():
		return itemEq[T, int16](inner, logical, eq, coerced.Interface().(int16)), nil
	case reflect.TypeFor[int32]():
		return itemEq[T, int32](inner, logical, eq, coerced.Interface().(int32)), nil
	case reflect.TypeFor[int64]():
		return itemEq[T, int64](inner, logical, eq, coerced.Interface().(int64)), nil
	case reflect.TypeFor[uint]():
		return itemEq[T, uint](inner, logical, eq, coerced.Interface().(uint)), nil
	case reflect.TypeFor[uint8]():
		return itemEq[T, uint8](inner, logical, eq, coerced.Interface().(uint8)), nil
	case reflect.TypeFor[uint16]():
		return itemEq[T, uint16](inner, logical, eq, coerced.Interface().(uint16)), nil
	case reflect.TypeFor[uint32]():
		return itemEq[T, uint32](inner, logical, eq, coerced.Interface().(uint32)), nil
	case reflect.TypeFor[uint64]():
		return itemEq[T, uint64](inner, logical, eq, coerced.Interface().(uint64)), nil
	case reflect.TypeFor[float32]():
		return itemEq[T, float32](inner, logical, eq, coerced.Interface().(float32)), nil
	case reflect.TypeFor[float64]():
		return itemEq[T, float64](inner, logical, eq, coerced.Interface().(float64)), nil
	case itemRowIDType:
		return itemEq[T, ids.RowID](inner, logical, eq, coerced.Interface().(ids.RowID)), nil
	case itemBytesType:
		return itemEq[T, [16]byte](inner, logical, eq, coerced.Interface().([16]byte)), nil
	case itemUUIDType:
		return itemEq[T, rime.UUID](inner, logical, eq, coerced.Interface().(rime.UUID)), nil
	default:
		dynamic, err := rimeadapter.DynamicFieldOf(inner, logical)
		if err != nil {
			return nil, err
		}
		operator := "="
		if !eq {
			operator = "!="
		}
		return dynamic.Compare(operator, coerced.Interface())
	}
}

func translateItemRange[T any](inner *rimeadapter.Table[T], logical, field string, fieldType reflect.Type, op q.Op, value any) (rime.Expr[T], error) {
	coerced, err := coerceItemValue(fieldType, value)
	if err != nil {
		return nil, fmt.Errorf("murmur: field %q: %w", field, err)
	}
	switch fieldType {
	case itemStringType:
		return itemRange[T, string](inner, logical, op, coerced.Interface().(string))
	case reflect.TypeFor[int]():
		return itemRange[T, int](inner, logical, op, coerced.Interface().(int))
	case reflect.TypeFor[int8]():
		return itemRange[T, int8](inner, logical, op, coerced.Interface().(int8))
	case reflect.TypeFor[int16]():
		return itemRange[T, int16](inner, logical, op, coerced.Interface().(int16))
	case reflect.TypeFor[int32]():
		return itemRange[T, int32](inner, logical, op, coerced.Interface().(int32))
	case reflect.TypeFor[int64]():
		return itemRange[T, int64](inner, logical, op, coerced.Interface().(int64))
	case reflect.TypeFor[uint]():
		return itemRange[T, uint](inner, logical, op, coerced.Interface().(uint))
	case reflect.TypeFor[uint8]():
		return itemRange[T, uint8](inner, logical, op, coerced.Interface().(uint8))
	case reflect.TypeFor[uint16]():
		return itemRange[T, uint16](inner, logical, op, coerced.Interface().(uint16))
	case reflect.TypeFor[uint32]():
		return itemRange[T, uint32](inner, logical, op, coerced.Interface().(uint32))
	case reflect.TypeFor[uint64]():
		return itemRange[T, uint64](inner, logical, op, coerced.Interface().(uint64))
	case reflect.TypeFor[float32]():
		return itemRange[T, float32](inner, logical, op, coerced.Interface().(float32))
	case reflect.TypeFor[float64]():
		return itemRange[T, float64](inner, logical, op, coerced.Interface().(float64))
	default:
		dynamic, err := rimeadapter.DynamicFieldOf(inner, logical)
		if err != nil {
			return nil, err
		}
		return dynamic.Compare(op.String(), coerced.Interface())
	}
}

func translateItemIn[T any](inner *rimeadapter.Table[T], binding *itemBinding, in q.InMatcher) (rime.Expr[T], error) {
	fieldType, ok := binding.fields[in.Field]
	if !ok {
		return nil, fmt.Errorf("murmur: unknown field %q for %s", in.Field, binding.goType)
	}
	if len(in.Values) == 0 {
		return rime.False[T](), nil
	}
	coerced := make([]reflect.Value, len(in.Values))
	for i, value := range in.Values {
		if _, ok := value.(q.Placeholder); ok {
			return nil, fmt.Errorf("murmur: field %q has an unbound parameter; compile the query and bind a value", in.Field)
		}
		converted, err := coerceItemValue(fieldType, value)
		if err != nil {
			return nil, fmt.Errorf("murmur: field %q: %w", in.Field, err)
		}
		coerced[i] = converted
	}
	logical := binding.logical[in.Field]
	switch fieldType {
	case itemStringType:
		return itemIn[T, string](inner, logical, coerced), nil
	case itemBoolType:
		return itemIn[T, bool](inner, logical, coerced), nil
	case reflect.TypeFor[int]():
		return itemIn[T, int](inner, logical, coerced), nil
	case reflect.TypeFor[int8]():
		return itemIn[T, int8](inner, logical, coerced), nil
	case reflect.TypeFor[int16]():
		return itemIn[T, int16](inner, logical, coerced), nil
	case reflect.TypeFor[int32]():
		return itemIn[T, int32](inner, logical, coerced), nil
	case reflect.TypeFor[int64]():
		return itemIn[T, int64](inner, logical, coerced), nil
	case reflect.TypeFor[uint]():
		return itemIn[T, uint](inner, logical, coerced), nil
	case reflect.TypeFor[uint8]():
		return itemIn[T, uint8](inner, logical, coerced), nil
	case reflect.TypeFor[uint16]():
		return itemIn[T, uint16](inner, logical, coerced), nil
	case reflect.TypeFor[uint32]():
		return itemIn[T, uint32](inner, logical, coerced), nil
	case reflect.TypeFor[uint64]():
		return itemIn[T, uint64](inner, logical, coerced), nil
	case reflect.TypeFor[float32]():
		return itemIn[T, float32](inner, logical, coerced), nil
	case reflect.TypeFor[float64]():
		return itemIn[T, float64](inner, logical, coerced), nil
	case itemRowIDType:
		return itemIn[T, ids.RowID](inner, logical, coerced), nil
	case itemBytesType:
		return itemIn[T, [16]byte](inner, logical, coerced), nil
	case itemUUIDType:
		return itemIn[T, rime.UUID](inner, logical, coerced), nil
	default:
		dynamic, err := rimeadapter.DynamicFieldOf(inner, logical)
		if err != nil {
			return nil, err
		}
		values := make([]any, len(coerced))
		for i, value := range coerced {
			values[i] = value.Interface()
		}
		return dynamic.In(values...)
	}
}

func translateItemOrder[T any](inner *rimeadapter.Table[T], binding *itemBinding, field string) (rime.OrderField[T], error) {
	fieldType, ok := binding.fields[field]
	if !ok {
		return nil, fmt.Errorf("murmur: unknown field %q for %s", field, binding.goType)
	}
	logical := binding.logical[field]
	switch fieldType {
	case itemStringType:
		return rimeadapter.FieldOf[T, string](inner, logical), nil
	case itemBoolType:
		return rimeadapter.FieldOf[T, bool](inner, logical), nil
	case reflect.TypeFor[int]():
		return rimeadapter.FieldOf[T, int](inner, logical), nil
	case reflect.TypeFor[int8]():
		return rimeadapter.FieldOf[T, int8](inner, logical), nil
	case reflect.TypeFor[int16]():
		return rimeadapter.FieldOf[T, int16](inner, logical), nil
	case reflect.TypeFor[int32]():
		return rimeadapter.FieldOf[T, int32](inner, logical), nil
	case reflect.TypeFor[int64]():
		return rimeadapter.FieldOf[T, int64](inner, logical), nil
	case reflect.TypeFor[uint]():
		return rimeadapter.FieldOf[T, uint](inner, logical), nil
	case reflect.TypeFor[uint8]():
		return rimeadapter.FieldOf[T, uint8](inner, logical), nil
	case reflect.TypeFor[uint16]():
		return rimeadapter.FieldOf[T, uint16](inner, logical), nil
	case reflect.TypeFor[uint32]():
		return rimeadapter.FieldOf[T, uint32](inner, logical), nil
	case reflect.TypeFor[uint64]():
		return rimeadapter.FieldOf[T, uint64](inner, logical), nil
	case reflect.TypeFor[float32]():
		return rimeadapter.FieldOf[T, float32](inner, logical), nil
	case reflect.TypeFor[float64]():
		return rimeadapter.FieldOf[T, float64](inner, logical), nil
	case itemRowIDType:
		return rimeadapter.FieldOf[T, ids.RowID](inner, logical), nil
	case itemBytesType:
		return rimeadapter.FieldOf[T, [16]byte](inner, logical), nil
	case itemUUIDType:
		return rimeadapter.FieldOf[T, rime.UUID](inner, logical), nil
	default:
		return rimeadapter.DynamicFieldOf(inner, logical)
	}
}

// itemEq builds a typed equality predicate over one logical column.
func itemEq[T any, V comparable](inner *rimeadapter.Table[T], logical string, eq bool, value V) rime.Expr[T] {
	field := rimeadapter.FieldOf[T, V](inner, logical)
	if eq {
		return field.Eq(value)
	}
	return field.Ne(value)
}

// itemRange builds a typed range predicate over one logical column.
func itemRange[T any, V rimeadapter.Ordered](inner *rimeadapter.Table[T], logical string, op q.Op, value V) (rime.Expr[T], error) {
	field := rimeadapter.OrderedFieldOf[T, V](inner, logical)
	switch op {
	case q.OpGt:
		return field.Gt(value), nil
	case q.OpGte:
		return field.Ge(value), nil
	case q.OpLt:
		return field.Lt(value), nil
	case q.OpLte:
		return field.Le(value), nil
	default:
		return nil, fmt.Errorf("murmur: unsupported operator")
	}
}

// itemIn builds a typed membership predicate over one logical column.
func itemIn[T any, V comparable](inner *rimeadapter.Table[T], logical string, values []reflect.Value) rime.Expr[T] {
	typed := make([]V, len(values))
	for i, value := range values {
		typed[i] = value.Interface().(V)
	}
	return rimeadapter.FieldOf[T, V](inner, logical).In(typed...)
}

// Explain returns the selected RIME plan without executing the query.
func (iq *ItemQuery) Explain() (string, error) {
	if err := iq.check(); err != nil {
		return "", err
	}
	return iq.binding.ops.explain(iq.db, iq.spec())
}
