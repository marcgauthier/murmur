package rime

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"
)

// DynamicField is a checked runtime field for scalar and timestamp predicates.
// Values must have the registered Go type. Typed Field constructors retain
// their existing exact type checks.
type DynamicField[T any] struct {
	table    *Table[T]
	name     string
	typ      reflect.Type
	get      func(*T) any
	cmp      func(any, any) int
	nullable bool
}

// DynamicFieldOf builds a runtime field without panicking on invalid schemas.
// Supported fields are strings, bools, numeric scalars, 16-byte identities,
// and time.Time, including named scalar types.
func DynamicFieldOf[T any](table *Table[T], name string) (DynamicField[T], error) {
	var field DynamicField[T]
	if table == nil {
		return field, fmt.Errorf("%w: nil table", ErrBadSchema)
	}
	get, typ, err := getter[T](table.sch, name)
	if err != nil {
		return field, err
	}
	nullable := typ.Kind() == reflect.Pointer
	if nullable {
		original := get
		get = func(rec *T) any {
			value := reflect.ValueOf(original(rec))
			for value.Kind() == reflect.Pointer {
				if value.IsNil() {
					return nil
				}
				value = value.Elem()
			}
			return normalizeIndexValue(value.Interface())
		}
		for typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
	}
	cmp, err := comparatorFor(typ)
	if typ.Kind() == reflect.Bool {
		cmp = func(a, b any) int {
			av, bv := reflect.ValueOf(a).Bool(), reflect.ValueOf(b).Bool()
			if av == bv {
				return 0
			}
			if !av {
				return -1
			}
			return 1
		}
		err = nil
	}
	if typ.Kind() == reflect.Array && typ.Len() == 16 && typ.Elem().Kind() == reflect.Uint8 {
		cmp = func(a, b any) int {
			var av, bv [16]byte
			ar, br := reflect.ValueOf(a), reflect.ValueOf(b)
			for i := 0; i < 16; i++ {
				av[i], bv[i] = byte(ar.Index(i).Uint()), byte(br.Index(i).Uint())
			}
			return bytes.Compare(av[:], bv[:])
		}
		err = nil
	}
	if err != nil {
		return field, fmt.Errorf("%w: unsupported dynamic field %s.%s (%s)", ErrBadSchema, table.name, name, typ)
	}
	baseCompare := cmp
	cmp = func(a, b any) int {
		if a == nil {
			if b == nil {
				return 0
			}
			return -1
		}
		if b == nil {
			return 1
		}
		return baseCompare(a, b)
	}
	return DynamicField[T]{table: table, name: name, typ: typ, get: get, cmp: cmp, nullable: nullable}, nil
}

func canonicalTime(t time.Time) time.Time { return t.Round(0).UTC() }

// normalizeIndexValue keeps timestamp keys equal across zones and monotonic
// clocks without altering the application record or persisted encoding.
func normalizeIndexValue(value any) any {
	if timestamp, ok := value.(time.Time); ok {
		return canonicalTime(timestamp)
	}
	return value
}

func (f DynamicField[T]) operand(value any) (any, error) {
	if value != nil {
		reflected := reflect.ValueOf(value)
		for reflected.Kind() == reflect.Pointer {
			if reflected.IsNil() {
				value = nil
				break
			}
			reflected = reflected.Elem()
		}
		if value != nil {
			value = reflected.Interface()
		}
	}
	if f.nullable && value == nil {
		return nil, nil
	}
	if f.get == nil || reflect.TypeOf(value) != f.typ {
		return nil, fmt.Errorf("%w: field %s requires %s, got %T", ErrBadSchema, f.name, f.typ, value)
	}
	return normalizeIndexValue(value), nil
}

// Compare constructs =, !=, >, >=, <, or <= predicates. Bool and identities
// support equality only; ordering of those fields is available for sorting.
func (f DynamicField[T]) Compare(operator string, value any) (Expr[T], error) {
	value, err := f.operand(value)
	if err != nil {
		return nil, err
	}
	var op predOp
	switch operator {
	case "=":
		op = opEq
	case "!=":
		op = opNe
	case ">":
		op = opGt
	case ">=":
		op = opGe
	case "<":
		op = opLt
	case "<=":
		op = opLe
	default:
		return nil, fmt.Errorf("%w: invalid comparison %q", ErrBadSchema, operator)
	}
	if op != opEq && op != opNe && (f.typ.Kind() == reflect.Bool || f.typ.Kind() == reflect.Array) {
		return nil, fmt.Errorf("%w: field %s supports equality only", ErrBadSchema, f.name)
	}
	fetch := func(rec any) any { return f.get(rec.(*T)) }
	fn := func(rec *T) bool {
		current := f.get(rec)
		if current == nil || value == nil {
			if op == opEq {
				return current == nil && value == nil
			}
			if op == opNe {
				return (current == nil) != (value == nil)
			}
			return false
		}
		if op == opEq {
			return current == value
		}
		if op == opNe {
			return current != value
		}
		if f.typ.Kind() == reflect.Float32 || f.typ.Kind() == reflect.Float64 {
			if math.IsNaN(reflect.ValueOf(current).Float()) || math.IsNaN(reflect.ValueOf(value).Float()) {
				return false
			}
		}
		c := f.cmp(current, value)
		switch op {
		case opGt:
			return c > 0
		case opGe:
			return c >= 0
		case opLt:
			return c < 0
		default:
			return c <= 0
		}
	}
	predicate := &predInfo{field: f.name, op: op, vals: []any{value}, fetch: fetch}
	if f.nullable {
		predicate = nil
	} // Pointer indexes cannot identify dereferenced values.
	return &expr[T]{k: kPred, fn: fn, desc: f.name + " " + op.String() + " " + quote(value), p: predicate}, nil
}

// In matches any of the supplied values, all of the registered Go type.
func (f DynamicField[T]) In(values ...any) (Expr[T], error) {
	operands := make([]any, len(values))
	for i, value := range values {
		var err error
		operands[i], err = f.operand(value)
		if err != nil {
			return nil, err
		}
	}
	if f.get == nil {
		return nil, fmt.Errorf("%w: uninitialized dynamic field", ErrBadSchema)
	}
	if f.nullable {
		return &expr[T]{k: kPred, desc: f.name + " IN", fn: func(rec *T) bool {
			actual := f.get(rec)
			for _, value := range operands {
				if actual == value {
					return true
				}
			}
			return false
		}}, nil
	}
	return newPredMulti(Field[T, any]{tbl: f.table, name: f.name, get: f.get}, opIn, operands), nil
}

// StringMatch constructs STARTS WITH, ENDS WITH, CONTAINS, or LIKE predicates.
// LIKE uses the existing byte-oriented % and _ matching rules.
func (f DynamicField[T]) StringMatch(operator, pattern string) (Expr[T], error) {
	if f.typ == nil || f.typ.Kind() != reflect.String {
		return nil, fmt.Errorf("%w: field %s is not a string", ErrBadSchema, f.name)
	}
	if f.nullable || operator == "EQUAL FOLD" || operator == "CONTAINS FOLD" {
		match := func(rec *T) bool {
			actual := f.get(rec)
			if actual == nil {
				return false
			}
			value := reflect.ValueOf(actual).String()
			switch operator {
			case "EQUAL FOLD":
				return strings.EqualFold(value, pattern)
			case "CONTAINS FOLD":
				return strings.Contains(strings.ToLower(value), strings.ToLower(pattern))
			case "STARTS WITH":
				return strings.HasPrefix(value, pattern)
			case "ENDS WITH":
				return strings.HasSuffix(value, pattern)
			case "CONTAINS":
				return strings.Contains(value, pattern)
			case "LIKE":
				return likeMatch(value, pattern)
			}
			return false
		}
		switch operator {
		case "EQUAL FOLD", "CONTAINS FOLD", "STARTS WITH", "ENDS WITH", "CONTAINS", "LIKE":
		default:
			return nil, fmt.Errorf("%w: invalid string comparison %q", ErrBadSchema, operator)
		}
		return &expr[T]{k: kPred, fn: match, desc: f.name + " " + operator + " " + quote(pattern)}, nil
	}
	field := StringField[T]{Field: Field[T, string]{tbl: f.table, name: f.name, get: func(rec *T) string { return reflect.ValueOf(f.get(rec)).String() }}}
	switch operator {
	case "STARTS WITH":
		return field.StartsWith(pattern), nil
	case "ENDS WITH":
		return field.EndsWith(pattern), nil
	case "CONTAINS":
		return field.Contains(pattern), nil
	case "LIKE":
		return field.Like(pattern), nil
	}
	return nil, fmt.Errorf("%w: invalid string comparison %q", ErrBadSchema, operator)
}

func (f DynamicField[T]) orderName() string      { return f.name }
func (f DynamicField[T]) orderCmp(a, b *T) int   { return f.cmp(f.get(a), f.get(b)) }
func (f DynamicField[T]) orderLess(a, b *T) bool { return f.orderCmp(a, b) < 0 }

// Null matches absence without conflating a present zero value with NULL.
func (f DynamicField[T]) Null(not bool) Expr[T] {
	return &expr[T]{k: kPred, desc: f.name + " NULL", fn: func(rec *T) bool {
		absent := f.nullable && f.get(rec) == nil
		if not {
			return !absent
		}
		return absent
	}}
}

// FoldOrder returns a native case-insensitive text sort field.
func (f DynamicField[T]) FoldOrder() (DynamicField[T], error) {
	if f.typ.Kind() != reflect.String {
		return f, fmt.Errorf("%w: field %s is not text", ErrBadSchema, f.name)
	}
	f.name += " (fold)"
	f.cmp = func(a, b any) int {
		if a == nil {
			if b == nil {
				return 0
			}
			return -1
		}
		if b == nil {
			return 1
		}
		return strings.Compare(strings.ToLower(reflect.ValueOf(a).String()), strings.ToLower(reflect.ValueOf(b).String()))
	}
	return f, nil
}
