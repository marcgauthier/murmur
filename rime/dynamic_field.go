package rime

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"time"
)

// DynamicField is a checked runtime field for scalar and timestamp predicates.
// Values must have the registered Go type. Typed Field constructors retain
// their existing exact type checks.
type DynamicField[T any] struct {
	table *Table[T]
	name  string
	typ   reflect.Type
	get   func(*T) any
	cmp   func(any, any) int
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
	return DynamicField[T]{table, name, typ, get, cmp}, nil
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
	return &expr[T]{k: kPred, fn: fn, desc: f.name + " " + op.String() + " " + quote(value), p: &predInfo{field: f.name, op: op, vals: []any{value}, fetch: fetch}}, nil
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
	return newPredMulti(Field[T, any]{tbl: f.table, name: f.name, get: f.get}, opIn, operands), nil
}

// StringMatch constructs STARTS WITH, ENDS WITH, CONTAINS, or LIKE predicates.
// LIKE uses the existing byte-oriented % and _ matching rules.
func (f DynamicField[T]) StringMatch(operator, pattern string) (Expr[T], error) {
	if f.typ == nil || f.typ.Kind() != reflect.String {
		return nil, fmt.Errorf("%w: field %s is not a string", ErrBadSchema, f.name)
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
