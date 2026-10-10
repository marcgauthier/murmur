package rime

import (
	"fmt"
	"reflect"
	"time"
)

// OrderField exposes ordering for ORDER BY. Implemented by all field handles.
type OrderField[T any] interface {
	orderName() string
	orderLess(a, b *T) bool
	orderCmp(a, b *T) int
}

// GroupField exposes grouping keys for GROUP BY. Implemented by all handles.
type GroupField[T any] interface {
	groupName() string
	groupKey(rec *T) any
}

// Field is a typed comparable column handle with equality operators.
// Construct with F; invalid comparisons fail to compile.
type Field[T any, V comparable] struct {
	tbl      *Table[T]
	name     string
	get      func(*T) V
	nullable bool
	presence bool
}

// F returns a typed field handle for column name. It panics when the column
// does not exist or its Go type differs.
func F[T any, V comparable](t *Table[T], name string) Field[T, V] {
	fi, ok := t.sch.byName[name]
	if !ok {
		panic(errNoField(t.name, name))
	}
	_ = fi
	fm := t.sch.fields[fi]
	return Field[T, V]{tbl: t, name: name, get: typedGetter[T, V](t.sch, name), nullable: fm.nullable, presence: fm.optional}
}

// FieldOf is a readable alias for F. T and V are inferred from type arguments
// and the table; use it when the field's exact comparable type is known.
func FieldOf[T any, V comparable](t *Table[T], name string) Field[T, V] { return F[T, V](t, name) }

// FieldFrom creates a typed field with a direct Go accessor. It panics if the
// registered field name or type does not match.
func FieldFrom[T any, V comparable](t *Table[T], name string, get func(*T) V) Field[T, V] {
	fi, ok := t.sch.byName[name]
	if !ok {
		panic(errNoField(t.name, name))
	}
	if t.sch.fields[fi].typ != reflect.TypeFor[V]() {
		panic(fmt.Sprintf("rime: field %s.%s has type %s, accessor has type %s", t.name, name, t.sch.fields[fi].typ, reflect.TypeFor[V]()))
	}
	fm := t.sch.fields[fi]
	return Field[T, V]{tbl: t, name: name, get: get, nullable: fm.nullable, presence: fm.optional}
}

// StringFieldOf constructs a typed string field; T is inferred from the table.
func StringFieldOf[T any](t *Table[T], name string) StringField[T] { return SF(t, name) }

// StringFieldFrom constructs a string field with a direct accessor.
func StringFieldFrom[T any](t *Table[T], name string, get func(*T) string) StringField[T] {
	return StringField[T]{Field: FieldFrom(t, name, get)}
}

// OrderedFieldOf constructs an ordered field. Specify V only when Go cannot
// infer it from a surrounding typed value.
func OrderedFieldOf[V ordered, T any](t *Table[T], name string) OrderedField[T, V] {
	return OF[T, V](t, name)
}

// OrderedFieldFrom constructs an ordered field with a direct accessor.
func OrderedFieldFrom[T any, V ordered](t *Table[T], name string, get func(*T) V) OrderedField[T, V] {
	return OrderedField[T, V]{Field: FieldFrom(t, name, get), ord: cmpFor[V]()}
}

// NumericFieldOf constructs a numeric ordered field for aggregate helpers.
func NumericFieldOf[V Number, T any](t *Table[T], name string) OrderedField[T, V] {
	return OF[T, V](t, name)
}

// NumericFieldFrom constructs a numeric ordered field with a direct accessor.
func NumericFieldFrom[T any, V Number](t *Table[T], name string, get func(*T) V) OrderedField[T, V] {
	return OrderedFieldFrom(t, name, get)
}

// BoolFieldOf constructs a typed bool field; T is inferred from the table.
func BoolFieldOf[T any](t *Table[T], name string) BoolField[T] { return BF(t, name) }

// BoolFieldFrom constructs a bool field with a direct accessor.
func BoolFieldFrom[T any](t *Table[T], name string, get func(*T) bool) BoolField[T] {
	return BoolField[T]{Field: FieldFrom(t, name, get)}
}

// Name returns the column name.
func (f Field[T, V]) Name() string { return f.name }

// Eq matches records where the field equals v.
func (f Field[T, V]) Eq(v V) Expr[T] { return newPred(f, opEq, v) }

// Ne matches records where the field differs from v.
func (f Field[T, V]) Ne(v V) Expr[T] { return newPred(f, opNe, v) }

// In matches records where the field equals any listed value.
func (f Field[T, V]) In(vs ...V) Expr[T] { return newPredMulti(f, opIn, vs) }

// NotIn matches records where the field equals none of the listed values.
func (f Field[T, V]) NotIn(vs ...V) Expr[T] { return newPredMulti(f, opNotIn, vs) }

// IsNull matches records holding the zero value (nullable fields).
func (f Field[T, V]) IsNull() Expr[T] { return newNullPred(f, true) }

// IsNotNull matches records holding a non-zero value.
func (f Field[T, V]) IsNotNull() Expr[T] { return newNullPred(f, false) }

func (f Field[T, V]) orderName() string { return f.name }
func (f Field[T, V]) orderLess(a, b *T) bool {
	return f.orderCmp(a, b) < 0
}
func (f Field[T, V]) orderCmp(a, b *T) int {
	av, bv := f.get(a), f.get(b)
	if av == bv {
		return 0
	}
	if timestamp, ok := any(av).(time.Time); ok {
		return canonicalTime(timestamp).Compare(canonicalTime(any(bv).(time.Time)))
	}
	if lessAny(av, bv) {
		return -1
	}
	return 1
}
func (f Field[T, V]) groupName() string   { return f.name }
func (f Field[T, V]) groupKey(rec *T) any { return any(f.get(rec)) }
func (f Field[T, V]) anyGet(rec *T) any   { return any(f.get(rec)) }
func (f Field[T, V]) fieldName() string   { return f.name }

// KeyField abstracts field handles usable as join keys. It is implemented by
// Field, OrderedField (via promotion), StringField, and BoolField.
type KeyField[T any, K comparable] interface {
	keyOf(*T) K
	keyName() string
}

func (f Field[T, V]) keyOf(rec *T) V  { return f.get(rec) }
func (f Field[T, V]) keyName() string { return f.name }

// OrderedField is a handle for ordered columns with range operators.
type OrderedField[T any, V ordered] struct {
	Field[T, V]
	ord func(a, b V) int
}

// OF returns a typed ordered-field handle. It panics on unknown columns or
// type mismatch.
func OF[T any, V ordered](t *Table[T], name string) OrderedField[T, V] {
	base := F[T, V](t, name)
	return OrderedField[T, V]{Field: base, ord: cmpFor[V]()}
}

// Gt matches records where field > v. Analogous: Ge, Lt, Le.
func (f OrderedField[T, V]) Gt(v V) Expr[T] { return newPred(f.Field, opGt, v) }

// Ge matches records where field >= v.
func (f OrderedField[T, V]) Ge(v V) Expr[T] { return newPred(f.Field, opGe, v) }

// Lt matches records where field < v.
func (f OrderedField[T, V]) Lt(v V) Expr[T] { return newPred(f.Field, opLt, v) }

// Le matches records where field <= v.
func (f OrderedField[T, V]) Le(v V) Expr[T] { return newPred(f.Field, opLe, v) }

// Between matches records with lo <= field <= hi.
func (f OrderedField[T, V]) Between(lo, hi V) Expr[T] {
	return newRangePred(f.Field, opBetween, any(lo), any(hi), false, false)
}

func (f OrderedField[T, V]) orderCmp(a, b *T) int { return f.ord(f.get(a), f.get(b)) }
func (f OrderedField[T, V]) orderLess(a, b *T) bool {
	return f.ord(f.get(a), f.get(b)) < 0
}

// StringField is a handle for string columns with match operators.
type StringField[T any] struct {
	Field[T, string]
}

// SF returns a typed string-field handle.
func SF[T any](t *Table[T], name string) StringField[T] {
	return StringField[T]{Field: F[T, string](t, name)}
}

// StartsWith matches strings with the given prefix (prefix-index aware).
func (f StringField[T]) StartsWith(p string) Expr[T] {
	return newPred(f.Field, opStartsWith, p)
}

// EndsWith matches strings with the given suffix (filtered scan).
func (f StringField[T]) EndsWith(s string) Expr[T] { return newPred(f.Field, opEndsWith, s) }

// Contains matches strings containing the substring (filtered scan).
func (f StringField[T]) Contains(s string) Expr[T] { return newPred(f.Field, opContains, s) }

// Like matches wildcard patterns (% any run, _ one character). A trailing
// "literal%" shape uses the prefix index when one exists.
func (f StringField[T]) Like(pat string) Expr[T] { return newLikePred(f.Field, pat) }

// BoolField is a handle for bool columns.
type BoolField[T any] struct {
	Field[T, bool]
}

// BF returns a typed bool-field handle.
func BF[T any](t *Table[T], name string) BoolField[T] {
	return BoolField[T]{Field: F[T, bool](t, name)}
}
