// Package q provides Storm-style untyped query matchers for Murmur's
// generic item API. Matchers describe predicates over struct field names;
// Murmur translates them into typed RIME predicates so equality and range
// queries keep using indexes.
package q

import (
	"fmt"
	"strings"
	"sync/atomic"
)

// Op identifies a leaf comparison operator.
type Op int

const (
	// OpEq matches records where the field equals the value.
	OpEq Op = iota
	// OpNe matches records where the field differs from the value.
	OpNe
	// OpGt matches records where the field is greater than the value.
	OpGt
	// OpGte matches records where the field is greater than or equal.
	OpGte
	// OpLt matches records where the field is less than the value.
	OpLt
	// OpLte matches records where the field is less than or equal.
	OpLte
)

// String renders the operator symbol for debugging.
func (o Op) String() string {
	switch o {
	case OpEq:
		return "="
	case OpNe:
		return "!="
	case OpGt:
		return ">"
	case OpGte:
		return ">="
	case OpLt:
		return "<"
	case OpLte:
		return "<="
	}
	return "?"
}

// Matcher is a Storm-style predicate over struct field names. Build values
// with comparisons, membership, string matchers, And, Or, and Not; concrete types are
// exported so query bridges can inspect them.
type Matcher interface {
	isMatcher()
}

// Comparison matches one field against one value.
type Comparison struct {
	Field string
	Op    Op
	Value any
}

func (Comparison) isMatcher() {}

// InMatcher matches records where the field equals any listed value.
type InMatcher struct {
	Field  string
	Values []any
}

func (InMatcher) isMatcher() {}

// AndMatcher matches records satisfying every child matcher.
type AndMatcher struct {
	Matchers []Matcher
}

func (AndMatcher) isMatcher() {}

// OrMatcher matches records satisfying at least one child matcher.
type OrMatcher struct {
	Matchers []Matcher
}

func (OrMatcher) isMatcher() {}

// NotMatcher negates one child matcher.
type NotMatcher struct {
	Matcher Matcher
}

func (NotMatcher) isMatcher() {}

// Eq matches records where field equals value.
func Eq(field string, value any) Matcher { return Comparison{Field: field, Op: OpEq, Value: value} }

// Ne matches records where field differs from value.
func Ne(field string, value any) Matcher { return Comparison{Field: field, Op: OpNe, Value: value} }

// Gt matches records where field is greater than value.
func Gt(field string, value any) Matcher { return Comparison{Field: field, Op: OpGt, Value: value} }

// Gte matches records where field is greater than or equal to value.
func Gte(field string, value any) Matcher { return Comparison{Field: field, Op: OpGte, Value: value} }

// Lt matches records where field is less than value.
func Lt(field string, value any) Matcher { return Comparison{Field: field, Op: OpLt, Value: value} }

// Lte matches records where field is less than or equal to value.
func Lte(field string, value any) Matcher { return Comparison{Field: field, Op: OpLte, Value: value} }

// In matches records where field equals any of values.
func In(field string, values ...any) Matcher {
	return InMatcher{Field: field, Values: append([]any(nil), values...)}
}

// NotIn matches fields equal to none of the listed values.
func NotIn(field string, values ...any) Matcher { return Not(In(field, values...)) }

// Between matches an inclusive range. Reversed bounds match no rows.
func Between(field string, lo, hi any) Matcher {
	return And(Gte(field, lo), Lte(field, hi))
}

// StringMatcher describes a string predicate.
type StringMatcher struct {
	Field, Pattern string
	Op             StringOp
}

func (StringMatcher) isMatcher() {}

// StringOp identifies a string predicate.
type StringOp uint8

const (
	OpStartsWith StringOp = iota
	OpEndsWith
	OpContains
	OpLike
)

// StartsWith matches a string prefix and can use a prefix index.
func StartsWith(field, prefix string) Matcher { return StringMatcher{field, prefix, OpStartsWith} }

// EndsWith matches a string suffix.
func EndsWith(field, suffix string) Matcher { return StringMatcher{field, suffix, OpEndsWith} }

// Contains matches a substring.
func Contains(field, substring string) Matcher { return StringMatcher{field, substring, OpContains} }

// Like matches RIME's byte-oriented % and _ wildcard patterns.
func Like(field, pattern string) Matcher { return StringMatcher{field, pattern, OpLike} }

// And combines matchers conjunctively. Zero children match everything.
func And(filters ...Matcher) Matcher {
	return AndMatcher{Matchers: append([]Matcher(nil), filters...)}
}

// Or combines matchers disjunctively. Zero children match nothing.
func Or(filters ...Matcher) Matcher {
	return OrMatcher{Matchers: append([]Matcher(nil), filters...)}
}

// Not negates one matcher.
func Not(filter Matcher) Matcher { return NotMatcher{Matcher: filter} }

// AggOp identifies an aggregation.
type AggOp uint8

const (
	// OpCount tallies matching rows.
	OpCount AggOp = iota
	// OpSum totals a numeric field.
	OpSum
	// OpAvg averages a numeric field.
	OpAvg
	// OpMin takes the minimum of an ordered field.
	OpMin
	// OpMax takes the maximum of an ordered field.
	OpMax
)

// Aggregate describes one aggregation over a struct field. Count ignores
// Field; every other operation requires one.
type Aggregate struct {
	Op    AggOp
	Field string
}

// String renders the aggregation name for debugging.
func (o AggOp) String() string {
	switch o {
	case OpCount:
		return "COUNT"
	case OpSum:
		return "SUM"
	case OpAvg:
		return "AVG"
	case OpMin:
		return "MIN"
	case OpMax:
		return "MAX"
	}
	return "?"
}

// Count tallies matching rows.
func Count() Aggregate { return Aggregate{Op: OpCount} }

// Sum totals a numeric field.
func Sum(field string) Aggregate { return Aggregate{Op: OpSum, Field: field} }

// Avg averages a numeric field.
func Avg(field string) Aggregate { return Aggregate{Op: OpAvg, Field: field} }

// Min takes the minimum of an ordered field.
func Min(field string) Aggregate { return Aggregate{Op: OpMin, Field: field} }

// Max takes the maximum of an ordered field.
func Max(field string) Aggregate { return Aggregate{Op: OpMax, Field: field} }

// Placeholder is a positional placeholder for compiled item queries. Use it
// as a comparison, membership, or range value, then bind concrete arguments
// per execution. Create placeholders with Param; the zero value is not valid.
type Placeholder struct {
	seq int64
}

var paramSeq atomic.Int64

// Param returns a distinct placeholder for one bind slot. Reusing the same
// value in several positions binds one argument to every position.
func Param() Placeholder { return Placeholder{seq: paramSeq.Add(1)} }

// String renders the placeholder for Describe and error messages.
func (p Placeholder) String() string { return "?" }

// Describe renders a matcher for error messages and debugging.
func Describe(m Matcher) string {
	switch t := m.(type) {
	case nil:
		return "<nil>"
	case StringMatcher:
		names := []string{"STARTS WITH", "ENDS WITH", "CONTAINS", "LIKE"}
		if int(t.Op) >= len(names) {
			return fmt.Sprintf("%s INVALID STRING OP", t.Field)
		}
		return t.Field + " " + names[t.Op] + " " + fmt.Sprintf("%q", t.Pattern)
	case Comparison:
		return t.Field + " " + t.Op.String() + " " + fmt.Sprintf("%v", t.Value)
	case InMatcher:
		parts := make([]string, len(t.Values))
		for i, v := range t.Values {
			parts[i] = fmt.Sprintf("%v", v)
		}
		return t.Field + " IN (" + strings.Join(parts, ", ") + ")"
	case AndMatcher:
		parts := make([]string, len(t.Matchers))
		for i, c := range t.Matchers {
			parts[i] = Describe(c)
		}
		return "(" + strings.Join(parts, " AND ") + ")"
	case OrMatcher:
		parts := make([]string, len(t.Matchers))
		for i, c := range t.Matchers {
			parts[i] = Describe(c)
		}
		return "(" + strings.Join(parts, " OR ") + ")"
	case NotMatcher:
		return "NOT (" + Describe(t.Matcher) + ")"
	default:
		return fmt.Sprintf("%T", m)
	}
}
