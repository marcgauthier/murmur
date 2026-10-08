package rime

import "reflect"

// Optional represents a field that may be absent independently of its Go
// value. A present zero value is distinct from None[T]().
type Optional[T any] struct {
	Value   T
	Present bool
}

// Some returns a present value, including a Go zero value.
func Some[T any](value T) Optional[T] { return Optional[T]{Value: value, Present: true} }

// None returns an absent value.
func None[T any]() Optional[T] { return Optional[T]{} }

// Get returns the value and whether it is present.
func (o Optional[T]) Get() (T, bool) { return o.Value, o.Present }

// IsPresent reports whether the optional contains a value.
func (o Optional[T]) IsPresent() bool { return o.Present }

func (o Optional[T]) rimePresent() bool { return o.Present }

type optionalPresence interface{ rimePresent() bool }

func isOptionalType(typ reflect.Type) bool {
	v := reflect.New(typ).Elem()
	return v.CanInterface() && implementsOptional(v.Interface())
}

func implementsOptional(v any) bool {
	_, ok := v.(optionalPresence)
	return ok
}
