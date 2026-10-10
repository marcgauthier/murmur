package rime

import "reflect"

// Published records are immutable and shared by pointer; RIME never
// serializes, converts, or per-read copies them. Writers operate on private
// copies installed as new versions at commit.

// Cloner is implemented by record types needing custom copy semantics. The
// Clone method must return an independent copy the writer may mutate.
//
// Simple value-only structs need not implement Cloner; RIME copies them with
// a shallow struct copy, which is already fully independent.
type Cloner[T any] interface {
	Clone() *T
}

// CloneRecord returns an independent mutable copy of record using RIME's
// recursive ownership rules. Nil input returns nil. Use it when a published
// pointer returned by a read must be edited or retained as mutable state.
func CloneRecord[T any](record *T) *T {
	if record == nil {
		return nil
	}
	var clone func(*T) *T
	if _, ok := any(record).(Cloner[T]); ok {
		clone = func(src *T) *T { return any(src).(Cloner[T]).Clone() }
	}
	return cloneForUpdate(clone, record)
}

// cloneForUpdate copies src into an independent writable record.
func cloneForUpdate[T any](clone func(*T) *T, src *T) *T {
	if clone != nil {
		return clone(src)
	}
	dst := reflect.New(reflect.TypeFor[T]())
	cloneValue(dst.Elem(), reflect.ValueOf(src).Elem(), make(map[cloneVisit]reflect.Value))
	return dst.Interface().(*T)
}

type cloneVisit struct {
	typ reflect.Type
	ptr uintptr
	len int
	cap int
}

// cloneValue recursively copies the mutable members supported by ordinary Go
// records. Struct assignment preserves unexported fields; exported fields are
// then copied recursively because those are the fields RIME exposes as data.
func cloneValue(dst, src reflect.Value, seen map[cloneVisit]reflect.Value) {
	switch src.Kind() {
	case reflect.Pointer:
		if src.IsNil() {
			dst.SetZero()
			return
		}
		visit := cloneVisit{typ: src.Type(), ptr: src.Pointer()}
		if existing, ok := seen[visit]; ok {
			dst.Set(existing)
			return
		}
		copy := reflect.New(src.Type().Elem())
		seen[visit] = copy
		dst.Set(copy)
		cloneValue(copy.Elem(), src.Elem(), seen)
	case reflect.Interface:
		if src.IsNil() {
			dst.SetZero()
			return
		}
		v := reflect.New(src.Elem().Type()).Elem()
		cloneValue(v, src.Elem(), seen)
		dst.Set(v)
	case reflect.Slice:
		if src.IsNil() {
			dst.SetZero()
			return
		}
		visit := cloneVisit{typ: src.Type(), ptr: src.Pointer(), len: src.Len(), cap: src.Cap()}
		if existing, ok := seen[visit]; ok {
			dst.Set(existing)
			return
		}
		copy := reflect.MakeSlice(src.Type(), src.Len(), src.Len())
		seen[visit] = copy
		dst.Set(copy)
		for i := 0; i < src.Len(); i++ {
			cloneValue(dst.Index(i), src.Index(i), seen)
		}
	case reflect.Map:
		if src.IsNil() {
			dst.SetZero()
			return
		}
		visit := cloneVisit{typ: src.Type(), ptr: src.Pointer()}
		if existing, ok := seen[visit]; ok {
			dst.Set(existing)
			return
		}
		copy := reflect.MakeMapWithSize(src.Type(), src.Len())
		seen[visit] = copy
		dst.Set(copy)
		iter := src.MapRange()
		for iter.Next() {
			k := iter.Key() // map keys retain Go's original equality identity
			v := reflect.New(src.Type().Elem()).Elem()
			cloneValue(v, iter.Value(), seen)
			dst.SetMapIndex(k, v)
		}
	case reflect.Array:
		for i := 0; i < src.Len(); i++ {
			cloneValue(dst.Index(i), src.Index(i), seen)
		}
	case reflect.Struct:
		dst.Set(src)
		for i := 0; i < src.NumField(); i++ {
			if dst.Type().Field(i).PkgPath == "" {
				cloneValue(dst.Field(i), src.Field(i), seen)
			}
		}
	default:
		dst.Set(src)
	}
}
