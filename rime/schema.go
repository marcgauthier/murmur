package rime

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

// Field tags (rime:"...") are comma-separated directives:
//
//	primary        primary key (exactly one per struct)
//	uuid5          auto-generate deterministic UUIDv5 primary when zero
//	unique         unique hash index
//	index          non-unique hash index
//	ordered        ordered index (range, ORDER BY, MIN/MAX)
//	prefix         prefix/trie index for string fields
//	nullable       legacy scalar zero-value NULL behavior
//	notnull        require an optional field to be present
//	default=X      applied on insert/save when a pointer/Optional field is absent
//	fk=Table.Field (or fk:Table.Field) foreign key reference (enforced when enabled)
//	-              ignore this field
//
// Unexported fields are ignored. Anonymous structs are not traversed.
// keyKind selects kind-specialized index storage so hot paths hash and
// compare native values instead of boxing them into interfaces.
type keyKind uint8

const (
	kAny keyKind = iota
	kString
	kInt64
	kUint64
	kFloat64
	kBool
)

// keyKindOf normalizes a field type to its index storage kind. All int/uint
// widths (including named types) share one kind so queries and stored values
// always meet on the same normalized representation.
func keyKindOf(typ reflect.Type) keyKind {
	switch typ.Kind() {
	case reflect.String:
		return kString
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return kInt64
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return kUint64
	case reflect.Float32, reflect.Float64:
		return kFloat64
	case reflect.Bool:
		return kBool
	}
	return kAny
}

type fieldMeta struct {
	name     string  // logical column name (defaults to Go field name)
	goName   string  // Go struct field name
	index    []int   // struct field index path
	offset   uintptr // byte offset of the field within T (top-level fields)
	typ      reflect.Type
	primary  bool
	uuid5    bool
	unique   bool
	hash     bool
	ordered  bool
	prefix   bool
	nullable bool
	notNull  bool
	optional bool
	hasDef   bool
	defValue string
	fkTable  string
	fkField  string
}

// Schema is the compiled registration metadata for a table. Reflection is
// used once here, at registration; hot paths use compiled accessors.
type Schema struct {
	table     string
	typ       reflect.Type
	fields    []fieldMeta
	byName    map[string]int
	primary   int
	compound  []compoundDef
	cloneable bool
}

// compoundDef describes a compound index registered via WithCompound.
type compoundDef struct {
	name   string
	fields []string
}

func parseTag(tag string) (dirs map[string]string, skip bool) {
	dirs = map[string]string{}
	if tag == "" {
		return dirs, false
	}
	for _, part := range strings.Split(tag, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if part == "-" {
			return nil, true
		}
		if kv := strings.SplitN(part, "=", 2); len(kv) == 2 {
			dirs[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		} else if rest, ok := strings.CutPrefix(part, "fk:"); ok {
			dirs["fk"] = strings.TrimSpace(rest)
		} else {
			dirs[part] = ""
		}
	}
	return dirs, false
}

func buildSchema(table string, typ reflect.Type) (*Schema, error) {
	return buildSchemaPrimary(table, typ, "")
}

func buildSchemaPrimary(table string, typ reflect.Type, primary string) (*Schema, error) {
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: %s is %s, want struct", ErrBadSchema, typ, typ.Kind())
	}
	if primary == "" {
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			dirs, skip := parseTag(f.Tag.Get("rime"))
			if _, ok := dirs["ID"]; ok && !skip && f.PkgPath == "" {
				if primary != "" {
					return nil, fmt.Errorf("%w: multiple ID fields", ErrBadSchema)
				}
				primary = f.Name
			}
		}
	}
	s := &Schema{table: table, typ: typ, byName: map[string]int{}, primary: -1}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.PkgPath != "" { // unexported
			continue
		}
		dirs, skip := parseTag(f.Tag.Get("rime"))
		if skip {
			continue
		}
		fm := fieldMeta{
			name:   f.Name,
			goName: f.Name,
			index:  append([]int(nil), f.Index...),
			offset: f.Offset,
			typ:    f.Type,
		}
		fm.optional = f.Type.Kind() == reflect.Pointer || isOptionalType(f.Type)
		if fm.optional {
			fm.nullable = true
		}
		if n, ok := dirs["name"]; ok && n != "" {
			fm.name = n
		}
		if _, ok := dirs["primary"]; ok {
			fm.primary = true
		}
		if primary != "" {
			if fm.primary && f.Name != primary {
				fm.hash = true
			}
			fm.primary = f.Name == primary
		}
		if _, ok := dirs["uuid5"]; ok {
			fm.uuid5 = true
		}
		if _, ok := dirs["unique"]; ok {
			fm.unique = true
			fm.hash = true
		}
		if _, ok := dirs["index"]; ok {
			fm.hash = true
		}
		if _, ok := dirs["ordered"]; ok {
			fm.ordered = true
		}
		if _, ok := dirs["prefix"]; ok {
			fm.prefix = true
		}
		if _, ok := dirs["nullable"]; ok {
			fm.nullable = true
		}
		if _, ok := dirs["notnull"]; ok {
			fm.notNull = true
		}
		if d, ok := dirs["default"]; ok {
			fm.hasDef = true
			fm.defValue = d
		}
		if fk, ok := dirs["fk"]; ok {
			parts := strings.SplitN(fk, ".", 2)
			if len(parts) == 2 {
				fm.fkTable, fm.fkField = parts[0], parts[1]
			}
		}
		if fm.primary {
			if s.primary >= 0 {
				return nil, fmt.Errorf("%w: multiple primary keys (%s, %s)", ErrBadSchema, s.fields[s.primary].name, fm.name)
			}
			if !isSupportedKeyType(fm.typ) {
				return nil, fmt.Errorf("%w: primary key %s has unsupported type %s", ErrBadSchema, fm.name, fm.typ)
			}
			s.primary = len(s.fields)
		}
		if _, duplicate := s.byName[fm.name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate logical field %s", ErrBadSchema, fm.name)
		}
		s.byName[fm.name] = len(s.fields)
		s.fields = append(s.fields, fm)
	}
	if s.primary < 0 {
		return nil, fmt.Errorf("%w: struct %s", ErrNoPrimaryKey, typ)
	}
	return s, nil
}

func isSupportedKeyType(t reflect.Type) bool {
	if t == reflect.TypeOf(UUID{}) {
		return true
	}
	switch t.Kind() {
	case reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	if t.Kind() == reflect.Array && t.Elem().Kind() == reflect.Uint8 && t.Len() == 16 {
		return true
	}
	return false
}

// getter compiles a fast field reader for records of type T.
func getter[T any](s *Schema, name string) (func(*T) any, reflect.Type, error) {
	fi, ok := s.byName[name]
	if !ok {
		return nil, nil, fmt.Errorf("%w: table %s has no field %q", ErrBadSchema, s.table, name)
	}
	idx := s.fields[fi].index
	if s.fields[fi].typ == reflect.TypeFor[time.Time]() {
		return func(rec *T) any { return canonicalTime(recordStruct(rec).FieldByIndex(idx).Interface().(time.Time)) }, s.fields[fi].typ, nil
	}
	return func(rec *T) any {
		v := recordStruct(rec).FieldByIndex(idx)
		return v.Interface()
	}, s.fields[fi].typ, nil
}

// typedGetter compiles a type-asserted reader; it panics at field-handle
// construction when the requested Go type does not match the struct field.
//
// Exact type matches compile to an unsafe offset load: one pointer add and
// one typed read with no reflection and no allocation. Registration records
// top-level field offsets, so the read is exactly what `rec.Field` lowers to.
// Assignable-but-different types (e.g. named string kinds) keep the slow
// reflective conversion path, used only when the caller requested a converted
// type rather than the field's own type.
func typedGetter[T any, V any](s *Schema, name string) func(*T) V {
	fi, ok := s.byName[name]
	if !ok {
		panic(fmt.Errorf("%w: table %s has no field %q", ErrBadSchema, s.table, name))
	}
	fm := s.fields[fi]
	var zero V
	want := reflect.TypeOf(zero)
	if want == fm.typ && s.typ == reflect.TypeFor[T]() {
		off := fm.offset
		return func(rec *T) V {
			return *(*V)(unsafe.Pointer(uintptr(unsafe.Pointer(rec)) + off))
		}
	}
	if !fm.typ.AssignableTo(want) && !want.AssignableTo(fm.typ) {
		panic(fmt.Errorf("rime: field %s.%s is %s, not %s", s.table, name, fm.typ, want))
	}
	get, _, err := getter[T](s, name)
	if err != nil {
		panic(err)
	}
	return func(rec *T) V {
		v := get(rec)
		if out, ok := v.(V); ok {
			return out
		}
		rv := reflect.ValueOf(v)
		if rv.Type().AssignableTo(want) {
			return rv.Convert(want).Interface().(V)
		}
		var z V
		return z
	}
}

// fieldOffset returns the byte offset of a top-level field when the requested
// Go type exactly matches the field type, so callers can emit direct unsafe
// loads instead of reflective access. ok=false keeps the slow path.
func fieldOffset[T any, V any](s *Schema, name string) (off uintptr, ok bool) {
	fi, found := s.byName[name]
	if !found {
		return 0, false
	}
	fm := s.fields[fi]
	var zero V
	if reflect.TypeOf(zero) != fm.typ || s.typ != reflect.TypeFor[T]() {
		return 0, false
	}
	return fm.offset, true
}

// isZero reports whether v is the zero value for its type.
func isZero(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Interface, reflect.Chan, reflect.Func:
		return rv.IsNil()
	case reflect.Array:
		if rv.Type() == reflect.TypeOf(UUID{}) {
			return v.(UUID).IsZero()
		}
		z := true
		for i := 0; i < rv.Len(); i++ {
			if !rv.Index(i).IsZero() {
				z = false
				break
			}
		}
		return z
	}
	return rv.IsZero()
}

func isAbsent(v any) bool {
	if v == nil {
		return true
	}
	if optional, ok := v.(optionalPresence); ok {
		return !optional.rimePresent()
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Pointer && rv.IsNil()
}

func isNullField(v any, optional, zeroNull bool) bool {
	if optional {
		return isAbsent(v)
	}
	return zeroNull && isZero(v)
}

// applyDefaults fills absent optional fields. Non-optional zero values remain
// present application data and are never replaced with a default.
func applyDefaults[T any](s *Schema, rec *T) {
	rv := recordStruct(rec)
	for _, fm := range s.fields {
		if !fm.hasDef {
			continue
		}
		fv := rv.FieldByIndex(fm.index)
		if !fv.CanSet() || !fm.optional || !isAbsent(fv.Interface()) {
			continue
		}
		if fv.Kind() == reflect.Pointer {
			value := reflect.New(fv.Type().Elem()).Elem()
			if setDefaultValue(value, fm.defValue) {
				ptr := reflect.New(fv.Type().Elem())
				ptr.Elem().Set(value)
				fv.Set(ptr)
			}
			continue
		}
		value := fv.FieldByName("Value")
		present := fv.FieldByName("Present")
		if value.IsValid() && present.IsValid() && present.CanSet() && setDefaultValue(value, fm.defValue) {
			present.SetBool(true)
		}
	}
}

func setDefaultValue(fv reflect.Value, value string) bool {
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(value)
		return true
	case reflect.Bool:
		if b, err := strconv.ParseBool(value); err == nil {
			fv.SetBool(b)
			return true
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if n, err := strconv.ParseInt(value, 10, 64); err == nil && !fv.OverflowInt(n) {
			fv.SetInt(n)
			return true
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if n, err := strconv.ParseUint(value, 10, 64); err == nil && !fv.OverflowUint(n) {
			fv.SetUint(n)
			return true
		}
	case reflect.Float32, reflect.Float64:
		if n, err := strconv.ParseFloat(value, fv.Type().Bits()); err == nil && !fv.OverflowFloat(n) {
			fv.SetFloat(n)
			return true
		}
	}
	return false
}

// equalFuncFor compiles an allocation-free field comparison between two
// records. Scalar kinds compare through typed reflect accessors without
// boxing values into interfaces. Floats treat NaN as equal to NaN to match
// the ordered-index comparator; exotic kinds conservatively report changed
// so index maintenance behaves exactly as before.
func equalFuncFor[T any](fm fieldMeta) func(*T, *T) bool {
	idx := fm.index
	at := func(r *T) reflect.Value { return recordStruct(r).FieldByIndex(idx) }
	switch fm.typ.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(a, b *T) bool { return at(a).Int() == at(b).Int() }
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return func(a, b *T) bool { return at(a).Uint() == at(b).Uint() }
	case reflect.String:
		return func(a, b *T) bool { return at(a).String() == at(b).String() }
	case reflect.Bool:
		return func(a, b *T) bool { return at(a).Bool() == at(b).Bool() }
	case reflect.Float32, reflect.Float64:
		return func(a, b *T) bool {
			af, bf := at(a).Float(), at(b).Float()
			return af == bf || (af != af && bf != bf)
		}
	default:
		return func(a, b *T) bool { return false }
	}
}

// recordStruct unwraps the native record carried by runtime registrations.
func recordStruct(record any) reflect.Value {
	v := reflect.ValueOf(record)
	for v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) {
		v = v.Elem()
	}
	return v
}
