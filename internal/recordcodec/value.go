package recordcodec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"
)

const RecordValueVersion byte = 1

var (
	ErrValue         = errors.New("recordcodec: invalid record value")
	ErrValueTooLarge = errors.New("recordcodec: value byte limit exceeded")
)

// Limits bounds both encoding and hostile decoding. Zero fields select defaults.
type Limits struct {
	MaxBytes    uint64
	MaxElements uint64
	MaxDepth    int
}

func (l Limits) normalized() Limits {
	if l.MaxBytes == 0 {
		l.MaxBytes = 16 << 20
	}
	if l.MaxElements == 0 {
		l.MaxElements = 1 << 20
	}
	if l.MaxDepth <= 0 {
		l.MaxDepth = 64
	}
	return l
}

// UnknownField preserves an unrecognized struct field byte-for-byte. Path names
// enclosing stable field IDs; Coordinates identify collection elements between
// those fields.
type UnknownField struct {
	ID      uint32
	Payload []byte
	// Path contains the stable IDs of enclosing record fields for an unknown
	// field inside a nested struct. Nil preserves the original top-level form.
	Path        []uint32
	Coordinates []CollectionCoordinate
}

// CollectionCoordinate identifies one array/slice element or map value while
// retaining an unknown nested field. Map keys use their canonical encoded key.
type CollectionCoordinate struct {
	Kind  byte
	Index uint64
	Key   []byte
}

const (
	coordinateIndex    byte = 1
	coordinateMapValue byte = 2
)

type CustomCodec struct {
	Identity CodecIdentity
	Encode   func(any) ([]byte, error)
	Decode   func([]byte, any) error
	Clone    func(any) (any, error)
	Equal    func(any, any) bool
}

// RegisterCodec adds executable deterministic codec operations for one exact Go type.
func (r *CodecRegistry) RegisterCodec(id string, version uint16, example any, encode func(any) ([]byte, error), decode func([]byte, any) error, clone func(any) (any, error), equal func(any, any) bool) error {
	if encode == nil || decode == nil || clone == nil || equal == nil {
		return fmt.Errorf("%w: encode, decode, clone and equal are required", ErrCodec)
	}
	if err := r.Register(id, version, example); err != nil {
		return err
	}
	if r.codecs == nil {
		r.codecs = make(map[reflect.Type]CustomCodec)
	}
	identity := r.byType[reflect.TypeOf(example)]
	r.codecs[identity.Type] = CustomCodec{identity, encode, decode, clone, equal}
	return nil
}

func Encode(schema *Schema, value any, unknown []UnknownField, registry *CodecRegistry, limits Limits) ([]byte, error) {
	if schema == nil || value == nil {
		return nil, fmt.Errorf("%w: schema and value required", ErrValue)
	}
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, fmt.Errorf("%w: nil record", ErrValue)
		}
		v = v.Elem()
	}
	if v.Type() != schema.Record.GoType {
		return nil, fmt.Errorf("%w: got %s want %s", ErrValue, v.Type(), schema.Record.GoType)
	}
	l := limits.normalized()
	fields := make(map[uint32][]byte, len(schema.Fields)+len(unknown))
	for _, f := range schema.Fields {
		p, e := encodeValue(f.Descriptor, fieldValue(v, f.GoName), registry, l, 0, make(map[visit]bool), []uint32{f.ID}, nil, unknown)
		if e != nil {
			return nil, fmt.Errorf("%s: %w", f.Path, e)
		}
		fields[f.ID] = p
	}
	for _, u := range unknown {
		if u.ID == 0 {
			return nil, fmt.Errorf("%w: zero unknown field ID", ErrValue)
		}
		if len(u.Path) != 0 {
			continue
		}
		if _, ok := fields[u.ID]; ok {
			continue
		}
		fields[u.ID] = append([]byte(nil), u.Payload...)
	}
	ids := make([]uint32, 0, len(fields))
	for id := range fields {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := []byte{'R', 'G', 'V', RecordValueVersion}
	out = appendUvarint(out, uint64(len(ids)))
	for _, id := range ids {
		out = appendUvarint(out, uint64(id))
		out = appendUvarint(out, uint64(len(fields[id])))
		out = append(out, fields[id]...)
	}
	if uint64(len(out)) > l.MaxBytes {
		return nil, fmt.Errorf("%w: %w", ErrValue, ErrValueTooLarge)
	}
	return out, nil
}

// EncodeField encodes one known top-level field using its stable descriptor.
// Murmur uses this boundary to persist and replicate per-field deltas.
func EncodeField(schema *Schema, fieldID uint32, value any, registry *CodecRegistry, limits Limits) ([]byte, error) {
	return EncodeFieldWithUnknown(schema, fieldID, value, nil, registry, limits)
}

// EncodeFieldWithUnknown encodes one top-level field while retaining unknown
// nested struct entries decoded from the same authoritative field payload.
func EncodeFieldWithUnknown(schema *Schema, fieldID uint32, value any, unknown []UnknownField, registry *CodecRegistry, limits Limits) ([]byte, error) {
	if schema == nil || value == nil {
		return nil, fmt.Errorf("%w: schema and value required", ErrValue)
	}
	f, ok := schema.FieldByID(fieldID)
	if !ok {
		return nil, fmt.Errorf("%w: unknown field id %d", ErrValue, fieldID)
	}
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, fmt.Errorf("%w: nil record", ErrValue)
		}
		v = v.Elem()
	}
	if v.Type() != schema.Record.GoType {
		return nil, fmt.Errorf("%w: got %s want %s", ErrValue, v.Type(), schema.Record.GoType)
	}
	l := limits.normalized()
	p, err := encodeValue(f.Descriptor, fieldValue(v, f.GoName), registry, l, 0, make(map[visit]bool), []uint32{f.ID}, nil, unknown)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", f.Path, err)
	}
	if uint64(len(p)) > l.MaxBytes {
		return nil, fmt.Errorf("%w: %w", ErrValue, ErrValueTooLarge)
	}
	return p, nil
}

// DecodeFieldWithUnknown decodes one persisted top-level field and returns
// opaque nested struct entries that are absent from schema's runtime type.
func DecodeFieldWithUnknown(schema *Schema, fieldID uint32, data []byte, registry *CodecRegistry, limits Limits) (any, []UnknownField, error) {
	if schema == nil || schema.Record == nil {
		return nil, nil, fmt.Errorf("%w: schema required", ErrValue)
	}
	f, ok := schema.FieldByID(fieldID)
	if !ok {
		return nil, nil, fmt.Errorf("%w: unknown field id %d", ErrValue, fieldID)
	}
	l := limits.normalized()
	if uint64(len(data)) > l.MaxBytes {
		return nil, nil, fmt.Errorf("%w: %w", ErrValue, ErrValueTooLarge)
	}
	root := reflect.New(schema.Record.GoType).Elem()
	var unknown []UnknownField
	if err := decodeValue(f.Descriptor, data, root.FieldByName(f.GoName), registry, l, 0, []uint32{f.ID}, nil, &unknown); err != nil {
		return nil, nil, fmt.Errorf("%s: %w", f.Path, err)
	}
	return root.FieldByName(f.GoName).Interface(), unknown, nil
}

// CloneRecord returns an owned copy of a typed record and delegates registered
// custom values to their Clone hooks.
func CloneRecord(schema *Schema, value any, registry *CodecRegistry) (any, error) {
	if schema == nil || schema.Record == nil || value == nil {
		return nil, fmt.Errorf("%w: schema and value required", ErrValue)
	}
	src := reflect.ValueOf(value)
	if src.Kind() == reflect.Pointer {
		if src.IsNil() {
			return nil, fmt.Errorf("%w: nil record", ErrValue)
		}
		src = src.Elem()
	}
	if src.Type() != schema.Record.GoType {
		return nil, fmt.Errorf("%w: got %s want %s", ErrValue, src.Type(), schema.Record.GoType)
	}
	dst := reflect.New(src.Type()).Elem()
	if err := cloneValue(schema.Record, src, dst, registry, make(map[visit]bool)); err != nil {
		return nil, err
	}
	return dst.Interface(), nil
}

// EqualField compares one field using registered custom Equal hooks and the
// canonical value semantics for built-in fields.
func EqualField(schema *Schema, fieldID uint32, left, right any, registry *CodecRegistry) (bool, error) {
	if schema == nil || schema.Record == nil || left == nil || right == nil {
		return false, fmt.Errorf("%w: schema and values required", ErrValue)
	}
	lf, rf := reflect.ValueOf(left), reflect.ValueOf(right)
	if lf.Kind() == reflect.Pointer {
		if lf.IsNil() {
			return false, fmt.Errorf("%w: nil left record", ErrValue)
		}
		lf = lf.Elem()
	}
	if rf.Kind() == reflect.Pointer {
		if rf.IsNil() {
			return false, fmt.Errorf("%w: nil right record", ErrValue)
		}
		rf = rf.Elem()
	}
	if lf.Type() != schema.Record.GoType || rf.Type() != schema.Record.GoType {
		return false, fmt.Errorf("%w: record type mismatch", ErrValue)
	}
	f, ok := schema.FieldByID(fieldID)
	if !ok {
		return false, fmt.Errorf("%w: unknown field id %d", ErrValue, fieldID)
	}
	return equalValue(f.Descriptor, fieldValue(lf, f.GoName), fieldValue(rf, f.GoName), registry)
}

func cloneValue(d *Descriptor, src, dst reflect.Value, registry *CodecRegistry, seen map[visit]bool) error {
	if !src.IsValid() || !dst.IsValid() || src.Type() != dst.Type() {
		return ErrValue
	}
	if d.Kind == KindCustom {
		if registry == nil {
			return ErrCodec
		}
		c, ok := registry.codecs[d.GoType]
		if !ok {
			return ErrCodec
		}
		copied, err := c.Clone(src.Interface())
		if err != nil {
			return err
		}
		value := reflect.ValueOf(copied)
		if !value.IsValid() || !value.Type().AssignableTo(dst.Type()) {
			return fmt.Errorf("%w: custom clone returned %T for %s", ErrCodec, copied, dst.Type())
		}
		dst.Set(value)
		return nil
	}
	if d.Kind == KindOptional {
		if src.Kind() == reflect.Pointer {
			if src.IsNil() {
				dst.SetZero()
				return nil
			}
			key := visit{typ: src.Type(), ptr: src.Pointer()}
			if seen[key] {
				return fmt.Errorf("%w: cyclic pointer graph", ErrValue)
			}
			seen[key] = true
			defer delete(seen, key)
			value := reflect.New(src.Type().Elem())
			if err := cloneValue(d.Element, src.Elem(), value.Elem(), registry, seen); err != nil {
				return err
			}
			dst.Set(value)
			return nil
		}
		present := src.FieldByName("Present")
		dst.Set(src)
		if present.IsValid() && present.Kind() == reflect.Bool && present.Bool() {
			return cloneValue(d.Element, src.FieldByName("Value"), dst.FieldByName("Value"), registry, seen)
		}
		return nil
	}
	switch d.Kind {
	case KindStruct:
		dst.Set(src)
		for _, f := range d.Fields {
			if err := cloneValue(f.Descriptor, src.FieldByName(f.GoName), dst.FieldByName(f.GoName), registry, seen); err != nil {
				return err
			}
		}
	case KindArray:
		dst.Set(src)
		if src.Type().Elem().Kind() != reflect.Uint8 {
			for i := 0; i < src.Len(); i++ {
				if err := cloneValue(d.Element, src.Index(i), dst.Index(i), registry, seen); err != nil {
					return err
				}
			}
		}
	case KindSlice:
		if src.IsNil() {
			dst.SetZero()
			return nil
		}
		out := reflect.MakeSlice(src.Type(), src.Len(), src.Len())
		for i := 0; i < src.Len(); i++ {
			if err := cloneValue(d.Element, src.Index(i), out.Index(i), registry, seen); err != nil {
				return err
			}
		}
		dst.Set(out)
	case KindBytes:
		if src.IsNil() {
			dst.SetZero()
		} else {
			dst.SetBytes(append([]byte(nil), src.Bytes()...))
		}
	case KindMap:
		if src.IsNil() {
			dst.SetZero()
			return nil
		}
		out := reflect.MakeMapWithSize(src.Type(), src.Len())
		iter := src.MapRange()
		for iter.Next() {
			key := reflect.New(src.Type().Key()).Elem()
			if err := cloneValue(d.Key, iter.Key(), key, registry, seen); err != nil {
				return err
			}
			value := reflect.New(src.Type().Elem()).Elem()
			if err := cloneValue(d.Value, iter.Value(), value, registry, seen); err != nil {
				return err
			}
			out.SetMapIndex(key, value)
		}
		dst.Set(out)
	default:
		dst.Set(src)
	}
	return nil
}

func equalValue(d *Descriptor, left, right reflect.Value, registry *CodecRegistry) (bool, error) {
	if d.Kind == KindCustom {
		if registry == nil {
			return false, ErrCodec
		}
		c, ok := registry.codecs[d.GoType]
		if !ok {
			return false, ErrCodec
		}
		return c.Equal(left.Interface(), right.Interface()), nil
	}
	if d.Kind == KindOptional {
		if left.Kind() == reflect.Pointer {
			if left.IsNil() || right.IsNil() {
				return left.IsNil() == right.IsNil(), nil
			}
			return equalValue(d.Element, left.Elem(), right.Elem(), registry)
		}
		lp, rp := left.FieldByName("Present"), right.FieldByName("Present")
		if lp.Bool() != rp.Bool() {
			return false, nil
		}
		if !lp.Bool() {
			return true, nil
		}
		return equalValue(d.Element, left.FieldByName("Value"), right.FieldByName("Value"), registry)
	}
	switch d.Kind {
	case KindStruct:
		for _, f := range d.Fields {
			equal, err := equalValue(f.Descriptor, left.FieldByName(f.GoName), right.FieldByName(f.GoName), registry)
			if err != nil || !equal {
				return equal, err
			}
		}
		return true, nil
	case KindArray, KindSlice:
		if left.Len() != right.Len() || (d.Kind == KindSlice && (left.IsNil() != right.IsNil())) {
			return false, nil
		}
		if d.Kind == KindArray && left.Type().Elem().Kind() == reflect.Uint8 {
			return reflect.DeepEqual(left.Interface(), right.Interface()), nil
		}
		for i := 0; i < left.Len(); i++ {
			equal, err := equalValue(d.Element, left.Index(i), right.Index(i), registry)
			if err != nil || !equal {
				return equal, err
			}
		}
		return true, nil
	case KindMap:
		if left.Len() != right.Len() || left.IsNil() != right.IsNil() {
			return false, nil
		}
		iter := left.MapRange()
		for iter.Next() {
			rv := right.MapIndex(iter.Key())
			if !rv.IsValid() {
				return false, nil
			}
			equal, err := equalValue(d.Value, iter.Value(), rv, registry)
			if err != nil || !equal {
				return equal, err
			}
		}
		return true, nil
	case KindInstant:
		return left.Interface().(time.Time).Equal(right.Interface().(time.Time)), nil
	case KindFloat32:
		return math.Float32bits(float32(left.Float())) == math.Float32bits(float32(right.Float())), nil
	case KindFloat64:
		return math.Float64bits(left.Float()) == math.Float64bits(right.Float()), nil
	default:
		return reflect.DeepEqual(left.Interface(), right.Interface()), nil
	}
}

// DecodeFields constructs a fresh typed value from stable field IDs. Missing
// fields retain Go zero values, while Optional fields remain absent.
func DecodeFields(schema *Schema, fields map[uint32][]byte, registry *CodecRegistry, limits Limits) (any, error) {
	if schema == nil || schema.Record == nil {
		return nil, fmt.Errorf("%w: schema required", ErrValue)
	}
	l := limits.normalized()
	root := reflect.New(schema.Record.GoType).Elem()
	for id, p := range fields {
		f, ok := schema.FieldByID(id)
		if !ok {
			continue
		}
		if uint64(len(p)) > l.MaxBytes {
			return nil, fmt.Errorf("%w: %w", ErrValue, ErrValueTooLarge)
		}
		if err := decodeValue(f.Descriptor, p, root.FieldByName(f.GoName), registry, l, 0, []uint32{f.ID}, nil, nil); err != nil {
			return nil, fmt.Errorf("%s: %w", f.Path, err)
		}
	}
	return root.Interface(), nil
}

// Decode fills a fresh record value and returns unknown fields for preservation.
func Decode(schema *Schema, data []byte, registry *CodecRegistry, limits Limits) (any, []UnknownField, error) {
	if schema == nil || schema.Record == nil {
		return nil, nil, fmt.Errorf("%w: schema required", ErrValue)
	}
	l := limits.normalized()
	if uint64(len(data)) > l.MaxBytes {
		return nil, nil, fmt.Errorf("%w: %w", ErrValue, ErrValueTooLarge)
	}
	if len(data) < 4 || !bytes.Equal(data[:3], []byte("RGV")) || data[3] != RecordValueVersion {
		return nil, nil, fmt.Errorf("%w: unsupported header/version", ErrValue)
	}
	r := valueReader{b: data[4:], limits: l}
	n, e := r.count()
	if e != nil {
		return nil, nil, e
	}
	if n > uint64(len(schema.Fields))+l.MaxElements {
		return nil, nil, fmt.Errorf("%w: field count limit", ErrValue)
	}
	root := reflect.New(schema.Record.GoType).Elem()
	known := make(map[uint32]Field, len(schema.Fields))
	for _, f := range schema.Fields {
		known[f.ID] = f
	}
	unknown := []UnknownField{}
	var prev uint64
	for i := uint64(0); i < n; i++ {
		id, e := r.count()
		if e != nil {
			return nil, nil, e
		}
		if id == 0 || id <= prev || id > math.MaxUint32 {
			return nil, nil, fmt.Errorf("%w: unordered/invalid field id", ErrValue)
		}
		prev = id
		size, e := r.count()
		if e != nil {
			return nil, nil, e
		}
		p, e := r.take(size)
		if e != nil {
			return nil, nil, e
		}
		f, ok := known[uint32(id)]
		if !ok {
			unknown = append(unknown, UnknownField{ID: uint32(id), Payload: append([]byte(nil), p...)})
			continue
		}
		dst := root.FieldByName(f.GoName)
		if e = decodeValue(f.Descriptor, p, dst, registry, l, 0, []uint32{f.ID}, nil, &unknown); e != nil {
			return nil, nil, fmt.Errorf("%s: %w", f.Path, e)
		}
	}
	if r.off != len(r.b) {
		return nil, nil, fmt.Errorf("%w: trailing bytes", ErrValue)
	}
	return root.Interface(), unknown, nil
}

type visit struct {
	typ reflect.Type
	ptr uintptr
}

func appendPath(path []uint32, id uint32) []uint32 {
	n := make([]uint32, len(path)+1)
	copy(n, path)
	n[len(path)] = id
	return n
}
func equalIDs(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func dFieldByID(d *Descriptor, id uint32) (Field, bool) {
	for _, f := range d.Fields {
		if f.ID == id {
			return f, true
		}
	}
	return Field{}, false
}

// sortStructWire canonicalizes a just-built nested struct payload, including
// preserved unknown entries, and rejects duplicate IDs rather than emitting
// ambiguous wire data.
func sortStructWire(data []byte, l Limits) ([]byte, error) {
	type entry struct {
		id uint64
		p  []byte
	}
	r := valueReader{b: data, limits: l}
	es := []entry{}
	for r.off < len(r.b) {
		id, e := r.count()
		if e != nil || id == 0 || id > math.MaxUint32 {
			return nil, ErrValue
		}
		p, e := r.blob()
		if e != nil {
			return nil, e
		}
		es = append(es, entry{id, append([]byte(nil), p...)})
	}
	sort.Slice(es, func(i, j int) bool { return es[i].id < es[j].id })
	out := make([]byte, 0, len(data))
	var prev uint64
	for _, e := range es {
		if e.id == prev {
			return nil, fmt.Errorf("%w: duplicate nested field id", ErrValue)
		}
		prev = e.id
		out = appendUvarint(out, e.id)
		out = appendBlob(out, e.p)
	}
	return out, nil
}

func fieldValue(v reflect.Value, name string) reflect.Value { return v.FieldByName(name) }
func appendUvarint(b []byte, n uint64) []byte               { return binary.AppendUvarint(b, n) }
func canonicalUvarint(data []byte) (uint64, int, bool) {
	n, k := binary.Uvarint(data)
	if k <= 0 {
		return 0, k, false
	}
	return n, k, bytes.Equal(binary.AppendUvarint(nil, n), data[:k])
}
func canonicalVarint(data []byte) (int64, int, bool) {
	n, k := binary.Varint(data)
	if k <= 0 {
		return 0, k, false
	}
	return n, k, bytes.Equal(binary.AppendVarint(nil, n), data[:k])
}
func appendBlob(b []byte, p []byte) []byte {
	b = appendUvarint(b, uint64(len(p)))
	return append(b, p...)
}

func encodeValue(d *Descriptor, v reflect.Value, reg *CodecRegistry, l Limits, depth int, seen map[visit]bool, path []uint32, coordinates []CollectionCoordinate, unknown []UnknownField) ([]byte, error) {
	if depth > l.MaxDepth {
		return nil, fmt.Errorf("%w: depth limit", ErrValue)
	}
	var current visit
	tracked := v.IsValid() && (v.Kind() == reflect.Pointer || v.Kind() == reflect.Slice) && !v.IsNil()
	if tracked {
		current = visit{typ: v.Type(), ptr: v.Pointer()}
		if seen[current] {
			return nil, fmt.Errorf("%w: cyclic value graph", ErrValue)
		}
		seen[current] = true
		defer delete(seen, current)
	}
	if d.Kind == KindOptional {
		present := true
		if d.GoType.Kind() == reflect.Pointer {
			present = !v.IsNil()
		} else {
			present = v.FieldByName("Present").Bool()
			v = v.FieldByName("Value")
		}
		if !present {
			return []byte{0}, nil
		}
		p, e := encodeValue(d.Element, deref(v), reg, l, depth+1, seen, path, coordinates, unknown)
		if e != nil {
			return nil, e
		}
		return append([]byte{1}, p...), nil
	}
	if d.Kind == KindCustom {
		if reg == nil {
			return nil, ErrCodec
		}
		c, ok := reg.codecs[d.GoType]
		if !ok {
			return nil, ErrCodec
		}
		p, e := c.Encode(v.Interface())
		if e != nil {
			return nil, e
		}
		if uint64(len(p)) > l.MaxBytes {
			return nil, ErrValue
		}
		return appendBlob(nil, p), nil
	}
	if d.Kind == KindInstant {
		t := v.Interface().(time.Time).UTC()
		b := binary.AppendVarint(nil, t.Unix())
		return binary.LittleEndian.AppendUint32(b, uint32(t.Nanosecond())), nil
	}
	switch d.Kind {
	case KindBool:
		if v.Bool() {
			return []byte{1}, nil
		}
		return []byte{0}, nil
	case KindInt:
		return binary.AppendVarint(nil, v.Int()), nil
	case KindUint:
		return appendUvarint(nil, v.Uint()), nil
	case KindFloat32:
		return binary.LittleEndian.AppendUint32(nil, math.Float32bits(float32(v.Float()))), nil
	case KindFloat64:
		return binary.LittleEndian.AppendUint64(nil, math.Float64bits(v.Float())), nil
	case KindString:
		return appendBlob(nil, []byte(v.String())), nil
	case KindBytes:
		if v.IsNil() {
			return []byte{0}, nil
		}
		return appendBlob([]byte{1}, v.Bytes()), nil
	case KindArray:
		b := []byte{}
		if d.GoType.Elem().Kind() == reflect.Uint8 {
			for i := 0; i < v.Len(); i++ {
				b = append(b, byte(v.Index(i).Uint()))
			}
			return b, nil
		}
		for i := 0; i < v.Len(); i++ {
			p, e := encodeValue(d.Element, v.Index(i), reg, l, depth+1, seen, path, appendIndex(coordinates, uint64(i)), unknown)
			if e != nil {
				return nil, e
			}
			b = appendBlob(b, p)
		}
		return b, nil
	case KindStruct:
		b := []byte{}
		fields := append([]Field(nil), d.Fields...)
		sort.Slice(fields, func(i, j int) bool { return fields[i].ID < fields[j].ID })
		for _, f := range fields {
			childPath := appendPath(path, f.ID)
			p, e := encodeValue(f.Descriptor, v.FieldByName(f.GoName), reg, l, depth+1, seen, childPath, coordinates, unknown)
			if e != nil {
				return nil, e
			}
			b = appendUvarint(b, uint64(f.ID))
			b = appendBlob(b, p)
		}
		for _, u := range unknown {
			if len(u.Path) != len(path) || !equalIDs(u.Path, path) || !equalCoordinates(u.Coordinates, coordinates) {
				continue
			}
			if _, ok := dFieldByID(d, u.ID); ok {
				continue
			}
			b = appendUvarint(b, uint64(u.ID))
			b = appendBlob(b, u.Payload)
		}
		// Unknown and known fields share one ID-ordered namespace. Sort the
		// encoded entries below to keep the nested representation canonical.
		b, err := sortStructWire(b, l)
		if err != nil {
			return nil, err
		}
		return b, nil
	case KindSlice:
		if v.IsNil() {
			return []byte{0}, nil
		}
		if uint64(v.Len()) > l.MaxElements {
			return nil, ErrValue
		}
		b := appendUvarint([]byte{1}, uint64(v.Len()))
		for i := 0; i < v.Len(); i++ {
			p, e := encodeValue(d.Element, v.Index(i), reg, l, depth+1, seen, path, appendIndex(coordinates, uint64(i)), unknown)
			if e != nil {
				return nil, e
			}
			b = appendBlob(b, p)
		}
		return b, nil
	case KindMap:
		if v.IsNil() {
			return []byte{0}, nil
		}
		if uint64(v.Len()) > l.MaxElements {
			return nil, ErrValue
		}
		type pair struct{ k, v []byte }
		pairs := make([]pair, 0, v.Len())
		it := v.MapRange()
		for it.Next() {
			k, e := encodeValue(d.Key, it.Key(), reg, l, depth+1, seen, path, coordinates, unknown)
			if e != nil {
				return nil, e
			}
			val, e := encodeValue(d.Value, it.Value(), reg, l, depth+1, seen, path, appendMapKey(coordinates, k), unknown)
			if e != nil {
				return nil, e
			}
			pairs = append(pairs, pair{k, val})
		}
		sort.Slice(pairs, func(i, j int) bool { return bytes.Compare(pairs[i].k, pairs[j].k) < 0 })
		b := appendUvarint([]byte{1}, uint64(len(pairs)))
		for _, p := range pairs {
			b = appendBlob(b, p.k)
			b = appendBlob(b, p.v)
		}
		return b, nil
	}
	return nil, ErrValue
}

func appendIndex(in []CollectionCoordinate, index uint64) []CollectionCoordinate {
	out := append([]CollectionCoordinate(nil), in...)
	out = append(out, CollectionCoordinate{Kind: coordinateIndex, Index: index})
	return out
}

func appendMapKey(in []CollectionCoordinate, key []byte) []CollectionCoordinate {
	out := append([]CollectionCoordinate(nil), in...)
	out = append(out, CollectionCoordinate{Kind: coordinateMapValue, Key: append([]byte(nil), key...)})
	return out
}

func equalCoordinates(a, b []CollectionCoordinate) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Kind != b[i].Kind || a[i].Index != b[i].Index || !bytes.Equal(a[i].Key, b[i].Key) {
			return false
		}
	}
	return true
}

func cloneCoordinates(in []CollectionCoordinate) []CollectionCoordinate {
	if len(in) == 0 {
		return nil
	}
	out := make([]CollectionCoordinate, len(in))
	for i, coordinate := range in {
		out[i] = coordinate
		out[i].Key = append([]byte(nil), coordinate.Key...)
	}
	return out
}
func deref(v reflect.Value) reflect.Value {
	for v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	return v
}

type valueReader struct {
	b        []byte
	off      int
	limits   Limits
	elements uint64
}

func (r *valueReader) count() (uint64, error) {
	n, k, canonical := canonicalUvarint(r.b[r.off:])
	if !canonical {
		return 0, fmt.Errorf("%w: malformed length", ErrValue)
	}
	r.off += k
	return n, nil
}
func (r *valueReader) take(n uint64) ([]byte, error) {
	if n > r.limits.MaxBytes || n > uint64(len(r.b)-r.off) {
		return nil, fmt.Errorf("%w: truncated/oversized value", ErrValue)
	}
	p := r.b[r.off : r.off+int(n)]
	r.off += int(n)
	return p, nil
}
func (r *valueReader) blob() ([]byte, error) {
	n, e := r.count()
	if e != nil {
		return nil, e
	}
	return r.take(n)
}

func decodeValue(d *Descriptor, data []byte, dst reflect.Value, reg *CodecRegistry, l Limits, depth int, path []uint32, coordinates []CollectionCoordinate, unknown *[]UnknownField) error {
	if depth > l.MaxDepth {
		return fmt.Errorf("%w: depth limit", ErrValue)
	}
	r := valueReader{b: data, limits: l}
	finish := func(e error) error {
		if e == nil && r.off != len(r.b) {
			return fmt.Errorf("%w: trailing field bytes", ErrValue)
		}
		return e
	}
	if d.Kind == KindOptional {
		if len(data) < 1 {
			return ErrValue
		}
		if data[0] == 0 {
			r.off = 1 // consume the absent presence tag before checking trailing bytes
			return finish(nil)
		}
		if data[0] != 1 {
			return ErrValue
		}
		if d.GoType.Kind() == reflect.Pointer {
			dst.Set(reflect.New(d.GoType.Elem()))
			return decodeValue(d.Element, data[1:], dst.Elem(), reg, l, depth+1, path, coordinates, unknown)
		}
		dst.FieldByName("Present").SetBool(true)
		return decodeValue(d.Element, data[1:], dst.FieldByName("Value"), reg, l, depth+1, path, coordinates, unknown)
	}
	if d.Kind == KindCustom {
		p, e := r.blob()
		if e != nil {
			return e
		}
		if reg == nil {
			return ErrCodec
		}
		c, ok := reg.codecs[d.GoType]
		if !ok {
			return ErrCodec
		}
		if e = c.Decode(p, dst.Addr().Interface()); e != nil {
			return e
		}
		return finish(nil)
	}
	if d.Kind == KindInstant {
		sec, k, canonical := canonicalVarint(data)
		if !canonical || len(data)-k != 4 {
			return ErrValue
		}
		nsec := binary.LittleEndian.Uint32(data[k:])
		if nsec >= 1_000_000_000 {
			return ErrValue
		}
		dst.Set(reflect.ValueOf(time.Unix(sec, int64(nsec)).UTC()))
		return nil
	}
	switch d.Kind {
	case KindBool:
		if len(data) != 1 || data[0] > 1 {
			return ErrValue
		}
		dst.SetBool(data[0] == 1)
		r.off = len(r.b)
	case KindInt:
		n, k, canonical := canonicalVarint(data)
		if !canonical || k != len(data) || dst.OverflowInt(n) {
			return ErrValue
		}
		dst.SetInt(n)
		r.off = len(r.b)
	case KindUint:
		n, k, canonical := canonicalUvarint(data)
		if !canonical || k != len(data) || dst.OverflowUint(n) {
			return ErrValue
		}
		dst.SetUint(n)
		r.off = len(r.b)
	case KindFloat32:
		if len(data) != 4 {
			return ErrValue
		}
		dst.SetFloat(float64(math.Float32frombits(binary.LittleEndian.Uint32(data))))
		r.off = len(r.b)
	case KindFloat64:
		if len(data) != 8 {
			return ErrValue
		}
		dst.SetFloat(math.Float64frombits(binary.LittleEndian.Uint64(data)))
		r.off = len(r.b)
	case KindString:
		p, e := r.blob()
		if e != nil {
			return e
		}
		dst.SetString(string(p))
	case KindBytes:
		if len(data) < 1 {
			return ErrValue
		}
		if data[0] == 0 {
			r.off = 1 // consume the nil/empty presence tag before checking trailing bytes
			return finish(nil)
		}
		if data[0] != 1 {
			return ErrValue
		}
		r.off = 1
		p, e := r.blob()
		if e != nil {
			return e
		}
		buf := make([]byte, len(p))
		copy(buf, p)
		dst.SetBytes(buf)
	case KindArray:
		if d.GoType.Elem().Kind() == reflect.Uint8 {
			if len(data) != dst.Len() {
				return ErrValue
			}
			for i, b := range data {
				dst.Index(i).SetUint(uint64(b))
			}
			r.off = len(r.b)
		} else {
			for i := 0; i < dst.Len(); i++ {
				p, e := r.blob()
				if e != nil {
					return e
				}
				if e = decodeValue(d.Element, p, dst.Index(i), reg, l, depth+1, path, appendIndex(coordinates, uint64(i)), unknown); e != nil {
					return e
				}
			}
		}
	case KindStruct:
		byID := map[uint32]Field{}
		for _, f := range d.Fields {
			byID[f.ID] = f
		}
		var prev uint64
		var fieldCount uint64
		for r.off < len(r.b) {
			fieldCount++
			if fieldCount > l.MaxElements {
				return fmt.Errorf("%w: nested field count limit", ErrValue)
			}
			id, e := r.count()
			if e != nil {
				return e
			}
			if id == 0 || id <= prev || id > math.MaxUint32 {
				return ErrValue
			}
			prev = id
			p, e := r.blob()
			if e != nil {
				return e
			}
			f, ok := byID[uint32(id)]
			if !ok {
				if unknown != nil {
					*unknown = append(*unknown, UnknownField{ID: uint32(id), Payload: append([]byte(nil), p...), Path: append([]uint32(nil), path...), Coordinates: cloneCoordinates(coordinates)})
				}
				continue
			}
			if e = decodeValue(f.Descriptor, p, dst.FieldByName(f.GoName), reg, l, depth+1, appendPath(path, f.ID), coordinates, unknown); e != nil {
				return e
			}
		}
		r.off = len(r.b)
	case KindSlice:
		if len(data) < 1 {
			return ErrValue
		}
		if data[0] == 0 {
			return finish(nil)
		}
		if data[0] != 1 {
			return ErrValue
		}
		r.off = 1
		n, e := r.count()
		if e != nil {
			return e
		}
		if n > l.MaxElements {
			return ErrValue
		}
		dst.Set(reflect.MakeSlice(d.GoType, int(n), int(n)))
		for i := 0; i < int(n); i++ {
			p, e := r.blob()
			if e != nil {
				return e
			}
			if e = decodeValue(d.Element, p, dst.Index(i), reg, l, depth+1, path, appendIndex(coordinates, uint64(i)), unknown); e != nil {
				return e
			}
		}
	case KindMap:
		if len(data) < 1 {
			return ErrValue
		}
		if data[0] == 0 {
			return finish(nil)
		}
		if data[0] != 1 {
			return ErrValue
		}
		r.off = 1
		n, e := r.count()
		if e != nil {
			return e
		}
		if n > l.MaxElements {
			return ErrValue
		}
		dst.Set(reflect.MakeMapWithSize(d.GoType, int(n)))
		var previous []byte
		for i := uint64(0); i < n; i++ {
			kp, e := r.blob()
			if e != nil {
				return e
			}
			if i > 0 && bytes.Compare(previous, kp) >= 0 {
				return ErrValue
			}
			previous = append(previous[:0], kp...)
			vp, e := r.blob()
			if e != nil {
				return e
			}
			k := reflect.New(d.GoType.Key()).Elem()
			v := reflect.New(d.GoType.Elem()).Elem()
			if e = decodeValue(d.Key, kp, k, reg, l, depth+1, path, coordinates, unknown); e != nil {
				return e
			}
			if e = decodeValue(d.Value, vp, v, reg, l, depth+1, path, appendMapKey(coordinates, kp), unknown); e != nil {
				return e
			}
			dst.SetMapIndex(k, v)
		}
	default:
		return ErrValue
	}
	return finish(nil)
}
