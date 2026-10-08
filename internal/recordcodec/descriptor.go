// Package recordcodec compiles runtime Go types into stable-ID descriptors for
// Murmur's canonical record-value encoding. RIME itself remains independent of
// persistence and encoding.
package recordcodec

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Kind identifies the canonical wire family for a Go value.
type Kind uint8

const (
	KindInvalid Kind = iota
	KindBool
	KindInt
	KindUint
	KindFloat32
	KindFloat64
	KindString
	KindBytes
	KindArray
	KindStruct
	KindSlice
	KindMap
	KindOptional
	KindInstant
	KindCustom
)

type MergePolicy string

const (
	MergeLWW     MergePolicy = "lww"
	MergeMin     MergePolicy = "min"
	MergeMax     MergePolicy = "max"
	MergeCounter MergePolicy = "counter"
	MergeORSet   MergePolicy = "orset"
)

var (
	ErrDescriptor = errors.New("recordcodec: invalid descriptor")
	ErrCodec      = errors.New("recordcodec: invalid custom codec")
)

// CodecIdentity binds an application type to a deterministic custom codec.
// The codec implementation itself is registered by the Murmur facade.
type CodecIdentity struct {
	ID      string
	Version uint16
	Type    reflect.Type
}

// CodecRegistry stores custom codec identities by their exact Go type.
type CodecRegistry struct {
	byType map[reflect.Type]CodecIdentity
	codecs map[reflect.Type]CustomCodec
}

func NewCodecRegistry() *CodecRegistry {
	return &CodecRegistry{byType: make(map[reflect.Type]CodecIdentity)}
}

// Register adds a custom codec identity. IDs are stable application protocol
// names, not Go type names.
func (r *CodecRegistry) Register(id string, version uint16, example any) error {
	if r == nil || id == "" || version == 0 || example == nil {
		return fmt.Errorf("%w: id, version and example are required", ErrCodec)
	}
	typ := reflect.TypeOf(example)
	if _, exists := r.byType[typ]; exists {
		return fmt.Errorf("%w: type %s already registered", ErrCodec, typ)
	}
	for _, entry := range r.byType {
		if entry.ID == id && entry.Version == version {
			return fmt.Errorf("%w: identity %q version %d already registered", ErrCodec, id, version)
		}
	}
	r.byType[typ] = CodecIdentity{ID: id, Version: version, Type: typ}
	return nil
}

// Field is one exported struct member with an explicit stable ID.
type Field struct {
	ID         uint32
	GoName     string
	Path       string
	Merge      MergePolicy
	Descriptor *Descriptor
}

// Descriptor describes one runtime value shape. GoType and field names are
// local bindings; only Kind, IDs, lengths, and custom codec identities belong
// in canonical metadata.
type Descriptor struct {
	Kind         Kind
	GoType       reflect.Type
	Length       int
	Element      *Descriptor
	Key          *Descriptor
	Value        *Descriptor
	Fields       []Field
	CodecID      string
	CodecVersion uint16
}

// Schema describes one replicated record table. Field IDs are explicit and
// independent of declaration order or table names.
type Schema struct {
	TableID   uint32
	PrimaryID uint32
	Record    *Descriptor
	Fields    []Field
	byID      map[uint32]Field
	byName    map[string]Field
}

// CompileOptions supplies stable IDs and the logical merge policy for each
// top-level field. FieldIDs keys are Go field paths (for example "Profile.Name").
type CompileOptions struct {
	TableID       uint32
	PrimaryField  string
	FieldIDs      map[string]uint32
	MergePolicies map[string]MergePolicy
	Codecs        *CodecRegistry
	MaxDepth      int
}

// Compile builds and validates a recursive record descriptor. All persisted
// fields require explicit stable IDs. The primary key must be a 16-byte array.
func Compile(recordType reflect.Type, opts CompileOptions) (*Schema, error) {
	if recordType == nil || opts.TableID == 0 || opts.PrimaryField == "" {
		return nil, fmt.Errorf("%w: record type, nonzero table ID and primary field are required", ErrDescriptor)
	}
	if recordType.Kind() == reflect.Pointer {
		recordType = recordType.Elem()
	}
	if recordType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: record %s must be a struct", ErrDescriptor, recordType)
	}
	if opts.MaxDepth <= 0 {
		opts.MaxDepth = 64
	}
	root, err := compileType(recordType, "", 0, opts, make(map[reflect.Type]*Descriptor))
	if err != nil {
		return nil, err
	}
	var primary Field
	for _, field := range root.Fields {
		if field.GoName == opts.PrimaryField {
			primary = field
			break
		}
	}
	if primary.ID == 0 {
		return nil, fmt.Errorf("%w: primary field %q is absent or ignored", ErrDescriptor, opts.PrimaryField)
	}
	if !isRowID(primary.Descriptor.GoType) {
		return nil, fmt.Errorf("%w: replicated primary field %s must be a 16-byte array", ErrDescriptor, opts.PrimaryField)
	}
	if primary.Merge != MergeLWW {
		return nil, fmt.Errorf("%w: primary field %s must use LWW", ErrDescriptor, opts.PrimaryField)
	}
	s := &Schema{TableID: opts.TableID, PrimaryID: primary.ID, Record: root,
		Fields: append([]Field(nil), root.Fields...), byID: make(map[uint32]Field, len(root.Fields)),
		byName: make(map[string]Field, len(root.Fields))}
	for _, field := range s.Fields {
		s.byID[field.ID] = field
		s.byName[field.GoName] = field
	}
	return s, nil
}

func (s *Schema) FieldByID(id uint32) (Field, bool) {
	if s == nil {
		return Field{}, false
	}
	f, ok := s.byID[id]
	return f, ok
}

func (s *Schema) FieldByName(name string) (Field, bool) {
	if s == nil {
		return Field{}, false
	}
	f, ok := s.byName[name]
	return f, ok
}

func compileType(typ reflect.Type, path string, depth int, opts CompileOptions, cache map[reflect.Type]*Descriptor) (*Descriptor, error) {
	if descriptor, ok := cache[typ]; ok {
		return descriptor, nil
	}
	if depth > opts.MaxDepth {
		return nil, fmt.Errorf("%w: nesting exceeds %d at %s", ErrDescriptor, opts.MaxDepth, path)
	}
	if typ == reflect.TypeOf(time.Time{}) {
		return &Descriptor{Kind: KindInstant, GoType: typ}, nil
	}
	if opts.Codecs != nil {
		if identity, ok := opts.Codecs.byType[typ]; ok {
			return &Descriptor{Kind: KindCustom, GoType: typ, CodecID: identity.ID, CodecVersion: identity.Version}, nil
		}
	}
	d := &Descriptor{GoType: typ}
	cache[typ] = d
	if isOptional(typ) {
		value, ok := typ.FieldByName("Value")
		if !ok {
			return nil, fmt.Errorf("%w: invalid Optional type %s", ErrDescriptor, typ)
		}
		d.Kind = KindOptional
		child, err := compileType(value.Type, childPath(path, "Value"), depth+1, opts, cache)
		if err != nil {
			return nil, err
		}
		d.Element = child
		return d, nil
	}
	switch typ.Kind() {
	case reflect.Bool:
		d.Kind = KindBool
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		d.Kind = KindInt
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		d.Kind = KindUint
	case reflect.Float32:
		d.Kind = KindFloat32
	case reflect.Float64:
		d.Kind = KindFloat64
	case reflect.String:
		d.Kind = KindString
	case reflect.Pointer:
		d.Kind = KindOptional
		child, err := compileType(typ.Elem(), path, depth+1, opts, cache)
		if err != nil {
			return nil, err
		}
		d.Element = child
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 {
			d.Kind = KindBytes
			break
		}
		d.Kind = KindSlice
		child, err := compileType(typ.Elem(), path+"[]", depth+1, opts, cache)
		if err != nil {
			return nil, err
		}
		d.Element = child
	case reflect.Array:
		if typ.Elem().Kind() == reflect.Uint8 && typ.Len() == 16 {
			d.Kind, d.Length = KindArray, typ.Len()
			break
		}
		d.Kind, d.Length = KindArray, typ.Len()
		child, err := compileType(typ.Elem(), path+"[]", depth+1, opts, cache)
		if err != nil {
			return nil, err
		}
		d.Element = child
	case reflect.Map:
		if !supportedMapKey(typ.Key()) {
			return nil, fmt.Errorf("%w: unsupported map key %s at %s", ErrDescriptor, typ.Key(), path)
		}
		d.Kind = KindMap
		key, err := compileType(typ.Key(), path+"{key}", depth+1, opts, cache)
		if err != nil {
			return nil, err
		}
		value, err := compileType(typ.Elem(), path+"{value}", depth+1, opts, cache)
		if err != nil {
			return nil, err
		}
		d.Key, d.Value = key, value
	case reflect.Struct:
		d.Kind = KindStruct
		seen := make(map[uint32]bool)
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if f.PkgPath != "" || f.Tag.Get("murmur") == "-" {
				continue
			}
			fieldPath := childPath(path, f.Name)
			id, ok := opts.FieldIDs[fieldPath]
			if !ok {
				return nil, fmt.Errorf("%w: field ID missing for %s", ErrDescriptor, fieldPath)
			}
			if id == 0 || seen[id] {
				return nil, fmt.Errorf("%w: zero or duplicate field ID %d in %s", ErrDescriptor, id, typ)
			}
			seen[id] = true
			child, err := compileType(f.Type, fieldPath, depth+1, opts, cache)
			if err != nil {
				return nil, err
			}
			merge := MergeLWW
			if path == "" {
				if configured, ok := opts.MergePolicies[f.Name]; ok {
					merge = configured
				}
			}
			if !validMerge(merge) {
				return nil, fmt.Errorf("%w: unsupported merge policy %q on %s", ErrDescriptor, merge, fieldPath)
			}
			if path == "" && !validMergeType(merge, child) {
				return nil, fmt.Errorf("%w: merge policy %q is incompatible with %s on %s", ErrDescriptor, merge, child.GoType, fieldPath)
			}
			d.Fields = append(d.Fields, Field{ID: id, GoName: f.Name, Path: fieldPath, Merge: merge, Descriptor: child})
		}
	case reflect.Interface:
		return nil, fmt.Errorf("%w: interface field %s needs an explicit custom codec", ErrDescriptor, path)
	default:
		return nil, fmt.Errorf("%w: unsupported Go kind %s at %s", ErrDescriptor, typ.Kind(), path)
	}
	return d, nil
}

func childPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "." + name
}

func isOptional(typ reflect.Type) bool {
	if typ.Kind() != reflect.Struct {
		return false
	}
	method, ok := typ.MethodByName("IsPresent")
	if !ok || method.Type.NumIn() != 1 || method.Type.NumOut() != 1 || method.Type.Out(0).Kind() != reflect.Bool {
		return false
	}
	value, valueOK := typ.FieldByName("Value")
	present, presentOK := typ.FieldByName("Present")
	return valueOK && presentOK && present.Type.Kind() == reflect.Bool && value.PkgPath == ""
}

func supportedMapKey(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	case reflect.Array:
		return typ.Elem().Kind() == reflect.Uint8
	default:
		return false
	}
}

func isRowID(typ reflect.Type) bool {
	return typ.Kind() == reflect.Array && typ.Len() == 16 && typ.Elem().Kind() == reflect.Uint8
}

func validMerge(policy MergePolicy) bool {
	switch policy {
	case MergeLWW, MergeMin, MergeMax, MergeCounter, MergeORSet:
		return true
	default:
		return false
	}
}

func validMergeType(policy MergePolicy, descriptor *Descriptor) bool {
	if descriptor == nil {
		return false
	}
	if descriptor.Kind == KindOptional {
		descriptor = descriptor.Element
	}
	switch policy {
	case MergeLWW:
		return true
	case MergeMin, MergeMax:
		switch descriptor.Kind {
		case KindInt, KindUint, KindFloat32, KindFloat64:
			return true
		default:
			return false
		}
	case MergeCounter:
		return descriptor.Kind == KindInt || descriptor.Kind == KindUint
	case MergeORSet:
		return descriptor.Kind == KindSlice || descriptor.Kind == KindMap || descriptor.Kind == KindBytes
	default:
		return false
	}
}

// StableFieldIDKey returns the path key used by Compile. Go field paths are
// registration inputs only; encoded manifests contain the resulting numeric IDs.
func StableFieldIDKey(path ...string) string { return strings.Join(path, ".") }
