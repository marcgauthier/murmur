package murmur

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/recordcodec"
	"github.com/marcgauthier/murmur/rime"
	"github.com/marcgauthier/murmur/schema"
)

// ModelOptions overrides automatic names and durable identities. PrimaryField
// selects a legacy row-ID field; it does not select the business primary key.
type ModelOptions struct {
	Name    string
	TableID uint32
	RecordOptions
}

// Model compiles a struct with automatic stable IDs and retains typed handles.
// At most one options value is accepted. Model is the only table-definition
// API; explicit identities flow through ModelOptions.
func Model[T any](options ...ModelOptions) (TableDefinition, error) {
	typ := reflect.TypeFor[T]()
	opts, identity, err := modelOptions(typ, options)
	if err != nil {
		return TableDefinition{}, err
	}
	return defineType[T](typ, opts.Name, opts.TableID, opts.RecordOptions, identity)
}

type modelIdentity struct {
	typ      reflect.Type
	name     string
	idField  string
	business string
	idIndex  []int
	keyIndex []int
	record   *recordcodec.Schema
	codecs   *recordcodec.CodecRegistry
}

func modelOptions(typ reflect.Type, options []ModelOptions) (ModelOptions, *modelIdentity, error) {
	if typ == nil || typ.Kind() != reflect.Struct || typ.Name() == "" {
		return ModelOptions{}, nil, fmt.Errorf("murmur: models must be named structs: %w", ErrUnsupportedSchema)
	}
	if len(options) > 1 {
		return ModelOptions{}, nil, fmt.Errorf("murmur: Model accepts at most one options value")
	}
	var opts ModelOptions
	if len(options) == 1 {
		opts = options[0]
	}
	if opts.Name == "" {
		opts.Name = typ.Name()
	}
	if opts.TableID == 0 {
		opts.TableID = schema.StableID("table:" + strings.ToLower(opts.Name))
	}
	idField := opts.PrimaryField
	if idField == "" {
		idField = "ID"
	}
	id, ok := typ.FieldByName(idField)
	if !ok || id.PkgPath != "" || len(id.Index) != 1 || !isItemRowIDType(id.Type) {
		return opts, nil, fmt.Errorf("murmur: model %s requires an exported %s RowID field: %w", typ, idField, ErrUnsupportedSchema)
	}
	if opts.PrimaryField == "" && id.Type != reflect.TypeFor[ids.RowID]() {
		return opts, nil, fmt.Errorf("murmur: model %s.ID must be ids.RowID: %w", typ, ErrUnsupportedSchema)
	}
	if !modelTag(id.Tag.Get("rime"), "ID") && !modelTag(id.Tag.Get("rime"), "primary") {
		return opts, nil, fmt.Errorf("murmur: model %s.%s must carry rime:\"ID\" (or legacy primary): %w", typ, idField, ErrUnsupportedSchema)
	}
	identity := &modelIdentity{typ: typ, name: opts.Name, idField: idField, idIndex: id.Index}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" || field.Name == idField {
			continue
		}
		if modelTag(field.Tag.Get("rime"), "ID") {
			return opts, nil, fmt.Errorf("murmur: model %s has another ID field %s: %w", typ, field.Name, ErrUnsupportedSchema)
		}
		if !modelTag(field.Tag.Get("rime"), "primary") {
			continue
		}
		if identity.business != "" {
			return opts, nil, fmt.Errorf("murmur: model %s has multiple business primary keys: %w", typ, ErrUnsupportedSchema)
		}
		switch field.Type.Kind() {
		case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		default:
			if !isItemRowIDType(field.Type) {
				return opts, nil, fmt.Errorf("murmur: unsupported business key %s.%s (%s): %w", typ, field.Name, field.Type, ErrUnsupportedSchema)
			}
		}
		if modelTag(field.Tag.Get("rime"), "-") || field.Tag.Get("murmur") == "-" {
			return opts, nil, fmt.Errorf("murmur: business key cannot be ignored: %w", ErrUnsupportedSchema)
		}
		identity.business, identity.keyIndex = field.Name, field.Index
	}
	opts.PrimaryField = idField
	fieldIDs := make(map[string]uint32, len(opts.FieldIDs))
	for name, id := range opts.FieldIDs {
		fieldIDs[name] = id
	}
	custom := make(map[reflect.Type]bool)
	for _, codec := range opts.Codecs {
		custom[reflect.TypeOf(codec.Example)] = true
	}
	type automaticField struct {
		owner      reflect.Type
		path, name string
		id         uint32
	}
	var discovered []automaticField
	var visit func(reflect.Type, string, string, map[reflect.Type]bool)
	visit = func(t reflect.Type, path, logicalPath string, stack map[reflect.Type]bool) {
		if stack[t] || t == reflect.TypeFor[time.Time]() || custom[t] {
			return
		}
		stack[t] = true
		defer delete(stack, t)
		switch t.Kind() {
		case reflect.Pointer:
			visit(t.Elem(), path, logicalPath, stack)
		case reflect.Slice, reflect.Array:
			visit(t.Elem(), path+"[]", logicalPath+"[]", stack)
		case reflect.Map:
			visit(t.Key(), path+"{key}", logicalPath+"{key}", stack)
			visit(t.Elem(), path+"{value}", logicalPath+"{value}", stack)
		case reflect.Struct:
			if t.PkgPath() == reflect.TypeFor[rime.Optional[int]]().PkgPath() && strings.HasPrefix(t.Name(), "Optional[") {
				value, _ := t.FieldByName("Value")
				visit(value.Type, modelPath(path, "Value"), modelPath(logicalPath, "Value"), stack)
				return
			}
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				skip, logical := itemRimeTag(f.Tag.Get("rime"))
				if f.PkgPath != "" || skip || f.Tag.Get("murmur") == "-" {
					continue
				}
				if logical == "" {
					logical = f.Name
				}
				childPath := modelPath(path, f.Name)
				childLogical := modelPath(logicalPath, logical)
				hashPath := childLogical
				if path != "" {
					hashPath = "nested:" + logical
				}
				discovered = append(discovered, automaticField{t, childPath, f.Name, schema.StableID("column:" + strings.ToLower(opts.Name) + ":" + strings.ToLower(hashPath))})
				visit(f.Type, childPath, childLogical, stack)
			}
		}
	}
	visit(typ, "", "", make(map[reflect.Type]bool))
	// Reused nested types have one descriptor. Propagate any explicit identity
	// override across their paths so declaration order cannot select an ID.
	overrides := make(map[reflect.Type]map[string]uint32)
	for _, field := range discovered {
		id, explicit := fieldIDs[field.path]
		if !explicit {
			continue
		}
		if overrides[field.owner] == nil {
			overrides[field.owner] = make(map[string]uint32)
		}
		previous, exists := overrides[field.owner][field.name]
		if exists && previous != id {
			return opts, nil, fmt.Errorf("murmur: conflicting field IDs for reused %s.%s: %w", field.owner, field.name, ErrUnsupportedSchema)
		}
		overrides[field.owner][field.name] = id
	}
	for _, field := range discovered {
		id := field.id
		if overridden, exists := overrides[field.owner][field.name]; exists {
			id = overridden
		}
		fieldIDs[field.path] = id
	}
	opts.FieldIDs = fieldIDs
	// Compile once for canonical business-key encoding; the definition compiler
	// performs the complete scope, merge, codec and storage validation.
	codecs := recordcodec.NewCodecRegistry()
	for _, c := range opts.Codecs {
		if err := codecs.RegisterCodec(c.ID, c.Version, c.Example, c.Encode, c.Decode, c.Clone, c.Equal); err != nil {
			return opts, nil, err
		}
	}
	policies := make(map[string]recordcodec.MergePolicy)
	for field, policy := range opts.MergePolicies {
		policies[field] = recordcodec.MergePolicy(policy)
	}
	identity.codecs = codecs
	var err error
	identity.record, err = recordcodec.Compile(typ, recordcodec.CompileOptions{TableID: opts.TableID, PrimaryField: idField, FieldIDs: fieldIDs, MergePolicies: policies, Codecs: codecs, MaxDepth: opts.MaxDepth})
	if err != nil {
		return opts, nil, err
	}
	if identity.business != "" {
		key, ok := identity.record.FieldByName(identity.business)
		if !ok {
			return opts, nil, fmt.Errorf("murmur: business primary key must be persisted: %w", ErrUnsupportedSchema)
		}
		if key.Merge != recordcodec.MergeLWW {
			return opts, nil, fmt.Errorf("murmur: business primary key must use LWW: %w", ErrUnsupportedSchema)
		}
	}
	return opts, identity, nil
}

func modelPath(parent, field string) string {
	if parent == "" {
		return field
	}
	return parent + "." + field
}
func modelTag(tag, want string) bool {
	for _, part := range strings.Split(tag, ",") {
		if strings.TrimSpace(part) == want {
			return true
		}
	}
	return false
}

func (m *modelIdentity) ensure(item any, create bool) error {
	value := reflect.ValueOf(item)
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return fmt.Errorf("murmur: nil model")
		}
		value = value.Elem()
	}
	if !value.IsValid() || value.Type() != m.typ {
		return fmt.Errorf("murmur: expected model %s, got %T", m.typ, item)
	}
	field := value.FieldByIndex(m.idIndex)
	id, err := itemRowID(field)
	if err != nil || !id.IsZero() {
		return err
	}
	if !field.CanSet() {
		return fmt.Errorf("murmur: automatic ID generation requires a writable *%s", m.typ)
	}
	if m.business != "" {
		if value.FieldByIndex(m.keyIndex).IsZero() {
			return fmt.Errorf("murmur: business primary key %s.%s is unset", m.typ, m.business)
		}
		key, _ := m.record.FieldByName(m.business)
		encoded, err := recordcodec.EncodeField(m.record, key.ID, item, m.codecs, recordcodec.Limits{})
		if err != nil {
			return err
		}
		id = ids.RowID(rime.NewUUIDv5(rime.TableNamespace(m.name), string(encoded)))
	} else {
		if !create {
			return fmt.Errorf("murmur: %s.%s is unset", m.typ, m.idField)
		}
		id = ids.NewRowID()
	}
	for i, b := range id {
		field.Index(i).SetUint(uint64(b))
	}
	return nil
}

func (m *modelIdentity) sameKey(old, next any) error {
	if m.business == "" {
		return nil
	}
	key, _ := m.record.FieldByName(m.business)
	equal, err := recordcodec.EqualField(m.record, key.ID, old, next, m.codecs)
	if err != nil {
		return err
	}
	if !equal {
		return fmt.Errorf("murmur: business primary key %s.%s is immutable", m.typ, m.business)
	}
	return nil
}

func compileModels(models []any) ([]TableDefinition, error) {
	definitions := make([]TableDefinition, 0, len(models))
	for i, model := range models {
		if definition, ok := model.(TableDefinition); ok {
			definitions = append(definitions, definition)
			continue
		}
		typ := reflect.TypeOf(model)
		if typ != nil && typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		opts, identity, err := modelOptions(typ, nil)
		if err != nil {
			return nil, fmt.Errorf("murmur: Models[%d]: %w", i, err)
		}
		definition, err := defineType[any](typ, opts.Name, opts.TableID, opts.RecordOptions, identity)
		if err != nil {
			return nil, fmt.Errorf("murmur: Models[%d]: %w", i, err)
		}
		definitions = append(definitions, definition)
	}
	return definitions, nil
}

// MigrateModels publishes an additive schema from the complete model set.
// Open retains compatibility checks and never implicitly publishes additions.
func (db *DB) MigrateModels(ctx context.Context, models []any) error {
	definitions, err := compileModels(models)
	if err != nil {
		return err
	}
	return db.MigrateRecords(ctx, definitions)
}

func runtimeItemOps(name string, typ reflect.Type) *itemOps {
	ops := newItemOps[any](name)
	fetch, find, first, update := ops.fetch, ops.find, ops.first, ops.update
	ops.fetch = func(db *DB, tx *Tx, read *recordReadTx, key ids.RowID) (any, error) {
		value, err := fetch(db, tx, read, key)
		if err != nil {
			return nil, err
		}
		native := *value.(*any)
		if reflect.TypeOf(native) != reflect.PointerTo(typ) {
			return nil, fmt.Errorf("murmur: model %s binding changed; rebuild the item reference: %w", typ, ErrUnsupportedSchema)
		}
		return native, nil
	}
	ops.find = func(db *DB, spec *itemQuerySpec) ([]any, error) {
		rows, err := find(db, spec)
		for i, row := range rows {
			native := *row.(*any)
			if reflect.TypeOf(native) != reflect.PointerTo(typ) {
				return nil, fmt.Errorf("murmur: model %s binding changed; rebuild the query: %w", typ, ErrUnsupportedSchema)
			}
			rows[i] = native
		}
		return rows, err
	}
	ops.first = func(db *DB, spec *itemQuerySpec) (any, error) {
		value, err := first(db, spec)
		if err != nil {
			return nil, err
		}
		native := *value.(*any)
		if reflect.TypeOf(native) != reflect.PointerTo(typ) {
			return nil, fmt.Errorf("murmur: model %s binding changed; rebuild the item reference: %w", typ, ErrUnsupportedSchema)
		}
		return native, nil
	}
	ops.update = func(db *DB, tx *Tx, key ids.RowID, apply func(any) error) error {
		return update(db, tx, key, func(value any) error {
			native := *value.(*any)
			if reflect.TypeOf(native) != reflect.PointerTo(typ) {
				return fmt.Errorf("murmur: model %s binding changed: %w", typ, ErrUnsupportedSchema)
			}
			return apply(native)
		})
	}
	return ops
}
