// Package rimeadapter joins RIME's managed publication lifecycle to Murmur's
// authoritative state store. The package is an integration boundary: RIME
// itself remains independent of storage and replication.
package rimeadapter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/recordcodec"
	"github.com/marcgauthier/murmur/rime"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/state"
)

var (
	ErrSchemaMismatch = errors.New("rimeadapter: runtime record does not match durable schema")
	ErrMaterializer   = errors.New("rimeadapter: durable state committed but RIME publication failed")
	ErrUnsupported    = errors.New("rimeadapter: unsupported schema or change")
	ErrUncertain      = errors.New("rimeadapter: durable commit outcome is uncertain")
)

type UncertainCommitError struct {
	TxID  ids.TxID
	Cause error
}

// RebuildProgress reports typed materializer reconstruction from durable rows.
type RebuildProgress struct {
	CurrentTable   string
	ProcessedItems uint64
	RowsInserted   uint64
	RowsSkipped    uint64
}

func (e *UncertainCommitError) Error() string {
	return fmt.Sprintf("%v (transaction %x): %v", ErrUncertain, e.TxID, e.Cause)
}

func (e *UncertainCommitError) Unwrap() []error {
	return []error{ErrUncertain, e.Cause}
}

// Adapter owns managed local writes. Transaction callbacks prepare concurrently;
// a coordinator orders Spool commits and RIME publication. A failed or uncertain
// publication makes the adapter fail closed.
type Adapter struct {
	mu                    sync.Mutex
	store                 *state.Store
	db                    *rime.DB
	manifest              *schema.Manifest
	registry              *recordcodec.CodecRegistry
	limits                recordcodec.Limits
	bindings              map[string]binding
	localHook             func(*codec.MutationBatch) error
	rowResolver           func(*state.Row) (*state.Row, error)
	beforeCommit          func() error
	afterCommit           func() error
	broken                error
	started               bool
	materialized          bool
	publicationGeneration uint64
}

// SetCommitHooks installs Murmur's commit-boundary fault hooks before the
// adapter is materialized. Production databases leave these hooks nil.
func (a *Adapter) SetCommitHooks(before, after func() error) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started || a.materialized || a.broken != nil {
		return fmt.Errorf("%w: commit hooks must be installed before materialization", ErrUnsupported)
	}
	a.beforeCommit = before
	a.afterCommit = after
	return nil
}

// SetManagedHooks installs Murmur-specific durable mutation and row
// projection hooks before the adapter is rebuilt or serves writes.
func (a *Adapter) SetManagedHooks(local func(*codec.MutationBatch) error, resolve func(*state.Row) (*state.Row, error)) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.started || a.materialized || a.broken != nil {
		return fmt.Errorf("%w: managed hooks must be installed before materialization", ErrUnsupported)
	}
	a.localHook = local
	a.rowResolver = resolve
	return nil
}

// PublicationGeneration advances after a managed RIME write is published,
// including ephemeral writes that have no Spool state generation.
func (a *Adapter) PublicationGeneration() uint64 {
	if a == nil {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.publicationGeneration
}

type binding struct {
	table     *schema.TableSchema
	record    *recordcodec.Schema
	codecs    *recordcodec.CodecRegistry
	local     bool
	ephemeral bool
	load      func(*rime.Tx, ids.RowID, map[uint32]codec.Value) error
	remove    func(*rime.Tx, ids.RowID) error
}

type counterDelta struct {
	table uint32
	row   ids.RowID
	field uint32
	delta int64
}

type setOperation struct {
	table uint32
	row   ids.RowID
	field uint32
	value string
	add   bool
}

type extremaOperation struct {
	table  uint32
	row    ids.RowID
	field  uint32
	policy recordcodec.MergePolicy
	before codec.Value
	value  codec.Value
}

func cloneCodecValue(value codec.Value) codec.Value {
	value.B = append([]byte(nil), value.B...)
	return value
}

func mergeSchemaPolicy(policy recordcodec.MergePolicy) schema.MergePolicy {
	switch policy {
	case recordcodec.MergeMin:
		return schema.MIN
	case recordcodec.MergeMax:
		return schema.MAX
	case recordcodec.MergeCounter:
		return schema.PN_COUNTER
	case recordcodec.MergeORSet:
		return schema.OR_SET
	default:
		return schema.LWW
	}
}

func New(store *state.Store, db *rime.DB, manifest *schema.Manifest, codecs *recordcodec.CodecRegistry, limits recordcodec.Limits) (*Adapter, error) {
	if store == nil || db == nil || manifest == nil {
		return nil, fmt.Errorf("rimeadapter: store, RIME database and manifest are required")
	}
	current, err := store.LoadSchemaManifest()
	if err != nil {
		return nil, err
	}
	if current == nil || !schema.EqualRevision(current, manifest) {
		return nil, fmt.Errorf("%w: manifest is not the current durable schema revision", ErrSchemaMismatch)
	}
	manifest, err = schema.DecodeManifest(schema.EncodeManifest(manifest))
	if err != nil {
		return nil, fmt.Errorf("rimeadapter: copy manifest: %w", err)
	}
	if codecs == nil {
		codecs = recordcodec.NewCodecRegistry()
	}
	return &Adapter{store: store, db: db, manifest: manifest, registry: codecs, limits: limits, bindings: make(map[string]binding)}, nil
}

// Register binds T to one rich table definition. LWW fields use canonical
// recordcodec BLOBs; supported PN_COUNTER and OR_SET fields use RIME text
// projections and explicit causal operations.
func Register[T any](a *Adapter, tableName string, opts recordcodec.CompileOptions) (*Table[T], error) {
	if a == nil || tableName == "" {
		return nil, fmt.Errorf("rimeadapter: adapter and table name are required")
	}
	var ts *schema.TableSchema
	for i := range a.manifest.Tables {
		if a.manifest.Tables[i].Name == tableName {
			ts = &a.manifest.Tables[i]
			break
		}
	}
	if ts == nil {
		return nil, fmt.Errorf("%w: table %q is absent", ErrSchemaMismatch, tableName)
	}
	return registerTable[T](a, tableName, ts, opts, false, false)
}

// RegisterLocal binds T to persistent node-local state. The caller supplies
// the compiled table schema because local tables are deliberately absent from
// the replicated manifest.
func RegisterLocal[T any](a *Adapter, tableName string, ts schema.TableSchema, opts recordcodec.CompileOptions) (*Table[T], error) {
	if a == nil || tableName == "" || ts.ID == 0 || ts.Name != tableName {
		return nil, fmt.Errorf("rimeadapter: adapter and matching local table schema are required")
	}
	return registerTable[T](a, tableName, &ts, opts, true, false)
}

// RegisterEphemeral binds T to an in-memory table that is reset on reopen and
// never enters durable state or replication.
func RegisterEphemeral[T any](a *Adapter, tableName string, ts schema.TableSchema, opts recordcodec.CompileOptions) (*Table[T], error) {
	if a == nil || tableName == "" || ts.ID == 0 || ts.Name != tableName {
		return nil, fmt.Errorf("rimeadapter: adapter and matching ephemeral table schema are required")
	}
	return registerTable[T](a, tableName, &ts, opts, false, true)
}

func registerTable[T any](a *Adapter, tableName string, ts *schema.TableSchema, opts recordcodec.CompileOptions, local, ephemeral bool) (*Table[T], error) {
	return registerTableType[T](a, tableName, ts, opts, local, ephemeral, reflect.TypeFor[T]())
}

// RegisterType binds a runtime struct to the ordinary managed adapter.
func RegisterType(a *Adapter, typ reflect.Type, tableName string, ts schema.TableSchema, opts recordcodec.CompileOptions, local, ephemeral bool) (*Table[any], error) {
	if a == nil || typ == nil || tableName == "" {
		return nil, fmt.Errorf("rimeadapter: adapter, type and table name are required")
	}
	if !local && !ephemeral {
		found := false
		for _, durable := range a.manifest.Tables {
			if strings.EqualFold(durable.Name, tableName) {
				ts = durable
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("%w: table %q is absent", ErrSchemaMismatch, tableName)
		}
	}
	return registerTableType[any](a, tableName, &ts, opts, local, ephemeral, typ)
}

func registerTableType[T any](a *Adapter, tableName string, ts *schema.TableSchema, opts recordcodec.CompileOptions, local, ephemeral bool, typ reflect.Type) (*Table[T], error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.broken != nil {
		return nil, fmt.Errorf("%w: %w", ErrMaterializer, a.broken)
	}
	if a.started {
		return nil, fmt.Errorf("%w: cannot register tables after materialization starts", ErrUnsupported)
	}
	if _, exists := a.bindings[tableName]; exists {
		return nil, fmt.Errorf("%w: table %q is already registered", ErrSchemaMismatch, tableName)
	}
	codecs := opts.Codecs
	if codecs == nil {
		codecs = a.registry
	}
	opts.TableID = ts.ID
	record, err := recordcodec.Compile(typ, opts)
	if err != nil {
		return nil, err
	}
	encoded, err := recordcodec.MarshalDescriptor(record)
	if err != nil {
		return nil, err
	}
	if !recordcodec.DescriptorSupersetPreservingNestedUnknown(encoded, ts.RecordDescriptor) || record.PrimaryID != ts.PK {
		return nil, fmt.Errorf("%w: table %q runtime descriptor is not a compatible subset or primary key differs", ErrSchemaMismatch, tableName)
	}
	for _, f := range record.Fields {
		column := ts.ColumnByID(f.ID)
		supported := f.Merge == recordcodec.MergeLWW && column != nil && column.Type == schema.ColBlob && column.MergePolicy == schema.LWW
		if f.Merge == recordcodec.MergeCounter && column != nil && column.Type == schema.ColText && column.MergePolicy == schema.PN_COUNTER && f.Descriptor.Kind == recordcodec.KindInt && f.Descriptor.GoType.Kind() == reflect.Int64 {
			supported = true
		}
		if f.Merge == recordcodec.MergeORSet && column != nil && column.Type == schema.ColText && column.MergePolicy == schema.OR_SET && f.Descriptor.Kind == recordcodec.KindSlice && f.Descriptor.Element.Kind == recordcodec.KindString && f.Descriptor.GoType.Kind() == reflect.Slice && f.Descriptor.GoType.Elem().Kind() == reflect.String {
			supported = true
		}
		if (f.Merge == recordcodec.MergeMin || f.Merge == recordcodec.MergeMax) && column != nil && column.MergePolicy == mergeSchemaPolicy(f.Merge) {
			supported = column.Type == schema.ColInteger && (f.Descriptor.Kind == recordcodec.KindInt || f.Descriptor.Kind == recordcodec.KindUint && f.Descriptor.GoType.Bits() <= 63) || column.Type == schema.ColReal && (f.Descriptor.Kind == recordcodec.KindFloat32 || f.Descriptor.Kind == recordcodec.KindFloat64)
		}
		if !supported {
			return nil, fmt.Errorf("%w: field %q has no supported storage/merge mapping", ErrUnsupported, f.Path)
		}
		goField, _ := record.Record.GoType.FieldByName(f.GoName)
		for _, directive := range strings.Split(goField.Tag.Get("rime"), ",") {
			directive = strings.TrimSpace(directive)
			if directive == "unique" || strings.HasPrefix(directive, "fk=") || strings.HasPrefix(directive, "fk:") {
				return nil, fmt.Errorf("%w: managed field %q declares %q", ErrUnsupported, f.Path, directive)
			}
		}
	}
	var inner *rime.Table[T]
	if typ == reflect.TypeFor[T]() {
		inner, err = rime.Register[T](a.db, rime.WithTableName[T](tableName), rime.WithPrimaryField[T](opts.PrimaryField), rime.WithPrimaryKey[T](func(value *T) any { id, _ := primaryID(record, nativeRecord(value)); return id }))
	} else {
		runtime, runtimeErr := rime.RegisterType(a.db, typ, rime.WithTableName[any](tableName), rime.WithPrimaryField[any](opts.PrimaryField), rime.WithPrimaryKey[any](func(value *any) any { id, _ := primaryID(record, nativeRecord(value)); return id }))
		err = runtimeErr
		inner, _ = any(runtime).(*rime.Table[T])
	}
	if err != nil {
		return nil, err
	}
	load := func(tx *rime.Tx, expected ids.RowID, fields map[uint32]codec.Value) error {
		encoded := make(map[uint32][]byte, len(fields))
		counterValues := make(map[uint32]int64)
		setValues := make(map[uint32][]string)
		extremaValues := make(map[uint32]codec.Value)
		for id, value := range fields {
			field, ok := record.FieldByID(id)
			if !ok {
				continue
			}
			column := ts.ColumnByID(id)
			if column == nil {
				continue
			}
			if field.Merge == recordcodec.MergeCounter {
				if value.Type == codec.TypeNull {
					counterValues[id] = 0
					continue
				}
				if value.Type != codec.TypeText {
					return fmt.Errorf("%w: counter field %d is not TEXT", ErrSchemaMismatch, id)
				}
				n, ok := new(big.Int).SetString(value.S, 10)
				if !ok || !n.IsInt64() {
					return fmt.Errorf("%w: counter field %d is outside int64", ErrSchemaMismatch, id)
				}
				counterValues[id] = n.Int64()
				continue
			}
			if field.Merge == recordcodec.MergeORSet {
				if value.Type == codec.TypeNull {
					setValues[id] = []string{}
					continue
				}
				if value.Type != codec.TypeText {
					return fmt.Errorf("%w: OR_SET field %d is not TEXT", ErrSchemaMismatch, id)
				}
				var projected []struct {
					Type  string `json:"type"`
					Value string `json:"value"`
				}
				if err := json.Unmarshal([]byte(value.S), &projected); err != nil {
					return fmt.Errorf("%w: invalid OR_SET projection for field %d", ErrSchemaMismatch, id)
				}
				strings := make([]string, len(projected))
				for i, element := range projected {
					if element.Type != "string" {
						return fmt.Errorf("%w: OR_SET field %d contains non-string element", ErrSchemaMismatch, id)
					}
					strings[i] = element.Value
				}
				sort.Strings(strings)
				setValues[id] = strings
				continue
			}
			if field.Merge == recordcodec.MergeMin || field.Merge == recordcodec.MergeMax {
				if value.Type != codec.TypeInteger && value.Type != codec.TypeReal && value.Type != codec.TypeNull {
					return fmt.Errorf("%w: extrema field %d has nonnumeric projection", ErrSchemaMismatch, id)
				}
				if value.Type != codec.TypeNull {
					extremaValues[id] = value
				}
				continue
			}
			if value.Type != codec.TypeBlob {
				return fmt.Errorf("%w: table %d field %d is not an encoded record blob", ErrSchemaMismatch, ts.ID, id)
			}
			encoded[id] = append([]byte(nil), value.B...)
		}
		decoded, err := recordcodec.DecodeFields(record, encoded, codecs, a.limits)
		if err != nil {
			return err
		}
		if reflect.TypeFor[T]() == reflect.TypeFor[any]() {
			decoded = recordPointer(decoded)
		}
		value, ok := decoded.(T)
		if !ok {
			return fmt.Errorf("%w: decoded %T does not match registered type", ErrSchemaMismatch, decoded)
		}
		valuePtr := reflect.ValueOf(nativeRecord(&value)).Elem()
		for id, counter := range counterValues {
			field, _ := record.FieldByID(id)
			goField := valuePtr.FieldByName(field.GoName)
			if !goField.IsValid() || !goField.CanSet() || goField.Kind() != reflect.Int64 {
				return fmt.Errorf("%w: counter field %s is not writable int64", ErrSchemaMismatch, field.Path)
			}
			goField.SetInt(counter)
		}
		for id, values := range setValues {
			field, _ := record.FieldByID(id)
			goField := valuePtr.FieldByName(field.GoName)
			if !goField.IsValid() || !goField.CanSet() || goField.Type() != reflect.TypeFor[[]string]() {
				return fmt.Errorf("%w: OR_SET field %s is not writable []string", ErrSchemaMismatch, field.Path)
			}
			goField.Set(reflect.ValueOf(values))
		}
		for id, value := range extremaValues {
			field, _ := record.FieldByID(id)
			goField := valuePtr.FieldByName(field.GoName)
			if err := setExtremaReflect(goField, value); err != nil {
				return fmt.Errorf("%w: extrema field %s: %v", ErrSchemaMismatch, field.Path, err)
			}
		}
		key, err := primaryID(record, nativeRecord(&value))
		if err != nil || key != expected {
			return fmt.Errorf("%w: encoded primary key %s does not match row identity %s (table %s, decode error %v)", ErrSchemaMismatch, key, expected, tableName, err)
		}
		return inner.In(tx).Upsert(&value)
	}
	remove := func(tx *rime.Tx, expected ids.RowID) error {
		bound := inner.In(tx)
		_, err := bound.Get(expected)
		if errors.Is(err, rime.ErrNotFound) {
			// A remote tombstone may arrive before this node materializes the
			// corresponding row. Reconciliation of authoritative state is
			// idempotent, so there is nothing to delete in that case.
			return nil
		}
		if err != nil {
			return err
		}
		return bound.Delete(expected)
	}
	a.bindings[tableName] = binding{table: ts, record: record, codecs: codecs, local: local, ephemeral: ephemeral, load: load, remove: remove}
	return &Table[T]{adapter: a, inner: inner, record: record, table: ts, codecs: codecs}, nil
}

// Table is a typed handle usable only through an adapter transaction.
type Table[T any] struct {
	adapter   *Adapter
	inner     *rime.Table[T]
	record    *recordcodec.Schema
	table     *schema.TableSchema
	codecs    *recordcodec.CodecRegistry
	prepare   func(any, bool) error
	immutable func(any, any) error
}

func (t *Table[T]) clone(value *T) (*T, error) {
	if t == nil || value == nil {
		return nil, errors.New("rimeadapter: cannot clone nil record")
	}
	cloned, err := recordcodec.CloneRecord(t.record, nativeRecord(value), t.codecs)
	if err != nil {
		return nil, err
	}
	if reflect.TypeFor[T]() == reflect.TypeFor[any]() {
		cloned = recordPointer(cloned)
	}
	result, ok := cloned.(T)
	if !ok {
		return nil, fmt.Errorf("rimeadapter: clone returned %T, want registered record", cloned)
	}
	return &result, nil
}

// Clone returns a detached copy using registered custom codec clone hooks.
func (t *Table[T]) Clone(value *T) (*T, error) { return t.clone(value) }

// PrimaryKey returns the stable RowID key for a record.
func (t *Table[T]) PrimaryKey(value *T) (ids.RowID, error) {
	if t == nil || value == nil {
		return ids.RowID{}, fmt.Errorf("rimeadapter: nil table or record")
	}
	return primaryID(t.record, nativeRecord(value))
}

// Equal compares known fields using canonical built-in equality and registered
// custom codec equality hooks.
func (t *Table[T]) Equal(a, b *T) (bool, error) {
	if t == nil || a == nil || b == nil {
		return a == nil && b == nil, nil
	}
	for _, field := range t.record.Fields {
		equal, err := recordcodec.EqualField(t.record, field.ID, nativeRecord(a), nativeRecord(b), t.codecs)
		if err != nil || !equal {
			return equal, err
		}
	}
	return true, nil
}

func (t *Table[T]) bound(tx *Tx) (rime.BoundTable[T], error) {
	if t == nil || t.adapter == nil || tx == nil || tx.adapter != t.adapter || tx.inner == nil || tx.done {
		return rime.BoundTable[T]{}, errors.New("rimeadapter: table operation requires its active adapter transaction")
	}
	t.adapter.mu.Lock()
	broken := t.adapter.broken
	t.adapter.mu.Unlock()
	if broken != nil {
		return rime.BoundTable[T]{}, fmt.Errorf("%w: %v", ErrMaterializer, broken)
	}
	return t.inner.In(tx.inner), nil
}

func (t *Table[T]) Insert(tx *Tx, value *T) error {
	b, err := t.bound(tx)
	if err != nil {
		return err
	}
	if t.prepare != nil {
		if err := t.prepare(nativeRecord(value), true); err != nil {
			return err
		}
	}
	cloned, err := t.clone(value)
	if err != nil {
		return err
	}
	return b.Insert(cloned)
}
func (t *Table[T]) InsertMany(tx *Tx, values []*T) error {
	if t.prepare != nil {
		return tx.Batch("insert", len(values), func(i int) error { return t.Insert(tx, values[i]) })
	}
	b, err := t.bound(tx)
	if err != nil {
		return err
	}
	cloned, err := t.cloneMany(values)
	if err != nil {
		return err
	}
	return b.InsertMany(cloned)
}
func (t *Table[T]) Upsert(tx *Tx, value *T) error {
	b, err := t.bound(tx)
	if err != nil {
		return err
	}
	if t.prepare != nil {
		if err := t.prepare(nativeRecord(value), true); err != nil {
			return err
		}
	}
	cloned, err := t.clone(value)
	if err != nil {
		return err
	}
	if t.immutable != nil {
		key, err := t.PrimaryKey(cloned)
		if err != nil {
			return err
		}
		old, err := b.Get(key)
		if err != nil && !errors.Is(err, rime.ErrNotFound) {
			return err
		}
		if err == nil {
			if err := t.immutable(nativeRecord(old), nativeRecord(cloned)); err != nil {
				return err
			}
		}
	}
	return b.Upsert(cloned)
}
func (t *Table[T]) UpsertMany(tx *Tx, values []*T) error {
	if t.prepare != nil {
		return tx.Batch("upsert", len(values), func(i int) error { return t.Upsert(tx, values[i]) })
	}
	b, err := t.bound(tx)
	if err != nil {
		return err
	}
	cloned, err := t.cloneMany(values)
	if err != nil {
		return err
	}
	return b.UpsertMany(cloned)
}
func (t *Table[T]) Update(tx *Tx, key any, fn func(*T) error) error {
	b, err := t.bound(tx)
	if err != nil {
		return err
	}
	return b.Update(key, func(current *T) error {
		cloned, err := t.clone(current)
		if err != nil {
			return err
		}
		if err := fn(cloned); err != nil {
			return err
		}
		if t.immutable != nil {
			if err := t.immutable(nativeRecord(current), nativeRecord(cloned)); err != nil {
				return err
			}
		}
		owned, err := t.clone(cloned)
		if err != nil {
			return err
		}
		*current = *owned
		return nil
	})
}

func (t *Table[T]) cloneMany(values []*T) ([]*T, error) {
	cloned := make([]*T, len(values))
	for i, value := range values {
		copy, err := t.clone(value)
		if err != nil {
			return nil, err
		}
		cloned[i] = copy
	}
	return cloned, nil
}
func (t *Table[T]) Delete(tx *Tx, key any) error {
	b, err := t.bound(tx)
	if err != nil {
		return err
	}
	return b.Delete(key)
}
func (t *Table[T]) DeleteMany(tx *Tx, keys []any) error {
	b, err := t.bound(tx)
	if err != nil {
		return err
	}
	return b.DeleteMany(keys)
}
func (t *Table[T]) Get(tx *Tx, key any) (*T, error) {
	b, err := t.bound(tx)
	if err != nil {
		return nil, err
	}
	value, err := b.Get(key)
	if err != nil {
		return nil, err
	}
	return t.clone(value)
}

// Lookup reads the current published value without opening a write
// transaction. The adapter must be healthy; reads never expose a writable
// RIME table handle.
func (t *Table[T]) Lookup(key any) (*T, bool, error) {
	if t == nil || t.adapter == nil || t.inner == nil {
		return nil, false, errors.New("rimeadapter: nil table")
	}
	t.adapter.mu.Lock()
	broken := t.adapter.broken
	t.adapter.mu.Unlock()
	if broken != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrMaterializer, broken)
	}
	value, ok, err := t.inner.Lookup(key)
	if err != nil || !ok {
		return nil, ok, err
	}
	cloned, err := t.clone(value)
	return cloned, err == nil, err
}

// LookupAt reads through an existing RIME read transaction. The transaction
// must belong to the same private RIME database generation as this table.
func (t *Table[T]) LookupAt(tx *rime.Tx, key any) (*T, bool, error) {
	if t == nil || t.adapter == nil || t.inner == nil || tx == nil {
		return nil, false, errors.New("rimeadapter: read lookup requires a table and transaction")
	}
	t.adapter.mu.Lock()
	broken := t.adapter.broken
	t.adapter.mu.Unlock()
	if broken != nil {
		return nil, false, fmt.Errorf("%w: %v", ErrMaterializer, broken)
	}
	value, ok, err := t.inner.In(tx).Lookup(key)
	if err != nil || !ok {
		return nil, ok, err
	}
	cloned, err := t.clone(value)
	return cloned, err == nil, err
}

// Where builds a read-only query over the table. The query itself exposes no
// adapter transaction or commit capability; callers must keep it private.
func (t *Table[T]) Where(exprs ...rime.Expr[T]) *rime.Query[T] {
	return t.inner.Where(exprs...)
}

// Compile builds a reusable read-only positional-parameter query.
func (t *Table[T]) Compile(exprs ...rime.Expr[T]) *rime.Compiled[T] {
	return t.inner.Compile(exprs...)
}

// WhereTx builds a query bound to an active adapter transaction, including
// its staged overlay.
func (t *Table[T]) WhereTx(tx *Tx, exprs ...rime.Expr[T]) (*rime.Query[T], error) {
	b, err := t.bound(tx)
	if err != nil {
		return nil, err
	}
	return b.Where(exprs...), nil
}

// CompileTx builds a reusable query over an active managed transaction.
func (t *Table[T]) CompileTx(tx *Tx, exprs ...rime.Expr[T]) (*rime.Compiled[T], error) {
	b, err := t.bound(tx)
	if err != nil {
		return nil, err
	}
	return b.Compile(exprs...), nil
}

// CompileReadTx builds a reusable query pinned to a read transaction.
func (t *Table[T]) CompileReadTx(tx *rime.Tx, ctx context.Context, exprs ...rime.Expr[T]) *rime.Compiled[T] {
	return t.inner.In(tx).WithContext(ctx).Compile(exprs...)
}

// FieldOf constructs a typed field expression handle without exposing the
// underlying mutable RIME table.
func FieldOf[T any, V comparable](t *Table[T], name string) rime.Field[T, V] {
	return rime.F[T, V](t.inner, name)
}

// StringFieldOf constructs a typed string predicate handle without exposing
// the underlying mutable RIME table.
func StringFieldOf[T any](t *Table[T], name string) rime.StringField[T] {
	return rime.StringFieldOf(t.inner, name)
}

// NumericFieldOf constructs a typed ordered field for numeric query operations
// without exposing the underlying mutable RIME table.
func NumericFieldOf[T any, V rime.Number](t *Table[T], name string) rime.OrderedField[T, V] {
	return rime.NumericFieldOf[V](t.inner, name)
}

// Ordered constrains ordered column types, mirroring RIME's ordered set
// (strings plus all int, uint, and float widths, including named types).
type Ordered interface {
	~string | ~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

// OrderedFieldOf constructs a typed ordered field for range predicates and
// ordering without exposing the underlying mutable RIME table.
func OrderedFieldOf[T any, V Ordered](t *Table[T], name string) rime.OrderedField[T, V] {
	return rime.OF[T, V](t.inner, name)
}

// InnerJoinOnRead joins adapter tables on a pinned RIME read transaction.
func InnerJoinOnRead[A, B any, K comparable](left *Table[A], lfield rime.KeyField[A, K], right *Table[B], rfield rime.KeyField[B, K], tx *rime.Tx, ctx context.Context) ([]rime.JoinRow[A, B], error) {
	return rime.InnerJoinOn(left.inner.In(tx).WithContext(ctx), lfield, right.inner.In(tx).WithContext(ctx), rfield)
}

// LeftJoinOnRead joins adapter tables on a pinned RIME read transaction,
// retaining left rows without a match.
func LeftJoinOnRead[A, B any, K comparable](left *Table[A], lfield rime.KeyField[A, K], right *Table[B], rfield rime.KeyField[B, K], tx *rime.Tx, ctx context.Context) ([]rime.JoinRow[A, B], error) {
	return rime.LeftJoinOn(left.inner.In(tx).WithContext(ctx), lfield, right.inner.In(tx).WithContext(ctx), rfield)
}

// Tx is an adapter-owned RIME transaction. It cannot commit around Spool.
type Tx struct {
	adapter    *Adapter
	inner      *rime.Tx
	done       bool
	counterOps []counterDelta
	setOps     []setOperation
	extremaOps []extremaOperation
}

// GroupCandidate contains a detached Spool batch for one managed transaction.
// It is eligible for synchronous group commit only when all of its durable
// changes are replicated mutations. Local and ephemeral writes use Commit.
type GroupCandidate struct {
	tx    *Tx
	batch *codec.MutationBatch
}

func (g *GroupCandidate) Batch() *codec.MutationBatch {
	if g == nil {
		return nil
	}
	return g.batch
}

func (g *GroupCandidate) Adapter() *Adapter {
	if g == nil || g.tx == nil {
		return nil
	}
	return g.tx.adapter
}

// PrepareGroupCandidate captures a transaction's durable payload without
// reserving RIME publication. Murmur orders candidates before durable group
// commit; PublishGroupCandidate later obtains and releases RIME's managed
// publication token in that same order.
func (a *Adapter) PrepareGroupCandidate(ctx context.Context, tx *Tx) (*GroupCandidate, bool, error) {
	if a == nil || tx == nil || tx.adapter != a || tx.inner == nil || tx.done {
		return nil, false, errors.New("rimeadapter: transaction is closed or belongs to another adapter")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		_ = tx.Rollback()
		return nil, false, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.broken != nil {
		_ = tx.Rollback()
		return nil, false, fmt.Errorf("%w: %w", ErrMaterializer, a.broken)
	}
	changes, err := tx.inner.PreparedChanges()
	if err != nil {
		_ = tx.Rollback()
		return nil, false, err
	}
	batch, local, ephemeral, err := a.batch(changes, tx.counterOps, tx.setOps, tx.extremaOps)
	if err != nil {
		_ = tx.Rollback()
		return nil, false, err
	}
	if a.localHook != nil {
		if err := a.localHook(batch); err != nil {
			_ = tx.Rollback()
			return nil, false, err
		}
	}
	if len(local) != 0 || ephemeral || len(batch.Mutations) == 0 {
		return nil, false, nil
	}
	return &GroupCandidate{tx: tx, batch: batch}, true, nil
}

// BeforeGroupCommit runs the normal pre-durable hook for an eligible member.
func (a *Adapter) BeforeGroupCommit(candidate *GroupCandidate) error {
	if a == nil || candidate == nil || candidate.tx == nil || candidate.tx.adapter != a || candidate.tx.done {
		return errors.New("rimeadapter: invalid group candidate")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.broken != nil {
		return fmt.Errorf("%w: %w", ErrMaterializer, a.broken)
	}
	if a.beforeCommit != nil {
		return a.beforeCommit()
	}
	return nil
}

// PublishGroupCandidate publishes one already-durable candidate. Call members
// in the same order in which their batches entered CommitLocalGroup.
func (a *Adapter) PublishGroupCandidate(ctx context.Context, candidate *GroupCandidate) error {
	if a == nil || candidate == nil || candidate.tx == nil || candidate.tx.adapter != a || candidate.tx.done {
		return errors.New("rimeadapter: invalid group candidate")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	tx := candidate.tx
	if a.broken != nil {
		_ = tx.Rollback()
		tx.done = true
		return fmt.Errorf("%w: %w", ErrMaterializer, a.broken)
	}
	var afterErr error
	if a.afterCommit != nil {
		afterErr = a.afterCommit()
	}
	prepared, err := tx.inner.PrepareCommit()
	if err != nil {
		_ = tx.Rollback()
		tx.done = true
		a.broken = err
		return fmt.Errorf("%w: durable group member could not reserve RIME publication: %v", ErrMaterializer, err)
	}
	if err := prepared.Publish(); err != nil {
		_ = prepared.Abort()
		tx.done = true
		a.broken = err
		return fmt.Errorf("%w: %v", ErrMaterializer, err)
	}
	tx.done = true
	a.materialized = true
	a.publicationGeneration++
	if afterErr != nil {
		return &UncertainCommitError{TxID: candidate.batch.TxID, Cause: afterErr}
	}
	return nil
}

func (a *Adapter) AbortGroupCandidate(candidate *GroupCandidate) {
	if candidate == nil || candidate.tx == nil || candidate.tx.done {
		return
	}
	_ = candidate.tx.Rollback()
}

// PreparedChangeCount reports the coalesced record count staged in the
// adapter transaction without reserving publication or changing its state.
func (t *Tx) PreparedChangeCount() (int, error) {
	if t == nil || t.inner == nil || t.done {
		return 0, rime.ErrTxClosed
	}
	changes, err := t.inner.PreparedChanges()
	if err != nil {
		return 0, err
	}
	return len(changes), nil
}

// CounterAdd stages an int64 PN_COUNTER delta and updates its typed RIME
// projection in the same managed transaction.
func (t *Table[T]) CounterAdd(tx *Tx, key ids.RowID, fieldName string, delta int64) error {
	if delta == 0 {
		return nil
	}
	bound, err := t.bound(tx)
	if err != nil {
		return err
	}
	field, ok := t.record.FieldByName(fieldName)
	if !ok || field.Merge != recordcodec.MergeCounter || field.Descriptor.GoType.Kind() != reflect.Int64 {
		return fmt.Errorf("%w: %s is not an int64 PN_COUNTER field", ErrUnsupported, fieldName)
	}
	goName := field.GoName
	err = bound.Update(key, func(value *T) error {
		if value == nil {
			return fmt.Errorf("%w: nil counter record", ErrUnsupported)
		}
		member := reflect.ValueOf(value).Elem().FieldByName(goName)
		if !member.IsValid() || !member.CanSet() || member.Kind() != reflect.Int64 {
			return fmt.Errorf("%w: counter field %s is not settable int64", ErrUnsupported, fieldName)
		}
		current := member.Int()
		const maxInt64 = int64(1<<63 - 1)
		const minInt64 = -1 << 63
		if delta > 0 && current > maxInt64-delta || delta < 0 && current < minInt64-delta {
			return fmt.Errorf("%w: counter field %s overflows int64", ErrUnsupported, fieldName)
		}
		member.SetInt(current + delta)
		return nil
	})
	if err != nil {
		return err
	}
	tx.counterOps = append(tx.counterOps, counterDelta{table: t.table.ID, row: key, field: field.ID, delta: delta})
	return nil
}

// Extrema stages a numeric MIN or MAX update and its RIME projection together.
func (t *Table[T]) Extrema(tx *Tx, key ids.RowID, fieldName string, candidate any, policy recordcodec.MergePolicy) error {
	bound, err := t.bound(tx)
	if err != nil {
		return err
	}
	field, ok := t.record.FieldByName(fieldName)
	if !ok || field.Merge != policy || (policy != recordcodec.MergeMin && policy != recordcodec.MergeMax) {
		return fmt.Errorf("%w: %s is not a %s extrema field", ErrUnsupported, fieldName, policy)
	}
	input := reflect.ValueOf(candidate)
	if !input.IsValid() || input.Type() != field.Descriptor.GoType {
		return fmt.Errorf("%w: extrema value for %s must have type %s", ErrUnsupported, fieldName, field.Descriptor.GoType)
	}
	value, err := extremaValue(input)
	if err != nil {
		return err
	}
	var before codec.Value
	err = bound.Update(key, func(record *T) error {
		if record == nil {
			return fmt.Errorf("%w: nil extrema record", ErrUnsupported)
		}
		member := reflect.ValueOf(record).Elem().FieldByName(field.GoName)
		if !member.IsValid() || !member.CanSet() || member.Type() != field.Descriptor.GoType {
			return fmt.Errorf("%w: extrema field %s is not writable", ErrUnsupported, fieldName)
		}
		before, err = extremaValue(member)
		if err != nil {
			return err
		}
		cmp, err := compareExtrema(value, before)
		if err != nil {
			return err
		}
		if policy == recordcodec.MergeMax && cmp > 0 || policy == recordcodec.MergeMin && cmp < 0 {
			member.Set(input)
		}
		return nil
	})
	if err != nil {
		return err
	}
	tx.extremaOps = append(tx.extremaOps, extremaOperation{table: t.table.ID, row: key, field: field.ID, policy: policy, before: before, value: value})
	return nil
}

// SetAdd stages one typed string addition to an OR_SET field.
func (t *Table[T]) SetAdd(tx *Tx, key ids.RowID, fieldName, value string) error {
	return t.changeSet(tx, key, fieldName, value, true)
}

// SetRemove stages one typed string removal from an OR_SET field.
func (t *Table[T]) SetRemove(tx *Tx, key ids.RowID, fieldName, value string) error {
	return t.changeSet(tx, key, fieldName, value, false)
}

func (t *Table[T]) changeSet(tx *Tx, key ids.RowID, fieldName, value string, add bool) error {
	bound, err := t.bound(tx)
	if err != nil {
		return err
	}
	field, ok := t.record.FieldByName(fieldName)
	if !ok || field.Merge != recordcodec.MergeORSet || field.Descriptor.GoType != reflect.TypeFor[[]string]() {
		return fmt.Errorf("%w: %s is not a []string OR_SET field", ErrUnsupported, fieldName)
	}
	changed := false
	err = bound.Update(key, func(record *T) error {
		if record == nil {
			return fmt.Errorf("%w: nil set record", ErrUnsupported)
		}
		member := reflect.ValueOf(record).Elem().FieldByName(field.GoName)
		if !member.IsValid() || !member.CanSet() || member.Type() != reflect.TypeFor[[]string]() {
			return fmt.Errorf("%w: OR_SET field %s is not writable []string", ErrUnsupported, fieldName)
		}
		values := append([]string{}, member.Interface().([]string)...)
		found := false
		for _, item := range values {
			if item == value {
				found = true
				break
			}
		}
		if add {
			if !found {
				values = append(values, value)
				sort.Strings(values)
				changed = true
			}
		} else if found {
			filtered := values[:0]
			for _, item := range values {
				if item != value {
					filtered = append(filtered, item)
				}
			}
			values = filtered
			changed = true
		}
		member.Set(reflect.ValueOf(values))
		return nil
	})
	if err != nil {
		return err
	}
	if !add && !changed {
		return nil
	}
	tx.setOps = append(tx.setOps, setOperation{table: t.table.ID, row: key, field: field.ID, value: value, add: add})
	return nil
}

// Rebuild materializes visible typed rows from authoritative state. Call it
// against a fresh RIME database before exposing table handles to applications.
// Batches bound temporary memory; an error leaves this adapter failed closed
// because earlier batches may already be visible in the private RIME database.
func (a *Adapter) Rebuild(ctx context.Context, batchSize int, observers ...func(RebuildProgress)) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if batchSize <= 0 {
		batchSize = 1024
	}
	var observe func(RebuildProgress)
	if len(observers) > 0 {
		observe = observers[0]
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.broken != nil {
		return fmt.Errorf("%w: %w", ErrMaterializer, a.broken)
	}
	if a.materialized {
		return fmt.Errorf("%w: rebuild must precede durable writes and run only once", ErrUnsupported)
	}
	tables := make([]binding, 0, len(a.bindings))
	for _, b := range a.bindings {
		tables = append(tables, b)
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].table.ID < tables[j].table.ID })
	var tx *rime.Tx
	count := 0
	var rebuildProgress RebuildProgress
	startBatch := func() error {
		var err error
		tx, err = a.db.BeginTx(ctx)
		return err
	}
	if err := startBatch(); err != nil {
		return err
	}
	flush := func() error {
		if count == 0 {
			_ = tx.Rollback()
			tx = nil
			return nil
		}
		prepared, err := tx.PrepareCommit()
		if err != nil {
			_ = tx.Rollback()
			tx = nil
			return err
		}
		if err := prepared.Publish(); err != nil {
			tx = nil
			return err
		}
		count = 0
		tx = nil
		return startBatch()
	}
	failed := true
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
		if failed {
			a.broken = errors.New("startup rebuild did not complete")
		}
	}()
	for _, b := range tables {
		if err := ctx.Err(); err != nil {
			return err
		}
		if b.ephemeral {
			continue
		}
		iterate := a.store.IterateTable
		if b.local {
			iterate = a.store.IterateLocalTable
		}
		err := iterate(b.table.ID, func(row *state.Row) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if a.rowResolver != nil {
				var err error
				row, err = a.rowResolver(row)
				if err != nil {
					return err
				}
			}
			rebuildProgress.CurrentTable = b.table.Name
			rebuildProgress.ProcessedItems += uint64(len(row.Cells))
			if !row.Visible() {
				rebuildProgress.RowsSkipped++
				if observe != nil {
					observe(rebuildProgress)
				}
				return nil
			}
			fields := make(map[uint32]codec.Value, len(row.Cells))
			for id, cell := range row.Cells {
				fields[id] = cloneCodecValue(cell.Value)
			}
			if err := b.load(tx, row.ID, fields); err != nil {
				return err
			}
			count++
			rebuildProgress.RowsInserted++
			if observe != nil {
				observe(rebuildProgress)
			}
			if count >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	if err := flush(); err != nil {
		return err
	}
	failed = false
	a.started = true
	a.materialized = true
	return nil
}

// ApplyRemote materializes rows affected by an already accepted remote merge.
// It reads complete current rows from one authoritative state snapshot, then
// prepares and publishes all affected typed rows as one RIME transaction. It
// never captures those changes as local Spool mutations. Any read, decode, or
// publication failure makes the adapter fail closed because state has already
// accepted the remote winners.
func (a *Adapter) ApplyRemote(ctx context.Context, result state.MergeResult) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !result.Applied || len(result.Winners) == 0 {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.broken != nil {
		return fmt.Errorf("%w: %w", ErrMaterializer, a.broken)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	seen := make(map[state.RowRef]struct{}, len(result.Winners))
	refs := make([]state.RowRef, 0, len(result.Winners))
	for _, winner := range result.Winners {
		var found *binding
		for _, b := range a.bindings {
			if b.table.ID == winner.TableID {
				copy := b
				found = &copy
				break
			}
		}
		if found == nil {
			continue // this adapter does not own the winning table
		}
		ref := state.RowRef{Table: winner.TableID, ID: winner.RowID}
		if _, ok := seen[ref]; !ok {
			seen[ref] = struct{}{}
			refs = append(refs, ref)
		}
	}
	if len(refs) == 0 {
		return nil
	}
	if len(refs) > state.MaxRowsPerStream {
		err := fmt.Errorf("%w: remote merge touches %d typed rows (limit %d)", ErrUnsupported, len(refs), state.MaxRowsPerStream)
		a.broken = err
		return fmt.Errorf("%w: %v", ErrMaterializer, err)
	}
	tx, err := a.db.BeginTx(ctx)
	if err != nil {
		a.broken = err
		return fmt.Errorf("%w: begin remote materialization: %w", ErrMaterializer, err)
	}
	err = a.store.ForEachRowSnapshot(ctx, refs, func(i int, row *state.Row) error {
		if a.rowResolver != nil {
			var err error
			row, err = a.rowResolver(row)
			if err != nil {
				return err
			}
		}
		if row.Table != refs[i].Table || row.ID != refs[i].ID {
			return fmt.Errorf("%w: authoritative row identity differs from requested row", ErrSchemaMismatch)
		}
		var b *binding
		for _, candidate := range a.bindings {
			if candidate.table.ID == row.Table {
				copy := candidate
				b = &copy
				break
			}
		}
		if b == nil {
			return nil
		}
		if !row.Visible() {
			return b.remove(tx, row.ID)
		} else {
			fields := make(map[uint32]codec.Value, len(row.Cells))
			for id, cell := range row.Cells {
				fields[id] = cloneCodecValue(cell.Value)
			}
			return b.load(tx, row.ID, fields)
		}
	})
	if err != nil {
		_ = tx.Rollback()
		a.broken = err
		return fmt.Errorf("%w: authoritative row snapshot or materialization: %w", ErrMaterializer, err)
	}
	prepared, err := tx.PrepareCommit()
	if err != nil {
		_ = tx.Rollback()
		a.broken = err
		return fmt.Errorf("%w: prepare remote materialization: %w", ErrMaterializer, err)
	}
	if err := prepared.Publish(); err != nil {
		a.broken = err
		return fmt.Errorf("%w: publish remote materialization: %w", ErrMaterializer, err)
	}
	a.started = true
	a.materialized = true
	return nil
}

func (a *Adapter) Write(ctx context.Context, fn func(*Tx) error) error {
	tx, err := a.Stage(ctx, fn)
	if err != nil {
		return err
	}
	return a.Commit(ctx, tx)
}

// Stage runs a transaction callback without acquiring the durable commit
// coordinator. Callers can prepare multiple independent callbacks in
// parallel, then serialize Commit through their persistence coordinator.
func (a *Adapter) Stage(ctx context.Context, fn func(*Tx) error) (*Tx, error) {
	if fn == nil {
		return nil, errors.New("rimeadapter: nil transaction callback")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	tx, err := a.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if err := call(tx, fn); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

// Begin opens a staged transaction. It reserves no commit-order resources;
// Commit validates the optimistic base and acquires the durable publication
// coordinator when the application is ready to commit.
func (a *Adapter) Begin(ctx context.Context) (*Tx, error) {
	if a == nil {
		return nil, errors.New("rimeadapter: nil adapter")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	if a.broken != nil {
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %w", ErrMaterializer, a.broken)
	}
	if fatal := a.store.Failed(); fatal != nil {
		a.broken = fatal
		a.mu.Unlock()
		return nil, fmt.Errorf("%w: %v", ErrMaterializer, fatal)
	}
	a.started = true
	a.mu.Unlock()
	inner, err := a.db.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	return &Tx{adapter: a, inner: inner}, nil
}

// Commit persists a staged transaction through Spool before publishing it to
// RIME. Every failure before durable acceptance aborts the RIME transaction;
// an ambiguous Spool failure returns an uncertain outcome and fails closed.
func (a *Adapter) Commit(ctx context.Context, tx *Tx) error {
	if a == nil || tx == nil || tx.adapter != a || tx.inner == nil || tx.done {
		return errors.New("rimeadapter: transaction is closed or belongs to another adapter")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		_ = tx.Rollback()
		tx.done = true
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.broken != nil {
		_ = tx.Rollback()
		tx.done = true
		return fmt.Errorf("%w: %w", ErrMaterializer, a.broken)
	}
	prepared, err := tx.inner.PrepareCommit()
	if err != nil {
		_ = tx.Rollback()
		tx.done = true
		return err
	}
	batch, localMutations, ephemeralChanged, err := a.batch(prepared.Changes(), tx.counterOps, tx.setOps, tx.extremaOps)
	if err != nil {
		_ = prepared.Abort()
		tx.done = true
		return err
	}
	if a.localHook != nil {
		if err := a.localHook(batch); err != nil {
			_ = prepared.Abort()
			tx.done = true
			return err
		}
	}
	if len(batch.Mutations) == 0 && len(localMutations) == 0 {
		tx.done = true
		if ephemeralChanged {
			a.materialized = true
			if err := prepared.Publish(); err != nil {
				a.broken = err
				return fmt.Errorf("%w: %v", ErrMaterializer, err)
			}
			a.publicationGeneration++
			return nil
		}
		_ = prepared.Abort()
		return nil
	}
	if fatal := a.store.Failed(); fatal != nil {
		_ = prepared.Abort()
		tx.done = true
		a.broken = fatal
		return fmt.Errorf("%w: %v", ErrMaterializer, fatal)
	}
	if a.beforeCommit != nil {
		if err := a.beforeCommit(); err != nil {
			_ = prepared.Abort()
			tx.done = true
			return err
		}
	}
	var commitErr error
	switch {
	case len(batch.Mutations) == 0:
		localBatch := *batch
		localBatch.Mutations = localMutations
		_, commitErr = a.store.CommitLocalRecords(ctx, &localBatch)
	case len(localMutations) > 0:
		_, commitErr = a.store.CommitLocalWithRecords(ctx, batch, localMutations)
	default:
		_, commitErr = a.store.CommitLocal(ctx, batch)
	}
	if commitErr != nil {
		_ = prepared.Abort()
		tx.done = true
		if fatal := a.store.Failed(); fatal != nil {
			a.materialized = true
			a.broken = fatal
			return &UncertainCommitError{TxID: batch.TxID, Cause: commitErr}
		}
		return commitErr
	}
	if a.afterCommit != nil {
		if err := a.afterCommit(); err != nil {
			_ = prepared.Abort()
			tx.done = true
			a.materialized = true
			a.broken = err
			return &UncertainCommitError{TxID: batch.TxID, Cause: err}
		}
	}
	a.materialized = true
	if err := prepared.Publish(); err != nil {
		a.broken = err
		tx.done = true
		return fmt.Errorf("%w (transaction %x): %v", ErrMaterializer, batch.TxID, err)
	}
	a.publicationGeneration++
	tx.done = true
	return nil
}

// Rollback discards staged changes without touching durable state.
func (tx *Tx) Rollback() error {
	if tx == nil || tx.done || tx.inner == nil {
		return nil
	}
	tx.done = true
	return tx.inner.Rollback()
}

func call(tx *Tx, fn func(*Tx) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = tx.inner.Rollback()
			tx.done = true
			panic(recovered)
		}
	}()
	return fn(tx)
}

func (a *Adapter) batch(changes []rime.PreparedChange, counterOps []counterDelta, setOps []setOperation, extremaOps []extremaOperation) (*codec.MutationBatch, []codec.Mutation, bool, error) {
	b, err := a.batchAll(changes, counterOps, setOps, extremaOps)
	if err != nil {
		return nil, nil, false, err
	}
	local := make([]codec.Mutation, 0)
	replicated := b.Mutations[:0]
	ephemeralChanged, durableChanged := false, false
	for _, change := range changes {
		bound, ok := a.bindings[change.Table]
		if !ok {
			return nil, nil, false, fmt.Errorf("%w: unknown changed table %q", ErrSchemaMismatch, change.Table)
		}
		if bound.ephemeral {
			ephemeralChanged = true
		} else {
			durableChanged = true
		}
	}
	if ephemeralChanged && durableChanged {
		return nil, nil, false, fmt.Errorf("%w: one transaction cannot mix ephemeral and durable tables", ErrUnsupported)
	}
	for _, mutation := range b.Mutations {
		binding, ok := a.bindingsByID(mutation.TableID)
		if !ok {
			return nil, nil, false, fmt.Errorf("%w: mutation references unknown table %d", ErrSchemaMismatch, mutation.TableID)
		}
		if binding.ephemeral {
			continue
		} else if binding.local {
			local = append(local, mutation)
		} else {
			replicated = append(replicated, mutation)
		}
	}
	b.Mutations = replicated
	return b, local, ephemeralChanged, nil
}

func (a *Adapter) batchAll(changes []rime.PreparedChange, counterOps []counterDelta, setOps []setOperation, extremaOps []extremaOperation) (*codec.MutationBatch, error) {
	epoch, hash, err := a.store.SchemaEpoch()
	if err != nil {
		return nil, err
	}
	b := &codec.MutationBatch{DBID: a.store.DBID(), OriginNode: a.store.NodeID(), TxID: ids.NewTxID(), HLC: a.store.ClockNow(), SchemaEpoch: epoch, SchemaHash: hash}
	deltas := make(map[counterKey]*big.Int)
	deletedRows := make(map[typedRowKey]struct{})
	for _, op := range counterOps {
		key := counterKey{table: op.table, row: op.row, field: op.field}
		if deltas[key] == nil {
			deltas[key] = new(big.Int)
		}
		deltas[key].Add(deltas[key], big.NewInt(op.delta))
	}
	for _, change := range changes {
		bound, ok := a.bindings[change.Table]
		if !ok {
			return nil, fmt.Errorf("%w: unknown changed table %q", ErrSchemaMismatch, change.Table)
		}
		key, err := rowID(change.Key)
		if err != nil {
			return nil, err
		}
		if change.Operation == rime.OpDelete {
			deletedRows[typedRowKey{table: bound.table.ID, row: key}] = struct{}{}
			b.Mutations = append(b.Mutations, codec.Mutation{Policy: schema.LWW, TableID: bound.table.ID, RowID: key, ColumnID: codec.ColumnTombstone, Flags: codec.FlagTombstone})
			continue
		}
		if change.New == nil {
			return nil, fmt.Errorf("%w: non-delete change has no final record", ErrUnsupported)
		}
		recordID, err := primaryID(bound.record, change.New)
		if err != nil || recordID != key {
			return nil, fmt.Errorf("%w: changed record primary key does not match RIME key", ErrSchemaMismatch)
		}
		var storedFields map[uint32]codec.CellState
		if bound.local {
			storedFields, _, _, err = a.store.GetLocalRow(bound.table.ID, key)
		} else if !bound.ephemeral {
			storedFields, err = a.store.GetRow(bound.table.ID, key)
		} else {
			storedFields = make(map[uint32]codec.CellState)
		}
		if err != nil {
			return nil, fmt.Errorf("rimeadapter: read prior row for unknown-field retention: %w", err)
		}
		for _, field := range bound.record.Fields {
			column := bound.table.ColumnByID(field.ID)
			if column == nil {
				return nil, fmt.Errorf("%w: field %d is absent from the durable schema", ErrSchemaMismatch, field.ID)
			}
			if field.Merge == recordcodec.MergeCounter {
				oldValue, nextValue := int64(0), int64(0)
				if change.Old != nil {
					oldValue, err = counterFieldValue(change.Old, field)
					if err != nil {
						return nil, err
					}
				}
				nextValue, err = counterFieldValue(change.New, field)
				if err != nil {
					return nil, err
				}
				delta := deltas[counterKey{table: bound.table.ID, row: key, field: field.ID}]
				if delta == nil {
					delta = new(big.Int)
				}
				want := new(big.Int).Add(big.NewInt(oldValue), delta)
				if !want.IsInt64() || want.Int64() != nextValue {
					return nil, fmt.Errorf("%w: counter field %s changed without matching CounterAdd operations", ErrUnsupported, field.Path)
				}
				if change.Old == nil && len(counterOpsFor(counterOps, bound.table.ID, key, field.ID)) == 0 {
					if nextValue != 0 {
						return nil, fmt.Errorf("%w: new counter field %s must start at zero or use CounterAdd", ErrUnsupported, field.Path)
					}
					b.Mutations = append(b.Mutations, codec.Mutation{Policy: schema.PN_COUNTER, TableID: bound.table.ID, RowID: key, ColumnID: field.ID, Value: codec.Text("0")})
				}
				continue
			}
			if field.Merge == recordcodec.MergeORSet {
				oldValues, nextValues := []string{}, []string{}
				if change.Old != nil {
					oldValues, err = stringSetFieldValue(change.Old, field)
					if err != nil {
						return nil, err
					}
				}
				nextValues, err = stringSetFieldValue(change.New, field)
				if err != nil {
					return nil, err
				}
				ops := setOpsFor(setOps, bound.table.ID, key, field.ID)
				expected := append([]string{}, oldValues...)
				for _, op := range ops {
					expected = applySetProjection(expected, op.value, op.add)
				}
				if !equalStringSlice(expected, nextValues) {
					return nil, fmt.Errorf("%w: OR_SET field %s changed without matching SetAdd/SetRemove operations", ErrUnsupported, field.Path)
				}
				if change.Old == nil && len(ops) == 0 {
					if len(nextValues) != 0 {
						return nil, fmt.Errorf("%w: new OR_SET field %s must start empty or use SetAdd", ErrUnsupported, field.Path)
					}
					b.Mutations = append(b.Mutations, codec.Mutation{Policy: schema.OR_SET, TableID: bound.table.ID, RowID: key, ColumnID: field.ID, Value: codec.Text("[]")})
				}
				continue
			}
			if field.Merge == recordcodec.MergeMin || field.Merge == recordcodec.MergeMax {
				oldValue, nextValue := codec.Value{}, codec.Value{}
				if change.Old != nil {
					oldValue, err = extremaFieldValue(change.Old, field)
					if err != nil {
						return nil, err
					}
				}
				nextValue, err = extremaFieldValue(change.New, field)
				if err != nil {
					return nil, err
				}
				ops := extremaOpsFor(extremaOps, bound.table.ID, key, field.ID)
				if change.Old == nil {
					seed := nextValue
					if len(ops) > 0 {
						seed = ops[0].before
					}
					b.Mutations = append(b.Mutations, codec.Mutation{Policy: column.MergePolicy, TableID: bound.table.ID, RowID: key, ColumnID: field.ID, Value: seed})
				} else if len(ops) == 0 && !oldValue.Equal(nextValue) {
					return nil, fmt.Errorf("%w: extrema field %s changed without RecordMin/RecordMax", ErrUnsupported, field.Path)
				}
				if len(ops) > 0 {
					projected := ops[0].before
					if change.Old != nil && !projected.Equal(oldValue) {
						return nil, fmt.Errorf("%w: extrema operation base does not match committed value for %s", ErrSchemaMismatch, field.Path)
					}
					for _, op := range ops {
						if op.policy != field.Merge || !op.before.Equal(projected) {
							return nil, fmt.Errorf("%w: invalid extrema operation chain for %s", ErrSchemaMismatch, field.Path)
						}
						cmp, err := compareExtrema(op.value, projected)
						if err != nil {
							return nil, err
						}
						if op.policy == recordcodec.MergeMax && cmp > 0 || op.policy == recordcodec.MergeMin && cmp < 0 {
							projected = op.value
						}
						b.Mutations = append(b.Mutations, codec.Mutation{Policy: column.MergePolicy, TableID: bound.table.ID, RowID: key, ColumnID: field.ID, Value: op.value})
					}
					if !projected.Equal(nextValue) {
						return nil, fmt.Errorf("%w: extrema projection differs from operations for %s", ErrUnsupported, field.Path)
					}
				}
				continue
			}
			if column.MergePolicy != schema.LWW || column.Type != schema.ColBlob {
				return nil, fmt.Errorf("%w: field %d is not an LWW BLOB", ErrUnsupported, field.ID)
			}
			var unknown []recordcodec.UnknownField
			if stored, ok := storedFields[field.ID]; ok {
				if stored.Value.Type != codec.TypeBlob {
					return nil, fmt.Errorf("%w: stored field %d is not an encoded record blob", ErrSchemaMismatch, field.ID)
				}
				_, unknown, err = recordcodec.DecodeFieldWithUnknown(bound.record, field.ID, stored.Value.B, bound.codecs, a.limits)
				if err != nil {
					return nil, fmt.Errorf("rimeadapter: decode prior field %d for unknown-field retention: %w", field.ID, err)
				}
			}
			next, err := recordcodec.EncodeFieldWithUnknown(bound.record, field.ID, change.New, unknown, bound.codecs, a.limits)
			if err != nil {
				return nil, err
			}
			if change.Old != nil {
				equal, err := recordcodec.EqualField(bound.record, field.ID, change.Old, change.New, bound.codecs)
				if err != nil {
					return nil, err
				}
				// A newer update can resurrect a row after a concurrent
				// tombstone. Re-emit its immutable identity so the durable
				// row remains self-describing even when older cells are
				// hidden by that tombstone during merge/rebuild.
				if equal && field.ID != bound.record.PrimaryID {
					continue
				}
			}
			b.Mutations = append(b.Mutations, codec.Mutation{Policy: schema.LWW, TableID: bound.table.ID, RowID: key, ColumnID: field.ID, Value: codec.Blob(next)})
		}
	}
	for _, op := range extremaOps {
		if _, deleted := deletedRows[typedRowKey{table: op.table, row: op.row}]; deleted {
			return nil, fmt.Errorf("%w: extrema operation targets a deleted typed row", ErrUnsupported)
		}
	}
	for _, op := range counterOps {
		if _, deleted := deletedRows[typedRowKey{table: op.table, row: op.row}]; deleted {
			return nil, fmt.Errorf("%w: counter operation targets a deleted typed row", ErrUnsupported)
		}
		bound, ok := a.bindingsByID(op.table)
		if !ok {
			return nil, fmt.Errorf("%w: counter operation references unknown table %d", ErrSchemaMismatch, op.table)
		}
		field, ok := bound.record.FieldByID(op.field)
		column := bound.table.ColumnByID(op.field)
		if !ok || field.Merge != recordcodec.MergeCounter || column == nil || column.MergePolicy != schema.PN_COUNTER {
			return nil, fmt.Errorf("%w: counter operation references incompatible field %d", ErrSchemaMismatch, op.field)
		}
		b.Mutations = append(b.Mutations, codec.Mutation{Policy: schema.PN_COUNTER, TableID: op.table, RowID: op.row, ColumnID: op.field, Flags: codec.FlagCounterDelta, Value: codec.Text(big.NewInt(op.delta).String())})
	}
	setRecords := make(map[counterKey]map[string][]byte)
	for i, op := range setOps {
		if _, deleted := deletedRows[typedRowKey{table: op.table, row: op.row}]; deleted {
			return nil, fmt.Errorf("%w: set operation targets a deleted typed row", ErrUnsupported)
		}
		bound, ok := a.bindingsByID(op.table)
		if !ok {
			return nil, fmt.Errorf("%w: set operation references unknown table %d", ErrSchemaMismatch, op.table)
		}
		field, ok := bound.record.FieldByID(op.field)
		column := bound.table.ColumnByID(op.field)
		if !ok || field.Merge != recordcodec.MergeORSet || column == nil || column.MergePolicy != schema.OR_SET {
			return nil, fmt.Errorf("%w: set operation references incompatible field %d", ErrSchemaMismatch, op.field)
		}
		cell := counterKey{table: op.table, row: op.row, field: op.field}
		records := setRecords[cell]
		if records == nil {
			current, err := a.store.CRDTRecords(op.table, op.row, op.field)
			if err != nil {
				return nil, err
			}
			records = make(map[string][]byte, len(current)+len(setOps))
			for _, record := range current {
				records[string(record.Key)] = append([]byte(nil), record.Data...)
			}
			setRecords[cell] = records
		}
		encoded, err := codec.SetString(op.value).Encode()
		if err != nil {
			return nil, err
		}
		mutation := codec.Mutation{Policy: schema.OR_SET, TableID: op.table, RowID: op.row, ColumnID: op.field, Value: codec.Null()}
		if op.add {
			tag := state.SetTag(b.DBID, b.OriginNode, b.TxID, uint32(i))
			mutation.Records = append(mutation.Records, codec.CRDTRecord{Key: tag, Data: encoded})
			records[string(tag)] = encoded
		} else {
			keys := make([]string, 0, len(records))
			for tag := range records {
				if len(tag) == 53 && tag[0] == 'a' {
					keys = append(keys, tag)
				}
			}
			sort.Strings(keys)
			for _, tag := range keys {
				if !bytes.Equal(records[tag], encoded) {
					continue
				}
				removed := "r" + tag[1:]
				if _, exists := records[removed]; exists {
					continue
				}
				mutation.Records = append(mutation.Records, codec.CRDTRecord{Key: []byte(removed), Data: append([]byte(nil), encoded...)})
				records[removed] = append([]byte(nil), encoded...)
			}
		}
		if len(mutation.Records) == 0 {
			if !op.add {
				return nil, fmt.Errorf("%w: OR_SET projection has no matching causal additions to remove", ErrSchemaMismatch)
			}
			return nil, fmt.Errorf("%w: OR_SET add generated no causal record", ErrSchemaMismatch)
		}
		b.Mutations = append(b.Mutations, mutation)
	}
	return b, nil
}

type counterKey struct {
	table uint32
	row   ids.RowID
	field uint32
}

type typedRowKey struct {
	table uint32
	row   ids.RowID
}

func counterOpsFor(ops []counterDelta, table uint32, row ids.RowID, field uint32) []counterDelta {
	var matched []counterDelta
	for _, op := range ops {
		if op.table == table && op.row == row && op.field == field {
			matched = append(matched, op)
		}
	}
	return matched
}

func setOpsFor(ops []setOperation, table uint32, row ids.RowID, field uint32) []setOperation {
	var matched []setOperation
	for _, op := range ops {
		if op.table == table && op.row == row && op.field == field {
			matched = append(matched, op)
		}
	}
	return matched
}

func extremaOpsFor(ops []extremaOperation, table uint32, row ids.RowID, field uint32) []extremaOperation {
	var matched []extremaOperation
	for _, op := range ops {
		if op.table == table && op.row == row && op.field == field {
			matched = append(matched, op)
		}
	}
	return matched
}

func extremaFieldValue(record any, field recordcodec.Field) (codec.Value, error) {
	v := reflect.ValueOf(record)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return codec.Value{}, fmt.Errorf("%w: nil extrema record", ErrUnsupported)
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return codec.Value{}, fmt.Errorf("%w: extrema record is not a struct", ErrUnsupported)
	}
	member := v.FieldByName(field.GoName)
	if !member.IsValid() {
		return codec.Value{}, fmt.Errorf("%w: extrema field %s is missing", ErrUnsupported, field.Path)
	}
	return extremaValue(member)
}

func extremaValue(value reflect.Value) (codec.Value, error) {
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return codec.Int(value.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := value.Uint()
		if u > math.MaxInt64 {
			return codec.Value{}, fmt.Errorf("%w: unsigned extrema exceeds int64 storage", ErrUnsupported)
		}
		return codec.Int(int64(u)), nil
	case reflect.Float32, reflect.Float64:
		f := value.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return codec.Value{}, fmt.Errorf("%w: extrema values must be finite", ErrUnsupported)
		}
		return codec.Real(f), nil
	default:
		return codec.Value{}, fmt.Errorf("%w: extrema values must be numeric", ErrUnsupported)
	}
}

func compareExtrema(left, right codec.Value) (int, error) {
	if left.Type != right.Type {
		return 0, fmt.Errorf("%w: extrema value types differ", ErrSchemaMismatch)
	}
	switch left.Type {
	case codec.TypeInteger:
		if left.I < right.I {
			return -1, nil
		}
		if left.I > right.I {
			return 1, nil
		}
		return 0, nil
	case codec.TypeReal:
		if math.IsNaN(left.F) || math.IsInf(left.F, 0) || math.IsNaN(right.F) || math.IsInf(right.F, 0) {
			return 0, fmt.Errorf("%w: extrema values must be finite", ErrUnsupported)
		}
		if left.F < right.F {
			return -1, nil
		}
		if left.F > right.F {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("%w: extrema projection is not numeric", ErrSchemaMismatch)
	}
}

func setExtremaReflect(dst reflect.Value, value codec.Value) error {
	if !dst.IsValid() || !dst.CanSet() {
		return fmt.Errorf("field is not settable")
	}
	switch dst.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value.Type != codec.TypeInteger || dst.OverflowInt(value.I) {
			return fmt.Errorf("integer projection overflows %s", dst.Type())
		}
		dst.SetInt(value.I)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if value.Type != codec.TypeInteger || value.I < 0 || dst.OverflowUint(uint64(value.I)) {
			return fmt.Errorf("integer projection overflows %s", dst.Type())
		}
		dst.SetUint(uint64(value.I))
	case reflect.Float32, reflect.Float64:
		var projected float64
		switch value.Type {
		case codec.TypeInteger:
			projected = float64(value.I)
		case codec.TypeReal:
			projected = value.F
		default:
			return fmt.Errorf("real projection overflows %s", dst.Type())
		}
		if math.IsNaN(projected) || math.IsInf(projected, 0) || dst.OverflowFloat(projected) {
			return fmt.Errorf("real projection overflows %s", dst.Type())
		}
		dst.SetFloat(projected)
	default:
		return fmt.Errorf("field type %s is not numeric", dst.Type())
	}
	return nil
}

func stringSetFieldValue(record any, field recordcodec.Field) ([]string, error) {
	value := reflect.ValueOf(record)
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, fmt.Errorf("%w: nil OR_SET record", ErrUnsupported)
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: OR_SET record is not a struct", ErrUnsupported)
	}
	member := value.FieldByName(field.GoName)
	if !member.IsValid() || member.Type() != reflect.TypeFor[[]string]() {
		return nil, fmt.Errorf("%w: OR_SET field %s must be []string", ErrUnsupported, field.Path)
	}
	return append([]string{}, member.Interface().([]string)...), nil
}

func applySetProjection(values []string, value string, add bool) []string {
	found := false
	for _, item := range values {
		if item == value {
			found = true
			break
		}
	}
	if add {
		if !found {
			values = append(values, value)
			sort.Strings(values)
		}
		return values
	}
	if !found {
		return values
	}
	filtered := values[:0]
	for _, item := range values {
		if item != value {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

func equalStringSlice(a, b []string) bool {
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

func (a *Adapter) bindingsByID(tableID uint32) (binding, bool) {
	for _, bound := range a.bindings {
		if bound.table.ID == tableID {
			return bound, true
		}
	}
	return binding{}, false
}

func counterFieldValue(record any, field recordcodec.Field) (int64, error) {
	v := reflect.ValueOf(record)
	for v.IsValid() && v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 0, fmt.Errorf("%w: nil counter record", ErrUnsupported)
		}
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return 0, fmt.Errorf("%w: counter field %s has invalid record", ErrUnsupported, field.Path)
	}
	member := v.FieldByName(field.GoName)
	if !member.IsValid() || member.Kind() != reflect.Int64 {
		return 0, fmt.Errorf("%w: counter field %s is not int64", ErrUnsupported, field.Path)
	}
	return member.Int(), nil
}

func rowID(value any) (ids.RowID, error) {
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Pointer && !v.IsNil() {
		v = v.Elem()
	}
	if v.Kind() != reflect.Array || v.Len() != len(ids.RowID{}) || v.Type().Elem().Kind() != reflect.Uint8 {
		return ids.RowID{}, fmt.Errorf("%w: primary key must be a 16-byte array, got %T", ErrUnsupported, value)
	}
	var id ids.RowID
	for i := range id {
		id[i] = byte(v.Index(i).Uint())
	}
	return id, nil
}

func primaryID(schema *recordcodec.Schema, value any) (ids.RowID, error) {
	f, ok := schema.FieldByID(schema.PrimaryID)
	if !ok {
		return ids.RowID{}, fmt.Errorf("%w: primary field is absent", ErrSchemaMismatch)
	}
	v := reflect.ValueOf(value)
	for v.IsValid() && v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return ids.RowID{}, fmt.Errorf("%w: nil record", ErrSchemaMismatch)
		}
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return ids.RowID{}, fmt.Errorf("%w: primary key source is not a record", ErrSchemaMismatch)
	}
	key := v.FieldByName(f.GoName)
	if !key.IsValid() || !key.CanInterface() {
		return ids.RowID{}, fmt.Errorf("%w: primary field %q is inaccessible", ErrSchemaMismatch, f.GoName)
	}
	return rowID(key.Interface())
}

func nativeRecord(record any) any {
	if carrier, ok := record.(*any); ok {
		return *carrier
	}
	return record
}

func recordPointer(record any) any {
	v := reflect.ValueOf(record)
	if v.Kind() == reflect.Pointer {
		return record
	}
	p := reflect.New(v.Type())
	p.Elem().Set(v)
	return p.Interface()
}

// SetModelPolicy configures local identity rules before this handle is published.
func (t *Table[T]) SetModelPolicy(prepare func(any, bool) error, immutable func(any, any) error) {
	t.prepare, t.immutable = prepare, immutable
}

// Batch uses RIME's operation savepoint and retains preceding staged writes.
func (tx *Tx) Batch(operation string, n int, apply func(int) error) error {
	if tx == nil || tx.done || tx.inner == nil {
		return rime.ErrTxClosed
	}
	c, s, e := len(tx.counterOps), len(tx.setOps), len(tx.extremaOps)
	err := tx.inner.Batch(operation, n, apply)
	if err != nil {
		tx.counterOps = tx.counterOps[:c]
		tx.setOps = tx.setOps[:s]
		tx.extremaOps = tx.extremaOps[:e]
	}
	return err
}

// RecordType identifies the native struct behind typed or runtime registrations.
func (t *Table[T]) RecordType() reflect.Type { return t.record.Record.GoType }

// DynamicFieldOf returns a checked runtime scalar or timestamp field.
func DynamicFieldOf[T any](table *Table[T], name string) (rime.DynamicField[T], error) {
	return rime.DynamicFieldOf(table.inner, name)
}
