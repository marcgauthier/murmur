package murmur

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/recordcodec"
	"github.com/marcgauthier/murmur/internal/rimeadapter"
	"github.com/marcgauthier/murmur/replication"
	"github.com/marcgauthier/murmur/rime"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/state"
)

// RecordMergePolicy selects the replicated merge rule for a top-level record
// field. Rich record persistence materializes LWW fields, numeric MIN/MAX,
// top-level int64 PN_COUNTER fields, and top-level []string OR_SET fields
// through explicit typed operations.
type RecordMergePolicy string

const (
	RecordMergeLWW     RecordMergePolicy = "lww"
	RecordMergeMin     RecordMergePolicy = "min"
	RecordMergeMax     RecordMergePolicy = "max"
	RecordMergeCounter RecordMergePolicy = "counter"
	RecordMergeORSet   RecordMergePolicy = "orset"
)

// RecordOptions defines stable record identity and field semantics. Every
// persisted field, including nested fields, needs an explicit stable ID.
type RecordOptions struct {
	PrimaryField  string
	FieldIDs      map[string]uint32
	MergePolicies map[string]RecordMergePolicy
	Scope         TableScope
	Codecs        []RecordCodec
	MaxDepth      int
}

// TableScope controls whether table values enter the replication protocol.
// Replicated tables are the default; node-local tables persist only on the
// current node and never enter replication logs or snapshots.
type TableScope uint8

const (
	TableScopeReplicated TableScope = iota
	TableScopeNodeLocal
	TableScopeEphemeral
)

// RecordCodec defines the stable, deterministic encoding for one custom Go
// field type. Applications on every replica must use the same identity and
// equivalent codec behavior.
type RecordCodec struct {
	ID      string
	Version uint16
	Example any
	Encode  func(any) ([]byte, error)
	Decode  func([]byte, any) error
	Clone   func(any) (any, error)
	Equal   func(any, any) bool
}

// TableDefinition is an immutable, compiled typed-table definition for Config.
// Construct it with Define[T].
type TableDefinition struct {
	name      string
	id        uint32
	local     bool
	ephemeral bool

	table       schema.TableSchema
	register    func(*rimeadapter.Adapter) (any, error)
	bridgeExist func(any, ids.RowID) (bool, error)
	bridgeCheck func(uint32, ids.RowID, codec.Value) error
}

// BridgeSourceReceipt binds one imported Low transaction identity to its
// committed typed bridge effects.
type BridgeSourceReceipt struct {
	TxID     ids.TxID
	Origin   ids.NodeID
	Sequence uint64
}

// Define compiles T into a stable rich-record schema. Runtime RIME field/index
// options remain on T's `rime` tags; replicated field IDs are supplied here.
func Define[T any](name string, tableID uint32, options RecordOptions) (TableDefinition, error) {
	if name == "" || tableID == 0 {
		return TableDefinition{}, fmt.Errorf("murmur: record table name and nonzero ID are required: %w", ErrUnsupportedSchema)
	}
	if options.Scope != TableScopeReplicated && options.Scope != TableScopeNodeLocal && options.Scope != TableScopeEphemeral {
		return TableDefinition{}, fmt.Errorf("murmur: invalid table scope: %w", ErrUnsupportedSchema)
	}
	fieldIDs := make(map[string]uint32, len(options.FieldIDs))
	for path, id := range options.FieldIDs {
		fieldIDs[path] = id
	}
	policies := make(map[string]recordcodec.MergePolicy, len(options.MergePolicies))
	for path, policy := range options.MergePolicies {
		policies[path] = recordcodec.MergePolicy(policy)
	}
	codecs := recordcodec.NewCodecRegistry()
	for _, codec := range options.Codecs {
		if err := codecs.RegisterCodec(codec.ID, codec.Version, codec.Example, codec.Encode, codec.Decode, codec.Clone, codec.Equal); err != nil {
			return TableDefinition{}, fmt.Errorf("murmur: register record codec %q: %w", codec.ID, err)
		}
	}
	compile := recordcodec.CompileOptions{TableID: tableID, PrimaryField: options.PrimaryField, FieldIDs: fieldIDs, MergePolicies: policies, Codecs: codecs, MaxDepth: options.MaxDepth}
	typ := reflect.TypeFor[T]()
	if typ.Kind() != reflect.Struct {
		return TableDefinition{}, fmt.Errorf("murmur: typed table records must be structs: %w", ErrUnsupportedSchema)
	}
	primary, ok := typ.FieldByName(options.PrimaryField)
	if !ok || !hasRIMEPrimaryTag(primary.Tag.Get("rime")) {
		return TableDefinition{}, fmt.Errorf("murmur: primary field %q must carry the rime primary tag: %w", options.PrimaryField, ErrUnsupportedSchema)
	}
	record, err := recordcodec.Compile(typ, compile)
	if err != nil {
		return TableDefinition{}, fmt.Errorf("murmur: define record table %q: %w", name, err)
	}
	descriptor, err := recordcodec.MarshalDescriptor(record)
	if err != nil {
		return TableDefinition{}, err
	}
	table := schema.TableSchema{ID: tableID, Name: name, PK: record.PrimaryID, RecordDescriptor: descriptor}
	for _, field := range record.Fields {
		if options.Scope != TableScopeReplicated && field.Merge != recordcodec.MergeLWW {
			return TableDefinition{}, fmt.Errorf("murmur: local table field %s must use LWW: %w", field.Path, ErrUnsupportedSchema)
		}
		merge, err := schemaMergePolicy(field.Merge)
		if err != nil {
			return TableDefinition{}, fmt.Errorf("murmur: field %s: %w", field.Path, err)
		}
		columnType := schema.ColBlob
		nullable := field.ID != record.PrimaryID
		switch field.Merge {
		case recordcodec.MergeLWW:
		case recordcodec.MergeCounter:
			if field.Descriptor.Kind != recordcodec.KindInt || field.Descriptor.GoType.Kind() != reflect.Int64 {
				return TableDefinition{}, fmt.Errorf("murmur: counter field %s must be int64: %w", field.Path, ErrUnsupportedSchema)
			}
			columnType = schema.ColText
			nullable = false
		case recordcodec.MergeORSet:
			if field.Descriptor.Kind != recordcodec.KindSlice || field.Descriptor.Element.Kind != recordcodec.KindString || field.Descriptor.GoType.Kind() != reflect.Slice || field.Descriptor.GoType.Elem().Kind() != reflect.String {
				return TableDefinition{}, fmt.Errorf("murmur: OR_SET field %s must be a []string: %w", field.Path, ErrUnsupportedSchema)
			}
			columnType = schema.ColText
			nullable = false
		case recordcodec.MergeMin, recordcodec.MergeMax:
			switch field.Descriptor.Kind {
			case recordcodec.KindInt, recordcodec.KindUint:
				if field.Descriptor.Kind == recordcodec.KindUint && field.Descriptor.GoType.Bits() > 63 {
					return TableDefinition{}, fmt.Errorf("murmur: unsigned extrema field %s exceeds the signed durable integer range: %w", field.Path, ErrUnsupportedSchema)
				}
				columnType = schema.ColInteger
			case recordcodec.KindFloat32, recordcodec.KindFloat64:
				columnType = schema.ColReal
			default:
				return TableDefinition{}, fmt.Errorf("murmur: extrema field %s must be numeric: %w", field.Path, ErrUnsupportedSchema)
			}
			nullable = false
		default:
			return TableDefinition{}, fmt.Errorf("murmur: unsupported merge policy for field %s: %w", field.Path, ErrUnsupportedSchema)
		}
		table.Columns = append(table.Columns, schema.ColumnSchema{
			ID: field.ID, Name: field.GoName, Type: columnType,
			Nullable: nullable, MergePolicy: merge,
		})
	}
	definition := TableDefinition{name: name, id: tableID, local: options.Scope == TableScopeNodeLocal, ephemeral: options.Scope == TableScopeEphemeral, table: table}
	definition.register = func(adapter *rimeadapter.Adapter) (any, error) {
		if options.Scope == TableScopeNodeLocal {
			return rimeadapter.RegisterLocal[T](adapter, name, table, compile)
		}
		if options.Scope == TableScopeEphemeral {
			return rimeadapter.RegisterEphemeral[T](adapter, name, table, compile)
		}
		return rimeadapter.Register[T](adapter, name, compile)
	}
	definition.bridgeExist = func(handle any, key ids.RowID) (bool, error) {
		table, ok := handle.(*rimeadapter.Table[T])
		if !ok || table == nil {
			return false, fmt.Errorf("murmur: typed bridge table %q is unavailable: %w", name, ErrUnsupportedSchema)
		}
		_, exists, err := table.Lookup(key)
		return exists, err
	}
	definition.bridgeCheck = func(fieldID uint32, key ids.RowID, value codec.Value) error {
		column := table.ColumnByID(fieldID)
		field, ok := record.FieldByID(fieldID)
		if column == nil || !ok {
			return fmt.Errorf("murmur: typed bridge field %d is not defined in %q: %w", fieldID, name, ErrUnsupportedSchema)
		}
		if column.MergePolicy != schema.LWW {
			return nil // Merge values and causal records are validated by Store.
		}
		if value.Type != codec.TypeBlob {
			return fmt.Errorf("murmur: typed bridge field %s.%s must be a canonical BLOB", name, field.Path)
		}
		decoded, _, err := recordcodec.DecodeFieldWithUnknown(record, fieldID, value.B, codecs, recordcodec.Limits{})
		if err != nil {
			return fmt.Errorf("murmur: typed bridge field %s.%s: %w", name, field.Path, err)
		}
		if fieldID == record.PrimaryID {
			got := reflect.ValueOf(decoded)
			if !got.IsValid() || got.Kind() != reflect.Array || got.Len() != len(key) || got.Type().Elem().Kind() != reflect.Uint8 {
				return fmt.Errorf("murmur: typed bridge primary field %s.%s is not a RowID", name, field.Path)
			}
			for i := range key {
				if byte(got.Index(i).Uint()) != key[i] {
					return fmt.Errorf("murmur: typed bridge primary field %s.%s does not match row identity", name, field.Path)
				}
			}
		}
		return nil
	}
	return definition, nil
}

// TableName returns the registered durable table name.
func (d TableDefinition) TableName() string { return d.name }

// TableID returns the stable durable table identifier.
func (d TableDefinition) TableID() uint32 { return d.id }

func hasRIMEPrimaryTag(tag string) bool {
	for _, directive := range strings.Split(tag, ",") {
		if strings.TrimSpace(directive) == "primary" {
			return true
		}
	}
	return false
}

func schemaMergePolicy(policy recordcodec.MergePolicy) (schema.MergePolicy, error) {
	switch policy {
	case recordcodec.MergeLWW:
		return schema.LWW, nil
	case recordcodec.MergeMin:
		return schema.MIN, nil
	case recordcodec.MergeMax:
		return schema.MAX, nil
	case recordcodec.MergeCounter:
		return schema.PN_COUNTER, nil
	case recordcodec.MergeORSet:
		return schema.OR_SET, nil
	default:
		return schema.LWW, fmt.Errorf("unsupported record merge policy %q", policy)
	}
}

func (d TableDefinition) schemaTable() schema.TableSchema {
	t := d.table
	t.Columns = append([]schema.ColumnSchema(nil), t.Columns...)
	t.RecordDescriptor = append([]byte(nil), t.RecordDescriptor...)
	return t
}

func (c *Config) applyTableDefinitions() error {
	if len(c.Tables) == 0 {
		return nil
	}
	if len(c.Schema.Tables) != 0 {
		return fmt.Errorf("murmur: Config.Tables and Schema.Tables cannot both be set: %w", ErrUnsupportedSchema)
	}
	c.Schema.Tables = make([]schema.TableSchema, 0, len(c.Tables))
	seenNames := make(map[string]bool, len(c.Tables))
	seenIDs := make(map[uint32]bool, len(c.Tables))
	for i, definition := range c.Tables {
		if definition.name == "" || definition.id == 0 || definition.register == nil {
			return fmt.Errorf("murmur: Config.Tables[%d] is not a compiled definition: %w", i, ErrUnsupportedSchema)
		}
		name := strings.ToLower(definition.name)
		if seenNames[name] || seenIDs[definition.id] {
			return fmt.Errorf("murmur: duplicate typed table name or ID at Config.Tables[%d]: %w", i, ErrUnsupportedSchema)
		}
		seenNames[name], seenIDs[definition.id] = true, true
		if !definition.local && !definition.ephemeral {
			c.Schema.Tables = append(c.Schema.Tables, definition.schemaTable())
		}
	}
	return nil
}

// MigrateRecords publishes an additive typed-schema revision and rebinds the
// supplied Go record types to a rebuilt RIME materializer. Pass the complete
// table-definition set for the new application version; dropping tables,
// fields, changing stable IDs, types, or merge policies is rejected. The
// authoritative Spool manifest is committed before the materializer switches.
func (db *DB) MigrateRecords(ctx context.Context, definitions []TableDefinition) error {
	if db == nil {
		return fmt.Errorf("murmur: typed record schemas are not configured: %w", ErrUnsupportedSchema)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := db.requireWrite(); err != nil {
		return err
	}
	ticket, err := db.sched.Admit(ctx, WriterMaintenance)
	if err != nil {
		return err
	}
	defer ticket.Release()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.drainGroupCommitsLocked()
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	if err := db.requireWrite(); err != nil {
		return err
	}
	if len(definitions) == 0 {
		return fmt.Errorf("murmur: typed migration requires the complete non-empty table set: %w", ErrUnsupportedSchema)
	}
	cur, err := db.store.LoadSchemaManifest()
	if err != nil {
		return err
	}
	if cur == nil {
		return fmt.Errorf("murmur: no published schema: %w", ErrSchemaMismatch)
	}
	tables := make([]schema.TableSchema, 0, len(definitions))
	seenNames := make(map[string]bool, len(definitions))
	seenIDs := make(map[uint32]bool, len(definitions))
	for i, definition := range definitions {
		if definition.name == "" || definition.id == 0 || definition.register == nil {
			return fmt.Errorf("murmur: typed migration table %d is not a compiled definition: %w", i, ErrUnsupportedSchema)
		}
		name := strings.ToLower(definition.name)
		if seenNames[name] || seenIDs[definition.id] {
			return fmt.Errorf("murmur: duplicate typed migration table %q or ID %d: %w", definition.name, definition.id, ErrUnsupportedSchema)
		}
		seenNames[name], seenIDs[definition.id] = true, true
		if !definition.local && !definition.ephemeral {
			tables = append(tables, definition.schemaTable())
		}
	}
	assigned, err := schema.AssignIDs(cur.Tables, tables)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnsupportedSchema, err)
	}
	if !schema.IsSuperset(cur.Tables, assigned) {
		return fmt.Errorf("murmur: typed schema changes must be additive: %w", ErrUnsupportedSchema)
	}
	if _, err := schema.BuildRegistry(cur.Version+1, assigned); err != nil {
		return fmt.Errorf("%w: %w", ErrUnsupportedSchema, err)
	}
	if err := db.checkMigrationBackfill(cur.Tables, assigned); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	changed := schema.ContentHash(cur.Version, assigned) != cur.Hash
	var next *schema.Manifest
	if changed {
		next, err = schema.NewAuthoredRevision(cur, assigned, db.cfg.NodeID, db.store.ClockNow())
		if err != nil {
			return fmt.Errorf("%w: %w", ErrUnsupportedSchema, err)
		}
	}
	oldDefinitions := db.currentRecordDefinitions()
	db.recordMu.Lock()
	db.recordDefinitions = append([]TableDefinition(nil), definitions...)
	db.recordMu.Unlock()
	if changed {
		err = db.publishSchemaRevision(cur, next)
	} else {
		// A source-level rename can bind a new Go type without changing its
		// stable descriptor. Rebuild to make the new typed handles available.
		db.setState(StateMaterializerDirty)
		err = db.rebuildRecordMaterializer(ctx)
		if err == nil {
			db.setState(StateReady)
			if db.subMgr != nil {
				db.subMgr.notifyChange(true)
			}
		}
	}
	if err != nil {
		if db.store.Failed() == nil && db.getState() != StateFailed {
			db.recordMu.Lock()
			db.recordDefinitions = oldDefinitions
			db.recordMu.Unlock()
			db.setState(StateReady)
		} else {
			db.setState(StateFailed)
		}
		return err
	}
	if changed {
		db.metrics.schemaMigrations.Add(1)
	}
	return nil
}

// Tx is a managed typed write transaction. It cannot publish RIME
// changes without first committing them to Murmur's durable store.
type Tx struct {
	db         *DB
	inner      *rimeadapter.Tx
	ctx        context.Context
	generation uint64
	done       bool
}

// RecordTx is retained as a compatibility alias while callers migrate to Tx.
type RecordTx = Tx

// BeginTx opens a managed typed write transaction. All writes commit through
// Spool before RIME publication.
func (db *DB) BeginTx(ctx context.Context) (*Tx, error) {
	if err := db.requireWrite(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db.recordMu.RLock()
	adapter, generation := db.recordAdapter, db.recordGeneration
	if adapter == nil {
		db.recordMu.RUnlock()
		return nil, ErrUnsupportedSchema
	}
	inner, err := adapter.Begin(ctx)
	db.recordMu.RUnlock()
	if err != nil {
		return nil, err
	}
	return &RecordTx{db: db, inner: inner, ctx: ctx, generation: generation}, nil
}

// Commit durably commits the staged records before publishing them to RIME.
func (tx *Tx) Commit() error {
	if tx == nil || tx.db == nil || tx.inner == nil || tx.done {
		return errors.New("murmur: typed transaction is closed")
	}
	return tx.db.commitRecordTx(tx.ctx, tx)
}

// CommitContext commits using ctx as the durability and cancellation context.
func (tx *Tx) CommitContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if tx == nil || tx.db == nil || tx.inner == nil || tx.done {
		return errors.New("murmur: typed transaction is closed")
	}
	return tx.db.commitRecordTx(ctx, tx)
}

// Rollback discards staged typed changes without touching durable state.
func (tx *Tx) Rollback() error {
	if tx == nil || tx.done {
		return nil
	}
	tx.done = true
	if tx.inner != nil {
		return tx.inner.Rollback()
	}
	return nil
}

// RecordReadTx pins one local RIME MVCC snapshot. It is single-goroutine owned
// and must be closed promptly so old versions can be reclaimed.
type RecordReadTx struct {
	db     *DB
	inner  *rime.Tx
	ctx    context.Context
	tables map[string]any
	gen    uint64
	done   bool
}

// RecordSnapshotID identifies a commit snapshot in one local materializer
// generation. It cannot be used on another node or after a rebuild.
type RecordSnapshotID struct {
	generation uint64
	commit     rime.TxID
}

// RecordJoinRow contains detached records returned by a managed typed join.
// Right is nil for an unmatched left-join row.
type RecordJoinRow[A, B any] struct {
	Left  *A
	Right *B
}

// RecordTable is a typed table handle owned by a Murmur DB.
type RecordTable[T any] struct {
	db   *DB
	name string
}

// RecordQuery is a read-only typed query. It deliberately omits RIME's query
// mutation methods so query execution cannot bypass Murmur's durable writer.
type RecordQuery[T any] struct {
	db    *DB
	inner *rime.Query[T]
	clone func(*T) (*T, error)
	err   error
}

// RecordGroupedQuery is a read-only typed grouping wrapper.
type RecordGroupedQuery[T any] struct {
	db    *DB
	inner *rime.GroupedQuery[T]
	err   error
}

// RecordCompiledQuery is a reusable read-only query with typed positional
// parameters. It exposes no RIME mutation methods.
type RecordCompiledQuery[T any] struct {
	db    *DB
	inner *rime.Compiled[T]
	clone func(*T) (*T, error)
	err   error
}

// TableOf returns a registered typed table and verifies the Go record type.
func TableOf[T any](db *DB, name string) (*RecordTable[T], error) {
	if db == nil {
		return nil, fmt.Errorf("murmur: typed record tables are not configured: %w", ErrUnsupportedSchema)
	}
	db.recordMu.RLock()
	defer db.recordMu.RUnlock()
	if db.recordAdapter == nil {
		return nil, fmt.Errorf("murmur: typed record tables are not configured: %w", ErrUnsupportedSchema)
	}
	inner, ok := db.recordTables[strings.ToLower(name)].(*rimeadapter.Table[T])
	if !ok || inner == nil {
		return nil, fmt.Errorf("murmur: typed table %q is absent or has record type %T, requested %T: %w", name, db.recordTables[strings.ToLower(name)], (*rimeadapter.Table[T])(nil), ErrUnsupportedSchema)
	}
	return &RecordTable[T]{db: db, name: strings.ToLower(name)}, nil
}

// BridgeTypedRowExists checks row identity against the native typed materializer.
// The second result is false when the requested table is unregistered.
func (db *DB) BridgeTypedRowExists(table string, key ids.RowID) (exists, typed bool, err error) {
	if db == nil {
		return false, false, ErrClosed
	}
	if err := db.requireRead(); err != nil {
		return false, true, err
	}
	db.recordMu.RLock()
	defer db.recordMu.RUnlock()
	name := strings.ToLower(table)
	handle, ok := db.recordTables[name]
	if !ok {
		return false, true, fmt.Errorf("murmur: typed bridge table %q is not registered: %w", table, ErrUnsupportedSchema)
	}
	for _, definition := range db.recordDefinitions {
		if strings.ToLower(definition.name) == name {
			if definition.local || definition.ephemeral {
				return false, true, fmt.Errorf("murmur: local table %q cannot participate in typed bridges: %w", table, ErrUnsupportedSchema)
			}
			if definition.bridgeExist == nil {
				return false, true, fmt.Errorf("murmur: typed bridge table %q has no row lookup: %w", table, ErrUnsupportedSchema)
			}
			exists, err := definition.bridgeExist(handle, key)
			return exists, true, err
		}
	}
	return false, true, fmt.Errorf("murmur: typed bridge table %q has no definition: %w", table, ErrUnsupportedSchema)
}

// ValidateBridgeTypedField validates an imported field against its registered
// rich-record descriptor. The second result is false when the table is absent.
func (db *DB) ValidateBridgeTypedField(table string, fieldID uint32, key ids.RowID, value codec.Value) (typed bool, err error) {
	if db == nil {
		return false, ErrClosed
	}
	if err := db.requireRead(); err != nil {
		return true, err
	}
	db.recordMu.RLock()
	defer db.recordMu.RUnlock()
	name := strings.ToLower(table)
	for _, definition := range db.recordDefinitions {
		if strings.ToLower(definition.name) == name {
			if definition.local || definition.ephemeral {
				return true, fmt.Errorf("murmur: local table %q cannot participate in typed bridges: %w", table, ErrUnsupportedSchema)
			}
			if definition.bridgeCheck == nil {
				return true, fmt.Errorf("murmur: typed bridge table %q has no field validator: %w", table, ErrUnsupportedSchema)
			}
			return true, definition.bridgeCheck(fieldID, key, value)
		}
	}
	return true, fmt.Errorf("murmur: typed bridge table %q is not registered: %w", table, ErrUnsupportedSchema)
}

func (db *DB) configureRecordAdapter(adapter *rimeadapter.Adapter) error {
	if err := adapter.SetManagedHooks(db.prepareTypedLocalMutations, db.resolveTypedBridgeRow); err != nil {
		return err
	}
	return adapter.SetCommitHooks(
		func() error { return db.fireCrash(func(h *crashHooks) func() error { return h.beforeDurable }) },
		func() error { return db.fireCrash(func(h *crashHooks) func() error { return h.afterDurable }) },
	)
}

func (db *DB) prepareTypedLocalMutations(batch *codec.MutationBatch) error {
	tx := &policyTx{db: db, txID: batch.TxID}
	routed, err := tx.routeMergeOwnership(batch.Mutations)
	if err != nil {
		return fmt.Errorf("murmur: typed bridge ownership: %w", err)
	}
	policy, err := policyMutationsForTx(db, tx, routed)
	if err != nil {
		return fmt.Errorf("murmur: typed bridge policy: %w", err)
	}
	routed = append(routed, policy...)
	if len(routed) > db.cfg.MaxBatchMutations || int64(codec.EncodedMutationsSize(routed)) > db.cfg.MaxTransactionBytes {
		return fmt.Errorf("murmur: typed transaction exceeds configured batch limits: %w", ErrBatchTooLarge)
	}
	batch.Mutations = routed
	return nil
}

// CommitTypedBridgeImport durably commits typed row mutations, bridge
// provenance, and source receipts together, then materializes the accepted
// winners through the managed RIME adapter. All callers must pass mutations
// prepared from an authenticated and policy-filtered bridge bundle.
func (db *DB) CommitTypedBridgeImport(ctx context.Context, txID ids.TxID, source ids.DBID, stream string, bundle ids.TxID, first, last uint64, allowHighDelete bool, receipts []BridgeSourceReceipt, mutations []codec.Mutation) error {
	if txID.IsZero() || source.IsZero() || stream == "" || len(stream) > 1024 || first == 0 || last < first {
		return fmt.Errorf("murmur: invalid typed bridge import provenance")
	}
	if len(mutations) == 0 {
		return fmt.Errorf("murmur: typed bridge import has no row mutations")
	}
	if err := db.requireWrite(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	owned := make([]codec.Mutation, len(mutations))
	for i, mutation := range mutations {
		owned[i] = mutation
		owned[i].Value = mutation.Value
		owned[i].Value.B = append([]byte(nil), mutation.Value.B...)
		owned[i].Records = make([]codec.CRDTRecord, len(mutation.Records))
		for j, record := range mutation.Records {
			owned[i].Records[j] = codec.CRDTRecord{Key: append([]byte(nil), record.Key...), Data: append([]byte(nil), record.Data...)}
		}
	}
	reg := db.schemaRegistry()
	for _, mutation := range owned {
		if mutation.TableID == BridgePolicyTableID || mutation.Flags&codec.FlagBridgeReceipt != 0 {
			return fmt.Errorf("murmur: typed bridge row mutation uses a reserved bridge field")
		}
		table := reg.TableByID(mutation.TableID)
		if table == nil || mutation.RowID.IsZero() {
			return fmt.Errorf("murmur: typed bridge mutation has an unknown table or zero row identity: %w", ErrUnsupportedSchema)
		}
		if mutation.IsTombstone() {
			continue
		}
		column := table.ColumnByID(mutation.ColumnID)
		if column == nil || mutation.Policy != column.MergePolicy {
			return fmt.Errorf("murmur: typed bridge mutation has an unknown field or merge policy: %w", ErrUnsupportedSchema)
		}
		if mutation.Policy == schema.LWW {
			typed, err := db.ValidateBridgeTypedField(table.Name, mutation.ColumnID, mutation.RowID, mutation.Value)
			if err != nil {
				return err
			}
			if !typed {
				return fmt.Errorf("murmur: typed bridge import requires Config.Tables: %w", ErrUnsupportedSchema)
			}
		} else if (mutation.Policy == schema.PN_COUNTER || mutation.Policy == schema.OR_SET) && mutation.Flags&codec.FlagCRDTImport == 0 {
			return fmt.Errorf("murmur: typed bridge merge field lacks imported causal records: %w", ErrUnsupportedSchema)
		}
	}
	for _, receipt := range receipts {
		if receipt.TxID.IsZero() || receipt.Origin.IsZero() || receipt.Sequence == 0 {
			return fmt.Errorf("murmur: invalid typed bridge source receipt")
		}
	}
	ticket, err := db.sched.Admit(ctx, WriterLocal)
	if err != nil {
		return fmt.Errorf("murmur: writer admission: %w", err)
	}
	defer ticket.Release()
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.drainGroupCommitsLocked()
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	if err := db.requireWrite(); err != nil {
		return err
	}
	info := &bridgeImportInfo{SourceDomain: source, Stream: stream, BundleID: bundle, FirstSeq: first, LastSeq: last, AllowHighDelete: allowHighDelete}
	bridgeTx := &policyTx{db: db, txID: txID, bridgeImport: info}
	policyMutations, err := policyMutationsForTx(db, bridgeTx, owned)
	if err != nil {
		return fmt.Errorf("murmur: typed bridge provenance: %w", err)
	}
	owned = append(owned, policyMutations...)
	for _, receipt := range receipts {
		value := append([]byte(nil), receipt.Origin[:]...)
		value = binary.BigEndian.AppendUint64(value, receipt.Sequence)
		owned = append(owned, codec.Mutation{
			TableID: BridgePolicyTableID, RowID: ids.RowID(receipt.TxID), ColumnID: 2,
			Flags: codec.FlagBridgeReceipt, Value: codec.Blob(value),
		})
	}
	if len(owned) > db.cfg.MaxBatchMutations || int64(codec.EncodedMutationsSize(owned)) > db.cfg.MaxTransactionBytes {
		return fmt.Errorf("murmur: typed bridge transaction exceeds configured batch limits: %w", ErrBatchTooLarge)
	}
	identity := db.schemaIdentity()
	batch := &codec.MutationBatch{
		ProtocolVersion: replication.ProtocolVersion,
		DBID:            db.store.DBID(), TxID: txID, OriginNode: db.cfg.NodeID,
		HLC: db.store.ClockNow(), SchemaEpoch: identity.Epoch, SchemaHash: identity.Hash,
		Mutations: owned,
	}
	previousGeneration := db.materializedGeneration.Load()
	result, err := db.store.CommitLocal(ctx, batch)
	if err != nil {
		if fatal := db.store.Failed(); fatal != nil {
			db.setState(StateFailed)
			return &CommitOutcomeUncertainError{TxID: txID, Cause: err}
		}
		return err
	}
	if err := db.applyRemoteRecords(ctx, result); err != nil {
		db.setState(StateFailed)
		return &CommitOutcomeUncertainError{TxID: txID, Cause: err}
	}
	db.metrics.localCommits.Add(1)
	db.metrics.localCommitMutations.Add(uint64(len(owned)))
	if result.Generation > previousGeneration {
		if repl := db.replManager(); repl != nil {
			repl.NotifyLocal()
		}
	}
	return nil
}

func (t *RecordTable[T]) lockInner() (*rimeadapter.Table[T], func(), error) {
	if t == nil || t.db == nil {
		return nil, nil, ErrClosed
	}
	t.db.recordMu.RLock()
	inner, ok := t.db.recordTables[t.name].(*rimeadapter.Table[T])
	if !ok || inner == nil {
		t.db.recordMu.RUnlock()
		return nil, nil, fmt.Errorf("murmur: typed table %q is unavailable: %w", t.name, ErrUnsupportedSchema)
	}
	return inner, t.db.recordMu.RUnlock, nil
}

// Get reads one currently published record by its replicated RowID.
func (t *RecordTable[T]) Get(key ids.RowID) (*T, error) {
	if t == nil || t.db == nil {
		return nil, ErrClosed
	}
	if err := t.db.requireRead(); err != nil {
		return nil, err
	}
	inner, unlock, err := t.lockInner()
	if err != nil {
		return nil, err
	}
	defer unlock()
	value, ok, err := inner.Lookup(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, rime.ErrNotFound
	}
	return rime.CloneRecord(value), nil
}

// GetTx reads through the transaction's RIME snapshot and staged overlay.
func (t *RecordTable[T]) GetTx(tx *Tx, key ids.RowID) (*T, error) {
	if tx == nil {
		return nil, rime.ErrTxClosed
	}
	inner, unlock, err := t.lockInner()
	if err != nil {
		return nil, err
	}
	defer unlock()
	value, err := inner.Get(tx.inner, key)
	if err != nil {
		return nil, err
	}
	return rime.CloneRecord(value), nil
}

// GetRead reads one record from a pinned read transaction.
func (t *RecordTable[T]) GetRead(tx *RecordReadTx, key ids.RowID) (*T, error) {
	if t == nil || t.db == nil || tx == nil || tx.done || tx.db != t.db {
		return nil, rime.ErrTxClosed
	}
	inner, ok := tx.tables[t.name].(*rimeadapter.Table[T])
	if !ok || inner == nil {
		return nil, fmt.Errorf("murmur: typed table %q is unavailable in this snapshot: %w", t.name, ErrUnsupportedSchema)
	}
	value, ok, err := inner.LookupAt(tx.inner, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, rime.ErrNotFound
	}
	return rime.CloneRecord(value), nil
}

// Insert stages a new record. The row ID must be a nonzero 16-byte value.
func (t *RecordTable[T]) Insert(tx *Tx, value *T) error {
	if tx == nil {
		return rime.ErrTxClosed
	}
	inner, unlock, err := t.lockInner()
	if err != nil {
		return err
	}
	defer unlock()
	return inner.Insert(tx.inner, value)
}

// InsertMany stages a bounded atomic insert batch in tx.
func (t *RecordTable[T]) InsertMany(tx *Tx, values []*T) error {
	if tx == nil {
		return rime.ErrTxClosed
	}
	inner, unlock, err := t.lockInner()
	if err != nil {
		return err
	}
	defer unlock()
	return inner.InsertMany(tx.inner, values)
}

// Save inserts or replaces a complete record. Unknown durable top-level fields
// and compatible nested-struct payloads are retained by the adapter.
func (t *RecordTable[T]) Save(tx *Tx, value *T) error {
	if tx == nil {
		return rime.ErrTxClosed
	}
	inner, unlock, err := t.lockInner()
	if err != nil {
		return err
	}
	defer unlock()
	return inner.Upsert(tx.inner, value)
}

// SaveMany stages an atomic insert-or-replace batch in tx.
func (t *RecordTable[T]) SaveMany(tx *Tx, values []*T) error {
	if tx == nil {
		return rime.ErrTxClosed
	}
	inner, unlock, err := t.lockInner()
	if err != nil {
		return err
	}
	defer unlock()
	return inner.UpsertMany(tx.inner, values)
}

// Update stages an update to a private copy of a currently visible record.
func (t *RecordTable[T]) Update(tx *Tx, key ids.RowID, fn func(*T) error) error {
	if tx == nil {
		return rime.ErrTxClosed
	}
	if fn == nil {
		return errors.New("murmur: nil typed update callback")
	}
	inner, unlock, err := t.lockInner()
	if err != nil {
		return err
	}
	defer unlock()
	return inner.Update(tx.inner, key, fn)
}

// Delete stages a tombstone for one record.
func (t *RecordTable[T]) Delete(tx *Tx, key ids.RowID) error {
	if tx == nil {
		return rime.ErrTxClosed
	}
	inner, unlock, err := t.lockInner()
	if err != nil {
		return err
	}
	defer unlock()
	return inner.Delete(tx.inner, key)
}

// DeleteMany stages an atomic tombstone batch in tx.
func (t *RecordTable[T]) DeleteMany(tx *Tx, keys []ids.RowID) error {
	if tx == nil {
		return rime.ErrTxClosed
	}
	anyKeys := make([]any, len(keys))
	for i := range keys {
		anyKeys[i] = keys[i]
	}
	inner, unlock, err := t.lockInner()
	if err != nil {
		return err
	}
	defer unlock()
	return inner.DeleteMany(tx.inner, anyKeys)
}

// RecordCounterAdd stages one PN_COUNTER delta against a typed int64 field.
// Direct record replacement cannot change counter fields; use this operation
// so Spool receives causal actor components instead of an LWW projection.
func RecordCounterAdd[T any](tx *Tx, table *RecordTable[T], key ids.RowID, field string, delta int64) error {
	if tx == nil {
		return rime.ErrTxClosed
	}
	inner, unlock, err := table.lockInner()
	if err != nil {
		return err
	}
	defer unlock()
	return inner.CounterAdd(tx.inner, key, field, delta)
}

// RecordSetAdd adds a string to a top-level []string OR_SET field.
func RecordSetAdd[T any](tx *Tx, table *RecordTable[T], key ids.RowID, field, value string) error {
	if tx == nil {
		return rime.ErrTxClosed
	}
	inner, unlock, err := table.lockInner()
	if err != nil {
		return err
	}
	defer unlock()
	return inner.SetAdd(tx.inner, key, field, value)
}

// RecordSetRemove removes a string from a top-level []string OR_SET field.
func RecordSetRemove[T any](tx *Tx, table *RecordTable[T], key ids.RowID, field, value string) error {
	if tx == nil {
		return rime.ErrTxClosed
	}
	inner, unlock, err := table.lockInner()
	if err != nil {
		return err
	}
	defer unlock()
	return inner.SetRemove(tx.inner, key, field, value)
}

// RecordMax applies a numeric maximum to a top-level MIN/MAX-policy field.
// The supplied value must have exactly the field's Go type.
func RecordMax[T any](tx *Tx, table *RecordTable[T], key ids.RowID, field string, value any) error {
	if tx == nil {
		return rime.ErrTxClosed
	}
	inner, unlock, err := table.lockInner()
	if err != nil {
		return err
	}
	defer unlock()
	err = inner.Extrema(tx.inner, key, field, value, recordcodec.MergeMax)
	if errors.Is(err, rimeadapter.ErrUnsupported) {
		return fmt.Errorf("murmur: invalid MAX field operation: %w: %w", ErrUnsupportedSchema, err)
	}
	return err
}

// RecordMin applies a numeric minimum to a top-level MIN/MAX-policy field.
// The supplied value must have exactly the field's Go type.
func RecordMin[T any](tx *Tx, table *RecordTable[T], key ids.RowID, field string, value any) error {
	if tx == nil {
		return rime.ErrTxClosed
	}
	inner, unlock, err := table.lockInner()
	if err != nil {
		return err
	}
	defer unlock()
	err = inner.Extrema(tx.inner, key, field, value, recordcodec.MergeMin)
	if errors.Is(err, rimeadapter.ErrUnsupported) {
		return fmt.Errorf("murmur: invalid MIN field operation: %w: %w", ErrUnsupportedSchema, err)
	}
	return err
}

// FieldOf creates a typed field handle for predicates and ordering.
func FieldOf[T any, V comparable](table *RecordTable[T], name string) rime.Field[T, V] {
	inner, unlock, err := table.lockInner()
	if err != nil {
		panic(err)
	}
	defer unlock()
	return rimeadapter.FieldOf[T, V](inner, name)
}

// StringFieldOf creates a typed string field handle for prefix, suffix,
// substring, and LIKE predicates without exposing a writable RIME table.
func StringFieldOf[T any](table *RecordTable[T], name string) rime.StringField[T] {
	inner, unlock, err := table.lockInner()
	if err != nil {
		panic(err)
	}
	defer unlock()
	return rimeadapter.StringFieldOf(inner, name)
}

// NumericFieldOf creates a typed numeric field handle for ordering and
// aggregate builders without exposing a writable RIME table.
func NumericFieldOf[T any, V rime.Number](table *RecordTable[T], name string) rime.OrderedField[T, V] {
	inner, unlock, err := table.lockInner()
	if err != nil {
		panic(err)
	}
	defer unlock()
	return rimeadapter.NumericFieldOf[T, V](inner, name)
}

// Where starts a latest-snapshot read query.
func (t *RecordTable[T]) Where(exprs ...rime.Expr[T]) *RecordQuery[T] {
	inner, unlock, err := t.lockInner()
	if err != nil {
		return &RecordQuery[T]{db: t.db, err: err}
	}
	defer unlock()
	return &RecordQuery[T]{db: t.db, inner: inner.Where(exprs...), clone: inner.Clone}
}

// Compile builds a reusable read-only query with typed positional parameters.
func (t *RecordTable[T]) Compile(exprs ...rime.Expr[T]) *RecordCompiledQuery[T] {
	inner, unlock, err := t.lockInner()
	if err != nil {
		return &RecordCompiledQuery[T]{db: t.db, err: err}
	}
	defer unlock()
	return &RecordCompiledQuery[T]{db: t.db, inner: inner.Compile(exprs...), clone: inner.Clone}
}

// WhereTx starts a query bound to tx's snapshot and staged writes.
func (t *RecordTable[T]) WhereTx(tx *Tx, exprs ...rime.Expr[T]) (*RecordQuery[T], error) {
	if tx == nil {
		return nil, rime.ErrTxClosed
	}
	inner, unlock, err := t.lockInner()
	if err != nil {
		return nil, err
	}
	defer unlock()
	query, err := inner.WhereTx(tx.inner, exprs...)
	if err != nil {
		return nil, err
	}
	return &RecordQuery[T]{db: t.db, inner: query.WithContext(tx.ctx), clone: inner.Clone}, nil
}

// CompileTx builds a reusable query bound to tx's snapshot and staged writes.
func (t *RecordTable[T]) CompileTx(tx *Tx, exprs ...rime.Expr[T]) (*RecordCompiledQuery[T], error) {
	if tx == nil {
		return nil, rime.ErrTxClosed
	}
	inner, unlock, err := t.lockInner()
	if err != nil {
		return nil, err
	}
	defer unlock()
	compiled, err := inner.CompileTx(tx.inner, exprs...)
	if err != nil {
		return nil, err
	}
	return &RecordCompiledQuery[T]{db: t.db, inner: compiled, clone: inner.Clone}, nil
}

// WhereReadTx starts a read query bound to tx's pinned MVCC snapshot.
func (t *RecordTable[T]) WhereReadTx(tx *RecordReadTx, exprs ...rime.Expr[T]) (*RecordQuery[T], error) {
	if t == nil || t.db == nil || tx == nil || tx.done || tx.db != t.db {
		return nil, rime.ErrTxClosed
	}
	inner, ok := tx.tables[t.name].(*rimeadapter.Table[T])
	if !ok || inner == nil {
		return nil, fmt.Errorf("murmur: typed table %q is unavailable in this snapshot: %w", t.name, ErrUnsupportedSchema)
	}
	return &RecordQuery[T]{db: t.db, inner: inner.Where(exprs...).In(tx.inner).WithContext(tx.ctx), clone: inner.Clone}, nil
}

// CompileReadTx builds a reusable query pinned to tx's read snapshot.
func (t *RecordTable[T]) CompileReadTx(tx *RecordReadTx, exprs ...rime.Expr[T]) (*RecordCompiledQuery[T], error) {
	if t == nil || t.db == nil || tx == nil || tx.done || tx.db != t.db {
		return nil, rime.ErrTxClosed
	}
	inner, ok := tx.tables[t.name].(*rimeadapter.Table[T])
	if !ok || inner == nil {
		return nil, fmt.Errorf("murmur: typed table %q is unavailable in this snapshot: %w", t.name, ErrUnsupportedSchema)
	}
	return &RecordCompiledQuery[T]{db: t.db, inner: inner.CompileReadTx(tx.inner, tx.ctx, exprs...), clone: inner.Clone}, nil
}

// ReadTxContext opens a context-aware snapshot over all currently registered
// typed tables.
func (db *DB) ReadTxContext(ctx context.Context) (*RecordReadTx, error) {
	if db == nil {
		return nil, fmt.Errorf("murmur: typed read transactions are not configured: %w", ErrUnsupportedSchema)
	}
	if err := db.requireRead(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db.recordMu.RLock()
	defer db.recordMu.RUnlock()
	if db.recordDB == nil {
		return nil, ErrClosed
	}
	tx := db.recordDB.ReadTxContext(ctx)
	return &RecordReadTx{db: db, inner: tx, ctx: ctx, tables: db.recordTables, gen: db.recordGeneration}, nil
}

// ReadAt opens a typed read transaction pinned to a retained local RIME
// snapshot ID returned by RecordReadTx.Snapshot or another RIME snapshot.
func (db *DB) ReadAt(snapshot RecordSnapshotID) (*RecordReadTx, error) {
	if db == nil {
		return nil, fmt.Errorf("murmur: typed read transactions are not configured: %w", ErrUnsupportedSchema)
	}
	if err := db.requireRead(); err != nil {
		return nil, err
	}
	db.recordMu.RLock()
	defer db.recordMu.RUnlock()
	if db.recordDB == nil {
		return nil, ErrClosed
	}
	if snapshot.generation == 0 || snapshot.generation != db.recordGeneration {
		return nil, rime.ErrSnapshotUnavailable
	}
	tx := db.recordDB.ReadAt(snapshot.commit)
	return &RecordReadTx{db: db, inner: tx, ctx: context.Background(), tables: db.recordTables, gen: db.recordGeneration}, nil
}

// Snapshot returns the local RIME commit ID pinned by tx.
func (tx *RecordReadTx) Snapshot() RecordSnapshotID {
	if tx == nil || tx.inner == nil {
		return RecordSnapshotID{}
	}
	return RecordSnapshotID{generation: tx.gen, commit: tx.inner.Snapshot()}
}

// Close releases the pinned RIME snapshot. Repeated calls are harmless.
func (tx *RecordReadTx) Close() error {
	if tx == nil || tx.done {
		return nil
	}
	tx.done = true
	if tx.inner != nil {
		tx.inner.Close()
	}
	return nil
}

func (q *RecordCompiledQuery[T]) WithContext(ctx context.Context) *RecordCompiledQuery[T] {
	if q == nil {
		return &RecordCompiledQuery[T]{}
	}
	if q.inner == nil {
		return &RecordCompiledQuery[T]{db: q.db, err: q.err}
	}
	return &RecordCompiledQuery[T]{db: q.db, inner: q.inner.WithContext(ctx), clone: q.clone}
}

func (q *RecordCompiledQuery[T]) Limit(n int) *RecordCompiledQuery[T] {
	if q == nil || q.inner == nil {
		if q == nil {
			return &RecordCompiledQuery[T]{}
		}
		return &RecordCompiledQuery[T]{db: q.db, err: q.err}
	}
	return &RecordCompiledQuery[T]{db: q.db, inner: q.inner.Limit(n), clone: q.clone}
}

func (q *RecordCompiledQuery[T]) Offset(n int) *RecordCompiledQuery[T] {
	if q == nil || q.inner == nil {
		if q == nil {
			return &RecordCompiledQuery[T]{}
		}
		return &RecordCompiledQuery[T]{db: q.db, err: q.err}
	}
	return &RecordCompiledQuery[T]{db: q.db, inner: q.inner.Offset(n), clone: q.clone}
}

func (q *RecordCompiledQuery[T]) OrderByAsc(field rime.OrderField[T]) *RecordCompiledQuery[T] {
	if q == nil || q.inner == nil {
		if q == nil {
			return &RecordCompiledQuery[T]{}
		}
		return &RecordCompiledQuery[T]{db: q.db, err: q.err}
	}
	return &RecordCompiledQuery[T]{db: q.db, inner: q.inner.OrderByAsc(field), clone: q.clone}
}

func (q *RecordCompiledQuery[T]) OrderByDesc(field rime.OrderField[T]) *RecordCompiledQuery[T] {
	if q == nil || q.inner == nil {
		if q == nil {
			return &RecordCompiledQuery[T]{}
		}
		return &RecordCompiledQuery[T]{db: q.db, err: q.err}
	}
	return &RecordCompiledQuery[T]{db: q.db, inner: q.inner.OrderByDesc(field), clone: q.clone}
}

// Find executes a compiled query and returns detached records.
func (q *RecordCompiledQuery[T]) Find(args ...any) ([]*T, error) {
	if err := q.check(); err != nil {
		return nil, err
	}
	q.db.recordMu.RLock()
	defer q.db.recordMu.RUnlock()
	if err := q.db.requireRead(); err != nil {
		return nil, err
	}
	rows, err := q.inner.Find(args...)
	if err != nil {
		return nil, err
	}
	owned := make([]*T, len(rows))
	for i, row := range rows {
		owned[i], err = q.clone(row)
		if err != nil {
			return nil, err
		}
	}
	return owned, nil
}

// Count executes a compiled query count.
func (q *RecordCompiledQuery[T]) Count(args ...any) (int, error) {
	if err := q.check(); err != nil {
		return 0, err
	}
	q.db.recordMu.RLock()
	defer q.db.recordMu.RUnlock()
	if err := q.db.requireRead(); err != nil {
		return 0, err
	}
	return q.inner.Count(args...)
}

func (q *RecordCompiledQuery[T]) check() error {
	if q == nil {
		return rime.ErrBadView
	}
	if q.err != nil {
		return q.err
	}
	if q.db == nil || q.inner == nil {
		return rime.ErrBadView
	}
	return nil
}

// InnerJoinReadTx joins two registered tables using one pinned local snapshot.
// Use FieldOf handles for both keys. Returned records are detached clones.
func InnerJoinReadTx[A, B any, K comparable](tx *RecordReadTx, left *RecordTable[A], lfield rime.KeyField[A, K], right *RecordTable[B], rfield rime.KeyField[B, K]) ([]RecordJoinRow[A, B], error) {
	return joinRecordTables(tx, left, lfield, right, rfield, false)
}

// LeftJoinReadTx joins two registered tables using one pinned local snapshot,
// retaining left records with no match. Returned records are detached clones.
func LeftJoinReadTx[A, B any, K comparable](tx *RecordReadTx, left *RecordTable[A], lfield rime.KeyField[A, K], right *RecordTable[B], rfield rime.KeyField[B, K]) ([]RecordJoinRow[A, B], error) {
	return joinRecordTables(tx, left, lfield, right, rfield, true)
}

func joinRecordTables[A, B any, K comparable](tx *RecordReadTx, left *RecordTable[A], lfield rime.KeyField[A, K], right *RecordTable[B], rfield rime.KeyField[B, K], outer bool) ([]RecordJoinRow[A, B], error) {
	if tx == nil || tx.done || tx.inner == nil || left == nil || right == nil || left.db == nil || left.db != right.db || tx.db != left.db {
		return nil, rime.ErrTxClosed
	}
	db := tx.db
	db.recordMu.RLock()
	defer db.recordMu.RUnlock()
	if err := db.requireRead(); err != nil {
		return nil, err
	}
	lt, ok := tx.tables[left.name].(*rimeadapter.Table[A])
	if !ok || lt == nil {
		return nil, fmt.Errorf("murmur: typed table %q is unavailable in this snapshot: %w", left.name, ErrUnsupportedSchema)
	}
	rt, ok := tx.tables[right.name].(*rimeadapter.Table[B])
	if !ok || rt == nil {
		return nil, fmt.Errorf("murmur: typed table %q is unavailable in this snapshot: %w", right.name, ErrUnsupportedSchema)
	}
	var joined []rime.JoinRow[A, B]
	var err error
	if outer {
		joined, err = rimeadapter.LeftJoinOnRead(lt, lfield, rt, rfield, tx.inner, tx.ctx)
	} else {
		joined, err = rimeadapter.InnerJoinOnRead(lt, lfield, rt, rfield, tx.inner, tx.ctx)
	}
	if err != nil {
		return nil, err
	}
	out := make([]RecordJoinRow[A, B], len(joined))
	for i, row := range joined {
		out[i].Left, err = lt.Clone(row.Left)
		if err != nil {
			return nil, err
		}
		if row.Right != nil {
			out[i].Right, err = rt.Clone(row.Right)
			if err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func (q *RecordQuery[T]) WithContext(ctx context.Context) *RecordQuery[T] {
	if q == nil {
		return &RecordQuery[T]{}
	}
	if q.inner == nil {
		return &RecordQuery[T]{db: q.db, err: q.err}
	}
	return &RecordQuery[T]{db: q.db, inner: q.inner.WithContext(ctx), clone: q.clone}
}

func (q *RecordQuery[T]) Where(exprs ...rime.Expr[T]) *RecordQuery[T] {
	if q == nil {
		return &RecordQuery[T]{}
	}
	if q.inner == nil {
		return &RecordQuery[T]{db: q.db, err: q.err}
	}
	return &RecordQuery[T]{db: q.db, inner: q.inner.Where(exprs...), clone: q.clone}
}

func (q *RecordQuery[T]) Limit(n int) *RecordQuery[T] {
	if q == nil {
		return &RecordQuery[T]{}
	}
	if q.inner == nil {
		return &RecordQuery[T]{db: q.db, err: q.err}
	}
	return &RecordQuery[T]{db: q.db, inner: q.inner.Limit(n), clone: q.clone}
}

func (q *RecordQuery[T]) Offset(n int) *RecordQuery[T] {
	if q == nil {
		return &RecordQuery[T]{}
	}
	if q.inner == nil {
		return &RecordQuery[T]{db: q.db, err: q.err}
	}
	return &RecordQuery[T]{db: q.db, inner: q.inner.Offset(n), clone: q.clone}
}

func (q *RecordQuery[T]) OrderByAsc(field rime.OrderField[T]) *RecordQuery[T] {
	if q == nil {
		return &RecordQuery[T]{}
	}
	if q.inner == nil {
		return &RecordQuery[T]{db: q.db, err: q.err}
	}
	return &RecordQuery[T]{db: q.db, inner: q.inner.OrderByAsc(field), clone: q.clone}
}

func (q *RecordQuery[T]) OrderByDesc(field rime.OrderField[T]) *RecordQuery[T] {
	if q == nil {
		return &RecordQuery[T]{}
	}
	if q.inner == nil {
		return &RecordQuery[T]{db: q.db, err: q.err}
	}
	return &RecordQuery[T]{db: q.db, inner: q.inner.OrderByDesc(field), clone: q.clone}
}

func (q *RecordQuery[T]) Find() ([]*T, error) {
	if err := q.check(); err != nil {
		return nil, err
	}
	q.db.recordMu.RLock()
	defer q.db.recordMu.RUnlock()
	if err := q.db.requireRead(); err != nil {
		return nil, err
	}
	rows, err := q.inner.Find()
	if err != nil {
		return nil, err
	}
	owned := make([]*T, len(rows))
	for i, row := range rows {
		if q.clone != nil {
			owned[i], err = q.clone(row)
			if err != nil {
				return nil, err
			}
		} else {
			owned[i] = rime.CloneRecord(row)
		}
	}
	return owned, nil
}

func (q *RecordQuery[T]) First() (*T, error) {
	rows, err := q.Limit(1).Find()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, rime.ErrNotFound
	}
	return rows[0], nil
}

func (q *RecordQuery[T]) Count() (int, error) {
	if err := q.check(); err != nil {
		return 0, err
	}
	q.db.recordMu.RLock()
	defer q.db.recordMu.RUnlock()
	if err := q.db.requireRead(); err != nil {
		return 0, err
	}
	return q.inner.Count()
}

func (q *RecordQuery[T]) Exists() (bool, error) {
	if err := q.check(); err != nil {
		return false, err
	}
	q.db.recordMu.RLock()
	defer q.db.recordMu.RUnlock()
	if err := q.db.requireRead(); err != nil {
		return false, err
	}
	return q.inner.Exists()
}

// Aggregate runs read-only RIME aggregates over the query matches. Aggregate
// descriptors cannot mutate records, and returned values are scalar results.
func (q *RecordQuery[T]) Aggregate(aggs ...rime.Agg[T]) ([]any, error) {
	if err := q.check(); err != nil {
		return nil, err
	}
	q.db.recordMu.RLock()
	defer q.db.recordMu.RUnlock()
	if err := q.db.requireRead(); err != nil {
		return nil, err
	}
	return q.inner.Aggregate(aggs...)
}

// AggregateContext runs read-only aggregates with cancellation.
func (q *RecordQuery[T]) AggregateContext(ctx context.Context, aggs ...rime.Agg[T]) ([]any, error) {
	if err := q.check(); err != nil {
		return nil, err
	}
	q.db.recordMu.RLock()
	defer q.db.recordMu.RUnlock()
	if err := q.db.requireRead(); err != nil {
		return nil, err
	}
	return q.inner.WithContext(ctx).Aggregate(aggs...)
}

// GroupBy groups query matches by typed fields. It is read-only and preserves
// the query's snapshot, filters, ordering and pagination.
func (q *RecordQuery[T]) GroupBy(fields ...rime.GroupField[T]) *RecordGroupedQuery[T] {
	if err := q.check(); err != nil {
		return &RecordGroupedQuery[T]{db: q.db, err: err}
	}
	return &RecordGroupedQuery[T]{db: q.db, inner: q.inner.GroupBy(fields...)}
}

// Aggregate runs aggregates within each group.
func (q *RecordGroupedQuery[T]) Aggregate(aggs ...rime.Agg[T]) ([]rime.GroupRow, error) {
	if q == nil || q.db == nil || q.inner == nil {
		if q != nil && q.err != nil {
			return nil, q.err
		}
		return nil, rime.ErrBadView
	}
	q.db.recordMu.RLock()
	defer q.db.recordMu.RUnlock()
	if err := q.db.requireRead(); err != nil {
		return nil, err
	}
	return q.inner.Aggregate(aggs...)
}

// AggregateContext runs grouped aggregates with cancellation.
func (q *RecordGroupedQuery[T]) AggregateContext(ctx context.Context, aggs ...rime.Agg[T]) ([]rime.GroupRow, error) {
	if q == nil || q.db == nil || q.inner == nil {
		if q != nil && q.err != nil {
			return nil, q.err
		}
		return nil, rime.ErrBadView
	}
	q.db.recordMu.RLock()
	defer q.db.recordMu.RUnlock()
	if err := q.db.requireRead(); err != nil {
		return nil, err
	}
	return q.inner.WithContext(ctx).Aggregate(aggs...)
}

func (q *RecordQuery[T]) Each(fn func(*T) error) error {
	if err := q.check(); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("murmur: nil typed query callback")
	}
	q.db.recordMu.RLock()
	defer q.db.recordMu.RUnlock()
	if err := q.db.requireRead(); err != nil {
		return err
	}
	return q.inner.Each(func(row *T) error {
		if q.clone == nil {
			return fn(rime.CloneRecord(row))
		}
		cloned, err := q.clone(row)
		if err != nil {
			return err
		}
		return fn(cloned)
	})
}

func (q *RecordQuery[T]) check() error {
	if q == nil {
		return rime.ErrBadView
	}
	if q.err != nil {
		return q.err
	}
	if q.db == nil || q.inner == nil {
		return rime.ErrBadView
	}
	return nil
}

// WriteTxContext runs fn in a managed typed transaction. It follows the root
// writer scheduler and commit coordinator so accepted changes are durable
// before they become visible. Callbacks stage optimistically before taking
// the exclusive writer ticket; only commit and publication are serialized.
func (db *DB) WriteTxContext(ctx context.Context, fn func(*Tx) error) error {
	if fn == nil {
		return errors.New("murmur: nil typed transaction callback")
	}
	if err := db.requireWrite(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := db.beginRecordPrepare(ctx); err != nil {
		return err
	}
	db.recordMu.RLock()
	adapter, recordGeneration := db.recordAdapter, db.recordGeneration
	db.recordMu.RUnlock()
	if adapter == nil {
		return fmt.Errorf("murmur: typed record adapter is unavailable: %w", ErrUnsupportedSchema)
	}
	staged, err := func() (*rimeadapter.Tx, error) {
		defer db.endRecordPrepare()
		return adapter.Stage(ctx, func(inner *rimeadapter.Tx) error {
			return fn(&RecordTx{db: db, inner: inner, ctx: ctx, generation: recordGeneration})
		})
	}()
	if err != nil {
		if errors.Is(err, rime.ErrConflict) || errors.Is(err, rime.ErrSnapshotUnavailable) {
			return rime.ErrConflict
		}
		if errors.Is(err, recordcodec.ErrValueTooLarge) {
			return fmt.Errorf("%w: %w", ErrValueTooLarge, err)
		}
		return err
	}
	tx := &Tx{db: db, inner: staged, ctx: ctx, generation: recordGeneration}
	if err := tx.CommitContext(ctx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return nil
}

func (db *DB) beginRecordPrepare(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		db.recordPrepMu.Lock()
		if err := db.requireWrite(); err != nil {
			db.recordPrepMu.Unlock()
			return err
		}
		if gate := db.recordPrepGate; gate != nil {
			db.recordPrepMu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-gate:
				continue
			}
		}
		if db.recordPreparers == 0 {
			db.recordPrepDrain = make(chan struct{})
		}
		db.recordPreparers++
		db.recordPrepMu.Unlock()
		return nil
	}
}

func (db *DB) endRecordPrepare() {
	db.recordPrepMu.Lock()
	db.recordPreparers--
	if db.recordPreparers == 0 {
		if db.recordPrepDrain != nil {
			close(db.recordPrepDrain)
			db.recordPrepDrain = nil
		}
		db.recordPrepCond.Broadcast()
	}
	db.recordPrepMu.Unlock()
}

// drainGroupCommitsLocked closes the pending group and waits for all admitted
// groups to finish. Callers must hold writeMu so no new member can be added
// between the flush and wait.
func (db *DB) drainGroupCommitsLocked() {
	if db.grouper != nil {
		db.grouper.flush()
	}
	db.groupWG.Wait()
}

// quiesceRecordPreparation blocks new callback-based typed transactions and
// waits for active callbacks to finish staging. The snapshot barrier then
// drains the ordered commit queue under writeMu. Call release on every exit.
func (db *DB) quiesceRecordPreparation(ctx context.Context) (release func(), err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db.recordBarrierMu.Lock()
	db.recordPrepMu.Lock()
	gate := make(chan struct{})
	db.recordPrepGate = gate
	drain := db.recordPrepDrain
	db.recordPrepMu.Unlock()
	if drain != nil {
		select {
		case <-ctx.Done():
			db.releaseRecordPreparationGate(gate)
			db.recordBarrierMu.Unlock()
			return nil, ctx.Err()
		case <-drain:
		}
	}
	return func() {
		db.releaseRecordPreparationGate(gate)
		db.recordBarrierMu.Unlock()
	}, nil
}

func (db *DB) releaseRecordPreparationGate(gate chan struct{}) {
	db.recordPrepMu.Lock()
	if db.recordPrepGate == gate {
		db.recordPrepGate = nil
		close(gate)
	}
	db.recordPrepMu.Unlock()
}

// commitRecordGroup persists an ordered set of managed replicated writes in
// one Spool commit, then publishes their RIME transactions in the same order.
// The candidates were detached while writeMu assigned their group position.
func (db *DB) commitRecordGroup(members []*groupMember) []groupMemberResult {
	results := make([]groupMemberResult, len(members))
	finishAll := func(err error) []groupMemberResult {
		for i, member := range members {
			if member.record != nil {
				member.record.Adapter().AbortGroupCandidate(member.record)
			}
			var uncertain *CommitOutcomeUncertainError
			if member.record != nil && errors.As(err, &uncertain) && uncertain.TxID.IsZero() {
				results[i].err = &CommitOutcomeUncertainError{TxID: member.record.Batch().TxID, Cause: uncertain.Cause}
			} else {
				results[i].err = err
			}
			db.groupWG.Done()
		}
		return results
	}
	if len(members) == 0 {
		return results
	}
	for _, member := range members {
		if member.record == nil {
			return finishAll(ErrUnsupportedSchema)
		}
	}
	adapter := members[0].record.Adapter()
	needsRebuild := false
	for _, member := range members {
		if err := adapter.BeforeGroupCommit(member.record); err != nil {
			if errors.Is(err, rimeadapter.ErrMaterializer) && errors.Is(err, rime.ErrConflict) {
				// A preceding durable group already forced reconstruction. This
				// staged candidate belongs to that retired materializer, so include
				// its Spool batch and rebuild once more after the group is durable.
				needsRebuild = true
				continue
			}
			return finishAll(err)
		}
	}

	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	batches := make([]*codec.MutationBatch, len(members))
	for i, member := range members {
		batches[i] = member.record.Batch()
	}
	group, err := db.store.CommitLocalGroup(context.Background(), batches)
	if err != nil {
		if errors.Is(err, state.ErrTooBig) {
			err = fmt.Errorf("%w: %w", ErrBatchTooLarge, err)
		}
		if fatal := db.store.Failed(); fatal != nil {
			db.setState(StateFailed)
			err = &CommitOutcomeUncertainError{Cause: errors.Join(err, fatal)}
		}
		return finishAll(err)
	}
	db.metrics.groupCommits.Add(1)
	db.metrics.groupCommitMembers.Add(uint64(len(members)))
	var ambiguous error
	for i, member := range members {
		if needsRebuild {
			adapter.AbortGroupCandidate(member.record)
			continue
		}
		err := adapter.PublishGroupCandidate(context.Background(), member.record)
		if errors.Is(err, rimeadapter.ErrMaterializer) {
			needsRebuild = true
			continue
		}
		if err != nil {
			results[i].err = db.handleRecordCommitError(err)
			if errors.Is(err, rimeadapter.ErrUncertain) {
				ambiguous = errors.Join(ambiguous, err)
			}
		}
	}
	if needsRebuild {
		for _, member := range members {
			if member.record != nil {
				adapter.AbortGroupCandidate(member.record)
			}
		}
		if err := db.rebuildRecordMaterializer(context.Background()); err != nil {
			db.setState(StateFailed)
			return finishAll(fmt.Errorf("murmur: rebuild after durable record group: %w", err))
		}
	}
	generation, err := db.store.StateGeneration()
	if err != nil {
		db.setState(StateFailed)
		return finishAll(err)
	}
	db.materializedGeneration.Store(generation)
	for i, member := range members {
		db.metrics.localCommits.Add(1)
		db.metrics.localCommitMutations.Add(uint64(len(member.record.Batch().Mutations)))
		db.metrics.localCommitLatencyNanos.Add(uint64(time.Since(member.start)))
		results[i].applied = group.Members[i].Applied
		results[i].gen = group.Members[i].Generation
		db.groupWG.Done()
	}
	if db.subMgr != nil {
		db.subMgr.notifyChange(needsRebuild)
	}
	if repl := db.replManager(); repl != nil {
		repl.NotifyLocal()
	}
	if ambiguous != nil {
		return results
	}
	return results
}

func (db *DB) commitRecordTx(ctx context.Context, tx *Tx) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := db.requireWrite(); err != nil {
		_ = tx.Rollback()
		return err
	}
	waitStart := time.Now()
	ticket, err := db.sched.Admit(ctx, WriterLocal)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer ticket.Release()
	db.writeMu.Lock()
	writeHeld := true
	defer func() {
		if writeHeld {
			db.writeMu.Unlock()
		}
	}()
	db.metrics.writeAcquisitions.Add(1)
	db.metrics.writeQueueWaitNanos.Add(uint64(time.Since(waitStart)))
	if err := db.requireWrite(); err != nil {
		_ = tx.Rollback()
		return err
	}
	db.recordMu.RLock()
	adapter, generation := db.recordAdapter, db.recordGeneration
	valid := adapter != nil && generation == tx.generation
	db.recordMu.RUnlock()
	if !valid {
		_ = tx.Rollback()
		tx.done = true
		return rime.ErrConflict
	}
	if db.grouper != nil {
		candidate, grouped, err := adapter.PrepareGroupCandidate(ctx, tx.inner)
		if err != nil {
			tx.done = true
			// A staged transaction can outlive RIME's retained MVCC history
			// while unrelated commits advance the materializer. Nothing has
			// reached Spool yet, so callers can safely rerun the callback on a
			// fresh snapshot just as they do for an optimistic write conflict.
			if errors.Is(err, rime.ErrConflict) || errors.Is(err, rime.ErrSnapshotUnavailable) {
				return rime.ErrConflict
			}
			return db.handleRecordCommitError(err)
		}
		if grouped {
			member := &groupMember{record: candidate, start: time.Now(), resCh: make(chan groupMemberResult, 1)}
			member.batch = candidate.Batch()
			db.groupWG.Add(1)
			groupTicket, err := db.grouper.enqueue(member)
			if err != nil {
				db.groupWG.Done()
				candidate.Adapter().AbortGroupCandidate(candidate)
				tx.done = true
				return err
			}
			ticket.Release()
			writeHeld = false
			db.writeMu.Unlock()
			return db.grouper.await(groupTicket).err
		}
	}
	db.applyMu.Lock()
	defer db.applyMu.Unlock()
	changeCount, err := tx.inner.PreparedChangeCount()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	previousGeneration := db.materializedGeneration.Load()
	previousPublication := adapter.PublicationGeneration()
	commitStarted := time.Now()
	err = adapter.Commit(ctx, tx.inner)
	tx.done = true
	if err != nil {
		if errors.Is(err, rime.ErrConflict) || errors.Is(err, rime.ErrSnapshotUnavailable) {
			return rime.ErrConflict
		}
		return db.handleRecordCommitError(err)
	}
	db.metrics.localCommits.Add(1)
	db.metrics.localCommitMutations.Add(uint64(changeCount))
	db.metrics.localCommitLatencyNanos.Add(uint64(time.Since(commitStarted)))
	generationNow, generationErr := db.store.StateGeneration()
	changed := generationErr != nil || generationNow > previousGeneration || adapter.PublicationGeneration() > previousPublication
	if generationErr == nil {
		db.materializedGeneration.Store(generationNow)
	}
	if changed {
		if db.subMgr != nil {
			db.subMgr.notifyChange(false)
		}
		if repl := db.replManager(); repl != nil {
			repl.NotifyLocal()
		}
	}
	return nil
}

func (db *DB) handleRecordCommitError(err error) error {
	if errors.Is(err, recordcodec.ErrValueTooLarge) {
		return fmt.Errorf("%w: %w", ErrValueTooLarge, err)
	}
	if errors.Is(err, rimeadapter.ErrMaterializer) || errors.Is(err, rimeadapter.ErrUncertain) {
		db.setState(StateFailed)
	}
	var uncertain *rimeadapter.UncertainCommitError
	if errors.As(err, &uncertain) {
		return &CommitOutcomeUncertainError{TxID: uncertain.TxID, Cause: uncertain.Cause}
	}
	if errors.Is(err, rimeadapter.ErrUnsupported) {
		return fmt.Errorf("%w: %v", ErrUnsupportedSchema, err)
	}
	return err
}

// applyRemoteRecords materializes one accepted merge while the root apply
// coordinator is held.
func (db *DB) applyRemoteRecords(ctx context.Context, result state.MergeResult) error {
	return db.applyRecordWinners(ctx, result, true)
}

func (db *DB) applyRecordWinners(ctx context.Context, result state.MergeResult, remote bool) error {
	if err := db.recordAdapter.ApplyRemote(ctx, result); err != nil {
		return err
	}
	if remote {
		db.metrics.remoteApplyWinners.Add(uint64(len(result.Winners)))
	}
	db.materializedGeneration.Store(result.Generation)
	if db.subMgr != nil {
		db.subMgr.notifyChange(false)
	}
	return nil
}

func (db *DB) rebuildRecordMaterializer(ctx context.Context) error {
	definitions := db.currentRecordDefinitions()
	if len(definitions) == 0 {
		return nil
	}
	manifest, err := db.store.LoadSchemaManifest()
	if err != nil {
		return err
	}
	if manifest == nil {
		return fmt.Errorf("murmur: typed rebuild has no schema manifest: %w", ErrSchemaMismatch)
	}
	rdb := rime.New()
	adapter, err := rimeadapter.New(db.store, rdb, manifest, recordcodec.NewCodecRegistry(), recordcodec.Limits{
		MaxBytes: uint64(db.cfg.MaxReplicatedValueBytes),
	})
	if err != nil {
		rdb.Close()
		return err
	}
	if err := db.configureRecordAdapter(adapter); err != nil {
		rdb.Close()
		return err
	}
	tables := make(map[string]any, len(definitions))
	for _, definition := range definitions {
		table, err := definition.register(adapter)
		if err != nil {
			rdb.Close()
			return err
		}
		tables[strings.ToLower(definition.name)] = table
	}
	if err := adapter.Rebuild(ctx, 1024); err != nil {
		rdb.Close()
		return err
	}
	db.recordMu.Lock()
	if db.recordDB != nil {
		db.retiredRecordDBs = append(db.retiredRecordDBs, db.recordDB)
	}
	db.recordDB = rdb
	db.recordAdapter = adapter
	db.recordTables = tables
	db.recordGeneration++
	db.recordMu.Unlock()
	if generation, err := db.store.StateGeneration(); err != nil {
		return err
	} else {
		db.materializedGeneration.Store(generation)
	}
	return nil
}

func (db *DB) currentRecordDefinitions() []TableDefinition {
	db.recordMu.RLock()
	defer db.recordMu.RUnlock()
	return append([]TableDefinition(nil), db.recordDefinitions...)
}
