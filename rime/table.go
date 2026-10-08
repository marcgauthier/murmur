package rime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
)

// TableOption configures a table at registration.
type TableOption[T any] func(*tableConfig[T])

type tableConfig[T any] struct {
	name      string
	shards    int
	compounds []compoundDef
	checks    []func(*T) error
	fks       []fkDef
	fkEnforce *bool
	cloner    func(*T) *T
}

type fkDef struct {
	local    string
	refTable string
	refField string
}

// WithTableName overrides the default table name (the Go struct name).
func WithTableName[T any](name string) TableOption[T] {
	return func(c *tableConfig[T]) { c.name = name }
}

// WithTableShards sets the shard count for this table.
func WithTableShards[T any](n int) TableOption[T] {
	return func(c *tableConfig[T]) {
		if n > 0 {
			c.shards = n
		}
	}
}

// WithCompound registers a compound index over the named fields.
func WithCompound[T any](name string, fields ...string) TableOption[T] {
	return func(c *tableConfig[T]) {
		c.compounds = append(c.compounds, compoundDef{name: name, fields: append([]string(nil), fields...)})
	}
}

// WithCheck registers a CHECK constraint evaluated before commit.
func WithCheck[T any](fn func(*T) error) TableOption[T] {
	return func(c *tableConfig[T]) { c.checks = append(c.checks, fn) }
}

// WithForeignKey registers a foreign key: local field values must exist in
// refTable.refField. Enforcement follows WithTableForeignKeys or the DB default.
func WithForeignKey[T any](local, refTable, refField string) TableOption[T] {
	return func(c *tableConfig[T]) {
		c.fks = append(c.fks, fkDef{local: local, refTable: refTable, refField: refField})
	}
}

// WithTableForeignKeys enables or disables FK enforcement for this table,
// overriding the database default.
func WithTableForeignKeys[T any](enforce bool) TableOption[T] {
	return func(c *tableConfig[T]) { c.fkEnforce = &enforce }
}

// WithCloner overrides the Clone mechanism (normally auto-detected via the
// Cloner[T] interface).
func WithCloner[T any](fn func(*T) *T) TableOption[T] {
	return func(c *tableConfig[T]) { c.cloner = fn }
}

// Table is a registered, sharded, MVCC collection of records of type T.
type Table[T any] struct {
	db     *DB
	name   string
	sch    *Schema
	pk     fieldMeta
	pkGet  func(*T) any
	clone  func(*T) *T
	shards []*tShard[T]
	idx    *indexSet
	gets   map[string]func(*T) any
	// equals compares one field between two records without boxing values
	// into interfaces; used for unchanged-field fast paths.
	equals map[string]func(*T, *T) bool
	// keyKind selects per-field index storage; the key* maps hold native
	// extractors (populated only for matching-kind fields) so index
	// maintenance hashes native values with no interface boxing.
	keyKind map[string]keyKind
	keyStr  map[string]func(*T) string
	keyI64  map[string]func(*T) int64
	keyU64  map[string]func(*T) uint64
	keyF64  map[string]func(*T) float64
	keyBool map[string]func(*T) bool

	checks []func(*T) error
	fks    []fkDef
	fkOn   *bool

	lastChange atomic.Uint64 // newest commit touching this table
	pc         *planCache
	planHits   atomic.Uint64
	planMisses atomic.Uint64

	// hasBeforeOp/hasBeforeCommit/hasAfter skip hook dispatch (including
	// the table lock) on the common no-hook path. Hooks are append-only:
	// a flag set under mu is never cleared, so a stale false is
	// impossible and a stale true only takes the slow path.
	hasBeforeOp     atomic.Bool
	hasBeforeCommit atomic.Bool
	hasAfter        atomic.Bool
	// skipValidate skips constraint validation when the schema has no
	// not-null fields, checks, or active foreign keys. Recomputed under mu
	// at every mutation; concurrent registration races resolve like the
	// hook flags (a commit may miss a concurrently added constraint,
	// exactly as if its lock had landed before the registration).
	skipValidate atomic.Bool

	mu           sync.RWMutex // guards hooks + event subscribers
	beforeInsert []func(*T) error
	afterInsert  []func(*T)
	beforeUpdate []func(old, newv *T) error
	afterUpdate  []func(old, newv *T)
	beforeDelete []func(*T) error
	afterDelete  []func(*T)
	beforeCommit []func(*Tx) error
	afterCommit  []func(TxID)
	afterSave    []func(Change[T])
	bus          *eventBus[T]
}

// Register compiles schema metadata for T and creates its sharded storage.
// The struct name is the default table name.
func Register[T any](db *DB, opts ...TableOption[T]) (*Table[T], error) {
	if db.faulted.Load() {
		return nil, ErrDBFaulted
	}
	if db.closed.Load() {
		return nil, ErrDBClosed
	}
	cfg := tableConfig[T]{shards: db.cfg.shardCount}
	for _, o := range opts {
		o(&cfg)
	}
	var zero T
	typ := reflect.TypeOf(zero)
	if typ == nil {
		return nil, fmt.Errorf("%w: nil record type", ErrBadSchema)
	}
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	name := typ.Name()
	if cfg.name != "" {
		name = cfg.name
	}
	sch, err := buildSchema(name, typ)
	if err != nil {
		return nil, err
	}
	sch.compound = append(sch.compound, cfg.compounds...)
	for _, cd := range sch.compound {
		for _, f := range cd.fields {
			if _, ok := sch.byName[f]; !ok {
				return nil, fmt.Errorf("%w: compound index %s references unknown field %s", ErrBadSchema, cd.name, f)
			}
		}
	}
	pk := sch.fields[sch.primary]
	pkGet, _, err := getter[T](sch, pk.name)
	if err != nil {
		return nil, err
	}
	clone := cfg.cloner
	if clone == nil {
		if c, ok := any(new(T)).(Cloner[T]); ok {
			_ = c
			clone = func(src *T) *T { return any(src).(Cloner[T]).Clone() }
		}
	}
	t := &Table[T]{
		db: db, name: name, sch: sch, pk: pk, pkGet: pkGet, clone: clone,
		gets:    map[string]func(*T) any{},
		equals:  map[string]func(*T, *T) bool{},
		keyKind: map[string]keyKind{},
		keyStr:  map[string]func(*T) string{},
		keyI64:  map[string]func(*T) int64{},
		keyU64:  map[string]func(*T) uint64{},
		keyF64:  map[string]func(*T) float64{},
		keyBool: map[string]func(*T) bool{},
		checks:  cfg.checks, fks: cfg.fks, fkOn: cfg.fkEnforce,
		pc: &planCache{items: map[string]*cachedPlan{}},
	}
	for _, fm := range sch.fields {
		g, _, err := getter[T](sch, fm.name)
		if err != nil {
			return nil, err
		}
		t.gets[fm.name] = g
		t.equals[fm.name] = equalFuncFor[T](fm)
		t.keyKind[fm.name] = keyKindOf(fm.typ)
		setFieldKey(t, fm)
	}
	n := cfg.shards
	if n <= 0 {
		n = DefaultShardCount
	}
	t.shards = make([]*tShard[T], n)
	for i := range t.shards {
		t.shards[i] = newTShard[T]()
	}
	t.idx, err = buildIndexes(sch)
	if err != nil {
		return nil, err
	}
	t.recomputeValidate()
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, dup := db.tables[name]; dup {
		return nil, fmt.Errorf("%w: %s", ErrTableExists, name)
	}
	db.tables[name] = t
	db.byType[typ] = t
	return t, nil
}

// Name returns the table name.
func (t *Table[T]) Name() string { return t.name }

// Schema returns the compiled schema metadata.
func (t *Table[T]) Schema() *Schema { return t.sch }

func (t *Table[T]) tableName() string { return t.name }

func (t *Table[T]) preparedChange(key any, base TxID, old, next any, op Operation) PreparedChange {
	change := PreparedChange{Table: t.name, Key: key, Operation: op, Base: base, owner: t}
	if old != nil {
		change.Old = cloneForUpdate(t.clone, old.(*T))
	}
	if next != nil {
		change.New = cloneForUpdate(t.clone, next.(*T))
	}
	return change
}
func (t *Table[T]) schemaOf() *Schema { return t.sch }

// In returns an execution handle bound to tx. The table itself is unchanged.
func (t *Table[T]) In(tx *Tx) BoundTable[T] {
	return BoundTable[T]{table: t, tx: tx, ctx: context.Background()}
}

// WithContext returns an execution handle using ctx for operations.
func (t *Table[T]) WithContext(ctx context.Context) BoundTable[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	return BoundTable[T]{table: t, ctx: ctx}
}

// All starts a query matching every record.
func (t *Table[T]) All() *Query[T] { return t.Where() }

// Count returns the number of records in the latest snapshot.
func (t *Table[T]) Count() (int, error) { return t.All().Count() }

// Exists reports whether the table contains any record.
func (t *Table[T]) Exists() (bool, error) { return t.All().Exists() }

// Get returns the latest record for key.
func (t *Table[T]) Get(key any) (*T, error) { return t.getAt(nil, key) }

// Lookup returns a record and found=false for an absent key.
func (t *Table[T]) Lookup(key any) (*T, bool, error) {
	v, err := t.Get(key)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	return v, err == nil, err
}

// Insert adds a record, failing if its primary key already exists.
func (t *Table[T]) Insert(rec *T) error {
	return t.upsertOperation(nil, func(tx *Tx) error { return t.insertAt(tx, rec) })
}

// Upsert inserts rec or replaces the record with the same primary key.
func (t *Table[T]) Upsert(rec *T) error { return t.upsertAt(nil, rec) }

// Update edits a private copy of an existing record.
func (t *Table[T]) Update(key any, fn func(*T) error) error {
	return t.upsertOperation(nil, func(tx *Tx) error { return t.updateAt(tx, key, fn) })
}

// Delete removes key.
func (t *Table[T]) Delete(key any) error {
	return t.upsertOperation(nil, func(tx *Tx) error { return t.deleteAt(tx, key) })
}

func (t *Table[T]) upsertOperation(tx *Tx, fn func(*Tx) error) error {
	if tx != nil {
		return fn(tx)
	}
	return t.db.WriteTx(fn)
}

// BoundTable is a lightweight table handle bound to an optional transaction
// and context. It shares storage and indexes with its source Table.
type BoundTable[T any] struct {
	table *Table[T]
	tx    *Tx
	ctx   context.Context
}

func (b BoundTable[T]) In(tx *Tx) BoundTable[T] { b.tx = tx; return b }
func (b BoundTable[T]) WithContext(ctx context.Context) BoundTable[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	b.ctx = ctx
	return b
}
func (b BoundTable[T]) Where(exprs ...Expr[T]) *Query[T] {
	q := b.table.Where(exprs...)
	q.tx, q.ctx = b.tx, b.context()
	return q
}
func (b BoundTable[T]) Compile(exprs ...Expr[T]) *Compiled[T] {
	c := b.table.Compile(exprs...)
	c.tx, c.ctx = b.tx, b.context()
	return c
}
func (b BoundTable[T]) All() *Query[T]        { return b.Where() }
func (b BoundTable[T]) Count() (int, error)   { return b.All().Count() }
func (b BoundTable[T]) Exists() (bool, error) { return b.All().Exists() }
func (b BoundTable[T]) context() context.Context {
	if b.ctx == nil {
		return context.Background()
	}
	return b.ctx
}
func (b BoundTable[T]) runWrite(fn func(*Tx) error) error {
	if err := b.context().Err(); err != nil {
		return err
	}
	if b.tx != nil {
		return fn(b.tx)
	}
	return b.table.db.WriteTxContext(b.context(), fn)
}
func (b BoundTable[T]) Get(key any) (*T, error) {
	if err := b.context().Err(); err != nil {
		return nil, err
	}
	return b.table.getAt(b.tx, key)
}
func (b BoundTable[T]) Lookup(key any) (*T, bool, error) {
	v, err := b.Get(key)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	return v, err == nil, err
}
func (b BoundTable[T]) Insert(rec *T) error {
	return b.runWrite(func(tx *Tx) error { return b.table.insertAt(tx, rec) })
}
func (b BoundTable[T]) Upsert(rec *T) error {
	return b.runWrite(func(tx *Tx) error { return b.table.upsertAt(tx, rec) })
}
func (b BoundTable[T]) Update(key any, fn func(*T) error) error {
	return b.runWrite(func(tx *Tx) error { return b.table.updateAt(tx, key, fn) })
}
func (b BoundTable[T]) Delete(key any) error {
	return b.runWrite(func(tx *Tx) error { return b.table.deleteAt(tx, key) })
}
func (b BoundTable[T]) InsertMany(records []*T) error {
	return b.runWrite(func(tx *Tx) error {
		return b.table.batchAt(tx, len(records), "insert", func(tx *Tx, i int) error {
			if err := b.context().Err(); err != nil {
				return err
			}
			return b.table.insertAt(tx, records[i])
		})
	})
}
func (b BoundTable[T]) UpsertMany(records []*T) error {
	return b.runWrite(func(tx *Tx) error { return b.table.upsertManyContextAt(b.context(), tx, records) })
}
func (b BoundTable[T]) DeleteMany(keys []any) error {
	return b.runWrite(func(tx *Tx) error {
		return b.table.batchAt(tx, len(keys), "delete", func(tx *Tx, i int) error {
			if err := b.context().Err(); err != nil {
				return err
			}
			return b.table.deleteAt(tx, keys[i])
		})
	})
}

func (t *Table[T]) InsertMany(records []*T) error { return t.In(nil).InsertMany(records) }
func (t *Table[T]) UpsertMany(records []*T) error { return t.In(nil).UpsertMany(records) }
func (t *Table[T]) DeleteMany(keys []any) error   { return t.In(nil).DeleteMany(keys) }

// setFieldKey compiles the native index-key extractor for one field.
// Reads use typed reflect accessors (.String/.Int/...) that never box the
// value into an interface.
func setFieldKey[T any](t *Table[T], fm fieldMeta) {
	idx := fm.index
	at := func(r *T) reflect.Value { return reflect.ValueOf(r).Elem().FieldByIndex(idx) }
	switch keyKindOf(fm.typ) {
	case kString:
		t.keyStr[fm.name] = func(r *T) string { return at(r).String() }
	case kInt64:
		t.keyI64[fm.name] = func(r *T) int64 { return at(r).Int() }
	case kUint64:
		t.keyU64[fm.name] = func(r *T) uint64 { return at(r).Uint() }
	case kFloat64:
		t.keyF64[fm.name] = func(r *T) float64 { return at(r).Float() }
	case kBool:
		t.keyBool[fm.name] = func(r *T) bool { return at(r).Bool() }
	}
}

func (t *Table[T]) shardFor(key any) *tShard[T] {
	return t.shards[t.shardIndexFor(key)]
}

// shardIndexFor resolves the owning shard without a pointer chase so staged
// writes can cache it once and reuse it at check and publish time.
func (t *Table[T]) shardIndexFor(key any) int {
	return int(hashKey(key) % uint64(len(t.shards)))
}

// runBeforeCommit executes table BeforeCommit hooks. It backs innerTable.
func (t *Table[T]) runBeforeCommit(tx *Tx) error {
	if !t.hasBeforeCommit.Load() {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, fn := range t.beforeCommit {
		if err := fn(tx); err != nil {
			return err
		}
	}
	return nil
}

// newPending builds one buffered write. Commit callbacks stay nil so commit
// dispatches to allocation-free table methods; tests may patch the fields.
func (t *Table[T]) newPending(key any, base TxID, present, baseTomb bool, val *T, del bool, op Operation, old *T) *pendingWrite {
	// Pooled: every field is overwritten so no state survives from the
	// struct's previous transaction.
	p := pendingWritePool.Get().(*pendingWrite)
	p.table = t
	p.key = key
	p.shard = t.shardIndexFor(key)
	p.base = base
	p.present = present
	p.baseTomb = baseTomb
	p.op = op
	p.del = del
	p.uniClean = false
	p.superseded = false
	p.val = nil
	p.old = nil
	if !del {
		p.val = val
	}
	if old != nil {
		p.old = old
	}
	p.check = nil
	p.releaseUnique = nil
	p.validate = nil
	p.apply = nil
	return p
}

// checkBase reports the live head version, honoring a patched check.
func (t *Table[T]) checkBase(p *pendingWrite) (TxID, bool) {
	if p.check != nil {
		return p.check()
	}
	h, ok := t.headRLockShard(t.shards[p.shard], p.key)
	if !ok {
		return 0, false
	}
	return h.commit, true
}

// releasePending releases the head's unique values into same-commit claims.
func (t *Table[T]) releasePending(p *pendingWrite, st *commitState) {
	if p.releaseUnique != nil {
		p.releaseUnique(st)
		return
	}
	head, ok := t.headRLockShard(t.shards[p.shard], p.key)
	if !ok || head.tomb {
		return
	}
	for _, field := range t.idx.uniqueFields {
		st.fieldClaims(t, field)[t.gets[field](head.val)] = nil
	}
}

// checkAndRelease verifies the optimistic base and stages unique releases
// under a single shard read. Final rows whose unique values equal the head's
// are marked clean so validation skips their unique checks.
func (t *Table[T]) checkAndRelease(p *pendingWrite, st *commitState) error {
	if p.check != nil || p.releaseUnique != nil {
		// Patched callbacks (tests): preserve exact legacy behavior.
		cur, present := t.checkBase(p)
		if cur != p.base || present != p.present {
			return ErrConflict
		}
		t.releasePending(p, st)
		return nil
	}
	s := t.shards[p.shard]
	s.mu.RLock()
	c, ok := s.rows[p.key]
	var h version[T]
	if ok {
		h, ok = c.latest()
	}
	s.mu.RUnlock()
	var cur TxID
	if ok {
		cur = h.commit
	}
	if cur != p.base || ok != p.present {
		// GC may collect our tombstone base between staging and commit;
		// absent-after-tombstone is still deleted, not a conflict. Any
		// intervening writer commit would leave an entry behind, so an
		// absent head proves no writer touched the key.
		if !(p.baseTomb && !ok) {
			return ErrConflict
		}
	}
	if p.superseded {
		return nil
	}
	p.uniClean = false
	if !ok || h.tomb || p.del {
		if !ok || h.tomb {
			return nil
		}
		for _, field := range t.idx.uniqueFields {
			st.fieldClaims(t, field)[t.gets[field](h.val)] = nil
		}
		return nil
	}
	rec := p.val.(*T)
	for _, field := range t.idx.uniqueFields {
		if !t.equals[field](h.val, rec) {
			for _, f2 := range t.idx.uniqueFields {
				st.fieldClaims(t, f2)[t.gets[f2](h.val)] = nil
			}
			return nil
		}
	}
	p.uniClean = true
	return nil
}

// validatePending runs the general multi-write validation for one row.
func (t *Table[T]) validatePending(p *pendingWrite, st *commitState) error {
	if p.validate != nil {
		return p.validate(st)
	}
	if p.del {
		return nil
	}
	rec := p.val.(*T)
	if t.pkGet(rec) != p.key {
		return ErrCheck
	}
	if err := t.validateNew(rec); err != nil {
		return err
	}
	if p.superseded {
		return nil
	}
	if p.uniClean {
		return nil
	}
	return idxCheckUnique(t.idx, t, p.key, rec, st)
}

// validateSingle validates a lone write without claim maps. Unique values
// identical to the operation-time record skip live-map checks entirely;
// new or changed values consult the live unique maps directly. There is no
// same-transaction interplay by construction (exactly one buffered write).
func (t *Table[T]) validateSingle(p *pendingWrite) error {
	rec := p.val.(*T)
	if t.pkGet(rec) != p.key {
		return ErrCheck
	}
	if err := t.validateNew(rec); err != nil {
		return err
	}
	var old *T
	if p.old != nil {
		old = p.old.(*T)
	}
	t.idx.mu.RLock()
	defer t.idx.mu.RUnlock()
	for _, field := range t.idx.uniqueFields {
		if old != nil && t.equals[field](old, rec) {
			continue
		}
		if owner, ok := t.uniqueLiveOwner(field, rec); ok && owner != nil && owner != p.key {
			return &ConstraintError{Table: t.name, Field: field, Value: t.gets[field](rec), Err: ErrUnique}
		}
	}
	return nil
}

// uniqueLiveOwner reports the live owner of rec's unique value using native
// keys with no boxing. The caller holds the index lock.
func (t *Table[T]) uniqueLiveOwner(field string, rec *T) (any, bool) {
	switch t.keyKind[field] {
	case kString:
		owner, ok := t.idx.uniqueStr[field][t.keyStr[field](rec)]
		return owner, ok
	case kInt64:
		owner, ok := t.idx.uniqueI64[field][t.keyI64[field](rec)]
		return owner, ok
	case kUint64:
		owner, ok := t.idx.uniqueU64[field][t.keyU64[field](rec)]
		return owner, ok
	case kFloat64:
		owner, ok := t.idx.uniqueF64[field][t.keyF64[field](rec)]
		return owner, ok
	case kBool:
		owner, ok := t.idx.uniqueBool[field][t.keyBool[field](rec)]
		return owner, ok
	default:
		owner, ok := t.idx.uniqueAny[field][t.gets[field](rec)]
		return owner, ok
	}
}

// applyPending publishes one version and returns its post-commit effect.
// Unique values publish separately through the claims overlay.
func (t *Table[T]) applyPending(commit TxID, p *pendingWrite) pendingEffect {
	ch := t.applyPublish(commit, p, false)
	return pendingEffect{table: t, op: ch.Operation, commit: commit, old: ch.Old, newv: ch.New}
}

// preparePendingChain constructs one immutable MVCC head before managed
// durability. previous is the prepared head for an earlier write to the same
// key in this transaction, or nil to read the stable committed head.
func (t *Table[T]) preparePendingChain(commit TxID, p *pendingWrite, previous any) any {
	var c *chain[T]
	if previous != nil {
		c = previous.(*chain[T])
	} else {
		s := t.shards[p.shard]
		s.mu.RLock()
		c = s.rows[p.key]
		s.mu.RUnlock()
	}
	v := version[T]{commit: commit, tomb: p.del}
	if !p.del {
		v.val = p.val.(*T)
	}
	if c == nil {
		return &chain[T]{vers: []version[T]{v}, total: 1}
	}
	return c.appendVersion(v)
}

type compoundIndexEffect struct {
	index     *compoundIdx
	oldValues []any
	newValues []any
}

type orderedIndexEffect struct {
	index  *orderedIdx
	old    any
	new    any
	hasOld bool
	hasNew bool
	node   *orderedNode
	item   *orderedItem
}

type prefixIndexEffect struct {
	index          *trieIdx
	old, new       string
	hasOld, hasNew bool
}

type hashIndexEffect struct {
	field          string
	kind           keyKind
	old, new       any
	hasOld, hasNew bool
}

type managedIndexEffects struct {
	compound []compoundIndexEffect
	ordered  []orderedIndexEffect
	prefix   []prefixIndexEffect
	hash     []hashIndexEffect
}

type preparedOrderedMap struct {
	index  *orderedIdx
	values map[any]*orderedItem
	hint   int
}

func (t *Table[T]) prepareOrderedMaps(writes []*pendingWrite) any {
	counts := make(map[*orderedIdx]map[any]bool)
	t.idx.mu.Lock()
	defer t.idx.mu.Unlock()
	for _, pending := range writes {
		effects, ok := pending.preparedIndexEffects.(managedIndexEffects)
		if !ok {
			continue
		}
		for _, effect := range effects.ordered {
			if effect.index == nil {
				continue
			}
			present := counts[effect.index]
			if present == nil {
				present = make(map[any]bool)
				counts[effect.index] = present
			}
			isPresent, known := present[pending.key]
			if !known {
				_, isPresent = effect.index.byKey[pending.key]
			}
			if effect.hasOld && !effect.hasNew {
				isPresent = false
			}
			if effect.hasNew {
				isPresent = true
			}
			present[pending.key] = isPresent
		}
	}
	if len(counts) == 0 {
		return nil
	}
	prepared := make([]preparedOrderedMap, 0, len(counts))
	for index, finalRows := range counts {
		// Account for keys touched by this transaction that are being removed.
		// Build the final cardinality from the old directory and the final
		// presence of each touched key rather than copying on every mutation.
		projected := len(index.byKey)
		for key, exists := range finalRows {
			_, was := index.byKey[key]
			if was && !exists {
				projected--
			} else if !was && exists {
				projected++
			}
		}
		target := index.byKey
		hint := index.byKeyHint
		replaced := false
		if projected > index.byKeyHint {
			if hint < 8 {
				hint = 8
			}
			for hint < projected {
				hint *= 2
			}
			target = make(map[any]*orderedItem, hint)
			replaced = true
			for key, item := range index.byKey {
				target[key] = item
			}
		}
		// Force backing groups for keys added by this transaction while the
		// index lock is held. Remove placeholders before readers can observe
		// them; the storage remains available to publication.
		reserved := make([]any, 0)
		for key, exists := range finalRows {
			if exists {
				if _, present := target[key]; !present {
					target[key] = nil
					reserved = append(reserved, key)
				}
			}
		}
		for _, key := range reserved {
			delete(target, key)
		}
		if replaced {
			prepared = append(prepared, preparedOrderedMap{index: index, values: target, hint: hint})
		}
	}
	if len(prepared) == 0 {
		return nil
	}
	return prepared
}

func (t *Table[T]) installPreparedOrderedMaps(value any) {
	prepared, ok := value.([]preparedOrderedMap)
	if !ok {
		panic("rime: invalid prepared ordered maps")
	}
	if len(prepared) == 0 {
		return
	}
	t.idx.mu.Lock()
	defer t.idx.mu.Unlock()
	for _, item := range prepared {
		item.index.byKey = item.values
		item.index.byKeyHint = item.hint
	}
}

// prepareIndexEffects extracts compound keys before a managed adapter makes
// the write durable. Index mutation itself remains in the ordered publish
// section, while this potentially allocating projection work happens first.
func (t *Table[T]) prepareIndexEffects(p *pendingWrite, reserve int) any {
	if len(t.idx.comp) == 0 && len(t.idx.ordered) == 0 && len(t.idx.prefix) == 0 && len(t.idx.hashFields) == 0 {
		return nil
	}
	var old *T
	if p.old != nil {
		old, _ = p.old.(*T)
	}
	var next *T
	if !p.del {
		next, _ = p.val.(*T)
	}
	prepared := managedIndexEffects{
		compound: make([]compoundIndexEffect, 0, len(t.idx.compoundOrder)),
		ordered:  make([]orderedIndexEffect, 0, len(t.idx.orderedFields)),
		prefix:   make([]prefixIndexEffect, 0, len(t.idx.prefix)),
		hash:     make([]hashIndexEffect, 0, len(t.idx.hashFields)),
	}
	for _, field := range t.idx.hashFields {
		if old != nil && next != nil && t.equals[field](old, next) {
			continue
		}
		effect := hashIndexEffect{field: field, kind: t.idx.hashKinds[field], hasOld: old != nil, hasNew: next != nil}
		if old != nil {
			effect.old = t.hashIndexValue(field, old)
		}
		if next != nil {
			effect.new = t.hashIndexValue(field, next)
		}
		prepared.hash = append(prepared.hash, effect)
	}
	t.idx.mu.Lock()
	for field, index := range t.idx.prefix {
		if old != nil && next != nil && t.equals[field](old, next) {
			continue
		}
		effect := prefixIndexEffect{index: index, hasOld: old != nil, hasNew: next != nil}
		if old != nil {
			effect.old = t.keyStr[field](old)
		}
		if next != nil {
			effect.new = t.keyStr[field](next)
			index.prepareAdd(effect.new, reserve)
		}
		prepared.prefix = append(prepared.prefix, effect)
	}
	t.idx.mu.Unlock()
	for _, field := range t.idx.orderedFields {
		if old != nil && next != nil && t.equals[field](old, next) {
			continue
		}
		effect := orderedIndexEffect{index: t.idx.ordered[field], hasOld: old != nil, hasNew: next != nil}
		if old != nil {
			effect.old = t.orderedIndexValue(field, old)
		}
		if next != nil {
			effect.new = t.orderedIndexValue(field, next)
			effect.node = &orderedNode{}
			effect.item = &orderedItem{}
		}
		prepared.ordered = append(prepared.ordered, effect)
	}
	for _, name := range t.idx.compoundOrder {
		index := t.idx.comp[name]
		if old != nil && next != nil {
			unchanged := true
			for _, field := range index.fields {
				if !t.equals[field](old, next) {
					unchanged = false
					break
				}
			}
			if unchanged {
				continue
			}
		}
		effect := compoundIndexEffect{index: index}
		if old != nil {
			effect.oldValues = make([]any, len(index.fields))
			for i, field := range index.fields {
				effect.oldValues[i] = t.gets[field](old)
			}
		}
		if next != nil {
			effect.newValues = make([]any, len(index.fields))
			for i, field := range index.fields {
				effect.newValues[i] = t.gets[field](next)
			}
			t.idx.mu.Lock()
			index.prepareAdd(effect.newValues, reserve)
			t.idx.mu.Unlock()
		}
		prepared.compound = append(prepared.compound, effect)
	}
	if len(prepared.ordered) == 0 && len(prepared.compound) == 0 && len(prepared.prefix) == 0 && len(prepared.hash) == 0 {
		// A typed empty slice distinguishes "prepared and unchanged" from
		// the nil sentinel used by standalone commits that have no prepared
		// index effects.
		return prepared
	}
	return prepared
}

func (t *Table[T]) hashIndexValue(field string, rec *T) any {
	switch t.idx.hashKinds[field] {
	case kString:
		return t.keyStr[field](rec)
	case kInt64:
		return t.keyI64[field](rec)
	case kUint64:
		return t.keyU64[field](rec)
	case kFloat64:
		return t.keyF64[field](rec)
	case kBool:
		return t.keyBool[field](rec)
	default:
		return t.gets[field](rec)
	}
}

func (t *Table[T]) finishPreparedIndexEffects(writes []*pendingWrite) {
	t.idx.mu.Lock()
	defer t.idx.mu.Unlock()
	for _, pending := range writes {
		if pending.table != t {
			continue
		}
		effects, ok := pending.preparedIndexEffects.(managedIndexEffects)
		if !ok {
			continue
		}
		for _, effect := range effects.prefix {
			if effect.hasOld {
				effect.index.prune(effect.old)
			}
		}
		for _, effect := range effects.compound {
			if effect.oldValues != nil {
				effect.index.prune(effect.oldValues)
			}
		}
	}
}

func (t *Table[T]) abortPreparedIndexEffects(writes []*pendingWrite) {
	t.idx.mu.Lock()
	defer t.idx.mu.Unlock()
	for _, pending := range writes {
		if pending.table != t {
			continue
		}
		effects, ok := pending.preparedIndexEffects.(managedIndexEffects)
		if !ok {
			continue
		}
		for _, effect := range effects.prefix {
			if effect.hasNew {
				effect.index.prune(effect.new)
			}
		}
		for _, effect := range effects.compound {
			if effect.newValues != nil {
				effect.index.prune(effect.newValues)
			}
		}
	}
}

func hashAddCounts[T any, K comparable](t *Table[T], writes []*pendingWrite, field string, extract func(*T) K) map[K]int {
	counts := make(map[K]int)
	for _, pending := range writes {
		if pending.table != t || pending.apply != nil {
			continue
		}
		var old *T
		if pending.old != nil {
			old, _ = pending.old.(*T)
		}
		var next *T
		if !pending.del {
			next, _ = pending.val.(*T)
		}
		if old != nil && next != nil && t.equals[field](old, next) {
			continue
		}
		if next != nil {
			counts[extract(next)]++
		}
	}
	return counts
}

func (t *Table[T]) prepareHashIndexes(writes []*pendingWrite) any {
	if len(t.idx.hashFields) == 0 {
		return nil
	}
	prepared := &preparedHashIndexes{}
	t.idx.mu.Lock()
	defer t.idx.mu.Unlock()
	for _, field := range t.idx.hashFields {
		switch t.idx.hashKinds[field] {
		case kString:
			reserveHashValues(t.idx, t.idx.hashStr, field, hashAddCounts(t, writes, field, t.keyStr[field]), &prepared.seeds)
		case kInt64:
			reserveHashValues(t.idx, t.idx.hashI64, field, hashAddCounts(t, writes, field, t.keyI64[field]), &prepared.seeds)
		case kUint64:
			reserveHashValues(t.idx, t.idx.hashU64, field, hashAddCounts(t, writes, field, t.keyU64[field]), &prepared.seeds)
		case kFloat64:
			reserveHashValues(t.idx, t.idx.hashF64, field, hashAddCounts(t, writes, field, t.keyF64[field]), &prepared.seeds)
		case kBool:
			reserveHashValues(t.idx, t.idx.hashBool, field, hashAddCounts(t, writes, field, t.keyBool[field]), &prepared.seeds)
		default:
			reserveHashValues(t.idx, t.idx.hashAny, field, hashAddCounts(t, writes, field, t.gets[field]), &prepared.seeds)
		}
	}
	return prepared
}

func (t *Table[T]) installPreparedHashIndexes(value any) {
	prepared, ok := value.(*preparedHashIndexes)
	if !ok || prepared == nil {
		panic("rime: invalid prepared hash indexes")
	}
	t.idx.mu.Lock()
	for _, seed := range prepared.seeds {
		seed()
	}
	t.idx.mu.Unlock()
}

func (t *Table[T]) orderedIndexValue(field string, rec *T) any {
	switch t.idx.ordered[field].kind {
	case kString:
		return t.keyStr[field](rec)
	case kInt64:
		return t.keyI64[field](rec)
	case kUint64:
		return t.keyU64[field](rec)
	case kFloat64:
		return t.keyF64[field](rec)
	default:
		return t.gets[field](rec)
	}
}

// prepareShardRows clones a touched shard map only when managed publication
// crosses its conservative growth threshold. The replacement reserves enough
// space for all new keys while avoiding an O(n) copy for every ordinary insert.
func (t *Table[T]) prepareShardRows(shard int, keys []any) any {
	s := t.shards[shard]
	s.mu.RLock()
	missing := 0
	for _, key := range keys {
		if _, ok := s.rows[key]; !ok {
			missing++
		}
	}
	if missing == 0 {
		s.mu.RUnlock()
		return nil
	}
	need := len(s.rows) + missing
	if need <= s.rowMapHint {
		s.mu.RUnlock()
		return nil
	}
	hint := s.rowMapHint
	if hint < 1 {
		hint = 6
	}
	for hint < need {
		hint *= 2
	}
	rows := make(map[any]*chain[T], hint)
	for key, c := range s.rows {
		rows[key] = c
	}
	// Populate and clear the new map with the transaction's missing keys so
	// the backing groups are allocated now, even when the replacement starts
	// empty and Go defers storage allocation for a capacity-only map.
	reserved := make([]any, 0, missing)
	for _, key := range keys {
		if _, exists := rows[key]; !exists {
			rows[key] = nil
			reserved = append(reserved, key)
		}
	}
	for _, key := range reserved {
		delete(rows, key)
	}
	s.mu.RUnlock()
	return preparedShardRows[T]{rows: rows, hint: hint}
}

func (t *Table[T]) installPreparedShardRows(shard int, rows any) {
	s := t.shards[shard]
	prepared := rows.(preparedShardRows[T])
	s.mu.Lock()
	s.rows = prepared.rows
	s.rowMapHint = prepared.hint
	s.mu.Unlock()
}

type preparedUniqueField struct {
	field   string
	kind    keyKind
	values  any
	updates []preparedUniqueClaim
	replace bool
	hint    int
}

type preparedUniqueClaim struct {
	key   any
	owner any
}

// prepareUniqueClaims clones each touched unique map and applies its final
// transaction claims before a managed adapter makes the write durable.
func (t *Table[T]) prepareUniqueClaims(claims map[string]map[any]any) any {
	prepared := make([]preparedUniqueField, 0, len(claims))
	t.idx.mu.RLock()
	defer t.idx.mu.RUnlock()
	for field, updates := range claims {
		kind := t.keyKind[field]
		hint := t.idx.uniqueHints[field]
		switch kind {
		case kString:
			prepared = append(prepared, prepareUniqueField(field, kind, t.idx.uniqueStr[field], updates, normStr, hint))
		case kInt64:
			prepared = append(prepared, prepareUniqueField(field, kind, t.idx.uniqueI64[field], updates, normI64, hint))
		case kUint64:
			prepared = append(prepared, prepareUniqueField(field, kind, t.idx.uniqueU64[field], updates, normU64, hint))
		case kFloat64:
			prepared = append(prepared, prepareUniqueField(field, kind, t.idx.uniqueF64[field], updates, normF64, hint))
		case kBool:
			prepared = append(prepared, prepareUniqueField(field, kind, t.idx.uniqueBool[field], updates, normBool, hint))
		default:
			prepared = append(prepared, prepareUniqueField(field, kind, t.idx.uniqueAny[field], updates, func(v any) (any, bool) {
				if v == nil {
					return nil, false
				}
				return v, true
			}, hint))
		}
	}
	return prepared
}

func prepareUniqueField[K comparable](field string, kind keyKind, live map[K]any, updates map[any]any, normalize func(any) (K, bool), hint int) preparedUniqueField {
	if hint < len(live) {
		hint = len(live)
	}
	if hint < uniqueMapInitialHint {
		hint = uniqueMapInitialHint
	}
	claims := make([]preparedUniqueClaim, 0, len(updates))
	projected := len(live)
	for value, owner := range updates {
		key, ok := normalize(value)
		if !ok {
			panic("rime: cannot normalize prepared unique value")
		}
		_, exists := live[key]
		if owner == nil && exists {
			projected--
		} else if owner != nil && !exists {
			projected++
		}
		claims = append(claims, preparedUniqueClaim{key: key, owner: owner})
	}
	if projected <= hint {
		return preparedUniqueField{field: field, kind: kind, updates: claims, hint: hint}
	}
	newHint := hint
	for newHint < projected {
		newHint *= 2
	}
	values := make(map[K]any, newHint)
	for value, owner := range live {
		values[value] = owner
	}
	applyPreparedUniqueClaims(values, claims)
	return preparedUniqueField{field: field, kind: kind, values: values, replace: true, hint: newHint}
}

func applyPreparedUniqueClaims[K comparable](values map[K]any, claims []preparedUniqueClaim) {
	// Release first so swaps and delete-plus-insert transactions do not need
	// temporary capacity beyond their validated final map size.
	for _, claim := range claims {
		if claim.owner == nil {
			delete(values, claim.key.(K))
		}
	}
	for _, claim := range claims {
		if claim.owner != nil {
			values[claim.key.(K)] = claim.owner
		}
	}
}

// installPreparedUniqueClaims swaps prebuilt maps under the index lock. Map
// growth and claim normalization have already happened before durable commit.
func (t *Table[T]) installPreparedUniqueClaims(value any) {
	prepared, ok := value.([]preparedUniqueField)
	if !ok {
		panic("rime: invalid prepared unique maps")
	}
	t.idx.mu.Lock()
	defer t.idx.mu.Unlock()
	for _, field := range prepared {
		switch field.kind {
		case kString:
			values := t.idx.uniqueStr[field.field]
			if field.replace {
				values = field.values.(map[string]any)
				t.idx.uniqueStr[field.field] = values
			} else {
				applyPreparedUniqueClaims(values, field.updates)
			}
		case kInt64:
			values := t.idx.uniqueI64[field.field]
			if field.replace {
				values = field.values.(map[int64]any)
				t.idx.uniqueI64[field.field] = values
			} else {
				applyPreparedUniqueClaims(values, field.updates)
			}
		case kUint64:
			values := t.idx.uniqueU64[field.field]
			if field.replace {
				values = field.values.(map[uint64]any)
				t.idx.uniqueU64[field.field] = values
			} else {
				applyPreparedUniqueClaims(values, field.updates)
			}
		case kFloat64:
			values := t.idx.uniqueF64[field.field]
			if field.replace {
				values = field.values.(map[float64]any)
				t.idx.uniqueF64[field.field] = values
			} else {
				applyPreparedUniqueClaims(values, field.updates)
			}
		case kBool:
			values := t.idx.uniqueBool[field.field]
			if field.replace {
				values = field.values.(map[bool]any)
				t.idx.uniqueBool[field.field] = values
			} else {
				applyPreparedUniqueClaims(values, field.updates)
			}
		default:
			values := t.idx.uniqueAny[field.field]
			if field.replace {
				values = field.values.(map[any]any)
				t.idx.uniqueAny[field.field] = values
			} else {
				applyPreparedUniqueClaims(values, field.updates)
			}
		}
		t.idx.uniqueHints[field.field] = field.hint
	}
}

// applyPendingSingle publishes one version for a lone-write transaction,
// maintaining unique values inline under the same index lock instead of a
// second acquisition.
func (t *Table[T]) applyPendingSingle(commit TxID, p *pendingWrite) pendingEffect {
	ch := t.applyPublish(commit, p, true)
	return pendingEffect{table: t, op: ch.Operation, commit: commit, old: ch.Old, newv: ch.New}
}

// installSingleUnique mirrors the claims overlay for one write: it removes
// the operation-time record's unique values and installs the new ones,
// skipping both when every unique value is unchanged. The caller holds the
// index lock.
func (t *Table[T]) installSingleUnique(p *pendingWrite) {
	if len(t.idx.uniqueFields) == 0 {
		return
	}
	var rec *T
	if !p.del {
		rec = p.val.(*T)
	}
	var old *T
	if p.old != nil {
		old = p.old.(*T)
	}
	if old != nil && rec != nil {
		unchanged := true
		for _, field := range t.idx.uniqueFields {
			if !t.equals[field](old, rec) {
				unchanged = false
				break
			}
		}
		if unchanged {
			return
		}
	}
	for _, field := range t.idx.uniqueFields {
		t.installUniqueField(field, p.key, old, rec)
	}
}

// installUniqueField syncs one unique map with native keys. The caller holds
// the index lock.
func (t *Table[T]) installUniqueField(field string, key any, old, rec *T) {
	switch t.keyKind[field] {
	case kString:
		installUniqueNative(t.idx.uniqueStr[field], t.keyStr[field], key, old, rec)
	case kInt64:
		installUniqueNative(t.idx.uniqueI64[field], t.keyI64[field], key, old, rec)
	case kUint64:
		installUniqueNative(t.idx.uniqueU64[field], t.keyU64[field], key, old, rec)
	case kFloat64:
		installUniqueNative(t.idx.uniqueF64[field], t.keyF64[field], key, old, rec)
	case kBool:
		installUniqueNative(t.idx.uniqueBool[field], t.keyBool[field], key, old, rec)
	default:
		installUniqueNative(t.idx.uniqueAny[field], t.gets[field], key, old, rec)
	}
}

func installUniqueNative[T any, K comparable](m map[K]any, ex func(*T) K, key any, old, rec *T) {
	if old != nil {
		ov := ex(old)
		if k, ok := m[ov]; ok && k == key {
			delete(m, ov)
		}
	}
	if rec != nil {
		m[ex(rec)] = key
	}
}

// fireAfterEffect fires hooks and events for one collected effect.
func (t *Table[T]) fireAfterEffect(e pendingEffect) {
	old, _ := e.old.(*T)
	newv, _ := e.newv.(*T)
	t.fireAfter(Change[T]{Operation: e.op, TxID: e.commit, Old: old, New: newv})
}

func (t *Table[T]) snapOf(tx *Tx) TxID {
	if tx == nil {
		return t.db.latest()
	}
	return tx.snap
}

// fkTarget reports whether val exists in field of this table at snap.
// It backs foreign-key checks from other tables.
func (t *Table[T]) fkTarget(field string, val any, snap TxID) bool {
	if field == t.pk.name {
		for _, s := range t.shards {
			s.mu.RLock()
			c, ok := s.rows[val]
			s.mu.RUnlock()
			if !ok {
				continue
			}
			if v, ok := c.visible(snap); ok && !v.tomb {
				return true
			}
		}
		return idxUniqueHas(t.idx, t, field, val, snap)
	}
	if idxUniqueHas(t.idx, t, field, val, snap) {
		return true
	}
	scope := t.db.newScope()
	defer scope.release()
	for k := range t.idx.candidatesInto(scope.cmap(), field, val) {
		if t.fkTarget(t.pk.name, k, snap) {
			return true
		}
	}
	return false
}

// ownPending returns the newest buffered write for key in tx, if any.
// ownPendingMapThreshold bounds the linear stacking scan: transactions at
// or below this many writes never build a staged map.
const ownPendingMapThreshold = 64

func (t *Table[T]) ownPending(tx *Tx, key any) *pendingWrite {
	if tx == nil {
		return nil
	}
	if tx.pendingByKey != nil {
		return tx.pendingByKey[writeID{t: t, k: key}]
	}
	// Small transactions scan the short slice; only large ones pay for a
	// staged map, built once with headroom for the rest of the batch.
	if len(tx.pending) > ownPendingMapThreshold {
		n := len(tx.pending) * 4
		if tx.stageHint > n {
			// Caller-declared size: trust it exactly.
			n = tx.stageHint
		} else if n > 4096 {
			n = 4096
		}
		m := make(map[writeID]*pendingWrite, n)
		for _, q := range tx.pending {
			m[writeID{t: q.table, k: q.key}] = q
		}
		tx.pendingByKey = m
		return m[writeID{t: t, k: key}]
	}
	for i := len(tx.pending) - 1; i >= 0; i-- {
		if q := tx.pending[i]; q.table == t && q.key == key {
			return q
		}
	}
	return nil
}

// head returns the live head version of key (commit lock or shard lock held
// by caller discipline: readers use RLock, commit path holds commitMu).
func (t *Table[T]) headRLock(key any) (version[T], bool) {
	return t.headRLockShard(t.shards[t.shardIndexFor(key)], key)
}

// headRLockShard reads the live head of a pre-resolved shard, skipping the
// key-hash lookup on commit paths that cached the shard at stage time.
func (t *Table[T]) headRLockShard(s *tShard[T], key any) (version[T], bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.rows[key]
	if !ok {
		return version[T]{}, false
	}
	return c.latest()
}

// Get returns the record visible at the transaction snapshot.
// A nil tx reads at the latest snapshot. The returned pointer is immutable.
func (t *Table[T]) getAt(tx *Tx, key any) (*T, error) {
	if err := t.checkTx(tx); err != nil {
		return nil, err
	}
	if tx != nil && (tx.write || tx.preview) {
		if p := t.ownPending(tx, key); p != nil {
			if p.del {
				return nil, ErrNotFound
			}
			return p.val.(*T), nil
		}
	}
	if tx == nil {
		t.db.viewMu.RLock()
	}
	snap := t.snapOf(tx)
	s := t.shardFor(key)
	s.mu.RLock()
	c, ok := s.rows[key]
	s.mu.RUnlock()
	if tx == nil {
		t.db.viewMu.RUnlock()
	}
	if err := tx.mustOpen(); err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	v, ok := c.visible(snap)
	if !ok || v.tomb {
		return nil, ErrNotFound
	}
	return v.val, nil
}

// prepareNew clones rec for storage, applying defaults and uuid5 generation.
func (t *Table[T]) prepareNew(rec *T) (*T, any, error) {
	if rec == nil {
		return nil, nil, fmt.Errorf("%w: nil record", ErrBadSchema)
	}
	out := cloneForUpdate(t.clone, rec)
	applyDefaults(t.sch, out)
	if t.pk.uuid5 {
		rv := reflect.ValueOf(out).Elem().FieldByIndex(t.pk.index)
		if isZero(rv.Interface()) {
			id := NewUUIDv5(TableNamespace(t.name), fmt.Sprintf("%s:%d", t.name, t.db.idSeq.Add(1)))
			switch rv.Kind() {
			case reflect.String:
				rv.SetString(id.String())
			case reflect.Array:
				if rv.Type() == reflect.TypeOf(UUID{}) {
					rv.Set(reflect.ValueOf(id))
				} else if rv.Type().Len() == 16 && rv.Type().Elem().Kind() == reflect.Uint8 {
					for i := 0; i < 16; i++ {
						rv.Index(i).SetUint(uint64(id[i]))
					}
				}
			}
		}
	}
	key := t.pkGet(out)
	return out, key, t.validateNew(out)
}

// validateNew runs NOT NULL, CHECK, and FK validation for a new value.
func (t *Table[T]) validateNew(rec *T) error {
	if t.skipValidate.Load() {
		return nil
	}
	rv := reflect.ValueOf(rec).Elem()
	for _, fm := range t.sch.fields {
		if fm.notNull && fm.optional && isAbsent(rv.FieldByIndex(fm.index).Interface()) {
			return &ConstraintError{Table: t.name, Field: fm.name, Err: ErrNotNull}
		}
	}
	t.mu.RLock()
	checks := append([]func(*T) error(nil), t.checks...)
	t.mu.RUnlock()
	for _, c := range checks {
		if err := c(rec); err != nil {
			return &ConstraintError{Table: t.name, Err: fmt.Errorf("%w: %v", ErrCheck, err)}
		}
	}
	return t.validateFK(rec)
}

func (t *Table[T]) fkEnabled() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.fkEnabledLocked()
}

func (t *Table[T]) fkEnabledLocked() bool {
	if t.fkOn != nil {
		return *t.fkOn
	}
	return t.db.cfg.enforceFK
}

// recomputeValidate refreshes the validation fast-path flag. The caller
// holds t.mu (or the table is still pre-publication at registration).
func (t *Table[T]) recomputeValidate() {
	enabled := t.fkEnabledLocked()
	if len(t.checks) > 0 || (len(t.fks) > 0 && enabled) {
		t.skipValidate.Store(false)
		return
	}
	for _, fm := range t.sch.fields {
		if fm.notNull || (fm.fkTable != "" && enabled) {
			t.skipValidate.Store(false)
			return
		}
	}
	t.skipValidate.Store(true)
}

func (t *Table[T]) validateFK(rec *T) error {
	t.db.viewMu.RLock()
	defer t.db.viewMu.RUnlock()
	if !t.fkEnabled() {
		return nil
	}
	snap := t.db.latest()
	t.mu.RLock()
	fks := append([]fkDef(nil), t.fks...)
	t.mu.RUnlock()
	for _, fk := range fks {
		g, ok := t.gets[fk.local]
		if !ok {
			continue
		}
		val := g(rec)
		if isZero(val) {
			continue
		}
		rt, ok := t.db.tableByName(fk.refTable)
		if !ok {
			return &ConstraintError{Table: t.name, Field: fk.local, Value: val, Err: fmt.Errorf("%w: %s", ErrTableNotFound, fk.refTable)}
		}
		if !rt.fkTarget(fk.refField, val, snap) {
			return &ConstraintError{Table: t.name, Field: fk.local, Value: val, Err: ErrForeignKey}
		}
	}
	// Tag-declared FKs (fk=Table.Field) on schema fields.
	for _, fm := range t.sch.fields {
		if fm.fkTable == "" {
			continue
		}
		val := t.gets[fm.name](rec)
		if isZero(val) {
			continue
		}
		rt, ok := t.db.tableByName(fm.fkTable)
		if !ok {
			continue // unknown ref table: enforced once registered; skip until then
		}
		if !rt.fkTarget(fm.fkField, val, snap) {
			return &ConstraintError{Table: t.name, Field: fm.name, Value: val, Err: ErrForeignKey}
		}
	}
	return nil
}

func (t *Table[T]) runBeforeInsert(rec *T) error {
	if !t.hasBeforeOp.Load() {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, fn := range t.beforeInsert {
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table[T]) runBeforeUpdate(old, newv *T) error {
	if !t.hasBeforeOp.Load() {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, fn := range t.beforeUpdate {
		if err := fn(old, newv); err != nil {
			return err
		}
	}
	return nil
}

func (t *Table[T]) runBeforeDelete(rec *T) error {
	if !t.hasBeforeOp.Load() {
		return nil
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, fn := range t.beforeDelete {
		if err := fn(rec); err != nil {
			return err
		}
	}
	return nil
}

// buffer appends a pending write, enforcing the per-tx mutation cap.
// The staged lookup map is built only once a transaction holds more than one
// write; single-write transactions use a linear fallback in ownPending.
func (t *Table[T]) buffer(tx *Tx, p *pendingWrite) error {
	if max := t.db.cfg.maxMutations; max > 0 && len(tx.pending)+1 > max {
		return ErrLimitExceeded
	}
	tx.pending = append(tx.pending, p)
	// The staged map exists only after ownPending builds it on demand;
	// write-only batches never pay for it.
	if tx.pendingByKey != nil {
		tx.pendingByKey[writeID{t: p.table, k: p.key}] = p
	}
	return nil
}

// growPending pre-sizes the pending slice for a batch of n writes so large
// batches pay one scratch allocation instead of repeated growth. The staged
// lookup map stays lazy: ownPending builds it only when a read or stacking
// check actually needs it.
func growPending(tx *Tx, n int) {
	total := len(tx.pending) + n
	if cap(tx.pending) < total {
		np := make([]*pendingWrite, len(tx.pending), total)
		copy(np, tx.pending)
		tx.pending = np
	}
	if total > tx.stageHint {
		tx.stageHint = total
	}
}

// Save inserts or replaces rec (upsert). A nil tx auto-commits.
func (t *Table[T]) upsertAt(tx *Tx, rec *T) error {
	if tx == nil {
		return t.db.WriteTx(func(tx *Tx) error { return t.upsertAt(tx, rec) })
	}
	if err := t.checkTx(tx); err != nil {
		return err
	}
	if err := tx.mustWrite(); err != nil {
		return err
	}
	stored, key, err := t.prepareNew(rec)
	if err != nil {
		return err
	}
	base, present := t.snapshotBase(tx, key)
	var old *T
	if prior := t.ownPending(tx, key); prior != nil {
		if !prior.del {
			old = prior.val.(*T)
		}
	} else if present && !base.tomb {
		old = base.val
	}
	if old != nil {
		if err := t.runBeforeUpdate(old, stored); err != nil {
			return err
		}
	} else {
		if err := t.runBeforeInsert(stored); err != nil {
			return err
		}
	}
	op := OpUpdate
	if old == nil {
		op = OpInsert
		if base.tomb {
			old = nil
		}
	}
	var baseID TxID
	if present {
		baseID = base.commit
	}
	return t.buffer(tx, t.newPending(key, baseID, present, present && base.tomb, stored, false, op, old))
}

// Insert adds rec and fails with ErrAlreadyExists when the key is visible.
func (t *Table[T]) insertAt(tx *Tx, rec *T) error {
	if tx == nil {
		return t.db.WriteTx(func(tx *Tx) error { return t.insertAt(tx, rec) })
	}
	if err := t.checkTx(tx); err != nil {
		return err
	}
	if err := tx.mustWrite(); err != nil {
		return err
	}
	stored, key, err := t.prepareNew(rec)
	if err != nil {
		return err
	}
	if _, err := t.getAt(tx, key); err == nil {
		return ErrAlreadyExists
	}
	base, present := t.snapshotBase(tx, key)
	var baseID TxID
	if present {
		baseID = base.commit
	}
	if err := t.runBeforeInsert(stored); err != nil {
		return err
	}
	return t.buffer(tx, t.newPending(key, baseID, present, present && base.tomb, stored, false, OpInsert, nil))
}

// Update copy-on-write updates the record at key. fn mutates a private copy;
// the published original is never touched. A nil tx auto-commits.
func (t *Table[T]) updateAt(tx *Tx, key any, fn func(*T) error) error {
	if tx == nil {
		return t.db.WriteTx(func(tx *Tx) error { return t.updateAt(tx, key, fn) })
	}
	if err := t.checkTx(tx); err != nil {
		return err
	}
	if err := tx.mustWrite(); err != nil {
		return err
	}
	cur, err := t.getAt(tx, key)
	if err != nil {
		return err
	}
	// Pin the optimistic base at read time, before any user code runs.
	base, present := t.snapshotBase(tx, key)
	var baseID TxID
	if present {
		baseID = base.commit
	}
	next := cloneForUpdate(t.clone, cur)
	if err := fn(next); err != nil {
		return err
	}
	if err := t.validateNew(next); err != nil {
		return err
	}
	newKey := t.pkGet(next)
	if newKey != key {
		return fmt.Errorf("%w: primary key is immutable", ErrCheck)
	}
	if err := t.runBeforeUpdate(cur, next); err != nil {
		return err
	}
	return t.buffer(tx, t.newPending(key, baseID, present, present && base.tomb, next, false, OpUpdate, cur))
}

// Delete tombstones the record at key. A nil tx auto-commits.
func (t *Table[T]) deleteAt(tx *Tx, key any) error {
	if tx == nil {
		return t.db.WriteTx(func(tx *Tx) error { return t.deleteAt(tx, key) })
	}
	if err := t.checkTx(tx); err != nil {
		return err
	}
	if err := tx.mustWrite(); err != nil {
		return err
	}
	cur, err := t.getAt(tx, key)
	if err != nil {
		return err
	}
	base, present := t.snapshotBase(tx, key)
	var baseID TxID
	if present {
		baseID = base.commit
	}
	if err := t.runBeforeDelete(cur); err != nil {
		return err
	}
	return t.buffer(tx, t.newPending(key, baseID, present, present && base.tomb, nil, true, OpDelete, cur))
}

// SaveMany buffers an upsert per record.
func (t *Table[T]) upsertManyAt(tx *Tx, recs []*T) error {
	return t.batchAt(tx, len(recs), "upsert", func(tx *Tx, i int) error { return t.upsertAt(tx, recs[i]) })
}

func (t *Table[T]) insertManyAt(tx *Tx, recs []*T) error {
	return t.batchAt(tx, len(recs), "insert", func(tx *Tx, i int) error { return t.insertAt(tx, recs[i]) })
}

func (t *Table[T]) batchAt(tx *Tx, n int, operation string, apply func(*Tx, int) error) error {
	if tx == nil {
		return t.db.WriteTx(func(w *Tx) error { return t.batchAt(w, n, operation, apply) })
	}
	if err := t.checkTx(tx); err != nil {
		return err
	}
	if err := tx.mustWrite(); err != nil {
		return err
	}
	start, hint := len(tx.pending), tx.stageHint
	growPending(tx, n)
	rollback := func() {
		for _, p := range tx.pending[start:] {
			*p = pendingWrite{}
			pendingWritePool.Put(p)
		}
		tx.pending = tx.pending[:start]
		tx.stageHint = hint
		if tx.pendingByKey != nil {
			clear(tx.pendingByKey)
			for _, p := range tx.pending {
				tx.pendingByKey[writeID{t: p.table, k: p.key}] = p
			}
		}
	}
	for i := 0; i < n; i++ {
		if err := tx.Context().Err(); err != nil {
			rollback()
			return err
		}
		if err := apply(tx, i); err != nil {
			rollback()
			return &BatchError{Operation: operation, Index: i, Err: err}
		}
	}
	return nil
}

// SaveManyContext is SaveMany with cancellation checks between records.
func (t *Table[T]) upsertManyContextAt(ctx context.Context, tx *Tx, recs []*T) error {
	if tx == nil {
		return t.db.WriteTxContext(ctx, func(tx *Tx) error { return t.upsertManyContextAt(ctx, tx, recs) })
	}
	return t.batchAt(tx, len(recs), "upsert", func(tx *Tx, i int) error {
		if ctx != nil && i%64 == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		return t.upsertAt(tx, recs[i])
	})
}

// DeleteMany buffers a delete per key.
func (t *Table[T]) deleteManyAt(tx *Tx, keys []any) error {
	return t.batchAt(tx, len(keys), "delete", func(tx *Tx, i int) error { return t.deleteAt(tx, keys[i]) })
}

// applyPublish appends one committed version and maintains indexes.
// The caller holds db.commitMu. Index removal uses the live head values,
// which keeps stacked same-transaction writes consistent.
func (t *Table[T]) applyPublish(commit TxID, p *pendingWrite, directUnique bool) Change[T] {
	s := t.shards[p.shard]
	s.mu.Lock()
	var oldHead *T
	var v version[T]
	if p.del {
		v = version[T]{commit: commit, tomb: true}
	} else {
		v = version[T]{commit: commit, val: p.val.(*T)}
	}
	if c, ok := s.rows[p.key]; !ok {
		if prepared, ok := p.preparedChain.(*chain[T]); ok {
			s.rows[p.key] = prepared
		} else {
			s.rows[p.key] = &chain[T]{vers: []version[T]{v}, total: 1}
		}
	} else {
		if h, ok := c.latest(); ok && !h.tomb {
			oldHead = h.val
		}
		// Copy-on-write: published chains are immutable, so lock-free
		// readers holding the old pointer stay safe. Only the head
		// chunk is copied; older chunks are shared.
		if prepared, ok := p.preparedChain.(*chain[T]); ok {
			s.rows[p.key] = prepared
		} else {
			s.rows[p.key] = c.appendVersion(v)
		}
	}
	s.mu.Unlock()
	t.lastChange.Store(uint64(commit))
	onCommit(t, oldHead, p, directUnique)
	var ch Change[T]
	ch.Operation = p.op
	ch.TxID = commit
	ch.Old = oldHead
	if !p.del {
		ch.New = p.val.(*T)
	}
	return ch
}

// fireAfter runs After* hooks, AfterSave, AfterCommit hooks, and async events
// for one committed change. It runs after the commit lock is released.
func (t *Table[T]) fireAfter(ch Change[T]) {
	if !t.hasAfter.Load() {
		return
	}
	t.mu.RLock()
	ai, au, ad, as, ac := t.afterInsert, t.afterUpdate, t.afterDelete, t.afterSave, t.afterCommit
	bus := t.bus
	t.mu.RUnlock()
	switch ch.Operation {
	case OpInsert:
		for _, fn := range ai {
			fn(ch.New)
		}
	case OpUpdate:
		for _, fn := range au {
			fn(ch.Old, ch.New)
		}
	case OpDelete:
		for _, fn := range ad {
			fn(ch.Old)
		}
	}
	for _, fn := range as {
		fn(ch)
	}
	for _, fn := range ac {
		fn(ch.TxID)
	}
	if bus != nil {
		bus.emit(ch)
	}
}

// checkTx validates transaction ownership as well as its lifetime.
func (t *Table[T]) checkTx(tx *Tx) error {
	if tx == nil {
		if t.db.faulted.Load() {
			return ErrDBFaulted
		}
		if t.db.closed.Load() {
			return ErrDBClosed
		}
		return nil
	}
	if tx.db != t.db {
		if tx.preview {
			tx.viewViolation = ErrTxDatabase
		}
		return ErrTxDatabase
	}
	if tx.preview && !tx.viewSources[t] {
		tx.viewViolation = ErrViewDependency
		return ErrViewDependency
	}
	return tx.mustOpen()
}

func (t *Table[T]) snapshotBase(tx *Tx, key any) (version[T], bool) {
	if p := t.ownPending(tx, key); p != nil {
		// Stacked write: the prior write is final no longer.
		p.superseded = true
		var val *T
		if p.old != nil {
			val = p.old.(*T)
		}
		return version[T]{commit: p.base, val: val, tomb: val == nil}, p.present
	}
	s := t.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.rows[key]
	if !ok {
		return version[T]{}, false
	}
	return c.visible(tx.snap)
}
