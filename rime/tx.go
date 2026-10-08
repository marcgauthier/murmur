package rime

import (
	"context"
	"sync"
	"time"
)

// Operation identifies the kind of change delivered to hooks and events.
type Operation int

const (
	OpInsert Operation = iota + 1
	OpUpdate
	OpDelete
)

func (o Operation) String() string {
	switch o {
	case OpInsert:
		return "insert"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	}
	return "unknown"
}

// Change describes one committed record change with native pointers.
// Old is nil for inserts, New is nil for deletes.
type Change[T any] struct {
	Operation Operation
	TxID      TxID
	Old       *T
	New       *T
}

// PreparedChange is a detached description of one key's final staged state.
// Old and New are typed *T values (nil when absent); callers may inspect or
// encode them without being able to mutate the transaction's pending records.
// Key is the table's comparable primary-key value.
type PreparedChange struct {
	Table     string
	Key       any
	Operation Operation
	Base      TxID
	Old       any
	New       any
	owner     innerTable
}

// Tx is a database transaction. Read transactions hold an immutable MVCC
// snapshot; write transactions buffer private changes with optimistic
// concurrency and publish them atomically at commit.
//
// Tx is not safe for concurrent use by multiple goroutines.
type Tx struct {
	db            *DB
	id            uint64
	snap          TxID
	write         bool
	managed       bool
	preview       bool // read-only view evaluation with borrowed pending writes
	viewSources   map[innerTable]bool
	viewViolation error
	start         time.Time
	ctx           context.Context

	pending      []*pendingWrite
	pendingByKey map[writeID]*pendingWrite
	// stageHint is a caller-declared write count (from batch APIs) used to
	// size the staged map when it is first built.
	stageHint  int
	afters     []func()
	closed     bool
	invalid    error
	prepared   bool
	preparedTx *PreparedTx
	onClose    []func()
	// beforeDone skips BeforeCommit tables already run by the single-write
	// fast path when a hook grows the transaction and commits promote.
	beforeDone map[innerTable]bool
}

// commitState carries tentative same-commit unique claims so validation sees
// the whole transaction before anything is published.
type commitState struct {
	claims map[innerTable]map[string]map[any]any
	// sizeHint pre-sizes per-field claim maps so large batches pay one
	// allocation instead of repeated growth.
	sizeHint int
}

func (s *commitState) claimsFor(t innerTable) map[string]map[any]any {
	m, ok := s.claims[t]
	if !ok {
		m = map[string]map[any]any{}
		s.claims[t] = m
	}
	return m
}

// fieldClaims returns the claim map for one table field, creating it sized
// from the transaction width when first touched. Creation is lazy, so the
// hint tracks actual fill and giant transactions pay no regrowth.
func (s *commitState) fieldClaims(t innerTable, field string) map[any]any {
	m := s.claimsFor(t)
	fc := m[field]
	if fc == nil {
		fc = make(map[any]any, s.sizeHint)
		m[field] = fc
	}
	return fc
}

// pendingWrite is one buffered record mutation.
//
// The check/releaseUnique/validate/apply fields are optional overrides (kept
// settable for tests): when nil, commit dispatches to the table's methods,
// which avoids one closure allocation per field per row.
type pendingWrite struct {
	table innerTable
	key   any
	shard int  // owning shard index, cached at stage time
	base  TxID // head commit observed at operation time
	// uniClean marks a final row whose unique values equal the commit-time
	// head's: the live unique maps are already correct, so validation
	// skips its unique checks entirely. Sound because a concurrent commit
	// can only move a live unique entry by touching this row's key, which
	// the base check would reject. Defaults false; set only on the
	// verified-unchanged release path, so any staleness fails safe.
	uniClean bool
	// superseded marks a write stacked over by a later write to the same
	// key in the same transaction. Only non-superseded (final) writes
	// publish unique claims and validate uniques.
	superseded bool
	present    bool // whether the key existed at operation time
	// baseTomb records that the operation-time base was a tombstone. GC may
	// legally collect it before commit (no active snapshot observes the key
	// as live), so validation must accept a now-absent head as still
	// deleted rather than report a conflict.
	baseTomb bool
	val      any
	del      bool
	op       Operation
	old      any
	// preparedChain is the copy-on-write MVCC head built before a managed
	// adapter's durable commit. It is installed directly during publication.
	preparedChain any
	// preparedIndexEffects contains compound index keys extracted before a
	// managed adapter's durable commit.
	preparedIndexEffects any

	check         func() (TxID, bool)
	releaseUnique func(st *commitState)
	validate      func(st *commitState) error
	apply         func(commit TxID) func()
}

// pendingWritePool recycles buffered mutations across transactions: one
// pooled struct replaces one allocation per staged row. Release clears every
// field (dropping record references so pooled structs never retain memory),
// and acquisition overwrites every field, so neither stale callbacks nor
// stale records can leak from one transaction into the next. Release is
// idempotent: both finish and Close nil the slice after releasing, so a
// transaction that is both committed and closed releases exactly once.
var pendingWritePool = sync.Pool{New: func() any { return &pendingWrite{} }}

// releasePendingWrites returns tx's buffered writes to the pool. It must run
// before tx.pending is cleared, on every transaction exit path.
func releasePendingWrites(tx *Tx) {
	for _, p := range tx.pending {
		*p = pendingWrite{}
		pendingWritePool.Put(p)
	}
	tx.pending = nil
}

// pendingEffect is one committed change collected without a callback closure.
// old/newv hold *T pointers (nil when absent); the table reattaches the type
// when firing hooks.
type pendingEffect struct {
	table  innerTable
	op     Operation
	commit TxID
	old    any
	newv   any
}

// commitAfter preserves post-commit order across dispatched effects and
// patched per-row callbacks: exactly one of table/effect or fn is set.
type commitAfter struct {
	table  innerTable
	effect pendingEffect
	fn     func()
}

// writeID identifies one buffered key for same-transaction stacking.
type writeID struct {
	t innerTable
	k any
}

type preparedPending struct {
	first *pendingWrite
	last  *pendingWrite
}

// PreparedChanges returns a detached, coalesced view of the transaction's
// final writes. It does not validate conflicts or reserve publication; Commit
// remains authoritative and may return ErrConflict.
func (tx *Tx) PreparedChanges() ([]PreparedChange, error) {
	if tx == nil {
		return nil, ErrTxClosed
	}
	if err := tx.mustOpen(); err != nil {
		return nil, err
	}
	if !tx.write {
		return nil, ErrTxReadOnly
	}
	groups := make([]preparedPending, 0, len(tx.pending))
	positions := make(map[writeID]int, len(tx.pending))
	for _, p := range tx.pending {
		id := writeID{t: p.table, k: p.key}
		if i, ok := positions[id]; ok {
			groups[i].last = p
			continue
		}
		positions[id] = len(groups)
		groups = append(groups, preparedPending{first: p, last: p})
	}
	out := make([]PreparedChange, 0, len(groups))
	for _, group := range groups {
		first, last := group.first, group.last
		var old, next any
		if first.old != nil {
			old = first.old
		}
		if !last.del {
			next = last.val
		}
		if old == nil && next == nil {
			continue // insert-then-delete, or delete of an absent key
		}
		op := OpUpdate
		switch {
		case old == nil:
			op = OpInsert
		case next == nil:
			op = OpDelete
		}
		out = append(out, first.table.preparedChange(first.key, first.base, old, next, op))
	}
	return out, nil
}

// PreparedTx owns the database commit coordinator from validation through
// publication. A persistence adapter must call Publish after durable success
// or Abort when it guarantees that no durable commit occurred.
type PreparedTx struct {
	tx          *Tx
	state       *commitState
	commit      TxID
	views       []func()
	changes     []PreparedChange
	rowMaps     []preparedShardMap
	hashMaps    []preparedHashMap
	uniqueMaps  []preparedUniqueMap
	orderedMaps []preparedShardMap
	// afters is sized before a managed caller commits durable state. The
	// publication critical section must not allocate its callback list after
	// persistence has accepted the transaction.
	afters []commitAfter
	done   bool
}

type preparedShardMap struct {
	table innerTable
	shard int
	rows  any
}

type preparedUniqueMap struct {
	table innerTable
	maps  any
}

type preparedHashMap struct {
	table innerTable
	maps  any
}

// PrepareCommit validates staged writes and reserves their ordered publication.
// The returned token holds the database commit coordinator until Publish or
// Abort, so adapters should keep the durable operation bounded.
func (tx *Tx) PrepareCommit() (*PreparedTx, error) {
	if tx == nil {
		return nil, ErrTxClosed
	}
	if err := tx.mustOpen(); err != nil {
		return nil, err
	}
	if !tx.write {
		return nil, ErrTxReadOnly
	}
	if !tx.managed {
		return nil, ErrManagedCommitRequired
	}
	if tx.prepared {
		return nil, ErrTxPrepared
	}
	if tx.db.cfg.maxTxAge > 0 && time.Since(tx.start) > tx.db.cfg.maxTxAge {
		return nil, ErrLimitExceeded
	}
	db := tx.db
	db.commitMu.Lock()
	locked := true
	defer func() {
		if locked {
			db.commitMu.Unlock()
		}
	}()
	if err := tx.mustOpen(); err != nil {
		return nil, err
	}

	// Run each table's commit hook once, including tables added by hooks.
	seenTables := make(map[innerTable]bool, len(tx.pending))
	for table := range tx.beforeDone {
		seenTables[table] = true
	}
	for i := 0; i < len(tx.pending); i++ {
		table := tx.pending[i].table
		if seenTables[table] {
			continue
		}
		seenTables[table] = true
		if err := table.runBeforeCommit(tx); err != nil {
			return nil, err
		}
	}
	if max := db.cfg.maxMutations; max > 0 && len(tx.pending) > max {
		return nil, ErrLimitExceeded
	}

	st := &commitState{claims: map[innerTable]map[string]map[any]any{}, sizeHint: len(tx.pending)}
	for _, p := range tx.pending {
		if err := p.table.checkAndRelease(p, st); err != nil {
			return nil, err
		}
	}
	commit := db.latest()
	var views []func()
	if len(tx.pending) > 0 {
		commit++
		for _, p := range tx.pending {
			if err := p.table.validatePending(p, st); err != nil {
				return nil, err
			}
		}
		if err := tx.mustOpen(); err != nil {
			return nil, err
		}
		views = db.prepareViews(tx, commit)
		if err := tx.mustOpen(); err != nil {
			return nil, err
		}
	}
	changes, err := tx.PreparedChanges()
	if err != nil {
		return nil, err
	}
	// The reservation prevents commits and GC from changing any base chain
	// until Publish or Abort. Build the complete sequence of MVCC heads here,
	// before an adapter is allowed to make the change durable.
	preparedHeads := make(map[writeID]any, len(tx.pending))
	shardKeys := make(map[innerTable]map[int]map[any]struct{})
	tableWriteCounts := make(map[innerTable]int)
	for _, w := range tx.pending {
		if w.apply == nil {
			tableWriteCounts[w.table]++
		}
	}
	for _, w := range tx.pending {
		if w.apply != nil { // test-only patched publication callback
			continue
		}
		id := writeID{t: w.table, k: w.key}
		previous, exists := preparedHeads[id]
		if !exists {
			previous = nil
		}
		w.preparedChain = w.table.preparePendingChain(commit, w, previous)
		w.preparedIndexEffects = w.table.prepareIndexEffects(w, tableWriteCounts[w.table])
		preparedHeads[id] = w.preparedChain
		byShard := shardKeys[w.table]
		if byShard == nil {
			byShard = make(map[int]map[any]struct{})
			shardKeys[w.table] = byShard
		}
		keys := byShard[w.shard]
		if keys == nil {
			keys = make(map[any]struct{})
			byShard[w.shard] = keys
		}
		keys[w.key] = struct{}{}
	}
	var hashMaps []preparedHashMap
	for table := range tableWriteCounts {
		if prepared := table.prepareHashIndexes(tx.pending); prepared != nil {
			hashMaps = append(hashMaps, preparedHashMap{table: table, maps: prepared})
		}
	}
	var rowMaps []preparedShardMap
	for table, byShard := range shardKeys {
		for shard, keysSet := range byShard {
			keys := make([]any, 0, len(keysSet))
			for key := range keysSet {
				keys = append(keys, key)
			}
			if rows := table.prepareShardRows(shard, keys); rows != nil {
				rowMaps = append(rowMaps, preparedShardMap{table: table, shard: shard, rows: rows})
			}
		}
	}
	var uniqueMaps []preparedUniqueMap
	patchedApply := false
	for _, pending := range tx.pending {
		patchedApply = patchedApply || pending.apply != nil
	}
	if !patchedApply {
		for table, claims := range st.claims {
			if len(claims) == 0 {
				continue
			}
			uniqueMaps = append(uniqueMaps, preparedUniqueMap{
				table: table,
				maps:  table.prepareUniqueClaims(claims),
			})
		}
	}
	var orderedMaps []preparedShardMap
	for table := range shardKeys {
		writes := make([]*pendingWrite, 0)
		for _, pending := range tx.pending {
			if pending.table == table {
				writes = append(writes, pending)
			}
		}
		if maps := table.prepareOrderedMaps(writes); maps != nil {
			orderedMaps = append(orderedMaps, preparedShardMap{table: table, rows: maps})
		}
	}
	token := &PreparedTx{
		tx: tx, state: st, commit: commit, views: views, changes: changes,
		afters: make([]commitAfter, 0, len(tx.pending)), rowMaps: rowMaps, hashMaps: hashMaps, uniqueMaps: uniqueMaps,
		orderedMaps: orderedMaps,
	}
	tx.prepared = true
	tx.preparedTx = token
	locked = false // ownership transfers to token
	return token, nil
}

// Changes returns the detached final-state deltas validated by PrepareCommit.
func (p *PreparedTx) Changes() []PreparedChange {
	if p == nil || p.done {
		return nil
	}
	out := make([]PreparedChange, len(p.changes))
	for i, change := range p.changes {
		out[i] = change.owner.preparedChange(change.Key, change.Base, change.Old, change.New, change.Operation)
	}
	return out
}

// CommitID is the RIME visibility generation this token will publish.
func (p *PreparedTx) CommitID() TxID {
	if p == nil {
		return 0
	}
	return p.commit
}

// Publish installs the prepared records and indexes atomically. Once the
// caller has durably committed, this method completes publication even if the
// transaction context is subsequently canceled.
func (p *PreparedTx) Publish() error {
	if p == nil || p.done || p.tx == nil || !p.tx.prepared {
		return ErrTxClosed
	}
	defer func() {
		clear(p.afters)
		p.afters = nil
		p.rowMaps = nil
	}()
	tx := p.tx
	defer tx.finish()
	if len(tx.pending) == 0 {
		p.done = true
		tx.prepared = false
		tx.preparedTx = nil
		tx.db.commitMu.Unlock()
	} else {
		afters, err := p.install()
		if err != nil {
			return err
		}
		for _, a := range afters {
			if a.table != nil {
				a.table.fireAfterEffect(a.effect)
			} else if a.fn != nil {
				a.fn()
			}
		}
	}
	for _, fn := range tx.afters {
		fn()
	}
	return nil
}

// install runs the locked publication section under the terminal-fault
// window: a panic here, after durable success, freezes the database with an
// uncertain outcome because the in-memory install may be partial. Locks are
// always released. After-commit callbacks run outside this window in Publish:
// the commit is already visible then, so a callback panic propagates like a
// standalone commit instead of faulting a healthy database.
func (p *PreparedTx) install() (afters []commitAfter, err error) {
	tx := p.tx
	db := tx.db
	viewLocked, commitLocked := false, true // PrepareCommit transferred commitMu.
	defer func() {
		if recovered := recover(); recovered != nil {
			// Durable callers cannot roll back now. Freeze the database because
			// the in-memory publication may be partial, and release both locks.
			db.faulted.Store(true)
			if viewLocked {
				db.viewMu.Unlock()
			}
			if commitLocked {
				db.commitMu.Unlock()
			}
			p.done = true
			tx.prepared = false
			tx.preparedTx = nil
			afters = nil
			err = &UncertainCommitError{Commit: p.commit, Cause: recovered}
		}
	}()
	db.viewMu.Lock()
	viewLocked = true
	for _, rows := range p.rowMaps {
		rows.table.installPreparedShardRows(rows.shard, rows.rows)
	}
	for _, hashes := range p.hashMaps {
		hashes.table.installPreparedHashIndexes(hashes.maps)
	}
	p.hashMaps = nil
	for _, maps := range p.orderedMaps {
		maps.table.installPreparedOrderedMaps(maps.rows)
	}
	p.orderedMaps = nil
	afters = p.afters[:0]
	for _, w := range tx.pending {
		if w.apply != nil {
			afters = append(afters, commitAfter{fn: w.apply(p.commit)})
			continue
		}
		afters = append(afters, commitAfter{table: w.table, effect: w.table.applyPending(p.commit, w)})
	}
	for i, w := range tx.pending {
		if w.apply != nil {
			continue
		}
		seen := false
		for j := 0; j < i; j++ {
			if tx.pending[j].apply == nil && tx.pending[j].table == w.table {
				seen = true
				break
			}
		}
		if !seen {
			w.table.finishPreparedIndexEffects(tx.pending)
		}
	}
	for _, prepared := range p.uniqueMaps {
		prepared.table.installPreparedUniqueClaims(prepared.maps)
	}
	if len(p.uniqueMaps) == 0 {
		// Test-only patched publication callbacks may create claims without
		// the normal typed-table preparation path.
		for table, claims := range p.state.claims {
			table.publishUniqueClaims(claims)
		}
	}
	p.uniqueMaps = nil
	for _, publish := range p.views {
		publish()
	}
	db.clock.Store(uint64(p.commit))
	db.viewMu.Unlock()
	viewLocked = false
	p.done = true
	tx.prepared = false
	tx.preparedTx = nil
	db.commitMu.Unlock()
	commitLocked = false
	return afters, nil
}

// Abort releases the publication reservation and discards the staged writes.
// Call it only when no durable commit was accepted.
func (p *PreparedTx) Abort() error {
	if p == nil || p.done || p.tx == nil {
		return nil
	}
	p.done = true
	tx := p.tx
	for i, pending := range tx.pending {
		if pending.apply != nil {
			continue
		}
		seen := false
		for j := 0; j < i; j++ {
			if tx.pending[j].apply == nil && tx.pending[j].table == pending.table {
				seen = true
				break
			}
		}
		if !seen {
			pending.table.abortPreparedIndexEffects(tx.pending)
		}
	}
	p.rowMaps = nil
	p.hashMaps = nil
	p.uniqueMaps = nil
	p.orderedMaps = nil
	tx.prepared = false
	tx.preparedTx = nil
	tx.db.commitMu.Unlock()
	tx.finish()
	return nil
}

// ReadTx opens a read transaction at the latest committed snapshot.
func (db *DB) ReadTx() *Tx {
	return db.ReadTxContext(context.Background())
}

// ReadTxContext opens a read transaction bound to ctx for cancellable scans.
func (db *DB) ReadTxContext(ctx context.Context) *Tx {
	if ctx == nil {
		ctx = context.Background()
	}
	db.viewMu.RLock()
	defer db.viewMu.RUnlock()
	tx := &Tx{db: db, snap: db.latest(), start: time.Now(), ctx: ctx}
	if db.faulted.Load() {
		tx.invalid = ErrDBFaulted
	} else if db.closed.Load() {
		tx.invalid = ErrDBClosed
	}
	if tx.invalid == nil {
		db.track(tx)
	}
	return tx
}

// ReadAt opens a read transaction pinned to a past snapshot. It is useful
// for repeatable historical reads and tests; snap must not exceed the latest
// committed ID. Operations return ErrSnapshotUnavailable when snap predates
// the GC retention horizon.
func (db *DB) ReadAt(snap TxID) *Tx {
	db.viewMu.RLock()
	defer db.viewMu.RUnlock()
	if latest := db.latest(); snap > latest {
		snap = latest
	}
	tx := &Tx{db: db, snap: snap, start: time.Now(), ctx: context.Background()}
	if snap < db.retainedFloor {
		tx.invalid = ErrSnapshotUnavailable
	}
	if db.closed.Load() {
		tx.invalid = ErrDBClosed
	}
	if db.faulted.Load() {
		tx.invalid = ErrDBFaulted
	}
	if tx.invalid == nil {
		db.track(tx)
	}
	return tx
}

// Snapshot returns the transaction's MVCC snapshot.
func (tx *Tx) Snapshot() TxID { return tx.snap }

// Age returns how long the transaction has been open.
func (tx *Tx) Age() time.Duration { return time.Since(tx.start) }

// Context returns the transaction's context.
func (tx *Tx) Context() context.Context {
	if tx.ctx == nil {
		return context.Background()
	}
	return tx.ctx
}

// Close releases the transaction snapshot. Buffered writes of an uncommitted
// write transaction are discarded (rollback).
func (tx *Tx) Close() {
	if tx.preparedTx != nil {
		tx.preparedTx.Abort()
		return
	}
	if tx.closed {
		return
	}
	tx.closed = true
	if !tx.preview {
		releasePendingWrites(tx)
	}
	tx.pending = nil
	tx.pendingByKey = nil
	tx.db.untrack(tx)
	for _, fn := range tx.onClose {
		fn()
	}
}

// Commit validates and publishes an explicit write transaction, then closes
// it whether publication succeeds or fails. A read transaction cannot commit.
func (tx *Tx) Commit() error {
	if tx == nil {
		return ErrTxClosed
	}
	if !tx.write {
		return ErrTxReadOnly
	}
	if tx.prepared {
		return ErrTxPrepared
	}
	defer tx.finish()
	return tx.commit()
}

// Rollback discards an explicit write transaction and releases its snapshot.
func (tx *Tx) Rollback() error {
	if tx == nil || tx.closed {
		return ErrTxClosed
	}
	if !tx.write {
		return ErrTxReadOnly
	}
	if tx.prepared {
		return ErrTxPrepared
	}
	tx.finish()
	return nil
}

// Grow pre-sizes transaction scratch for n additional writes, so large
// programmatic batches pay one scratch allocation instead of repeated
// growth. The batch APIs call this automatically; call it at the start of a
// WriteTx function when staging a known-large number of writes by hand. A
// nil receiver is a no-op.
func (tx *Tx) Grow(n int) {
	if tx == nil || n <= 0 || tx.preview || tx.prepared {
		return
	}
	growPending(tx, n)
}

func (tx *Tx) mustOpen() error {
	if tx == nil {
		return nil // nil Tx means auto-commit/auto-snapshot at the table layer
	}
	if tx.closed {
		return ErrTxClosed
	}
	if tx.invalid != nil {
		return tx.invalid
	}
	if tx.db.faulted.Load() {
		return ErrDBFaulted
	}
	if tx.write && tx.db.closed.Load() {
		return ErrDBClosed
	}
	if tx.db.cfg.maxTxAge > 0 && tx.Age() > tx.db.cfg.maxTxAge {
		return ErrLimitExceeded
	}
	if err := tx.Context().Err(); err != nil {
		return err
	}
	return nil
}

func (tx *Tx) mustWrite() error {
	if err := tx.mustOpen(); err != nil {
		return err
	}
	if tx != nil && !tx.write {
		if tx.preview {
			tx.viewViolation = ErrTxReadOnly
		}
		return ErrTxReadOnly
	}
	if tx != nil && tx.prepared {
		return ErrTxPrepared
	}
	return nil
}

// WriteTx runs fn inside a write transaction and commits on nil return.
// A non-nil return rolls back. Commit conflicts surface as ErrConflict.
func (db *DB) WriteTx(fn func(tx *Tx) error) error {
	return db.WriteTxContext(context.Background(), fn)
}

// WriteTxContext is WriteTx bound to ctx.
func (db *DB) WriteTxContext(ctx context.Context, fn func(tx *Tx) error) error {
	tx, err := db.beginTx(ctx, false)
	if err != nil {
		return err
	}
	defer tx.finish()
	if err := tx.mustOpen(); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.commit()
}

// BeginTx opens an explicit write transaction at the latest snapshot.
// Commit and Rollback both close the returned transaction.
func (db *DB) BeginTx(ctx context.Context) (*Tx, error) {
	return db.beginTx(ctx, true)
}

func (db *DB) beginTx(ctx context.Context, managed bool) (*Tx, error) {
	if db.faulted.Load() {
		return nil, ErrDBFaulted
	}
	if db.closed.Load() {
		return nil, ErrDBClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db.viewMu.RLock()
	tx := &Tx{db: db, snap: db.latest(), write: true, managed: managed, start: time.Now(), ctx: ctx}
	db.track(tx)
	db.viewMu.RUnlock()
	if err := tx.mustOpen(); err != nil {
		tx.finish()
		return nil, err
	}
	return tx, nil
}

// finish releases the transaction. A direct deferred call avoids the closure
// allocation of an inline defer func.
func (tx *Tx) finish() {
	tx.closed = true
	releasePendingWrites(tx)
	tx.pendingByKey = nil
	tx.afters = nil
	tx.beforeDone = nil
	tx.db.untrack(tx)
}

// commit publishes buffered writes atomically with optimistic conflict
// detection. Locks are acquired in a deterministic order: the global commit
// lock first, so multi-shard/multi-table commits cannot deadlock.
func (tx *Tx) commit() error {
	if err := tx.mustOpen(); err != nil {
		return err
	}
	if tx.db.cfg.maxTxAge > 0 && time.Since(tx.start) > tx.db.cfg.maxTxAge {
		return ErrLimitExceeded
	}
	if len(tx.pending) == 0 {
		return nil
	}
	if max := tx.db.cfg.maxMutations; max > 0 && len(tx.pending) > max {
		return ErrLimitExceeded
	}
	db := tx.db
	db.commitMu.Lock()
	locked := true
	unlock := func() {
		if locked {
			locked = false
			db.commitMu.Unlock()
		}
	}
	defer unlock()
	if err := tx.mustOpen(); err != nil {
		unlock()
		return err
	}
	if len(tx.pending) == 1 {
		done, err := tx.commitSingleLocked(tx.pending[0], unlock)
		if done || err != nil {
			return err
		}
		// A BeforeCommit hook grew the transaction: fall through to the
		// general path, skipping tables whose hooks already ran.
	}
	// BeforeCommit hooks run under the commit lock so their view is stable.
	// Hooks must be fast and must not open write transactions.
	seenTables := map[innerTable]bool{}
	for t := range tx.beforeDone {
		seenTables[t] = true
	}
	for _, p := range tx.pending {
		if !seenTables[p.table] {
			seenTables[p.table] = true
			if err := p.table.runBeforeCommit(tx); err != nil {
				unlock()
				return err
			}
		}
	}
	// Conflict check: the first buffered write per key must still sit at its
	// base version. Later writes to the same key stack on the earlier ones.
	// The published clock advances only after validation and publication.
	// Validation pass over the whole transaction before anything publishes.
	// Stacked writes re-verify idempotently; finality rides on each write.
	st := &commitState{claims: map[innerTable]map[string]map[any]any{},
		sizeHint: len(tx.pending)}
	for _, p := range tx.pending {
		if err := p.table.checkAndRelease(p, st); err != nil {
			unlock()
			return err
		}
	}
	commit := db.latest() + 1
	for _, p := range tx.pending {
		if err := p.table.validatePending(p, st); err != nil {
			unlock()
			return err
		}
	}
	if err := tx.mustOpen(); err != nil {
		unlock()
		return err
	}
	viewUpdates := db.prepareViews(tx, commit)
	if err := tx.mustOpen(); err != nil {
		return err
	}
	db.viewMu.Lock()
	afters := make([]commitAfter, 0, len(tx.pending))
	for _, p := range tx.pending {
		if p.apply != nil {
			// Patched per-row callback (tests): preserve exact behavior.
			afters = append(afters, commitAfter{fn: p.apply(commit)})
			continue
		}
		afters = append(afters, commitAfter{table: p.table, effect: p.table.applyPending(commit, p)})
	}
	for table, claims := range st.claims {
		table.publishUniqueClaims(claims)
	}
	for _, publish := range viewUpdates {
		publish()
	}
	db.clock.Store(uint64(commit))
	db.viewMu.Unlock()
	unlock()
	for _, a := range afters {
		if a.table != nil {
			a.table.fireAfterEffect(a.effect)
		} else {
			a.fn()
		}
	}
	for _, fn := range tx.afters {
		fn()
	}
	// Buffered writes stay attached until finish releases them to the pool.
	return nil
}

// commitSingleLocked commits a lone buffered write without the general
// bookkeeping maps (seen tables, checked keys, final writes, nested unique
// claims). It reports done=false (with err==nil) when a BeforeCommit hook
// grew the transaction, in which case the caller promotes to the general
// path. unlock releases the commit lock exactly once on every path.
func (tx *Tx) commitSingleLocked(p *pendingWrite, unlock func()) (bool, error) {
	db := tx.db
	if err := p.table.runBeforeCommit(tx); err != nil {
		unlock()
		return true, err
	}
	if len(tx.pending) != 1 {
		tx.beforeDone = map[innerTable]bool{p.table: true}
		return false, nil
	}
	cur, present := p.table.checkBase(p)
	if present != p.present || (present && cur != p.base) {
		// Tolerate GC collecting our tombstone base (see checkAndRelease):
		// absent-after-tombstone proves no writer intervened.
		if !(p.baseTomb && !present) {
			unlock()
			return true, ErrConflict
		}
	}
	if !p.del {
		if err := p.table.validateSingle(p); err != nil {
			unlock()
			return true, err
		}
	}
	if err := tx.mustOpen(); err != nil {
		unlock()
		return true, err
	}
	commit := db.latest() + 1
	viewUpdates := db.prepareViews(tx, commit)
	if err := tx.mustOpen(); err != nil {
		return true, err
	}
	db.viewMu.Lock()
	var effect pendingEffect
	var rareAfter func()
	hasRare := false
	if p.apply != nil {
		rareAfter, hasRare = p.apply(commit), true
	} else {
		effect = p.table.applyPendingSingle(commit, p)
	}
	for _, publish := range viewUpdates {
		publish()
	}
	db.clock.Store(uint64(commit))
	db.viewMu.Unlock()
	unlock()
	if hasRare {
		rareAfter()
	} else {
		effect.table.fireAfterEffect(effect)
	}
	for _, fn := range tx.afters {
		fn()
	}
	// Buffered writes stay attached until finish releases them to the pool.
	return true, nil
}
