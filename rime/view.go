package rime

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ViewSource identifies a registered source table. Only RIME table handles
// implement this interface; views cannot depend on other views.
type ViewSource interface {
	viewSource() (*DB, innerTable)
}

func (t *Table[T]) viewSource() (*DB, innerTable) {
	if t == nil {
		return nil, nil
	}
	return t.db, t
}

// ViewSnapshot contains an independent slice of immutable results. Commit is
// the last successful evaluation's commit, not necessarily the latest DB commit.
type ViewSnapshot[R any] struct {
	Rows   []R
	Commit TxID
}

// ViewError identifies a failed evaluation. Source writes still commit.
type ViewError struct {
	Name   string
	Commit TxID
	Err    error
}

func (e *ViewError) Error() string {
	return fmt.Sprintf("rime: view %q at commit %d: %v", e.Name, e.Commit, e.Err)
}
func (e *ViewError) Unwrap() error { return e.Err }

type maintainedView interface {
	prepare(*Tx, TxID, bool) func()
	prepareForCommit(*Tx, TxID) func()
	release()
}

// View caches a query result until closed. Reads are concurrent-safe. Result
// values, including nested mutable data, must be treated as immutable.
type View[R any] struct {
	db      *DB
	name    string
	sources map[innerTable]bool
	build   func(*Tx) ([]R, error)
	// delta prepares incremental membership changes without modifying live state.
	delta func(*Tx) (func(), error)
	key   func(R) any
	// All published fields below are guarded by db.viewMu.
	rows    []R
	members map[any]R
	commit  TxID
	err     error
	closed  bool
}

// NewView freezes query's configuration and maintains its result. Unordered,
// unpaginated filters update only changed rows; other queries are reevaluated.
// A transaction-bound query is rejected. query's context governs creation only.
func NewView[T any](name string, query *Query[T]) (*View[*T], error) {
	if query == nil || query.tbl == nil || query.tbl.db == nil || query.tx != nil {
		return nil, ErrBadView
	}
	q := query.clone()
	for i, e := range q.exprs {
		q.exprs[i] = freezeViewExpr(e)
	}
	v := &View[*T]{db: q.tbl.db, name: name, sources: map[innerTable]bool{q.tbl: true}}
	v.build = func(tx *Tx) ([]*T, error) { return q.findContext(tx.Context(), tx) }
	if len(q.orders) == 0 && q.limit < 0 && q.offset <= 0 {
		v.key = q.tbl.pkGet
		v.delta = func(tx *Tx) (func(), error) {
			changes := make(map[any]*T)
			n := len(v.members)
			scanned := 0
			for _, p := range tx.pending {
				if p.table != q.tbl || p.superseded {
					continue
				}
				scanned++
				if max := v.db.cfg.maxScan; max > 0 && scanned > max {
					return nil, ErrLimitExceeded
				}
				if err := tx.mustOpen(); err != nil {
					return nil, err
				}
				var rec *T
				if !p.del {
					rec = p.val.(*T)
					for _, e := range q.exprs {
						if !e.test(rec) {
							rec = nil
							break
						}
					}
				}
				if _, exists := v.members[p.key]; exists {
					n--
				}
				if rec != nil {
					n++
				}
				changes[p.key] = rec
			}
			if max := v.db.cfg.maxResults; max > 0 && n > max {
				return nil, ErrLimitExceeded
			}
			return func() {
				for k, r := range changes {
					if r == nil {
						delete(v.members, k)
					} else {
						v.members[k] = r
					}
				}
			}, nil
		}
	}
	return registerView(v, q.context())
}

func freezeViewExpr[T any](e Expr[T]) Expr[T] {
	if e == nil {
		return nil
	}
	kids := e.kids()
	copyKids := make([]Expr[T], len(kids))
	for i, k := range kids {
		copyKids[i] = freezeViewExpr(k)
	}
	switch e.kind() {
	case kAnd:
		return And(copyKids...)
	case kOr:
		return Or(copyKids...)
	case kNot:
		return Not(copyKids[0])
	default:
		return e
	}
}

// NewComputedView saves a pure Go query. Bind every source read to the supplied
// read-only transaction. build must not start transactions, write records,
// read views, retain tx, or perform external side effects. Errors and panics
// invalidate this view without rejecting an otherwise valid source commit.
func NewComputedView[R any](db *DB, name string, sources []ViewSource, build func(*Tx) ([]R, error)) (*View[R], error) {
	if db == nil || build == nil || len(sources) == 0 {
		return nil, ErrBadView
	}
	v := &View[R]{db: db, name: name, build: build, sources: make(map[innerTable]bool, len(sources))}
	for _, source := range sources {
		if source == nil {
			return nil, ErrBadView
		}
		owner, table := source.viewSource()
		if owner == nil || table == nil {
			return nil, ErrBadView
		}
		if owner != db {
			return nil, ErrTxDatabase
		}
		v.sources[table] = true
	}
	return registerView(v, context.Background())
}

func registerView[R any](v *View[R], ctx context.Context) (*View[R], error) {
	if strings.TrimSpace(v.name) == "" {
		return nil, ErrBadView
	}
	db := v.db
	db.commitMu.Lock()
	defer db.commitMu.Unlock()
	if db.closed.Load() {
		return nil, ErrDBClosed
	}
	if _, ok := db.views[v.name]; ok {
		return nil, ErrViewExists
	}
	apply := v.prepare(viewTx(db, ctx, nil, v.sources), db.latest(), true)
	db.viewMu.Lock()
	defer db.viewMu.Unlock()
	if db.closed.Load() {
		return nil, ErrDBClosed
	}
	apply()
	if v.err != nil {
		err := v.err
		v.release()
		return nil, err
	}
	if db.views == nil {
		db.views = make(map[string]maintainedView)
		db.viewsBySource = make(map[innerTable]map[maintainedView]bool)
	}
	db.views[v.name] = v
	for source := range v.sources {
		if db.viewsBySource[source] == nil {
			db.viewsBySource[source] = make(map[maintainedView]bool)
		}
		db.viewsBySource[source][v] = true
	}
	return v, nil
}

// viewTx is private and untracked: commitMu excludes both commits and GC.
// Its pending slice is borrowed, so closing it must not return writes to pools.
func viewTx(db *DB, ctx context.Context, pending []*pendingWrite, sources map[innerTable]bool) *Tx {
	if ctx == nil {
		ctx = context.Background()
	}
	return &Tx{db: db, snap: db.latest(), ctx: ctx, start: time.Now(), preview: true, pending: pending, viewSources: sources}
}

// prepare returns only publication work. Evaluation runs without viewMu, and
// commitMu prevents other maintenance from changing the fields inspected here.
func (v *View[R]) prepare(tx *Tx, commit TxID, force bool) (publish func()) {
	defer func() {
		tx.closed = true
		if p := recover(); p != nil {
			publish = v.failure(commit, fmt.Errorf("callback panic: %v", p))
		}
	}()
	if err := tx.mustOpen(); err != nil {
		return v.failure(commit, err)
	}
	var update func()
	var err error
	if !force && v.err == nil && v.delta != nil {
		update, err = v.delta(tx)
	} else {
		var rows []R
		rows, err = v.build(tx)
		if err == nil {
			if max := v.db.cfg.maxResults; max > 0 && len(rows) > max {
				err = ErrLimitExceeded
			}
		}
		if err == nil {
			if v.key != nil {
				members := make(map[any]R, len(rows))
				for _, row := range rows {
					members[v.key(row)] = row
				}
				update = func() { v.members = members; v.rows = nil }
			} else {
				rows = append([]R(nil), rows...)
				update = func() { v.rows = rows }
			}
		}
	}
	if err == nil {
		err = tx.mustOpen()
	}
	if tx.viewViolation != nil {
		err = tx.viewViolation
	}
	if err != nil {
		return v.failure(commit, err)
	}
	return func() { update(); v.commit = commit; v.err = nil }
}

func (v *View[R]) failure(commit TxID, err error) func() {
	return func() {
		v.rows, v.members = nil, nil
		v.err = &ViewError{Name: v.name, Commit: commit, Err: err}
	}
}

// Snapshot copies cached results without executing a source query. Unordered
// views have unspecified row order. Failed views return no rows and ViewError.
func (v *View[R]) Snapshot() (ViewSnapshot[R], error) {
	v.db.viewMu.RLock()
	defer v.db.viewMu.RUnlock()
	if v.db.closed.Load() {
		return ViewSnapshot[R]{}, ErrDBClosed
	}
	if v.closed {
		return ViewSnapshot[R]{}, ErrViewClosed
	}
	if v.err != nil {
		return ViewSnapshot[R]{}, v.err
	}
	rows := append([]R(nil), v.rows...)
	if v.key != nil {
		rows = make([]R, 0, len(v.members))
		for _, row := range v.members {
			rows = append(rows, row)
		}
	}
	return ViewSnapshot[R]{Rows: rows, Commit: v.commit}, nil
}

// Refresh rebuilds the view at the latest commit, including after a failure.
func (v *View[R]) Refresh(ctx context.Context) error {
	db := v.db
	db.commitMu.Lock()
	defer db.commitMu.Unlock()
	if db.closed.Load() {
		return ErrDBClosed
	}
	if v.closed {
		return ErrViewClosed
	}
	apply := v.prepare(viewTx(db, ctx, nil, v.sources), db.latest(), true)
	db.viewMu.Lock()
	defer db.viewMu.Unlock()
	if db.closed.Load() {
		return ErrDBClosed
	}
	apply()
	return v.err
}

// Close unregisters maintenance and releases cached results. It is idempotent.
func (v *View[R]) Close() error {
	db := v.db
	db.commitMu.Lock()
	defer db.commitMu.Unlock()
	db.viewMu.Lock()
	defer db.viewMu.Unlock()
	if v.closed {
		return nil
	}
	delete(db.views, v.name)
	for source := range v.sources {
		delete(db.viewsBySource[source], v)
		if len(db.viewsBySource[source]) == 0 {
			delete(db.viewsBySource, source)
		}
	}
	v.release()
	return nil
}

func (v *View[R]) release() {
	v.closed = true
	v.rows, v.members, v.build, v.delta, v.key, v.sources, v.err = nil, nil, nil, nil, nil, nil, nil
}

// prepareViews is called under commitMu after validation, before publication.
func (db *DB) prepareViews(tx *Tx, commit TxID) []func() {
	if len(db.views) == 0 {
		return nil
	}
	var affected map[maintainedView]bool
	for _, p := range tx.pending {
		for v := range db.viewsBySource[p.table] {
			if affected == nil {
				affected = make(map[maintainedView]bool)
			}
			affected[v] = true
		}
	}
	var updates []func()
	for v := range affected {
		// Each view needs its own dependency guard and borrowed preview.
		updates = append(updates, v.prepareForCommit(tx, commit))
	}
	return updates
}

func (v *View[R]) prepareForCommit(tx *Tx, commit TxID) func() {
	return v.prepare(viewTx(v.db, tx.Context(), tx.pending, v.sources), commit, false)
}
