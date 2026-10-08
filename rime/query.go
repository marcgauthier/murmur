package rime

import (
	"context"
	"sort"
)

// orderSpec is one ORDER BY key.
type orderSpec[T any] struct {
	f    OrderField[T]
	desc bool
}

// Query is a lazily-built record query. The zero value is invalid; build via
// Table.Where. A Query is not safe for concurrent mutation; Find may run
// concurrently on disjoint Query values.
type Query[T any] struct {
	tbl    *Table[T]
	tx     *Tx
	ctx    context.Context
	exprs  []Expr[T]
	limit  int
	offset int
	orders []orderSpec[T]
}

// Where starts a query over records matching all exprs.
func (t *Table[T]) Where(exprs ...Expr[T]) *Query[T] {
	return &Query[T]{tbl: t, exprs: append([]Expr[T](nil), exprs...), limit: -1, ctx: context.Background()}
}

// clone returns an independent builder sharing immutable expressions.
func (q *Query[T]) clone() *Query[T] {
	n := *q
	n.exprs = append([]Expr[T](nil), q.exprs...)
	n.orders = append([]orderSpec[T](nil), q.orders...)
	return &n
}

// In returns a query bound to tx.
func (q *Query[T]) In(tx *Tx) *Query[T] { n := q.clone(); n.tx = tx; return n }

// WithContext returns a query bound to ctx.
func (q *Query[T]) WithContext(ctx context.Context) *Query[T] {
	n := q.clone()
	if ctx == nil {
		ctx = context.Background()
	}
	n.ctx = ctx
	return n
}

// Where returns an independent query with conjunctive predicates appended.
func (q *Query[T]) Where(exprs ...Expr[T]) *Query[T] {
	n := q.clone()
	n.exprs = append(n.exprs, exprs...)
	return n
}

// Limit caps returned records; negative clears.
func (q *Query[T]) Limit(n int) *Query[T] { c := q.clone(); c.limit = n; return c }

// Offset skips leading rows.
func (q *Query[T]) Offset(n int) *Query[T] { c := q.clone(); c.offset = n; return c }

// OrderByAsc appends an ascending sort key; OrderByDesc a descending one.
func (q *Query[T]) OrderByAsc(f OrderField[T]) *Query[T] {
	c := q.clone()
	c.orders = append(c.orders, orderSpec[T]{f: f})
	return c
}

// OrderByDesc appends a descending sort key.
func (q *Query[T]) OrderByDesc(f OrderField[T]) *Query[T] {
	c := q.clone()
	c.orders = append(c.orders, orderSpec[T]{f: f, desc: true})
	return c
}

// OrderBy is an alias for OrderByAsc; OrderByDescending aliases OrderByDesc.
func (q *Query[T]) OrderBy(f OrderField[T]) *Query[T] { return q.OrderByAsc(f) }

// OrderByDescending is an alias for OrderByDesc.
func (q *Query[T]) OrderByDescending(f OrderField[T]) *Query[T] { return q.OrderByDesc(f) }

// Find executes at the bound transaction or a fresh latest snapshot.
func (q *Query[T]) Find() ([]*T, error) {
	return q.findContext(q.context(), q.tx)
}

// Each visits matching records in query order without building a result slice
// when the plan can provide that order directly. Callback errors stop iteration.
// Plans requiring an in-memory sort fall back to Find.
func (q *Query[T]) Each(fn func(*T) error) error {
	ctx := q.context()
	if err := ctx.Err(); err != nil {
		return err
	}
	tx := q.tx
	if tx == nil {
		tx = q.tbl.db.ReadTxContext(ctx)
		defer tx.Close()
	}
	if err := q.tbl.checkTx(tx); err != nil {
		return err
	}
	if err := tx.mustOpen(); err != nil {
		return err
	}
	if q.limit == 0 {
		return nil
	}
	if (tx.write || tx.preview) && len(tx.pending) > 0 {
		rows, err := q.findContext(ctx, tx)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := fn(row); err != nil {
				return err
			}
		}
		return nil
	}
	snap := q.tbl.snapOf(tx)
	p := q.tbl.cachedOrPlan(q, snap)
	if len(q.orders) > 0 && !p.ordered {
		rows, err := q.findContext(ctx, tx)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := fn(row); err != nil {
				return err
			}
		}
		return nil
	}
	if p.empty {
		return nil
	}
	maxScan, maxResults := q.tbl.db.cfg.maxScan, q.tbl.db.cfg.maxResults
	offset := q.offset
	if offset < 0 {
		offset = 0
	}
	need := -1
	if q.limit >= 0 {
		need = offset + q.limit
	}
	visited := 0
	var callbackErr error
	examine := func(rec *T) bool {
		if callbackErr != nil {
			return true
		}
		for _, expr := range q.exprs {
			if !expr.test(rec) {
				return false
			}
		}
		if visited >= offset {
			if maxResults > 0 && visited-offset >= maxResults {
				callbackErr = ErrLimitExceeded
				need = visited + 1
				return true
			}
			if callbackErr = fn(rec); callbackErr != nil {
				need = visited + 1
				return true
			}
		}
		visited++
		return true
	}
	scanned, _, err := q.scanEach(ctx, snap, p, need, maxScan, examine)
	if err != nil {
		return err
	}
	if callbackErr != nil {
		return callbackErr
	}
	if maxScan > 0 && scanned > maxScan {
		return ErrLimitExceeded
	}
	if err := tx.mustOpen(); err != nil {
		return err
	}
	return ctx.Err()
}

func (q *Query[T]) context() context.Context {
	if q.ctx == nil {
		return context.Background()
	}
	return q.ctx
}

func (q *Query[T]) findContext(ctx context.Context, tx *Tx) ([]*T, error) {
	rows, _, err := q.collect(ctx, tx, true)
	return rows, err
}

// First returns the first match, or ErrNotFound.
func (q *Query[T]) First() (*T, error) {
	top := *q
	top.limit = 1
	rows, err := top.Find()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}
	return rows[0], nil
}

// Count returns the number of matching records without sorting.
func (q *Query[T]) Count() (int, error) {
	_, n, err := q.collect(q.context(), q.tx, false)
	return n, err
}

// Exists reports whether at least one row appears on the selected page.
func (q *Query[T]) Exists() (bool, error) {
	c := q.Limit(1)
	rows, err := c.Find()
	return len(rows) != 0, err
}

// Delete removes matching rows atomically and returns their count.
func (q *Query[T]) Delete() (int, error) {
	if q.tx == nil {
		var n int
		err := q.tbl.db.WriteTxContext(q.context(), func(tx *Tx) error {
			var err error
			n, err = q.In(tx).Delete()
			return err
		})
		return n, err
	}
	tx := q.tx
	if err := tx.mustWrite(); err != nil {
		return 0, err
	}
	rows, err := q.findContext(q.context(), tx)
	if err != nil {
		return 0, err
	}
	keys := make([]any, len(rows))
	for i, r := range rows {
		keys[i] = q.tbl.pkGet(r)
	}
	if err := q.tbl.deleteManyAt(tx, keys); err != nil {
		return 0, err
	}
	return len(rows), nil
}

// Update edits matching rows atomically and returns their count.
func (q *Query[T]) Update(fn func(*T) error) (int, error) {
	if q.tx == nil {
		var n int
		err := q.tbl.db.WriteTxContext(q.context(), func(tx *Tx) error {
			var err error
			n, err = q.In(tx).Update(fn)
			return err
		})
		return n, err
	}
	tx := q.tx
	if err := tx.mustWrite(); err != nil {
		return 0, err
	}
	rows, err := q.findContext(q.context(), tx)
	if err != nil {
		return 0, err
	}
	keys := make([]any, len(rows))
	for i, r := range rows {
		keys[i] = q.tbl.pkGet(r)
	}
	err = q.tbl.batchAt(tx, len(rows), "update", func(tx *Tx, i int) error { return q.tbl.updateAt(tx, keys[i], fn) })
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

// sortRows orders rows per the query ORDER BY list.
func (q *Query[T]) sortRows(rows []*T) {
	if len(q.orders) == 0 || len(rows) < 2 {
		return
	}
	orders := q.orders
	sort.SliceStable(rows, func(i, j int) bool {
		a, b := rows[i], rows[j]
		for _, o := range orders {
			c := o.f.orderCmp(a, b)
			if c == 0 {
				continue
			}
			if o.desc {
				return c > 0
			}
			return c < 0
		}
		return false
	})
}

// Find executes a compiled query with positional Param values.
func (c *Compiled[T]) Find(args ...any) ([]*T, error) {
	bound, err := c.bind(args)
	if err != nil {
		return nil, err
	}
	q := &Query[T]{tbl: c.tbl, tx: c.tx, ctx: c.ctx, exprs: bound, limit: c.limit, offset: c.offset, orders: append([]orderSpec[T](nil), c.orders...)}
	return q.Find()
}

// Count executes a compiled query count.
func (c *Compiled[T]) Count(args ...any) (int, error) {
	bound, err := c.bind(args)
	if err != nil {
		return 0, err
	}
	q := &Query[T]{tbl: c.tbl, tx: c.tx, ctx: c.ctx, exprs: bound, limit: c.limit, offset: c.offset, orders: append([]orderSpec[T](nil), c.orders...)}
	return q.Count()
}

// Explain shows the plan for bound args without executing.
func (c *Compiled[T]) Explain(args ...any) string {
	bound, err := c.bind(args)
	if err != nil {
		return "COMPILE ERROR: " + err.Error()
	}
	q := &Query[T]{tbl: c.tbl, exprs: bound, limit: c.limit, offset: c.offset, orders: c.orders}
	return q.Explain()
}
