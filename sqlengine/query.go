package sqlengine

import (
	"context"
	"database/sql"
	"sync"
)

// Rows is a standalone read result. Close must be called promptly: an open
// Rows holds a read lock that stalls writers. Exhausting the rows or canceling
// the query context also closes the cursor and releases the lock. Close is idempotent.
type Rows struct {
	rows     *sql.Rows
	release  func()
	cols     []string
	once     sync.Once
	cerr     error
	stopHook func() bool
}

// Query runs a standalone read on the read connection. The caller must
// Close the Rows. Only read statements are accepted: pool connections
// carry no capture hook, so anything that could write would diverge
// silently.
func (e *Engine) Query(ctx context.Context, query string, args ...any) (*Rows, error) {
	// Structural check first: a stacked input's first keyword can
	// mislead the verb gate, and the one-query-one-statement
	// contract reports ErrMultiStatement on every path.
	if err := checkSingleStatement(query); err != nil {
		return nil, err
	}
	if err := checkPoolQueryAllowed(query); err != nil {
		return nil, err
	}
	if err := e.lock.RLock(ctx); err != nil {
		return nil, err
	}
	stmt, err := e.readStmts.prepare(ctx, e.db, query)
	if err != nil {
		e.lock.RUnlock()
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		e.lock.RUnlock()
		return nil, err
	}
	r := &Rows{rows: rows, release: e.lock.RUnlock}
	if ctx.Done() != nil {
		r.stopHook = context.AfterFunc(ctx, func() {
			_ = r.Close()
		})
	}
	return r, nil
}

// QueryRowContext runs a single-row standalone read. Unlike Query it does not
// hold the read lock after returning: the row is scanned immediately, so fn
// receives the *sql.Row while the lock is held and must Scan synchronously.
func (e *Engine) QueryRowContext(ctx context.Context, query string, args []any, fn func(*sql.Row) error) error {
	if err := checkSingleStatement(query); err != nil {
		return err
	}
	if err := checkPoolQueryAllowed(query); err != nil {
		return err
	}
	if err := e.lock.RLock(ctx); err != nil {
		return err
	}
	defer e.lock.RUnlock()
	stmt, err := e.readStmts.prepare(ctx, e.db, query)
	if err != nil {
		return err
	}
	return fn(stmt.QueryRowContext(ctx, args...))
}

// Columns delegates to sql.Rows.
func (r *Rows) Columns() []string {
	if r.cols == nil {
		cols, err := r.rows.Columns()
		if err == nil {
			r.cols = cols
		}
	}
	return r.cols
}

// Next delegates to sql.Rows.
func (r *Rows) Next() bool {
	if r.rows.Next() {
		return true
	}
	_ = r.Close()
	return false
}

// Scan delegates to sql.Rows.
func (r *Rows) Scan(dest ...any) error { return r.rows.Scan(dest...) }

// Err delegates to sql.Rows.
func (r *Rows) Err() error { return r.rows.Err() }

// Close releases the read lock.
func (r *Rows) Close() error {
	r.once.Do(func() {
		if r.stopHook != nil {
			r.stopHook()
			r.stopHook = nil
		}
		if r.rows != nil {
			r.cerr = r.rows.Close()
		}
		if r.release != nil {
			r.release()
		}
	})
	return r.cerr
}

// EmptyResult is a sql.Result for statements without row counts.
type EmptyResult struct{}

// LastInsertId implements sql.Result.
func (EmptyResult) LastInsertId() (int64, error) { return 0, nil }

// RowsAffected implements sql.Result.
func (EmptyResult) RowsAffected() (int64, error) { return 0, nil }
