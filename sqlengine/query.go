package sqlengine

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

// Rows is a standalone read result. Close must be called promptly: an open
// Rows holds a read lock that stalls writers. Close is idempotent.
type Rows struct {
	rows    *sql.Rows
	release func()
	engine  *Engine
	cols    []string
	once    sync.Once
	cerr    error
}

// Query runs a standalone read on the read connection. The caller must
// Close the Rows.
func (e *Engine) Query(ctx context.Context, query string, args ...any) (*Rows, error) {
	e.rw.RLock()
	if e.closed {
		e.rw.RUnlock()
		return nil, fmt.Errorf("sqlengine: closed")
	}
	stmt, err := e.readStmts.prepare(ctx, e.read, query)
	if err != nil {
		e.rw.RUnlock()
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		e.rw.RUnlock()
		return nil, err
	}
	return &Rows{rows: rows, release: e.rw.RUnlock, engine: e}, nil
}

// QueryRowContext runs a single-row standalone read. Unlike Query it does not
// hold the read lock after returning: the row is scanned immediately, so fn
// receives the *sql.Row while the lock is held and must Scan synchronously.
func (e *Engine) QueryRowContext(ctx context.Context, query string, args []any, fn func(*sql.Row) error) error {
	e.rw.RLock()
	defer e.rw.RUnlock()
	if e.closed {
		return fmt.Errorf("sqlengine: closed")
	}
	stmt, err := e.readStmts.prepare(ctx, e.read, query)
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
func (r *Rows) Next() bool { return r.rows.Next() }

// Scan delegates to sql.Rows.
func (r *Rows) Scan(dest ...any) error { return r.rows.Scan(dest...) }

// Err delegates to sql.Rows.
func (r *Rows) Err() error { return r.rows.Err() }

// Close releases the read lock.
func (r *Rows) Close() error {
	r.once.Do(func() {
		r.cerr = r.rows.Close()
		r.release()
	})
	return r.cerr
}

// EmptyResult is a sql.Result for statements without row counts.
type EmptyResult struct{}

// LastInsertId implements sql.Result.
func (EmptyResult) LastInsertId() (int64, error) { return 0, nil }

// RowsAffected implements sql.Result.
func (EmptyResult) RowsAffected() (int64, error) { return 0, nil }
