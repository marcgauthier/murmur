package sqlengine

import (
	"context"
	"database/sql"
	"fmt"
)

// Tx is one engine-level SQL transaction on the serialized write connection.
// It is not safe for concurrent use; the owner holds the engine write lock.
type Tx struct {
	e    *Engine
	ctx  context.Context
	done bool
	// failCommit injects a COMMIT failure (tests only).
	failCommit func() error
}

// Begin starts an IMMEDIATE write transaction and arms change capture.
// The engine write lock is held until Commit or Rollback. Standalone reads
// drain before the transaction starts.
func (e *Engine) Begin(ctx context.Context) (*Tx, error) {
	if err := e.lock.Lock(ctx); err != nil {
		return nil, fmt.Errorf("sqlengine: begin: %w", err)
	}
	if _, err := e.write.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		e.lock.Unlock()
		return nil, fmt.Errorf("sqlengine: begin: %w", err)
	}
	e.beginCapture()
	return &Tx{e: e, ctx: ctx, failCommit: e.failCommit}, nil
}

// Exec runs a statement inside the transaction using the transaction context.
func (tx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	return tx.ExecContext(tx.ctx, query, args...)
}

// ExecContext runs a statement inside the transaction respecting ctx.
func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if tx.done {
		return nil, fmt.Errorf("sqlengine: tx done")
	}
	if err := tx.ctx.Err(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt, err := tx.e.writeStmts.prepare(ctx, tx.e.write, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

// Query runs a query inside the transaction on the write connection using the transaction context.
func (tx *Tx) Query(query string, args ...any) (*sql.Rows, error) {
	return tx.QueryContext(tx.ctx, query, args...)
}

// QueryContext runs a query inside the transaction on the write connection respecting ctx.
func (tx *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if tx.done {
		return nil, fmt.Errorf("sqlengine: tx done")
	}
	if err := tx.ctx.Err(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt, err := tx.e.writeStmts.prepare(ctx, tx.e.write, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

// QueryRow runs a single-row query inside the transaction using the transaction context.
func (tx *Tx) QueryRow(query string, args ...any) (*sql.Row, error) {
	return tx.QueryRowContext(tx.ctx, query, args...)
}

// QueryRowContext runs a single-row query inside the transaction respecting ctx.
func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) (*sql.Row, error) {
	if tx.done {
		return nil, fmt.Errorf("sqlengine: tx done")
	}
	if err := tx.ctx.Err(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt, err := tx.e.writeStmts.prepare(ctx, tx.e.write, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryRowContext(ctx, args...), nil
}

// Pending returns a copy of the changes captured so far. It allows the
// durability layer to validate/coalesce before SQL COMMIT so oversize
// transactions can roll back instead of dirtying the materializer.
func (tx *Tx) Pending() []RawChange {
	tx.e.capMu.Lock()
	defer tx.e.capMu.Unlock()
	return append([]RawChange(nil), tx.e.pending...)
}

// Commit commits SQL and returns the captured raw changes. On SQL commit
// failure the transaction is rolled back, capture is discarded, and the
// error is returned (the caller must NOT durably replicate).
func (tx *Tx) Commit() ([]RawChange, error) {
	if tx.done {
		return nil, fmt.Errorf("sqlengine: tx done")
	}
	tx.done = true
	defer tx.e.lock.Unlock()
	if tx.failCommit != nil {
		if err := tx.failCommit(); err != nil {
			tx.e.resetCapture()
			_, _ = tx.e.write.ExecContext(context.Background(), "ROLLBACK")
			return nil, fmt.Errorf("sqlengine: commit: %w", err)
		}
	}
	if _, err := tx.e.write.ExecContext(tx.ctx, "COMMIT"); err != nil {
		tx.e.resetCapture()
		_, _ = tx.e.write.ExecContext(context.Background(), "ROLLBACK")
		return nil, fmt.Errorf("sqlengine: commit: %w", err)
	}
	return tx.e.finishCapture(), nil
}

// Rollback aborts the transaction and discards capture.
func (tx *Tx) Rollback() error {
	if tx.done {
		return nil
	}
	tx.done = true
	defer tx.e.lock.Unlock()
	tx.e.resetCapture()
	if _, err := tx.e.write.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		return fmt.Errorf("sqlengine: rollback: %w", err)
	}
	return nil
}

// WriteSection runs fn with the writer + reader locks held (for apply and
// rebuild, which manage their own SQL transactions).
func (e *Engine) WriteSection(fn func(ctx context.Context) error) error {
	return e.WriteSectionContext(context.Background(), fn)
}

// WriteSectionContext runs fn with the writer + reader locks held respecting ctx.
func (e *Engine) WriteSectionContext(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := e.lock.Lock(ctx); err != nil {
		return err
	}
	defer e.lock.Unlock()
	return fn(ctx)
}
