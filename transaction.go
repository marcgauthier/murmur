package replicateddb

import (
	"context"
	"database/sql"
	"sync"

	"github.com/nomadsql/replicateddb/sqlengine"
)

// TxOptions configures an explicit transaction. Reserved for future
// isolation/read-only modes; currently unused.
type TxOptions struct{}

// Tx is one explicit local transaction. It holds the serialized write
// coordinator until Commit or Rollback and is not safe for concurrent use.
type Tx struct {
	db           *DB
	stx          *sqlengine.Tx
	txID         TxID
	done         bool
	ticket       *Ticket
	bridgeImport *bridgeImportInfo
}

// ExecContext executes a statement inside the transaction.
func (tx *Tx) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	if tx.done {
		return nil, ErrTxDone
	}
	return tx.stx.Exec(query, args...)
}

// QueryContext runs a query inside the transaction. Rows must be closed
// before Commit.
func (tx *Tx) QueryContext(_ context.Context, query string, args ...any) (*sql.Rows, error) {
	if tx.done {
		return nil, ErrTxDone
	}
	return tx.stx.Query(query, args...)
}

// QueryRowContext runs a single-row query inside the transaction.
func (tx *Tx) QueryRowContext(_ context.Context, query string, args ...any) *TxRow {
	if tx.done {
		return &TxRow{err: ErrTxDone}
	}
	row, err := tx.stx.QueryRow(query, args...)
	return &TxRow{row: row, err: err}
}

// TxID returns the transaction's idempotency key.
func (tx *Tx) TxID() TxID { return tx.txID }

// Commit commits: SQL COMMIT then one atomic Pebble commit. Success is
// reported only after Pebble durability.
func (tx *Tx) Commit() error {
	if tx.done {
		return ErrTxDone
	}
	tx.done = true
	defer tx.db.writeMu.Unlock()
	defer tx.ticket.Release()
	return tx.db.commitTx(tx)
}

// Rollback aborts the transaction.
func (tx *Tx) Rollback() error {
	if tx.done {
		return ErrTxDone
	}
	tx.done = true
	defer tx.db.writeMu.Unlock()
	defer tx.ticket.Release()
	return tx.stx.Rollback()
}

// Rows is a standalone read result. Close must be called promptly: an open
// Rows stalls writers. Close is idempotent.
type Rows struct {
	rows *sqlengine.Rows
}

// Columns returns the column names.
func (r *Rows) Columns() []string { return r.rows.Columns() }

// Next advances to the next row.
func (r *Rows) Next() bool { return r.rows.Next() }

// Scan copies the current row into dest.
func (r *Rows) Scan(dest ...any) error { return r.rows.Scan(dest...) }

// Err returns any iteration error.
func (r *Rows) Err() error { return r.rows.Err() }

// Close releases the read.
func (r *Rows) Close() error { return r.rows.Close() }

// Row is a deferred single-row read. The query runs on the first Scan.
type Row struct {
	db    *DB
	ctx   context.Context
	query string
	args  []any

	once sync.Once
	err  error
}

// Scan runs the query (once) and scans the first row into dest.
func (r *Row) Scan(dest ...any) error {
	r.once.Do(func() {
		if err := r.db.requireRead(); err != nil {
			r.err = err
			return
		}
		r.err = r.db.engine.QueryRowContext(r.ctx, r.query, r.args, func(row *sql.Row) error {
			return row.Scan(dest...)
		})
	})
	return r.err
}

// Err returns any query-construction error (always nil; Scan reports errors).
func (r *Row) Err() error { return nil }

// TxRow is a single-row read inside an explicit transaction.
type TxRow struct {
	row *sql.Row
	err error
}

// Scan scans the row into dest.
func (r *TxRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return r.row.Scan(dest...)
}

// Stmt is a prepared statement handle. Execution goes through the normal
// implicit-transaction paths; Close is a no-op because the engine owns a
// shared prepared-statement cache.
type Stmt struct {
	db    *DB
	query string
}

// ExecContext executes the statement as an implicit transaction.
func (s *Stmt) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, s.query, args...)
}

// QueryContext runs the statement as a read.
func (s *Stmt) QueryContext(ctx context.Context, args ...any) (*Rows, error) {
	return s.db.QueryContext(ctx, s.query, args...)
}

// QueryRowContext runs the statement as a single-row read.
func (s *Stmt) QueryRowContext(ctx context.Context, args ...any) *Row {
	return s.db.QueryRowContext(ctx, s.query, args...)
}

// Close releases the handle (no-op; see Stmt).
func (s *Stmt) Close() error { return nil }
