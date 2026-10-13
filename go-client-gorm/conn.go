package murmurgorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"time"

	murmur "github.com/marcgauthier/murmur/go-client"
)

// conn is a driver.Conn over one murmur clone. Outside a transaction every
// statement runs in its own Read/Transaction callback; while a driverTx is
// active, statements route into that transaction's pump.
type conn struct {
	mu     sync.Mutex
	db     *murmur.DB
	tx     *driverTx
	closed bool
}

// activeTx returns the live transaction, if any.
func (c *conn) activeTx() *driverTx {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	return c.tx
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, sql.ErrConnDone
	}
	// Murmur prepares inside the statement callback, so preparation here only
	// captures the text; each Exec/Query re-prepares natively.
	return &stmt{c: c, query: query}, nil
}

func (c *conn) Close() error {
	c.mu.Lock()
	tx := c.tx
	c.tx = nil
	db := c.db
	c.db = nil
	c.closed = true
	c.mu.Unlock()
	if tx != nil {
		_ = tx.Rollback() // best effort; pool is dropping this conn anyway
	}
	if db == nil {
		return nil
	}
	// Release drops this clone only; the caller's database stays open.
	return db.Release()
}

// Begin implements driver.Conn.
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements driver.ConnBeginTx. Transaction options are accepted and
// ignored: Murmur is an embedded single-writer engine with its own scheduler.
func (c *conn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, sql.ErrConnDone
	}
	if c.tx != nil {
		c.mu.Unlock()
		return nil, sql.ErrTxDone
	}
	db := c.db
	c.mu.Unlock()
	tx, err := beginPump(ctx, c, db)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		_ = tx.Rollback()
		return nil, sql.ErrConnDone
	}
	c.tx = tx
	c.mu.Unlock()
	return tx, nil
}

// detachTx clears a finished transaction. Called by driverTx on Commit/Rollback.
func (c *conn) detachTx(tx *driverTx) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tx == tx {
		c.tx = nil
	}
}

// CheckNamedValue implements driver.NamedValueChecker. time.Time is rendered
// to canonical SQLite TEXT here; every other type returns ErrSkip so
// database/sql falls back to DefaultParameterConverter (int/uint/float kinds,
// bool, string, []byte).
func (c *conn) CheckNamedValue(nv *driver.NamedValue) error {
	if t, ok := nv.Value.(time.Time); ok {
		nv.Value = formatTimestamp(t)
		return nil
	}
	return driver.ErrSkip
}

// ResetSession implements driver.SessionResetter. Connections hold no
// per-session native state between calls, so reuse needs no cleanup.
func (c *conn) ResetSession(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.tx != nil {
		return driver.ErrBadConn
	}
	return nil
}

// Ping implements driver.Pinger with a trivial read. Inside a transaction the
// ping routes through the pump so the clone never runs two callbacks at once.
func (c *conn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if tx := c.activeTx(); tx != nil {
		rows, err := tx.query(ctx, "SELECT 1", nil)
		if err != nil {
			return err
		}
		return rows.Close()
	}
	c.mu.Lock()
	db := c.db
	closed := c.closed
	c.mu.Unlock()
	if closed || db == nil {
		return driver.ErrBadConn
	}
	return db.Read(func(s *murmur.Sql) error {
		rows, err := s.Query("SELECT 1")
		if err != nil {
			return err
		}
		defer rows.Close()
		if !rows.Next() {
			return sql.ErrNoRows
		}
		return rows.Err()
	})
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := txControlKind(query); err != nil {
		return nil, err
	}
	if tx := c.activeTx(); tx != nil {
		if isDDL(query) {
			return nil, ErrDDLInTransaction
		}
		return tx.exec(ctx, query, args)
	}
	if isDDL(query) {
		if len(args) > 0 {
			return nil, errDDLWithArgs()
		}
		db, err := c.checkConnUsable()
		if err != nil {
			return nil, err
		}
		return migrateExec(db, query)
	}
	vals, err := namedArgsToPositional(args)
	if err != nil {
		return nil, err
	}
	db, err := c.checkConnUsable()
	if err != nil {
		return nil, err
	}
	var out murmur.Result
	err = db.Transaction(func(s *murmur.Sql) error {
		var err error
		out, err = s.Exec(query, vals...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result{rowsAffected: out.RowsAffected, lastInsertID: out.LastInsertRowID}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := txControlKind(query); err != nil {
		return nil, err
	}
	if tx := c.activeTx(); tx != nil {
		if isDDL(query) {
			return nil, ErrDDLInTransaction
		}
		return tx.query(ctx, query, args)
	}
	if isDDL(query) {
		if len(args) > 0 {
			return nil, errDDLWithArgs()
		}
		db, err := c.checkConnUsable()
		if err != nil {
			return nil, err
		}
		return migrateQuery(db, query)
	}
	vals, err := namedArgsToPositional(args)
	if err != nil {
		return nil, err
	}
	db, err := c.checkConnUsable()
	if err != nil {
		return nil, err
	}
	out := &bufferedRows{}
	if isReadQuery(query) {
		err = db.Read(func(s *murmur.Sql) error {
			return bufferQuery(s, query, vals, out)
		})
	} else {
		// A write issued through Query (e.g. INSERT ... RETURNING): reads
		// would deny it, so run it as a single-statement transaction.
		err = db.Transaction(func(s *murmur.Sql) error {
			return bufferQuery(s, query, vals, out)
		})
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// bufferQuery runs one SELECT inside an open callback and materializes it into
// out. The native cursor is always closed before the callback returns.
func bufferQuery(s *murmur.Sql, query string, args []any, out *bufferedRows) error {
	rows, err := s.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	out.columns = rows.Columns()
	for rows.Next() {
		values := rows.Values()
		row := make([]driver.Value, len(values))
		for i, v := range values {
			row[i] = toDriverValue(v)
		}
		out.rows = append(out.rows, row)
	}
	return rows.Err()
}
