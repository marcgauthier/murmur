package replicateddb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
	"time"
)

// DriverName is the database/sql driver name for replicateddb.
//
// Register a *DB under a handle, then open it through database/sql:
//
//	replicateddb.RegisterDriverDB("primary", db)
//	sqldb, err := sql.Open(replicateddb.DriverName, "primary")
//
// Alternatively, bypass the registry with sql.OpenDB:
//
//	sqldb := sql.OpenDB(replicateddb.NewConnector(db))
//
// All statements execute through the controlled transaction and capture
// APIs: Exec runs writes as implicit transactions, Query runs reads on the
// shared materialization, and Begin maps to DB.BeginTx. Closing the sql.DB
// (or its connections) never closes the underlying *DB.
const DriverName = "replicateddb"

// ErrDriverNotRegistered is returned when opening an unregistered handle.
var ErrDriverNotRegistered = fmt.Errorf("replicateddb: driver database not registered")

// ErrDriverTxActive is returned when beginning a transaction on a connection
// that already holds one.
var ErrDriverTxActive = fmt.Errorf("replicateddb: transaction already in progress")

// ErrDriverWriteQuery is returned when a write-capable statement is issued
// as a Query outside an explicit transaction. Writes issued through the
// read path would bypass change capture, so use Exec (implicit transaction)
// or Begin + Query + Commit instead.
var ErrDriverWriteQuery = fmt.Errorf("replicateddb: write statement requires Exec or an explicit transaction")

var (
	defaultSQLDriver = &sqlDriver{}

	driverRegistry = struct {
		sync.Mutex
		m map[string]*DB
	}{m: make(map[string]*DB)}
)

func init() {
	sql.Register(DriverName, defaultSQLDriver)
}

// RegisterDriverDB exposes db to sql.Open(DriverName, name). It overwrites
// any previous registration under name. UnregisterDriverDB removes it.
func RegisterDriverDB(name string, db *DB) {
	driverRegistry.Lock()
	defer driverRegistry.Unlock()
	driverRegistry.m[name] = db
}

// UnregisterDriverDB removes a previous RegisterDriverDB handle. Open
// connections are unaffected; only future opens fail.
func UnregisterDriverDB(name string) {
	driverRegistry.Lock()
	defer driverRegistry.Unlock()
	delete(driverRegistry.m, name)
}

func lookupDriverDB(name string) (*DB, bool) {
	driverRegistry.Lock()
	defer driverRegistry.Unlock()
	db, ok := driverRegistry.m[name]
	return db, ok
}

// Connector adapts a *DB to database/sql without the global registry.
type Connector struct {
	db *DB
}

// NewConnector returns a driver.Connector for db for use with sql.OpenDB.
func NewConnector(db *DB) driver.Connector {
	return &Connector{db: db}
}

// Connect implements driver.Connector. It never opens new storage; every
// connection shares the same *DB.
func (c *Connector) Connect(_ context.Context) (driver.Conn, error) {
	if c == nil || c.db == nil {
		return nil, fmt.Errorf("replicateddb: nil connector database")
	}
	switch c.db.getState() {
	case StateClosed, StateClosing:
		return nil, ErrClosed
	default:
		return &driverConn{db: c.db}, nil
	}
}

// Driver implements driver.Connector.
func (c *Connector) Driver() driver.Driver { return defaultSQLDriver }

type sqlDriver struct{}

// Open implements driver.Driver. The name is a RegisterDriverDB handle.
func (d *sqlDriver) Open(name string) (driver.Conn, error) {
	c, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext.
func (d *sqlDriver) OpenConnector(name string) (driver.Connector, error) {
	db, ok := lookupDriverDB(name)
	if !ok || db == nil {
		return nil, fmt.Errorf("%w: %q", ErrDriverNotRegistered, name)
	}
	return &Connector{db: db}, nil
}

// driverConn is one database/sql connection. It holds no storage of its
// own; statements delegate to the shared *DB, optionally inside one
// explicit *Tx started by BeginTx.
type driverConn struct {
	mu     sync.Mutex
	db     *DB
	tx     *Tx
	closed bool
}

func (c *driverConn) currentTx() (*Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	return c.tx, nil
}

// Prepare implements driver.Conn.
func (c *driverConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext implements driver.ConnPrepareContext.
func (c *driverConn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrClosed
	}
	return &driverStmt{conn: c, query: query}, nil
}

// Close implements driver.Conn. An open explicit transaction is rolled
// back so the serialized write coordinator is released. The *DB stays open.
func (c *driverConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	tx := c.tx
	c.tx = nil
	c.mu.Unlock()
	if tx != nil {
		return tx.Rollback()
	}
	return nil
}

// Begin implements driver.Conn.
func (c *driverConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements driver.ConnBeginTx. Isolation and read-only modes are
// accepted and ignored: every transaction uses the package's serialized
// local-write coordinator.
func (c *driverConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if c.tx != nil {
		c.mu.Unlock()
		return nil, ErrDriverTxActive
	}
	c.mu.Unlock()
	tx, err := c.db.BeginTx(ctx, &TxOptions{})
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		c.mu.Unlock()
		_ = tx.Rollback()
		c.mu.Lock()
		return nil, ErrClosed
	}
	c.tx = tx
	return &driverTx{conn: c, tx: tx}, nil
}

// Ping implements driver.Pinger.
func (c *driverConn) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return ErrClosed
	}
	return c.db.requireRead()
}

// ResetSession implements driver.SessionResetter. A leaked open transaction
// is rolled back (releasing the write coordinator) and the connection is
// discarded by returning an error.
func (c *driverConn) ResetSession(_ context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	tx := c.tx
	c.tx = nil
	c.mu.Unlock()
	if tx != nil {
		_ = tx.Rollback()
		return ErrDriverTxActive
	}
	return nil
}

// IsValid implements driver.Validator.
func (c *driverConn) IsValid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	switch c.db.getState() {
	case StateClosed, StateClosing, StateFailed:
		return false
	default:
		return true
	}
}

// driverStmt delegates to DB implicit-transaction paths, or to the
// connection's explicit *Tx when one is active.
type driverStmt struct {
	conn  *driverConn
	query string
}

// Close implements driver.Stmt. It is a no-op: the engine owns a shared
// prepared-statement cache.
func (s *driverStmt) Close() error { return nil }

// NumInput implements driver.Stmt. Unknown (-1) accepts any arity.
func (s *driverStmt) NumInput() int { return -1 }

// Exec implements driver.Stmt.
func (s *driverStmt) Exec(args []driver.Value) (driver.Result, error) {
	vals := make([]driver.NamedValue, len(args))
	for i, v := range args {
		vals[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.ExecContext(context.Background(), vals)
}

// Query implements driver.Stmt.
func (s *driverStmt) Query(args []driver.Value) (driver.Rows, error) {
	vals := make([]driver.NamedValue, len(args))
	for i, v := range args {
		vals[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	return s.QueryContext(context.Background(), vals)
}

// ExecContext implements driver.StmtExecContext.
func (s *driverStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	tx, err := s.conn.currentTx()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	vals := namedValuesToArgs(args)
	if tx != nil {
		return tx.ExecContext(ctx, s.query, vals...)
	}
	return s.conn.db.ExecContext(ctx, s.query, vals...)
}

// QueryContext implements driver.StmtQueryContext. Outside an explicit
// transaction only read-only statements are accepted: a write issued on
// the read path would bypass change capture.
func (s *driverStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	tx, err := s.conn.currentTx()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	vals := namedValuesToArgs(args)
	if tx != nil {
		rows, err := tx.QueryContext(ctx, s.query, vals...)
		if err != nil {
			return nil, err
		}
		cols, err := rows.Columns()
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
		return &txDriverRows{rows: rows, cols: cols}, nil
	}
	if !isReadOnlyStatement(s.query) {
		return nil, ErrDriverWriteQuery
	}
	rows, err := s.conn.db.QueryContext(ctx, s.query, vals...)
	if err != nil {
		return nil, err
	}
	return &standaloneDriverRows{rows: rows, cols: rows.Columns()}, nil
}

func namedValuesToArgs(args []driver.NamedValue) []any {
	vals := make([]any, len(args))
	for i, arg := range args {
		vals[i] = arg.Value
	}
	return vals
}

// driverTx maps database/sql commit/rollback onto the explicit *Tx.
type driverTx struct {
	conn *driverConn
	tx   *Tx
}

func (t *driverTx) detach() (*Tx, error) {
	t.conn.mu.Lock()
	defer t.conn.mu.Unlock()
	if t.conn.tx != t.tx {
		return nil, ErrTxDone
	}
	t.conn.tx = nil
	return t.tx, nil
}

// Commit implements driver.Tx.
func (t *driverTx) Commit() error {
	tx, err := t.detach()
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Rollback implements driver.Tx.
func (t *driverTx) Rollback() error {
	tx, err := t.detach()
	if err != nil {
		return err
	}
	return tx.Rollback()
}

// standaloneDriverRows adapts *Rows (read connection) to driver.Rows.
type standaloneDriverRows struct {
	rows *Rows
	cols []string
}

// Columns implements driver.Rows.
func (r *standaloneDriverRows) Columns() []string { return r.cols }

// Close implements driver.Rows. It must be called promptly: an open Rows
// stalls writers.
func (r *standaloneDriverRows) Close() error { return r.rows.Close() }

// Next implements driver.Rows.
func (r *standaloneDriverRows) Next(dest []driver.Value) error {
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	return scanToDriverValues(len(r.cols), r.rows.Scan, dest)
}

// txDriverRows adapts *sql.Rows (explicit transaction) to driver.Rows.
// Rows must be closed before the transaction commits.
type txDriverRows struct {
	rows *sql.Rows
	cols []string
}

// Columns implements driver.Rows.
func (r *txDriverRows) Columns() []string { return r.cols }

// Close implements driver.Rows.
func (r *txDriverRows) Close() error { return r.rows.Close() }

// Next implements driver.Rows.
func (r *txDriverRows) Next(dest []driver.Value) error {
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return io.EOF
	}
	return scanToDriverValues(len(r.cols), r.rows.Scan, dest)
}

func scanToDriverValues(ncols int, scan func(dest ...any) error, dest []driver.Value) error {
	if len(dest) != ncols {
		return fmt.Errorf("replicateddb: column count %d != dest %d", ncols, len(dest))
	}
	vals := make([]any, ncols)
	ptrs := make([]any, ncols)
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := scan(ptrs...); err != nil {
		return err
	}
	for i, v := range vals {
		dv, err := toDriverValue(v)
		if err != nil {
			return err
		}
		dest[i] = dv
	}
	return nil
}

// toDriverValue normalizes database/sql scan results to driver.Value.
func toDriverValue(v any) (driver.Value, error) {
	switch t := v.(type) {
	case nil:
		return nil, nil
	case int64:
		return t, nil
	case float64:
		return t, nil
	case bool:
		return t, nil
	case string:
		return t, nil
	case []byte:
		if t == nil {
			return nil, nil
		}
		return append([]byte(nil), t...), nil
	case time.Time:
		return t, nil
	case int:
		return int64(t), nil
	case int8:
		return int64(t), nil
	case int16:
		return int64(t), nil
	case int32:
		return int64(t), nil
	case uint:
		return uintToDriverValue(uint64(t))
	case uint8:
		return int64(t), nil
	case uint16:
		return int64(t), nil
	case uint32:
		return int64(t), nil
	case uint64:
		return uintToDriverValue(t)
	case float32:
		return float64(t), nil
	default:
		if driver.IsValue(v) {
			return v, nil
		}
		return nil, fmt.Errorf("replicateddb: unsupported column type %T", v)
	}
}

func uintToDriverValue(v uint64) (driver.Value, error) {
	const maxInt64 = uint64(1<<63 - 1)
	if v > maxInt64 {
		return nil, fmt.Errorf("replicateddb: unsigned value %d overflows int64", v)
	}
	return int64(v), nil
}
