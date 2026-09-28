// Package sqlengine implements the rebuildable SQL query materialization.
//
// The default engine uses modernc.org/sqlite, a pure-Go build with pre-update
// hook support. A build-tagged LumoSQL backend can instead use its CGO driver.
// It owns:
//
//   - base-table DDL creation and ordinal validation
//   - pre-update-hook change capture with a suppression mode for
//     remote apply / rebuild (replication echo prevention)
//   - transaction-local delta coalescing
//   - winner-only remote apply and bulk rebuild from durable state
//   - an LRU prepared-statement cache
package sqlengine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/schema"
)

// CaptureMode controls whether the pre-update hook records mutations.
type CaptureMode uint8

const (
	// CaptureLocal records base-table changes into the active capture.
	CaptureLocal CaptureMode = iota
	// CaptureSuppressed ignores changes (remote apply, rebuild, snapshot).
	CaptureSuppressed
)

// OpType is the row operation captured by the hook.
type OpType uint8

const (
	OpInsert OpType = iota + 1
	OpUpdate
	OpDelete
)

// RawChange is one captured row change. Old/New hold column values in
// CREATE TABLE ordinal order (nil for the absent side).
type RawChange struct {
	Table *schema.TableSchema
	Op    OpType
	Old   []codec.Value
	New   []codec.Value
}

// ChangeCapture collects a transaction's raw changes.
type ChangeCapture interface {
	Begin()
	Events() []RawChange
	Reset()
}

// Engine is a serialized in-memory SQLite query database with change capture.
//
// Concurrency: writers are fully serialized (one write txn at a time).
// Standalone reads run on a second shared-cache connection and proceed
// concurrently; they block only while a writer holds the write section,
// and writers drain in-flight reads before starting. Rows must be closed
// promptly, otherwise writers stall.
type Engine struct {
	reg *schema.Registry

	db    *sql.DB
	write *sql.Conn
	read  *sql.Conn

	tables map[string]*schema.TableSchema // lowercase name -> table

	wmu sync.Mutex   // serializes writers (held across explicit Tx)
	rw  sync.RWMutex // readers vs writers

	capMu   sync.Mutex
	mode    CaptureMode
	pending []RawChange
	inTx    bool

	writeStmts *stmtCache
	readStmts  *stmtCache

	ddl      []string
	localDDL []string

	// failCommit injects COMMIT failures (tests only).
	failCommit func() error

	closed         bool
	cleanupDir     string
	concurrentMVCC bool
}

// Open creates the in-memory query database, registers change capture, and
// creates the schema. ddl overrides generated DDL; localDDL holds
// local-only objects applied after every (re)build.
func Open(reg *schema.Registry, ddl, localDDL []string, stmtCacheEntries int) (*Engine, error) {
	return open(reg, ddl, localDDL, stmtCacheEntries, "", 0)
}

// OpenMMap opens a file-backed, disposable SQLite materialization with mmap
// enabled. Pebble remains authoritative; the temporary directory is removed
// when the engine closes and the materialization is rebuilt on the next open.
// This provides mmap-backed pages with the current pure-Go SQLite driver, but
// does not provide LMDB's lock-free MVCC semantics.
func OpenMMap(reg *schema.Registry, ddl, localDDL []string, stmtCacheEntries int, tempBase string, mmapBytes int64) (*Engine, error) {
	if mmapBytes <= 0 {
		mmapBytes = 256 << 20
	}
	dir, err := os.MkdirTemp(tempBase, "spedsql-query-")
	if err != nil {
		return nil, fmt.Errorf("sqlengine: create disposable query directory: %w", err)
	}
	e, err := open(reg, ddl, localDDL, stmtCacheEntries, filepath.Join(dir, "query.db"), mmapBytes)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	e.cleanupDir = dir
	return e, nil
}

func open(reg *schema.Registry, ddl, localDDL []string, stmtCacheEntries int, filePath string, mmapBytes int64) (*Engine, error) {
	if stmtCacheEntries <= 0 {
		stmtCacheEntries = 256
	}
	// Unique shared-cache name per engine: shared-cache memory databases
	// are keyed by name process-wide, so a fixed name would alias separate
	// engines (e.g., two nodes in one test process) onto one database.
	var randSuffix [8]byte
	if _, err := rand.Read(randSuffix[:]); err != nil {
		return nil, fmt.Errorf("sqlengine: rand: %w", err)
	}
	dsn := fmt.Sprintf("file:replicateddb_%x?mode=memory&cache=shared", randSuffix)
	if filePath != "" {
		dsn = (&url.URL{Scheme: "file", Path: filePath}).String()
	}
	if backendConcurrentMVCC {
		if strings.Contains(dsn, "?") {
			dsn += "&_busy_timeout=5000&_foreign_keys=off&_sync=off"
		} else {
			dsn += "?_busy_timeout=5000&_foreign_keys=off&_sync=off"
		}
	}
	db, err := sql.Open(sqlDriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlengine: open: %w", err)
	}
	// LumoSQL's independent MVCC readers require a file-backed LMDB
	// environment. Its named in-memory DSN is connection-local, so keep the
	// reserved read connection for Open() and enable the pool only for
	// OpenMMap().
	concurrentMVCC := backendConcurrentMVCC && filePath != ""
	maxConns := 2
	if concurrentMVCC {
		maxConns = 32
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	ctx := context.Background()
	write, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlengine: write conn: %w", err)
	}
	var read *sql.Conn
	if backendConcurrentMVCC && filePath == "" {
		// The LumoSQL memory DSN creates a private environment per connection.
		// Open() promises an in-memory engine, so share its one connection and
		// use the ordinary reader/writer lock instead of the file-backed MVCC
		// pool.
		read = write
	} else {
		read, err = db.Conn(ctx)
		if err != nil {
			write.Close()
			db.Close()
			return nil, fmt.Errorf("sqlengine: read conn: %w", err)
		}
	}
	e := &Engine{
		reg:            reg,
		db:             db,
		write:          write,
		read:           read,
		tables:         make(map[string]*schema.TableSchema, len(reg.Tables)),
		writeStmts:     newStmtCache(stmtCacheEntries),
		readStmts:      newStmtCache(stmtCacheEntries),
		ddl:            ddl,
		localDDL:       localDDL,
		concurrentMVCC: concurrentMVCC,
	}
	for _, t := range reg.Tables {
		e.tables[strings.ToLower(t.Name)] = t
	}
	for _, c := range []*sql.Conn{write, read} {
		pragmas := []string{
			"PRAGMA busy_timeout = 5000",
			"PRAGMA synchronous = OFF",
			"PRAGMA foreign_keys = OFF",
		}
		if !e.concurrentMVCC {
			pragmas = append(pragmas, "PRAGMA journal_mode = MEMORY")
		}
		for _, pr := range pragmas {
			if _, err := c.ExecContext(ctx, pr); err != nil {
				e.Close()
				return nil, fmt.Errorf("sqlengine: pragma %q: %w", pr, err)
			}
		}
		if mmapBytes > 0 && !e.concurrentMVCC {
			if _, err := c.ExecContext(ctx, fmt.Sprintf("PRAGMA mmap_size = %d", mmapBytes)); err != nil {
				e.Close()
				return nil, fmt.Errorf("sqlengine: configure mmap: %w", err)
			}
		}
	}
	if err := e.registerPreUpdateHook(write); err != nil {
		e.Close()
		return nil, err
	}
	if err := e.createSchema(ctx, ddl, localDDL); err != nil {
		e.Close()
		return nil, err
	}
	if err := e.validateOrdinals(ctx); err != nil {
		e.Close()
		return nil, err
	}
	return e, nil
}

// Registry returns the schema registry.
func (e *Engine) Registry() *schema.Registry { return e.reg }

// Close releases all resources.
func (e *Engine) Close() error {
	e.wmu.Lock()
	defer e.wmu.Unlock()
	e.rw.Lock()
	defer e.rw.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	e.writeStmts.close()
	e.readStmts.close()
	if e.write != nil {
		e.write.Close()
	}
	if e.read != nil && e.read != e.write {
		e.read.Close()
	}
	var first error
	if e.db != nil {
		first = e.db.Close()
	}
	if e.cleanupDir != "" {
		if err := os.RemoveAll(e.cleanupDir); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// createSchema builds replicated tables then local-only objects.
func (e *Engine) createSchema(ctx context.Context, ddl, localDDL []string) error {
	stmts := ddl
	if len(stmts) == 0 {
		for _, t := range e.reg.Tables {
			stmts = append(stmts, t.CreateTableDDL())
		}
	}
	for _, s := range stmts {
		if _, err := e.write.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("sqlengine: schema %q: %w", trunc(s, 120), err)
		}
	}
	for _, s := range localDDL {
		if _, err := e.write.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("sqlengine: local schema %q: %w", trunc(s, 120), err)
		}
	}
	return nil
}

// validateOrdinals pins the ordinal->column mapping: PRAGMA table_info must
// list registry columns in registry order.
func (e *Engine) validateOrdinals(ctx context.Context) error {
	for _, t := range e.reg.Tables {
		rows, err := e.write.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(t.Name)))
		if err != nil {
			return fmt.Errorf("sqlengine: table_info %s: %w", t.Name, err)
		}
		var names []string
		for rows.Next() {
			var (
				cid     int
				name    string
				ctype   string
				notnull int
				dflt    sql.NullString
				pk      int
			)
			if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
				rows.Close()
				return err
			}
			names = append(names, name)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		if len(names) != len(t.Columns) {
			return fmt.Errorf("sqlengine: table %s has %d columns, registry has %d",
				t.Name, len(names), len(t.Columns))
		}
		for i, c := range t.Columns {
			if !strings.EqualFold(names[i], c.Name) {
				return fmt.Errorf("sqlengine: table %s ordinal %d is %q, registry expects %q",
					t.Name, i, names[i], c.Name)
			}
		}
	}
	return nil
}

// pkOrdinal returns the CREATE TABLE ordinal of the table's PK.
func pkOrdinal(t *schema.TableSchema) int {
	for i := range t.Columns {
		if t.Columns[i].ID == t.PK {
			return i
		}
	}
	return -1
}

// rowIDOf extracts the RowID from ordinal values.
func rowIDOf(vals []codec.Value, pkOrd int) (ids.RowID, error) {
	var zero ids.RowID
	if pkOrd < 0 || pkOrd >= len(vals) {
		return zero, fmt.Errorf("sqlengine: pk ordinal out of range")
	}
	v := vals[pkOrd]
	if v.Type != codec.TypeBlob || len(v.B) != 16 {
		return zero, fmt.Errorf("sqlengine: primary key must be BLOB(16)")
	}
	var id ids.RowID
	copy(id[:], v.B)
	return id, nil
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
