package sqlengine

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/nomadsql/replicateddb/schema"
)

// OpenMMap opens a file-backed, disposable SQLite materialization with mmap
// enabled. Pebble remains authoritative; the temporary directory is removed
// and securely wiped when the engine closes, and the materialization is rebuilt
// on the next open.
func OpenMMap(reg *schema.Registry, ddl, localDDL []string, stmtCacheEntries int, tempBase string, mmapBytes int64) (*Engine, error) {
	if mmapBytes <= 0 {
		mmapBytes = 256 << 20
	}
	if tempBase == "" {
		tempBase = os.TempDir()
	}
	if err := os.MkdirAll(tempBase, 0700); err != nil {
		return nil, fmt.Errorf("sqlengine: create mmap base directory: %w", err)
	}
	purgeOrphanedQueryDirs(tempBase)

	dir, err := os.MkdirTemp(tempBase, "spedsql-query-")
	if err != nil {
		return nil, fmt.Errorf("sqlengine: create disposable query directory: %w", err)
	}
	_ = os.Chmod(dir, 0700)
	filePath := filepath.Join(dir, "query.db")

	if stmtCacheEntries <= 0 {
		stmtCacheEntries = 256
	}
	dsn := (&url.URL{Scheme: "file", Path: filePath}).String()
	db, err := sql.Open(sqlDriverName, dsn)
	if err != nil {
		_ = secureWipeAndRemoveDir(dir)
		return nil, fmt.Errorf("sqlengine: open: %w", err)
	}
	const maxConnections = 32
	db.SetMaxOpenConns(maxConnections)
	db.SetMaxIdleConns(maxConnections)

	ctx := context.Background()
	write, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		_ = secureWipeAndRemoveDir(dir)
		return nil, fmt.Errorf("sqlengine: write conn: %w", err)
	}

	e := &Engine{
		reg:        reg,
		db:         db,
		write:      write,
		tables:     make(map[string]*schema.TableSchema, len(reg.Tables)),
		writeStmts: newStmtCache(stmtCacheEntries),
		readStmts:  newStmtCache(stmtCacheEntries),
		ddl:        ddl,
		localDDL:   localDDL,
		cleanupDir: dir,
	}
	for _, t := range reg.Tables {
		e.tables[strings.ToLower(t.Name)] = t
	}

	pragmas := []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA synchronous = OFF",
		"PRAGMA foreign_keys = OFF",
		"PRAGMA journal_mode = WAL",
	}
	for _, pr := range pragmas {
		if _, err := write.ExecContext(ctx, pr); err != nil {
			_ = e.Close()
			return nil, fmt.Errorf("sqlengine: pragma %q: %w", pr, err)
		}
	}
	if mmapBytes > 0 {
		if _, err := write.ExecContext(ctx, fmt.Sprintf("PRAGMA mmap_size = %d", mmapBytes)); err != nil {
			_ = e.Close()
			return nil, fmt.Errorf("sqlengine: configure mmap: %w", err)
		}
	}
	if err := e.registerPreUpdateHook(write); err != nil {
		_ = e.Close()
		return nil, err
	}
	if err := e.createSchema(ctx, ddl, localDDL); err != nil {
		_ = e.Close()
		return nil, err
	}
	if err := e.validateOrdinals(ctx); err != nil {
		_ = e.Close()
		return nil, err
	}
	return e, nil
}

func purgeOrphanedQueryDirs(base string) {
	entries, err := os.ReadDir(base)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "spedsql-query-") {
			_ = secureWipeAndRemoveDir(filepath.Join(base, entry.Name()))
		}
	}
}

func secureWipeAndRemoveDir(dir string) error {
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() {
			if f, openErr := os.OpenFile(path, os.O_WRONLY, 0600); openErr == nil {
				size := info.Size()
				if size > 0 {
					zero := make([]byte, 32768)
					for written := int64(0); written < size; {
						n := int64(len(zero))
						if size-written < n {
							n = size - written
						}
						_, _ = f.Write(zero[:n])
						written += n
					}
					_ = f.Sync()
				}
				_ = f.Close()
			}
		}
		return nil
	})
	return os.RemoveAll(dir)
}
