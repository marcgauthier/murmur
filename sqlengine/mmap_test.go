package sqlengine

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/nomadsql/replicateddb/ids"
)

func TestOpenMMapUsesDisposableFileStore(t *testing.T) {
	base := t.TempDir()
	e, err := OpenMMap(testRegistry(t), nil, nil, 32, base, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	dir := e.cleanupDir
	if dir == "" {
		t.Fatal("mmap engine did not retain its disposable directory")
	}
	if info, err := os.Stat(dir); err != nil {
		t.Fatalf("query directory was not created: %v", err)
	} else if runtime.GOOS != "windows" && info.Mode().Perm() != 0700 {
		t.Fatalf("query directory permissions = %04o, want 0700", info.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(dir, "query.db")); err != nil {
		t.Fatalf("query database was not created: %v", err)
	}
	row := ids.NewRowID()
	tx, err := e.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO contacts (id, name) VALUES (?, ?)`, row[:], "mapped"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("disposable query directory still exists (stat err=%v)", err)
	}
}

func TestOpenMMapPurgesOrphanedCrashDirectories(t *testing.T) {
	base := t.TempDir()
	orphanDir := filepath.Join(base, "spedsql-query-crashed123")
	if err := os.MkdirAll(orphanDir, 0700); err != nil {
		t.Fatal(err)
	}
	dummyFile := filepath.Join(orphanDir, "query.db")
	if err := os.WriteFile(dummyFile, []byte("sensitive-plaintext-sql-pages"), 0600); err != nil {
		t.Fatal(err)
	}

	e, err := OpenMMap(testRegistry(t), nil, nil, 32, base, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if _, err := os.Stat(orphanDir); !os.IsNotExist(err) {
		t.Fatalf("orphaned query directory %s was not cleaned up on startup (err=%v)", orphanDir, err)
	}
}

func TestOpenMMapFileAccessMode(t *testing.T) {
	e, err := OpenMMap(testRegistry(t), nil, nil, 32, t.TempDir(), 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	ctx := context.Background()
	var mode string
	if err := e.write.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	var mmapSize int64
	if err := e.write.QueryRowContext(ctx, "PRAGMA mmap_size").Scan(&mmapSize); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Fatalf("file-backed journal mode = %q, want wal", mode)
	}
	if mmapSize != 8<<20 {
		t.Fatalf("file-backed mmap size = %d, want %d", mmapSize, 8<<20)
	}
}
