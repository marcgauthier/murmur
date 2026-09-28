package sqlengine

import (
	"context"
	"os"
	"path/filepath"
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
