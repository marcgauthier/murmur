//go:build lumosql && lumosql_mvcc && sqlite_preupdate_hook

package sqlengine

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/nomadsql/replicateddb/ids"
)

func TestLumoSQLReaderSnapshotDoesNotBlockWriter(t *testing.T) {
	e, err := OpenMMap(testRegistry(t), nil, nil, 16, t.TempDir(), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	id := ids.NewRowID()
	insert, err := e.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := insert.Exec(`INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "old"); err != nil {
		_ = insert.Rollback()
		t.Fatal(err)
	}
	if _, err := insert.Commit(); err != nil {
		t.Fatal(err)
	}

	rows, err := e.Query(context.Background(), `SELECT name FROM contacts WHERE id = ?`, id[:])
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		_ = rows.Close()
		t.Fatal("reader did not see the inserted row")
	}
	var old string
	if err := rows.Scan(&old); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if old != "old" {
		_ = rows.Close()
		t.Fatalf("snapshot value=%q, want old", old)
	}

	written := make(chan error, 1)
	go func() {
		tx, err := e.Begin(context.Background())
		if err != nil {
			written <- err
			return
		}
		if _, err := tx.Exec(`UPDATE contacts SET name = ? WHERE id = ?`, "new", id[:]); err != nil {
			_ = tx.Rollback()
			written <- err
			return
		}
		_, err = tx.Commit()
		written <- err
	}()
	select {
	case err := <-written:
		if err != nil {
			_ = rows.Close()
			t.Fatalf("writer failed with reader open: %v", err)
		}
	case <-time.After(3 * time.Second):
		_ = rows.Close()
		t.Fatal("writer was blocked by an open LMDB read snapshot")
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.QueryRowContext(context.Background(), `SELECT name FROM contacts WHERE id = ?`, []any{id[:]}, func(row *sql.Row) error {
		return row.Scan(&old)
	}); err != nil {
		t.Fatal(err)
	}
	if old != "new" {
		t.Fatalf("post-write query value=%q, want new", old)
	}
}
