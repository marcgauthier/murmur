package sqlengine

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
)

func TestQueryRowsCloseIsIdempotentAndUnblocksWriter(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	id := ids.NewRowID()
	tx, err := e.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`, id[:], "ann", "111"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	rows, err := e.Query(ctx, `SELECT name, phone FROM contacts WHERE id = ?`, id[:])
	if err != nil {
		t.Fatal(err)
	}
	if got, want := rows.Columns(), []string{"name", "phone"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Columns() = %v, want %v", got, want)
	}
	if !rows.Next() {
		t.Fatalf("expected row, Err() = %v", rows.Err())
	}
	var name, phone string
	if err := rows.Scan(&name, &phone); err != nil {
		t.Fatal(err)
	}
	if name != "ann" || phone != "111" {
		t.Fatalf("row = (%q, %q), want (ann, 111)", name, phone)
	}

	started := make(chan struct{})
	writerResult := make(chan error, 1)
	go func() {
		close(started)
		writer, err := e.Begin(ctx)
		if err == nil {
			err = writer.Rollback()
		}
		writerResult <- err
	}()
	<-started
	select {
	case err := <-writerResult:
		t.Fatalf("writer passed an open read Rows: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("second Close() = %v, want nil", err)
	}
	select {
	case err := <-writerResult:
		if err != nil {
			t.Fatalf("writer after Rows.Close(): %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("closing Rows did not release the reader lock for a writer")
	}
}

func TestQueryRowContextAndClosedEngineErrors(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.QueryRowContext(ctx, `SELECT count(*) FROM contacts`, nil, func(row *sql.Row) error {
		var count int
		if err := row.Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			t.Errorf("initial count = %d, want 0", count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Query(ctx, `SELECT 1`); err == nil {
		t.Fatal("Query on closed engine succeeded")
	}
	if err := e.QueryRowContext(ctx, `SELECT 1`, nil, func(_ *sql.Row) error { return nil }); err == nil {
		t.Fatal("QueryRowContext on closed engine succeeded")
	}
}
