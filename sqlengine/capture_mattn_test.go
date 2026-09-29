//go:build !modernc && sqlite_preupdate_hook

package sqlengine

import (
	"context"
	"testing"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
)

func TestMattnSQLitePreUpdatePreservesTextAndBlobTypes(t *testing.T) {
	e, err := Open(testRegistry(t), nil, nil, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	id := ids.NewRowID()
	const text = "text-value"
	tx, err := e.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], text); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	changes, err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || len(changes[0].New) != 3 {
		t.Fatalf("captured changes = %+v", changes)
	}
	if got := changes[0].New[1]; got.Type != codec.TypeText || got.S != text {
		t.Fatalf("text capture = %+v, want TEXT %q", got, text)
	}
	if got := changes[0].New[0]; got.Type != codec.TypeBlob || string(got.B) != string(id[:]) {
		t.Fatalf("primary-key capture = %+v, want BLOB %x", got, id[:])
	}
}
