package sqlengine

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/ids"
)

func TestCheckSingleStatement(t *testing.T) {
	accept := []string{
		`SELECT 1`,
		`SELECT * FROM contacts WHERE name = ?`,
		`INSERT INTO contacts (id, name) VALUES (?, ?)`,
		`INSERT INTO contacts (id, name) VALUES (?, ?);`,
		"INSERT INTO contacts (id, name) VALUES (?, ?);  \n\t",
		`SELECT 1; -- trailing comment`,
		`SELECT 1; /* trailing block */`,
		`SELECT ';'`,
		`SELECT 'it''s; fine'`,
		`SELECT "a;b"`,
		"SELECT `a;b`",
		`SELECT [a;b]`,
		`SELECT 1 -- comment; with semicolon`,
		`SELECT /* block; comment */ 1`,
		`-- leading comment;` + "\n" + `SELECT 1`,
		`SELECT x'0a3b'`,
		``,
		`-- only a comment;`,
	}
	for _, q := range accept {
		if err := checkSingleStatement(q); err != nil {
			t.Errorf("checkSingleStatement(%q) = %v, want nil", q, err)
		}
	}
	reject := []string{
		`SELECT 1; SELECT 2`,
		`INSERT INTO contacts (id) VALUES (?); DELETE FROM contacts`,
		`SELECT 1;DELETE FROM contacts`,
		`SELECT 1; -- comment` + "\n" + `DELETE FROM contacts`,
		`SELECT 1; /* c */ DELETE FROM contacts;`,
		`SELECT ';'; DROP TABLE contacts`,
		`SELECT 1;; SELECT 2`,
	}
	for _, q := range reject {
		if err := checkSingleStatement(q); !errors.Is(err, ErrMultiStatement) {
			t.Errorf("checkSingleStatement(%q) = %v, want ErrMultiStatement", q, err)
		}
	}
}

// TestStackedStatementsRejected pins the backend-independent contract: a
// stacked write fails and applies nothing (neither the first statement nor
// the trailing one), on whichever SQLite driver the build uses.
func TestStackedStatementsRejected(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	id := ids.NewRowID()
	tx, err := e.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "ann"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	stacked := []string{
		`INSERT INTO contacts (id, name) VALUES (?, ?); DELETE FROM contacts`,
		"SELECT * FROM contacts WHERE name = 'x'; DROP TABLE contacts",
	}
	for _, q := range stacked {
		tx, err := e.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		evilID := ids.NewRowID()
		_, execErr := tx.Exec(q, evilID[:], "mallory")
		_ = tx.Rollback()
		if !errors.Is(execErr, ErrMultiStatement) {
			t.Fatalf("tx.Exec(%q) = %v, want ErrMultiStatement", q, execErr)
		}
		evilID2 := ids.NewRowID()
		if _, err := e.Query(ctx, q, evilID2[:]); !errors.Is(err, ErrMultiStatement) {
			t.Fatalf("engine.Query(%q) = %v, want ErrMultiStatement", q, err)
		}
	}

	rows, err := e.Query(ctx, `SELECT name FROM contacts`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var dest [1]any
		ptrs := []any{&dest[0]}
		_ = rows.rows.Scan(ptrs...)
		names = append(names, strings.TrimSpace(strings.Join([]string{toString(dest[0])}, "")))
	}
	if len(names) != 1 || names[0] != "ann" {
		t.Fatalf("contacts after stacked attempts = %q, want [ann] (partial apply?)", names)
	}
}

func toString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		return ""
	}
}
