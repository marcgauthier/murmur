package replicateddb

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/sqlengine"
)

// This file locks section 6 at the engine boundary: raw DDL cannot
// bypass the replicated-schema rules, and normal Exec/Query paths
// cannot run schema changes, attach databases, flip pragmas, or
// smuggle writes past capture.

const guardBaseDDL = `CREATE TABLE contacts (id BLOB PRIMARY KEY NOT NULL, name TEXT, phone TEXT, score INTEGER)`

func openWithDDL(t *testing.T, ddl, localDDL []string) (*DB, error) {
	t.Helper()
	cfg := testConfig(t.TempDir())
	cfg.Schema.DDL = ddl
	cfg.Schema.LocalDDL = localDDL
	db, err := Open(context.Background(), cfg)
	if err == nil {
		t.Cleanup(func() { db.Close() })
	}
	return db, err
}

func TestOpenRejectsResultingSchemaViolations(t *testing.T) {
	cases := []struct {
		name     string
		ddl      []string
		localDDL []string
		want     string
	}{
		{"column unique", []string{`CREATE TABLE contacts (id BLOB PRIMARY KEY NOT NULL, name TEXT UNIQUE, phone TEXT, score INTEGER)`}, nil, "UNIQUE"},
		{"table unique", []string{`CREATE TABLE contacts (id BLOB PRIMARY KEY NOT NULL, name TEXT, phone TEXT, score INTEGER, UNIQUE (name))`}, nil, "UNIQUE"},
		{"standalone unique index", []string{guardBaseDDL, `CREATE UNIQUE INDEX uq ON contacts(name)`}, nil, "UNIQUE"},
		{"cross-column check", []string{`CREATE TABLE contacts (id BLOB PRIMARY KEY NOT NULL, name TEXT, phone TEXT CHECK (phone <> name), score INTEGER)`}, nil, "references column"},
		{"table check", []string{guardBaseDDL[:len(guardBaseDDL)-1] + `, CHECK (score > 0))`}, nil, "table-level"},
		{"autoincrement", []string{`CREATE TABLE contacts (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, name TEXT, phone TEXT, score INTEGER)`}, nil, "AUTOINCREMENT"},
		{"without rowid", []string{guardBaseDDL + ` WITHOUT ROWID`}, nil, "WITHOUT ROWID"},
		{"strict", []string{guardBaseDDL + ` STRICT`}, nil, "STRICT"},
		{"virtual table", []string{`CREATE VIRTUAL TABLE contacts USING fts5(name)`}, nil, "virtual"},
		{"pk without not null", []string{`CREATE TABLE contacts (id BLOB PRIMARY KEY, name TEXT, phone TEXT, score INTEGER)`}, nil, "nullability"},
		{"table pk without not null", []string{`CREATE TABLE contacts (id BLOB, name TEXT, phone TEXT, score INTEGER, PRIMARY KEY (id))`}, nil, "nullability"},
		{"pk on wrong column", []string{`CREATE TABLE contacts (id BLOB NOT NULL, name TEXT PRIMARY KEY, phone TEXT, score INTEGER)`}, nil, "PRIMARY KEY"},
		{"composite pk", []string{`CREATE TABLE contacts (id BLOB NOT NULL, name TEXT, phone TEXT, score INTEGER, PRIMARY KEY (id, name))`}, nil, "single-column"},
		{"wrong column type", []string{`CREATE TABLE contacts (id BLOB PRIMARY KEY NOT NULL, name TEXT, phone TEXT, score TEXT)`}, nil, "does not map"},
		{"unmappable decltype", []string{`CREATE TABLE contacts (id BLOB PRIMARY KEY NOT NULL, name TEXT, phone TEXT, score DATETIME)`}, nil, "does not map"},
		{"view in schema ddl", []string{guardBaseDDL, `CREATE VIEW v AS SELECT 1`}, nil, "LocalDDL"},
		{"drop in schema ddl", []string{guardBaseDDL, `DROP TABLE contacts`}, nil, "Migrate"},
		{"local unique index", []string{guardBaseDDL}, []string{`CREATE UNIQUE INDEX luq ON contacts(name)`}, "UNIQUE"},
		{"local drop", []string{guardBaseDDL}, []string{`DROP TABLE contacts`}, "not allowed"},
		{"local attach", []string{guardBaseDDL}, []string{`ATTACH DATABASE 'x.db' AS x`}, "not allowed"},
		{"local pragma", []string{guardBaseDDL}, []string{`PRAGMA foreign_keys = ON`}, "not allowed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := openWithDDL(t, tc.ddl, tc.localDDL)
			if err == nil {
				db.Close()
				t.Fatalf("open succeeded with %s, want rejection containing %q", tc.name, tc.want)
			}
			if !errors.Is(err, schema.ErrUnsupportedSchema) {
				t.Fatalf("open err = %v, want ErrUnsupportedSchema", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("open err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestOpenAcceptsToleratedDDL(t *testing.T) {
	cases := []struct {
		name     string
		ddl      []string
		localDDL []string
	}{
		{"single-column check", []string{`CREATE TABLE contacts (id BLOB PRIMARY KEY NOT NULL, name TEXT CHECK (name <> ''), phone TEXT, score INTEGER)`}, nil},
		{"check with function", []string{`CREATE TABLE contacts (id BLOB PRIMARY KEY NOT NULL, name TEXT CHECK (length(name) > 0), phone TEXT, score INTEGER)`}, nil},
		{"fk declaration", []string{`CREATE TABLE contacts (id BLOB PRIMARY KEY NOT NULL, name TEXT REFERENCES contacts(id), phone TEXT, score INTEGER)`}, nil},
		{"fk with actions", []string{`CREATE TABLE contacts (id BLOB PRIMARY KEY NOT NULL, name TEXT, phone TEXT, score INTEGER, FOREIGN KEY (phone) REFERENCES contacts(id) ON DELETE CASCADE)`}, nil},
		{"column default", []string{`CREATE TABLE contacts (id BLOB PRIMARY KEY NOT NULL, name TEXT DEFAULT 'x', phone TEXT, score INTEGER)`}, nil},
		{"unique word in default", []string{`CREATE TABLE contacts (id BLOB PRIMARY KEY NOT NULL, name TEXT DEFAULT 'unique', phone TEXT, score INTEGER)`}, nil},
		{"table-level single pk", []string{`CREATE TABLE contacts (id BLOB NOT NULL, name TEXT, phone TEXT, score INTEGER, PRIMARY KEY (id))`}, nil},
		{"sized types", []string{`CREATE TABLE contacts (id BLOB(16) PRIMARY KEY NOT NULL, name VARCHAR(32), phone TEXT, score BIGINT)`}, nil},
		{"local non-unique index", []string{guardBaseDDL}, []string{`CREATE INDEX IF NOT EXISTS idx_name ON contacts(name)`}},
		{"local view", []string{guardBaseDDL}, []string{`CREATE VIEW v AS SELECT id, name FROM contacts`}},
		{"local table with unique", []string{guardBaseDDL}, []string{`CREATE TABLE IF NOT EXISTS cache (key TEXT PRIMARY KEY, value BLOB)`}},
		{"local temp table", []string{guardBaseDDL}, []string{`CREATE TEMP TABLE IF NOT EXISTS tmp (a TEXT)`}},
		{"local trigger", []string{guardBaseDDL}, []string{`CREATE TRIGGER trg AFTER INSERT ON contacts BEGIN UPDATE contacts SET name = new.name WHERE id = new.id; END`}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := openWithDDL(t, tc.ddl, tc.localDDL)
			if err != nil {
				t.Fatalf("open rejected %s: %v", tc.name, err)
			}
			// The engine still serves reads and writes afterwards.
			id := NewRowID()
			if _, err := db.ExecContext(context.Background(), `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "ann"); err != nil {
				t.Fatalf("write after %s: %v", tc.name, err)
			}
		})
	}
}

func TestExecStatementGate(t *testing.T) {
	db, err := Open(context.Background(), testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()

	reject := []string{
		`CREATE TABLE evil (a TEXT)`, `CREATE UNIQUE INDEX i ON contacts(name)`,
		`CREATE INDEX i ON contacts(name)`, `DROP TABLE contacts`,
		`ALTER TABLE contacts ADD COLUMN x TEXT`, `ATTACH DATABASE 'x.db' AS x`,
		`DETACH x`, `PRAGMA foreign_keys = ON`, `PRAGMA table_info(contacts)`,
		`VACUUM`, `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT sp`, `RELEASE sp`,
		`begin immediate`, `-- hi
DROP TABLE contacts`, `/* x */ attach 'f' as g`,
	}
	for _, q := range reject {
		if _, err := db.ExecContext(ctx, q); err == nil {
			t.Fatalf("exec allowed %q", q)
		} else if !errors.Is(err, sqlengine.ErrStatementNotAllowed) {
			t.Fatalf("exec %q err = %v, want ErrStatementNotAllowed", q, err)
		}
	}
	{
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range reject {
			if _, err := tx.ExecContext(ctx, q); !errors.Is(err, sqlengine.ErrStatementNotAllowed) {
				t.Fatalf("tx exec %q err = %v, want ErrStatementNotAllowed", q, err)
			}
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}

	id := NewRowID()
	allowArgs := []struct {
		q    string
		args []any
	}{
		{`INSERT INTO contacts (id, name) VALUES (?, ?)`, []any{id[:], "ann"}},
		{`UPDATE contacts SET name = 'ann2' WHERE name = 'ann'`, nil},
		{`INSERT INTO contacts (id, name) VALUES (?, ?) ON CONFLICT DO NOTHING`, []any{id[:], "ann3"}},
		{`WITH x AS (SELECT 1) UPDATE contacts SET name = 'ann4' WHERE name = 'ann2'`, nil},
		{`DELETE FROM contacts WHERE name = 'nope'`, nil},
		{`REPLACE INTO contacts (id, name) VALUES (?, ?)`, []any{id[:], "ann5"}},
		{`SELECT 1`, nil}, {`EXPLAIN SELECT 1`, nil},
		{`WITH x AS (SELECT 1) SELECT * FROM x`, nil},
		{`ANALYZE`, nil}, {`REINDEX`, nil},
	}
	for _, tc := range allowArgs {
		if _, err := db.ExecContext(ctx, tc.q, tc.args...); err != nil {
			t.Fatalf("exec rejected %q: %v", tc.q, err)
		}
	}
}

func TestPoolQueryGate(t *testing.T) {
	db, err := Open(context.Background(), testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()

	reject := []string{
		`INSERT INTO contacts (id) VALUES (1)`, `UPDATE contacts SET name = 'x'`,
		`DELETE FROM contacts`, `REPLACE INTO contacts (id) VALUES (1)`,
		`CREATE TABLE evil (a)`, `DROP TABLE contacts`, `ATTACH 'f' AS x`,
		`PRAGMA foreign_keys = ON`, `WITH x AS (SELECT 1) DELETE FROM contacts`,
		`BEGIN`, `VACUUM`,
	}
	for _, q := range reject {
		rows, err := db.QueryContext(ctx, q)
		if err == nil {
			rows.Close()
			t.Fatalf("pool query allowed %q", q)
		}
		if !errors.Is(err, sqlengine.ErrStatementNotAllowed) {
			t.Fatalf("pool query %q err = %v, want ErrStatementNotAllowed", q, err)
		}
	}
	allow := []string{
		`SELECT 1`, `SELECT * FROM contacts`, `WITH x AS (SELECT 1) SELECT * FROM x`,
		`SELECT replace(name, 'a', 'b') FROM contacts`, `EXPLAIN SELECT 1`,
		`PRAGMA table_info(contacts)`, `VALUES (1)`, `SELECT * FROM contacts LIMIT 0`,
	}
	for _, q := range allow {
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			t.Fatalf("pool query rejected %q: %v", q, err)
		}
		rows.Close()
	}
}

func TestTxQueryGate(t *testing.T) {
	db, err := Open(context.Background(), testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`CREATE TABLE evil (a)`, `DROP TABLE contacts`, `ATTACH 'f' AS x`,
		`PRAGMA foreign_keys = ON`, `SAVEPOINT sp`, `COMMIT`,
	} {
		rows, err := tx.QueryContext(ctx, q)
		if err == nil {
			rows.Close()
			t.Fatalf("tx query allowed %q", q)
		}
		if !errors.Is(err, sqlengine.ErrStatementNotAllowed) {
			t.Fatalf("tx query %q err = %v, want ErrStatementNotAllowed", q, err)
		}
	}
	// Writes through in-tx Query replicate on commit (capture runs on
	// the write connection), so they stay allowed. Like any driver
	// Rows, the write executes as the rows are consumed, so the
	// RETURNING row must be read before Close.
	id := NewRowID()
	rows, err := tx.QueryContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?) RETURNING id`, id[:], "ann")
	if err != nil {
		t.Fatalf("tx query insert: %v", err)
	}
	var gotID []byte
	if !rows.Next() {
		rows.Close()
		t.Fatalf("tx query insert returned no row: %v", rows.Err())
	}
	if err := rows.Scan(&gotID); err != nil {
		rows.Close()
		t.Fatalf("tx query insert scan: %v", err)
	}
	rows.Close()
	if string(gotID) != string(id[:]) {
		t.Fatalf("tx query insert returned id %x, want %x", gotID, id[:])
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	got := queryAll(t, db, `SELECT name FROM contacts WHERE id = ?`, id[:])
	if len(got) != 1 {
		t.Fatalf("in-tx query insert did not persist: %v", got)
	}
}

func TestForeignKeysStayUnenforced(t *testing.T) {
	db, err := openWithDDL(t, []string{
		`CREATE TABLE contacts (id BLOB PRIMARY KEY NOT NULL, name TEXT, phone TEXT REFERENCES contacts(id) ON DELETE CASCADE, score INTEGER)`,
	}, nil)
	if err != nil {
		t.Fatalf("open with FK decl: %v", err)
	}
	ctx := context.Background()
	parent, child, ghost := NewRowID(), NewRowID(), NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, parent[:], "p"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, phone) VALUES (?, ?)`, child[:], parent[:]); err != nil {
		t.Fatal(err)
	}
	// A child may arrive before (or without) its parent: no violation.
	ghostParent := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, phone) VALUES (?, ?)`, ghost[:], ghostParent[:]); err != nil {
		t.Fatalf("orphan insert rejected (FK must stay unenforced): %v", err)
	}
	// No cascading deletes: removing the parent leaves the child.
	if _, err := db.ExecContext(ctx, `DELETE FROM contacts WHERE id = ?`, parent[:]); err != nil {
		t.Fatal(err)
	}
	got := queryAll(t, db, `SELECT COUNT(*) FROM contacts WHERE id = ?`, child[:])
	if len(got) != 1 || got[0][0] != int64(1) {
		t.Fatalf("child cascaded away: %v", got)
	}
}

func TestPrimaryKeyMustBe16Bytes(t *testing.T) {
	db, err := Open(context.Background(), testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	for name, pk := range map[string][]byte{
		"short": make([]byte, 3),
		"long":  make([]byte, 17),
		"empty": {},
	} {
		_, err := db.ExecContext(ctx, `INSERT INTO contacts (id) VALUES (?)`, pk)
		if err == nil {
			t.Fatalf("%s blob PK accepted", name)
		}
		if !strings.Contains(err.Error(), "BLOB(16)") {
			t.Fatalf("%s blob PK err = %v, want BLOB(16) reason", name, err)
		}
	}
}
