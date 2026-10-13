package murmurgorm

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	murmur "github.com/marcgauthier/murmur/go-client"
)

func testKey() []byte { return bytes.Repeat([]byte{7}, 32) }

// openNative opens a fresh Murmur database, closed on cleanup.
func openNative(t *testing.T) *murmur.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db")
	opts := murmur.DefaultOptions()
	opts.Mode = murmur.CreateNew
	db, err := murmur.Open(path, testKey(), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

// openSQL bridges a fresh native database to *sql.DB. Cleanup closes the pool
// before the native database.
func openSQL(t *testing.T) (*sql.DB, *murmur.DB) {
	t.Helper()
	native := openNative(t)
	pool := OpenDB(native)
	t.Cleanup(func() {
		if err := pool.Close(); err != nil {
			t.Error(err)
		}
	})
	return pool, native
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) sql.Result {
	t.Helper()
	res, err := db.Exec(query, args...)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestVersionProbe(t *testing.T) {
	db, _ := openSQL(t)
	var version string
	if err := db.QueryRow("select sqlite_version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("murmur sqlite_version() = %s", version)
	if version == "" {
		t.Fatal("empty version")
	}
}

func TestPing(t *testing.T) {
	db, _ := openSQL(t)
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestAutocommitCRUD(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, n INTEGER, r REAL, b BLOB, flag INTEGER, ts TEXT, nullable TEXT)`)
	when := time.Date(2024, 5, 6, 7, 8, 9, 123456789, time.FixedZone("X", 3600))
	res := mustExec(t, db, `INSERT INTO docs VALUES(?,?,?,?,?,?,?,?)`,
		10, "hello", int64(-42), 2.5, []byte{0, 1, 2}, true, when, nil)
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("rows affected = %d", n)
	}
	if id, _ := res.LastInsertId(); id != 10 {
		t.Fatalf("last insert id = %d", id)
	}

	var (
		id    int64
		title string
		n     int64
		r     float64
		b     []byte
		flag  bool
		ts    time.Time
		null  sql.NullString
	)
	err := db.QueryRow(`SELECT id,title,n,r,b,flag,ts,nullable FROM docs WHERE id=?`, 10).
		Scan(&id, &title, &n, &r, &b, &flag, &ts, &null)
	if err != nil {
		t.Fatal(err)
	}
	if id != 10 || title != "hello" || n != -42 || r != 2.5 {
		t.Fatalf("scalars: %d %q %d %v", id, title, n, r)
	}
	if !bytes.Equal(b, []byte{0, 1, 2}) {
		t.Fatalf("blob: %v", b)
	}
	if !flag {
		t.Fatal("bool lost")
	}
	if !ts.Equal(when) {
		t.Fatalf("time round-trip: got %v want %v", ts, when)
	}
	if null.Valid {
		t.Fatalf("null came back valid: %q", null.String)
	}

	mustExec(t, db, `UPDATE docs SET title=? WHERE id=?`, "bye", 10)
	var got string
	if err := db.QueryRow(`SELECT title FROM docs WHERE id=10`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "bye" {
		t.Fatal(got)
	}
	res = mustExec(t, db, `DELETE FROM docs WHERE id=?`, 10)
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("delete rows affected = %d", n)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal(count)
	}
}

func TestQueryWriteRouting(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT)`)
	// Writes issued through Query (GORM's RETURNING path) run as
	// single-statement transactions and return their rows.
	var id int64
	if err := db.QueryRow(`INSERT INTO docs(title) VALUES(?) RETURNING id`, "r").Scan(&id); err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatal(id)
	}
	var title string
	if err := db.QueryRow(`UPDATE docs SET title=? WHERE id=? RETURNING title`, "u", 1).Scan(&title); err != nil {
		t.Fatal(err)
	}
	if title != "u" {
		t.Fatal(title)
	}
	var v int64
	if err := db.QueryRow(`WITH x(v) AS (VALUES(42)) SELECT v FROM x`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 42 {
		t.Fatal(v)
	}
}

func TestQueryNoRows(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY)`)
	var id int64
	err := db.QueryRow(`SELECT id FROM docs WHERE id=?`, 1).Scan(&id)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
}

func TestSQLErrorPreserved(t *testing.T) {
	db, _ := openSQL(t)
	_, err := db.Exec(`SELECT * FROM nonexistent_table_xyz`)
	if err == nil {
		t.Fatal("expected error")
	}
	var merr *murmur.Error
	if !errors.As(err, &merr) {
		t.Fatalf("murmur error not preserved: %T %v", err, err)
	}
	t.Logf("code=%s sqlite=%v msg=%s", merr.Code, merr.SQLiteCode, merr.Message)
}

func TestConstraintCodeProbe(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY, email TEXT)`)
	mustExec(t, db, `INSERT INTO docs VALUES(?,?)`, 1, "a@x.io")
	_, err := db.Exec(`INSERT INTO docs VALUES(?,?)`, 1, "b@x.io")
	if err == nil {
		t.Fatal("expected primary key violation")
	}
	code, ok := sqliteCodeOf(err)
	t.Logf("pk violation: err=%v code=%d ok=%v", err, code, ok)
	if !ok || (code != sqliteConstraintUnique && code != sqliteConstraintPrimaryKey) {
		t.Fatalf("unexpected pk code mapping: %d (ok=%v)", code, ok)
	}
}

// Murmur's schema validation rejects non-PK uniqueness. This locks in the
// limitation (and its error shape) that GORM uniqueIndex AutoMigrate hits.
func TestNonPKUniqueRejected(t *testing.T) {
	db, _ := openSQL(t)
	_, err := db.Exec(`CREATE TABLE uniq_docs(id INTEGER PRIMARY KEY, email TEXT UNIQUE)`)
	if err == nil {
		t.Fatal("expected UNIQUE column to be rejected")
	}
	var merr *murmur.Error
	if !errors.As(err, &merr) || merr.Code != "Invalid" {
		t.Fatalf("expected Invalid murmur error, got %v", err)
	}
	t.Logf("unique column: %v", err)
	mustExec(t, db, `CREATE TABLE idx_docs(id INTEGER PRIMARY KEY, email TEXT)`)
	_, err = db.Exec(`CREATE UNIQUE INDEX idx_email ON idx_docs(email)`)
	t.Logf("unique index: %v", err)
}

func TestPreparedStatement(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT)`)
	stmt, err := db.Prepare(`INSERT INTO docs VALUES(?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	for i := 1; i <= 5; i++ {
		if _, err := stmt.Exec(i, "row"); err != nil {
			t.Fatal(err)
		}
	}
	q, err := db.Prepare(`SELECT title FROM docs WHERE id=?`)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	var title string
	if err := q.QueryRow(3).Scan(&title); err != nil {
		t.Fatal(err)
	}
	if title != "row" {
		t.Fatal(title)
	}
}

func TestTxCommit(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT)`)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES(?,?)`, 1, "one"); err != nil {
		t.Fatal(err)
	}
	var title string
	if err := tx.QueryRow(`SELECT title FROM docs WHERE id=1`).Scan(&title); err != nil {
		t.Fatal(err)
	}
	if title != "one" {
		t.Fatal(title)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal(count)
	}
	if err := tx.Commit(); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("second commit = %v", err)
	}
}

func TestTxRollback(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY)`)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES(?)`, 1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	// database/sql short-circuits the second rollback itself.
	if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("second rollback = %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal(count)
	}
}

func TestTxStatementErrorKeepsTxAlive(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY)`)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO missing_table_xyz VALUES(?)`, 1); err == nil {
		t.Fatal("expected statement error")
	}
	// Transaction must still be usable after the failed statement.
	if _, err := tx.Exec(`INSERT INTO docs VALUES(?)`, 7); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal(count)
	}
}

func TestSavepointsRejected(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY)`)
	for _, stmt := range []string{
		`SAVEPOINT sp1`,
		`RELEASE sp1`,
		`ROLLBACK TO SAVEPOINT sp1`,
		`  -- leading comment
		SAVEPOINT sp2`,
	} {
		if _, err := db.Exec(stmt); !errors.Is(err, ErrSavepointsUnsupported) {
			t.Fatalf("%q: got %v", stmt, err)
		}
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SAVEPOINT sp1`); !errors.Is(err, ErrSavepointsUnsupported) {
		t.Fatalf("tx savepoint: got %v", err)
	}
	// The transaction itself is unaffected by the rejected statements.
	if _, err := tx.Exec(`INSERT INTO docs VALUES(?)`, 1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestRawTxControlRejected(t *testing.T) {
	db, _ := openSQL(t)
	for _, stmt := range []string{`BEGIN`, `COMMIT`, `ROLLBACK`, `END`} {
		if _, err := db.Exec(stmt); !errors.Is(err, ErrTxControlSQL) {
			t.Fatalf("%q: got %v", stmt, err)
		}
	}
}

func TestDDLRouting(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT)`)
	mustExec(t, db, `CREATE INDEX idx_docs_title ON docs(title)`)
	mustExec(t, db, `ALTER TABLE docs ADD COLUMN n INTEGER`)
	mustExec(t, db, `INSERT INTO docs VALUES(?,?,?)`, 1, "a", 5)
	var n int64
	if err := db.QueryRow(`SELECT n FROM docs WHERE id=1`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatal(n)
	}
	// DDL through Query returns an empty result set.
	rows, err := db.Query(`DROP INDEX idx_docs_title`)
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("DDL query returned rows")
	}
	rows.Close()
	mustExec(t, db, `DROP TABLE docs`)
	if _, err := db.Exec(`SELECT COUNT(*) FROM docs`); err == nil {
		t.Fatal("dropped table still readable")
	}
}

func TestDDLErrors(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY)`)
	if _, err := db.Exec(`CREATE TABLE t2(id INTEGER)`, 1); err == nil {
		t.Fatal("expected DDL-with-args error")
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`CREATE TABLE t3(id INTEGER)`); !errors.Is(err, ErrDDLInTransaction) {
		t.Fatalf("DDL in tx: got %v", err)
	}
}

func mustTxExec(t *testing.T, tx *sql.Tx, query string, args ...any) {
	t.Helper()
	if _, err := tx.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func TestTxConcurrent(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY)`)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tx, err := db.Begin()
			if err != nil {
				t.Error(err)
				return
			}
			defer tx.Rollback()
			if _, err := tx.Exec(`INSERT INTO docs VALUES(?)`, id); err != nil {
				t.Error(err)
				return
			}
			if err := tx.Commit(); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 8 {
		t.Fatalf("count = %d", count)
	}
}

func TestTxContextCancel(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY)`)
	ctx, cancel := context.WithCancel(context.Background())
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO docs VALUES(?)`, 1); err != nil {
		t.Fatal(err)
	}
	cancel()
	// database/sql auto-rolls back on cancel (awaitDone): the pump is gone,
	// further statements fail, and rollback reports ErrTxDone.
	if _, err := tx.Exec(`INSERT INTO docs VALUES(?)`, 2); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("post-cancel statement = %v", err)
	}
	if err := tx.Rollback(); !errors.Is(err, sql.ErrTxDone) {
		t.Fatalf("rollback after cancel = %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("count = %d", count)
	}
}

func TestConcurrentAutocommit(t *testing.T) {
	db, _ := openSQL(t)
	mustExec(t, db, `CREATE TABLE docs(id INTEGER PRIMARY KEY)`)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if _, err := db.Exec(`INSERT INTO docs VALUES(?)`, id); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 16 {
		t.Fatalf("count = %d", count)
	}
}

func TestCloseReleasesClone(t *testing.T) {
	native := openNative(t)
	pool := OpenDB(native)
	mustExec(t, pool, `CREATE TABLE docs(id INTEGER PRIMARY KEY)`)
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	// The caller's database must still be fully usable.
	if _, err := native.Exec(`INSERT INTO docs VALUES(?)`, 1); err != nil {
		t.Fatalf("native handle dead after pool close: %v", err)
	}
	res, err := native.Query(`SELECT COUNT(*) FROM docs`)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows[0][0].Integer != 1 {
		t.Fatal(res)
	}
}
