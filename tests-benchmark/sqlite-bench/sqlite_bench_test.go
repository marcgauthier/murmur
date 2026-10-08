// In-memory SQLite baseline: mattn/go-sqlite3 (CGO) over two tables seeded
// with 100k rows each. Every phase runs against a fresh :memory: database
// with identical schema, pragmas, and data, covering bulk insert,
// row-by-row update, simple point/range queries, and join queries.
//
// Run with: cd tests-benchmark/sqlite-bench && go test . -run TestSQLiteMattnMemory -v -count=1
// Micro-benchmarks: cd tests-benchmark/sqlite-bench && go test . -bench . -benchtime 1s -run '^$'
package sqlitebench_test

import (
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const benchDriverName = "sqlite3"

func envInt(t *testing.T, key string, fallback int) int {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 1 {
		t.Fatalf("%s must be a positive integer, got %q", key, value)
	}
	return n
}

func openBenchDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(benchDriverName, ":memory:")
	if err != nil {
		t.Fatalf("mattn: open: %v", err)
	}
	// :memory: databases are per-connection; a single connection keeps
	// every phase on the same database.
	db.SetMaxOpenConns(1)
	// Same pragmas as the sqlengine in-memory setup: no durability cost,
	// memory journal.
	for _, pr := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA synchronous = OFF",
		"PRAGMA journal_mode = MEMORY",
		"PRAGMA foreign_keys = OFF",
	} {
		if _, err := db.Exec(pr); err != nil {
			db.Close()
			t.Fatalf("mattn: pragma %q: %v", pr, err)
		}
	}
	return db
}

const benchSchema = `
CREATE TABLE users (
	id    INTEGER PRIMARY KEY,
	name  TEXT NOT NULL,
	email TEXT NOT NULL,
	age   INTEGER NOT NULL
);
CREATE TABLE orders (
	id      INTEGER PRIMARY KEY,
	user_id INTEGER NOT NULL REFERENCES users(id),
	amount  REAL NOT NULL,
	note    TEXT NOT NULL
);
CREATE INDEX idx_orders_user ON orders(user_id);
`

func createBenchSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, stmt := range strings.Split(benchSchema, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("mattn: schema %q: %v", stmt, err)
		}
	}
}

// Deterministic seed data.
func userRow(i int) (id int, name, email string, age int) {
	return i, fmt.Sprintf("user-%06d", i), fmt.Sprintf("user-%06d@example.com", i), 18 + (i % 60)
}

func orderRow(i, users int) (id, userID int, amount float64, note string) {
	return i, (i-1)%users + 1, float64(i%1000)/10.0 + 0.5, fmt.Sprintf("order-%06d-note", i)
}

// lcgID spreads point-lookup ids pseudo-randomly but deterministically.
func lcgID(i, users int) int {
	return int((uint64(i)*1103515245+12345)%uint64(users)) + 1
}

type phaseResult struct {
	phase string
	ops   int
	took  time.Duration
}

func countRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("mattn: count %s: %v", table, err)
	}
	return n
}

func TestSQLiteMattnMemory(t *testing.T) {
	rows := envInt(t, "MURMUR_SQLITE_BENCH_ROWS", 100000)
	pointIters := envInt(t, "MURMUR_SQLITE_BENCH_POINT_ITERS", 20000)
	joinIters := envInt(t, "MURMUR_SQLITE_BENCH_JOIN_ITERS", 10000)
	scanIters := envInt(t, "MURMUR_SQLITE_BENCH_SCAN_ITERS", 10)
	joinScanIters := envInt(t, "MURMUR_SQLITE_BENCH_JOIN_SCAN_ITERS", 5)

	var results []phaseResult
	record := func(phase string, ops int, took time.Duration) {
		results = append(results, phaseResult{phase: phase, ops: ops, took: took})
	}

	db := openBenchDB(t)
	createBenchSchema(t, db)

	// Bulk insert: one transaction + prepared statement per table.
	start := time.Now()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare("INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= rows; i++ {
		id, name, email, age := userRow(i)
		if _, err := stmt.Exec(id, name, email, age); err != nil {
			t.Fatalf("mattn: insert users: %v", err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	record(fmt.Sprintf("insert users (%d rows, txn+prep)", rows), rows, time.Since(start))

	start = time.Now()
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err = tx.Prepare("INSERT INTO orders (id, user_id, amount, note) VALUES (?, ?, ?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= rows; i++ {
		id, userID, amount, note := orderRow(i, rows)
		if _, err := stmt.Exec(id, userID, amount, note); err != nil {
			t.Fatalf("mattn: insert orders: %v", err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	record(fmt.Sprintf("insert orders (%d rows, txn+prep)", rows), rows, time.Since(start))

	if got := countRows(t, db, "users"); got != rows {
		t.Fatalf("mattn: users count = %d, want %d", got, rows)
	}
	if got := countRows(t, db, "orders"); got != rows {
		t.Fatalf("mattn: orders count = %d, want %d", got, rows)
	}

	// Row-by-row update of every row through a prepared statement.
	start = time.Now()
	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	ustmt, err := tx.Prepare("UPDATE users SET age = age + 1 WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= rows; i++ {
		if _, err := ustmt.Exec(i); err != nil {
			t.Fatalf("mattn: update: %v", err)
		}
	}
	ustmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	record(fmt.Sprintf("update users age+1 (%d rows, txn+prep)", rows), rows, time.Since(start))

	// Spot-check the update landed (id=1 age incremented by 1).
	_, _, _, wantAge := userRow(1)
	wantAge++
	var age int
	if err := db.QueryRow("SELECT age FROM users WHERE id = 1").Scan(&age); err != nil || age != wantAge {
		t.Fatalf("mattn: update check id=1 age=%d err=%v, want %d", age, err, wantAge)
	}

	// Simple query: indexed point lookups.
	pstmt, err := db.Prepare("SELECT name, email, age FROM users WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	var name, email string
	var a int
	if err := pstmt.QueryRow(1).Scan(&name, &email, &a); err != nil || name != "user-000001" {
		t.Fatalf("mattn: point check: name=%q err=%v", name, err)
	}
	start = time.Now()
	for i := 0; i < pointIters; i++ {
		if err := pstmt.QueryRow(lcgID(i, rows)).Scan(&name, &email, &a); err != nil {
			t.Fatalf("mattn: point select: %v", err)
		}
	}
	record(fmt.Sprintf("point select by PK (%d lookups)", pointIters), pointIters, time.Since(start))
	pstmt.Close()

	// Simple query: full-table scan aggregate.
	start = time.Now()
	for i := 0; i < scanIters; i++ {
		var n int
		var avg float64
		if err := db.QueryRow("SELECT COUNT(*), AVG(age) FROM users").Scan(&n, &avg); err != nil {
			t.Fatalf("mattn: scan: %v", err)
		}
		if n != rows {
			t.Fatalf("mattn: scan count = %d, want %d", n, rows)
		}
	}
	record(fmt.Sprintf("full scan COUNT+AVG (%dx)", scanIters), scanIters, time.Since(start))

	// Join query: selective join by PK.
	jstmt, err := db.Prepare(`SELECT u.name, o.amount FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	for i := 0; i < joinIters; i++ {
		var nm string
		var amt float64
		if err := jstmt.QueryRow(lcgID(i, rows)).Scan(&nm, &amt); err != nil {
			t.Fatalf("mattn: join point: %v", err)
		}
	}
	record(fmt.Sprintf("join point lookup (%d lookups)", joinIters), joinIters, time.Since(start))
	jstmt.Close()

	// Join query: full join aggregate (every user has exactly one order).
	start = time.Now()
	for i := 0; i < joinScanIters; i++ {
		var n int
		var sum sql.NullFloat64
		if err := db.QueryRow(`SELECT COUNT(*), SUM(o.amount) FROM users u JOIN orders o ON o.user_id = u.id`).Scan(&n, &sum); err != nil {
			t.Fatalf("mattn: join scan: %v", err)
		}
		if n != rows {
			t.Fatalf("mattn: join count = %d, want %d", n, rows)
		}
	}
	record(fmt.Sprintf("join COUNT+SUM full (%dx)", joinScanIters), joinScanIters, time.Since(start))

	db.Close()

	// Report: per-phase table with ops/sec.
	t.Logf("SQLite :memory: mattn rows=%d pointIters=%d joinIters=%d go=%s cpus=%d",
		rows, pointIters, joinIters, runtime.Version(), runtime.NumCPU())
	width := 42
	header := fmt.Sprintf("%-*s | %-20s", width, "phase", "mattn")
	t.Log(header)
	t.Log(strings.Repeat("-", len(header)))
	for _, r := range results {
		t.Logf("%-*s | %8.2fs %9.0f/s", width, r.phase, r.took.Seconds(), float64(r.ops)/r.took.Seconds())
	}
}

// benchSeed opens a :memory: database and seeds both tables
// with rows rows each. Callers must close the returned DB.
func benchSeed(b *testing.B, rows int) *sql.DB {
	b.Helper()
	db, err := sql.Open(benchDriverName, ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	for _, pr := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA synchronous = OFF",
		"PRAGMA journal_mode = MEMORY",
		"PRAGMA foreign_keys = OFF",
	} {
		if _, err := db.Exec(pr); err != nil {
			db.Close()
			b.Fatal(err)
		}
	}
	for _, stmt := range strings.Split(benchSchema, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			b.Fatal(err)
		}
	}
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	ustmt, err := tx.Prepare("INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= rows; i++ {
		id, name, email, age := userRow(i)
		if _, err := ustmt.Exec(id, name, email, age); err != nil {
			b.Fatal(err)
		}
	}
	ustmt.Close()
	ostmt, err := tx.Prepare("INSERT INTO orders (id, user_id, amount, note) VALUES (?, ?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= rows; i++ {
		id, userID, amount, note := orderRow(i, rows)
		if _, err := ostmt.Exec(id, userID, amount, note); err != nil {
			b.Fatal(err)
		}
	}
	ostmt.Close()
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	return db
}

func benchRows() int {
	if v := os.Getenv("MURMUR_SQLITE_BENCH_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 100000
}

// Single-row autocommit inserts (fresh table, measures per-insert latency).
func BenchmarkInsert(b *testing.B) {
	db, err := sql.Open(benchDriverName, ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	db.SetMaxOpenConns(1)
	db.Exec("PRAGMA synchronous = OFF")
	db.Exec("PRAGMA journal_mode = MEMORY")
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, v REAL)"); err != nil {
		b.Fatal(err)
	}
	stmt, err := db.Prepare("INSERT INTO t (id, name, v) VALUES (?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { stmt.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := stmt.Exec(i, fmt.Sprintf("name-%d", i), float64(i)*1.5); err != nil {
			b.Fatal(err)
		}
	}
}

// Single-row autocommit updates over a seeded table.
func BenchmarkUpdate(b *testing.B) {
	rows := benchRows()
	db := benchSeed(b, rows)
	b.Cleanup(func() { db.Close() })
	stmt, err := db.Prepare("UPDATE users SET age = age + 1 WHERE id = ?")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { stmt.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := stmt.Exec(i%rows + 1); err != nil {
			b.Fatal(err)
		}
	}
}

// Indexed point selects over a seeded table.
func BenchmarkPointSelect(b *testing.B) {
	rows := benchRows()
	db := benchSeed(b, rows)
	b.Cleanup(func() { db.Close() })
	stmt, err := db.Prepare("SELECT name, email, age FROM users WHERE id = ?")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { stmt.Close() })
	var name, email string
	var age int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := stmt.QueryRow(lcgID(i, rows)).Scan(&name, &email, &age); err != nil {
			b.Fatal(err)
		}
	}
}

// Selective join by PK over seeded tables.
func BenchmarkJoinPoint(b *testing.B) {
	rows := benchRows()
	db := benchSeed(b, rows)
	b.Cleanup(func() { db.Close() })
	stmt, err := db.Prepare(`SELECT u.name, o.amount FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = ?`)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { stmt.Close() })
	var name string
	var amount float64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := stmt.QueryRow(lcgID(i, rows)).Scan(&name, &amount); err != nil {
			b.Fatal(err)
		}
	}
}

// Full join aggregate over seeded tables.
func BenchmarkJoinAggregate(b *testing.B) {
	rows := benchRows()
	db := benchSeed(b, rows)
	b.Cleanup(func() { db.Close() })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var n int
		var sum sql.NullFloat64
		if err := db.QueryRow(`SELECT COUNT(*), SUM(o.amount) FROM users u JOIN orders o ON o.user_id = u.id`).Scan(&n, &sum); err != nil {
			b.Fatal(err)
		}
	}
}
