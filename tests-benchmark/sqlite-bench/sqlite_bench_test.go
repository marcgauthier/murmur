// In-memory SQLite driver comparison: mattn/go-sqlite3 (CGO) versus
// modernc.org/sqlite (pure Go) over two tables seeded with 100k rows
// each. Every phase runs against a fresh :memory: database per driver
// with identical schema, pragmas, and data, covering bulk insert,
// row-by-row update, simple point/range queries, and join queries.
//
// Run with: go test ./tests-benchmark/sqlite-bench/ -run TestSQLiteDriverComparison -v -count=1
// Micro-benchmarks: go test ./tests-benchmark/sqlite-bench/ -bench . -benchtime 1s -run '^$'
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

	_ "modernc.org/sqlite"
)

type driverDef struct {
	label  string // report label
	name   string // database/sql driver name
	memory string // :memory: DSN for this driver
}

var drivers = []driverDef{
	{label: "mattn", name: "sqlite3", memory: ":memory:"},
	{label: "modernc", name: "sqlite", memory: ":memory:"},
}

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

func driverRegistered(name string) bool {
	for _, d := range sql.Drivers() {
		if d == name {
			return true
		}
	}
	return false
}

func openBenchDB(t *testing.T, d driverDef) *sql.DB {
	t.Helper()
	db, err := sql.Open(d.name, d.memory)
	if err != nil {
		t.Fatalf("%s: open: %v", d.label, err)
	}
	// :memory: databases are per-connection; a single connection keeps
	// every phase on the same database for both drivers.
	db.SetMaxOpenConns(1)
	// Same pragmas for both drivers so the comparison is fair (and matches
	// the sqlengine in-memory setup: no durability cost, memory journal).
	for _, pr := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA synchronous = OFF",
		"PRAGMA journal_mode = MEMORY",
		"PRAGMA foreign_keys = OFF",
	} {
		if _, err := db.Exec(pr); err != nil {
			db.Close()
			t.Fatalf("%s: pragma %q: %v", d.label, pr, err)
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

func createBenchSchema(t *testing.T, db *sql.DB, label string) {
	t.Helper()
	for _, stmt := range strings.Split(benchSchema, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s: schema %q: %v", label, stmt, err)
		}
	}
}

// Deterministic seed data shared by both drivers.
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
	byDrv map[string]time.Duration
}

func (r *phaseResult) set(label string, d time.Duration) {
	if r.byDrv == nil {
		r.byDrv = map[string]time.Duration{}
	}
	r.byDrv[label] = d
}

func countRows(t *testing.T, db *sql.DB, label, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("%s: count %s: %v", label, table, err)
	}
	return n
}

func TestSQLiteDriverComparison(t *testing.T) {
	rows := envInt(t, "SPEDSQL_SQLITE_BENCH_ROWS", 100000)
	pointIters := envInt(t, "SPEDSQL_SQLITE_BENCH_POINT_ITERS", 20000)
	joinIters := envInt(t, "SPEDSQL_SQLITE_BENCH_JOIN_ITERS", 10000)
	scanIters := envInt(t, "SPEDSQL_SQLITE_BENCH_SCAN_ITERS", 10)
	joinScanIters := envInt(t, "SPEDSQL_SQLITE_BENCH_JOIN_SCAN_ITERS", 5)

	var active []driverDef
	for _, d := range drivers {
		if !driverRegistered(d.name) {
			t.Logf("%s: driver %q not registered (CGO disabled?), skipping", d.label, d.name)
			continue
		}
		active = append(active, d)
	}
	if len(active) == 0 {
		t.Fatal("no SQLite drivers registered")
	}

	var results []*phaseResult
	record := func(phase string, ops int) *phaseResult {
		r := &phaseResult{phase: phase, ops: ops}
		results = append(results, r)
		return r
	}
	rInsertUsers := record(fmt.Sprintf("insert users (%d rows, txn+prep)", rows), rows)
	rInsertOrders := record(fmt.Sprintf("insert orders (%d rows, txn+prep)", rows), rows)
	rUpdate := record(fmt.Sprintf("update users age+1 (%d rows, txn+prep)", rows), rows)
	rPoint := record(fmt.Sprintf("point select by PK (%d lookups)", pointIters), pointIters)
	rScan := record(fmt.Sprintf("full scan COUNT+AVG (%dx)", scanIters), scanIters)
	rJoinPoint := record(fmt.Sprintf("join point lookup (%d lookups)", joinIters), joinIters)
	rJoinScan := record(fmt.Sprintf("join COUNT+SUM full (%dx)", joinScanIters), joinScanIters)

	for _, d := range active {
		db := openBenchDB(t, d)
		createBenchSchema(t, db, d.label)

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
				t.Fatalf("%s: insert users: %v", d.label, err)
			}
		}
		stmt.Close()
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		rInsertUsers.set(d.label, time.Since(start))

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
				t.Fatalf("%s: insert orders: %v", d.label, err)
			}
		}
		stmt.Close()
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		rInsertOrders.set(d.label, time.Since(start))

		if got := countRows(t, db, d.label, "users"); got != rows {
			t.Fatalf("%s: users count = %d, want %d", d.label, got, rows)
		}
		if got := countRows(t, db, d.label, "orders"); got != rows {
			t.Fatalf("%s: orders count = %d, want %d", d.label, got, rows)
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
				t.Fatalf("%s: update: %v", d.label, err)
			}
		}
		ustmt.Close()
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		rUpdate.set(d.label, time.Since(start))

		// Spot-check the update landed (id=1 age incremented by 1).
		_, _, _, wantAge := userRow(1)
		wantAge++
		var age int
		if err := db.QueryRow("SELECT age FROM users WHERE id = 1").Scan(&age); err != nil || age != wantAge {
			t.Fatalf("%s: update check id=1 age=%d err=%v, want %d", d.label, age, err, wantAge)
		}

		// Simple query: indexed point lookups.
		pstmt, err := db.Prepare("SELECT name, email, age FROM users WHERE id = ?")
		if err != nil {
			t.Fatal(err)
		}
		var name, email string
		var a int
		if err := pstmt.QueryRow(1).Scan(&name, &email, &a); err != nil || name != "user-000001" {
			t.Fatalf("%s: point check: name=%q err=%v", d.label, name, err)
		}
		start = time.Now()
		for i := 0; i < pointIters; i++ {
			if err := pstmt.QueryRow(lcgID(i, rows)).Scan(&name, &email, &a); err != nil {
				t.Fatalf("%s: point select: %v", d.label, err)
			}
		}
		rPoint.set(d.label, time.Since(start))
		pstmt.Close()

		// Simple query: full-table scan aggregate.
		start = time.Now()
		for i := 0; i < scanIters; i++ {
			var n int
			var avg float64
			if err := db.QueryRow("SELECT COUNT(*), AVG(age) FROM users").Scan(&n, &avg); err != nil {
				t.Fatalf("%s: scan: %v", d.label, err)
			}
			if n != rows {
				t.Fatalf("%s: scan count = %d, want %d", d.label, n, rows)
			}
		}
		rScan.set(d.label, time.Since(start))

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
				t.Fatalf("%s: join point: %v", d.label, err)
			}
		}
		rJoinPoint.set(d.label, time.Since(start))
		jstmt.Close()

		// Join query: full join aggregate (every user has exactly one order).
		start = time.Now()
		for i := 0; i < joinScanIters; i++ {
			var n int
			var sum sql.NullFloat64
			if err := db.QueryRow(`SELECT COUNT(*), SUM(o.amount) FROM users u JOIN orders o ON o.user_id = u.id`).Scan(&n, &sum); err != nil {
				t.Fatalf("%s: join scan: %v", d.label, err)
			}
			if n != rows {
				t.Fatalf("%s: join count = %d, want %d", d.label, n, rows)
			}
		}
		rJoinScan.set(d.label, time.Since(start))

		db.Close()
	}

	// Report: per-phase table with ops/sec and modernc/mattn ratio.
	t.Logf("SQLite :memory: comparison rows=%d pointIters=%d joinIters=%d go=%s cpus=%d",
		rows, pointIters, joinIters, runtime.Version(), runtime.NumCPU())
	width := 42
	header := fmt.Sprintf("%-*s", width, "phase")
	for _, d := range active {
		header += fmt.Sprintf(" | %-20s", d.label)
	}
	if len(active) == 2 {
		header += " | modernc/mattn"
	}
	t.Log(header)
	t.Log(strings.Repeat("-", len(header)))
	formatCell := func(r *phaseResult, label string) string {
		d, ok := r.byDrv[label]
		if !ok {
			return "skipped"
		}
		return fmt.Sprintf("%8.2fs %9.0f/s", d.Seconds(), float64(r.ops)/d.Seconds())
	}
	for _, r := range results {
		line := fmt.Sprintf("%-*s", width, r.phase)
		for _, d := range active {
			line += " | " + formatCell(r, d.label)
		}
		if len(active) == 2 {
			a, b := r.byDrv[active[0].label], r.byDrv[active[1].label]
			line += fmt.Sprintf(" | %.2fx", float64(b)/float64(a))
		}
		t.Log(line)
	}
}

// benchSeed opens a :memory: database for driver d and seeds both tables
// with rows rows each. Callers must close the returned DB.
func benchSeed(b *testing.B, d driverDef, rows int) *sql.DB {
	b.Helper()
	db, err := sql.Open(d.name, d.memory)
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
	if v := os.Getenv("SPEDSQL_SQLITE_BENCH_ROWS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 100000
}

func runPerDriver(b *testing.B, fn func(b *testing.B, d driverDef)) {
	b.Helper()
	for _, d := range drivers {
		if !driverRegistered(d.name) {
			b.Run(d.label, func(b *testing.B) { b.Skipf("driver %q not registered", d.name) })
			continue
		}
		b.Run(d.label, func(b *testing.B) { fn(b, d) })
	}
}

// Single-row autocommit inserts (fresh table, measures per-insert latency).
func BenchmarkInsert(b *testing.B) {
	runPerDriver(b, func(b *testing.B, d driverDef) {
		db, err := sql.Open(d.name, d.memory)
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
	})
}

// Single-row autocommit updates over a seeded table.
func BenchmarkUpdate(b *testing.B) {
	rows := benchRows()
	runPerDriver(b, func(b *testing.B, d driverDef) {
		db := benchSeed(b, d, rows)
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
	})
}

// Indexed point selects over a seeded table.
func BenchmarkPointSelect(b *testing.B) {
	rows := benchRows()
	runPerDriver(b, func(b *testing.B, d driverDef) {
		db := benchSeed(b, d, rows)
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
	})
}

// Selective join by PK over seeded tables.
func BenchmarkJoinPoint(b *testing.B) {
	rows := benchRows()
	runPerDriver(b, func(b *testing.B, d driverDef) {
		db := benchSeed(b, d, rows)
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
	})
}

// Full join aggregate over seeded tables.
func BenchmarkJoinAggregate(b *testing.B) {
	rows := benchRows()
	runPerDriver(b, func(b *testing.B, d driverDef) {
		db := benchSeed(b, d, rows)
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
	})
}
