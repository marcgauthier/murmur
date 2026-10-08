package rime_test

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/marcgauthier/murmur/rime"
)

// Rime and SQLite entities for benchmarking.
type BenchUser struct {
	ID    int    `rime:"primary"`
	Name  string `rime:"index"`
	Email string `rime:"index"`
	Age   int    `rime:"ordered"`
}

type BenchOrder struct {
	ID     int     `rime:"primary"`
	UserID int     `rime:"index"`
	Amount float64 `rime:"ordered"`
	Note   string
}

const sqliteBenchDriver = "sqlite3"

const sqliteBenchSchema = `
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

func openSQLiteBenchDB(t testing.TB) *sql.DB {
	return openSQLiteBenchDSN(t, ":memory:", 1)
}

func openSQLiteBenchDSN(t testing.TB, dsn string, connections int) *sql.DB {
	t.Helper()
	db, err := sql.Open(sqliteBenchDriver, dsn)
	if err != nil {
		t.Fatalf("sqlite open: %v", err)
	}
	db.SetMaxOpenConns(connections)
	db.SetMaxIdleConns(connections)
	for _, pr := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA synchronous = OFF",
		"PRAGMA journal_mode = MEMORY",
		"PRAGMA foreign_keys = OFF",
	} {
		if _, err := db.Exec(pr); err != nil {
			db.Close()
			t.Fatalf("sqlite pragma %q: %v", pr, err)
		}
	}
	for _, stmt := range strings.Split(sqliteBenchSchema, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			t.Fatalf("sqlite exec schema %q: %v", stmt, err)
		}
	}
	if os.Getenv("RIME_BENCH_MATCH_INDEXES") == "1" {
		for _, stmt := range []string{
			"CREATE INDEX idx_users_name ON users(name)",
			"CREATE INDEX idx_users_email ON users(email)",
			"CREATE INDEX idx_users_age ON users(age)",
			"CREATE INDEX idx_orders_amount ON orders(amount)",
		} {
			if _, err := db.Exec(stmt); err != nil {
				db.Close()
				t.Fatalf("sqlite matching indexes: %v", err)
			}
		}
	}
	return db
}

func openRimeBenchDB(t testing.TB) (*rime.DB, *rime.Table[BenchUser], *rime.Table[BenchOrder]) {
	t.Helper()
	db := rime.New()
	users, err := rime.Register[BenchUser](db, rime.WithTableName[BenchUser]("users"))
	if err != nil {
		t.Fatalf("rime register users: %v", err)
	}
	orders, err := rime.Register[BenchOrder](db, rime.WithTableName[BenchOrder]("orders"))
	if err != nil {
		t.Fatalf("rime register orders: %v", err)
	}
	return db, users, orders
}

func benchUserRow(i int) (id int, name, email string, age int) {
	return i, fmt.Sprintf("user-%06d", i), fmt.Sprintf("user-%06d@example.com", i), 18 + (i % 60)
}

func benchOrderRow(i, totalUsers int) (id, userID int, amount float64, note string) {
	return i, (i-1)%totalUsers + 1, float64(i%1000)/10.0 + 0.5, fmt.Sprintf("order-%06d-note", i)
}

func benchLCGID(i, totalUsers int) int {
	return int((uint64(i)*1103515245+12345)%uint64(totalUsers)) + 1
}

func getBenchEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

type benchPhaseMetric struct {
	phase     string
	ops       int
	sqliteDur time.Duration
	rimeDur   time.Duration
}

// TestCompareRimeVsSQLiteMemory performs a direct, side-by-side run of
// Rime vs SQLite :memory: across insert, update, query, join, and delete.
func TestCompareRimeVsSQLiteMemory(t *testing.T) {
	rows := getBenchEnvInt("RIME_BENCH_ROWS", 100000)
	pointIters := getBenchEnvInt("RIME_BENCH_POINT_ITERS", 20000)
	joinIters := getBenchEnvInt("RIME_BENCH_JOIN_ITERS", 10000)
	scanIters := 5
	deleteIters := min(10000, rows)

	metrics := make([]benchPhaseMetric, 0, 10)

	record := func(phase string, ops int, sqDur, rimeDur time.Duration) {
		metrics = append(metrics, benchPhaseMetric{
			phase:     phase,
			ops:       ops,
			sqliteDur: sqDur,
			rimeDur:   rimeDur,
		})
	}

	// -------------------------------------------------------------
	// 1. Setup SQLite
	// -------------------------------------------------------------
	sqDB := openSQLiteBenchDB(t)
	defer sqDB.Close()

	// -------------------------------------------------------------
	// 2. Setup Rime
	// -------------------------------------------------------------
	rDB, rUsers, rOrders := openRimeBenchDB(t)
	defer rDB.Close()

	// -------------------------------------------------------------
	// Phase 1: Bulk Insert Users
	// -------------------------------------------------------------
	uList := make([]*BenchUser, rows)
	for i := 1; i <= rows; i++ {
		id, name, email, age := benchUserRow(i)
		uList[i-1] = &BenchUser{ID: id, Name: name, Email: email, Age: age}
	}

	// SQLite Insert Users (in transaction with prepared statement)
	start := time.Now()
	tx, err := sqDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	ustmt, err := tx.Prepare("INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	for _, u := range uList {
		if _, err := ustmt.Exec(u.ID, u.Name, u.Email, u.Age); err != nil {
			t.Fatalf("sqlite insert user: %v", err)
		}
	}
	ustmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	sqInsertUsersTook := time.Since(start)

	// Rime Insert Users (via SaveMany)
	start = time.Now()
	if err := rUsers.UpsertMany(uList); err != nil {
		t.Fatalf("rime insert users: %v", err)
	}
	rimeInsertUsersTook := time.Since(start)
	record(fmt.Sprintf("bulk insert users (%d rows)", rows), rows, sqInsertUsersTook, rimeInsertUsersTook)

	// -------------------------------------------------------------
	// Phase 2: Bulk Insert Orders
	// -------------------------------------------------------------
	oList := make([]*BenchOrder, rows)
	for i := 1; i <= rows; i++ {
		id, userID, amount, note := benchOrderRow(i, rows)
		oList[i-1] = &BenchOrder{ID: id, UserID: userID, Amount: amount, Note: note}
	}

	// SQLite Insert Orders
	start = time.Now()
	tx, err = sqDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	ostmt, err := tx.Prepare("INSERT INTO orders (id, user_id, amount, note) VALUES (?, ?, ?, ?)")
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range oList {
		if _, err := ostmt.Exec(o.ID, o.UserID, o.Amount, o.Note); err != nil {
			t.Fatalf("sqlite insert order: %v", err)
		}
	}
	ostmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	sqInsertOrdersTook := time.Since(start)

	// Rime Insert Orders
	start = time.Now()
	if err := rOrders.UpsertMany(oList); err != nil {
		t.Fatalf("rime insert orders: %v", err)
	}
	rimeInsertOrdersTook := time.Since(start)
	record(fmt.Sprintf("bulk insert orders (%d rows)", rows), rows, sqInsertOrdersTook, rimeInsertOrdersTook)

	// -------------------------------------------------------------
	// Phase 3: Update Users (increment age by 1)
	// -------------------------------------------------------------
	// SQLite Update
	start = time.Now()
	tx, err = sqDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	updStmt, err := tx.Prepare("UPDATE users SET age = age + 1 WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= rows; i++ {
		if _, err := updStmt.Exec(i); err != nil {
			t.Fatalf("sqlite update: %v", err)
		}
	}
	updStmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	sqUpdateTook := time.Since(start)

	// Rime Update (in WriteTx)
	start = time.Now()
	err = rDB.WriteTx(func(tx *rime.Tx) error {
		for i := 1; i <= rows; i++ {
			if err := rUsers.In(tx).Update(i, func(u *BenchUser) error {
				u.Age++
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("rime update: %v", err)
	}
	rimeUpdateTook := time.Since(start)
	record(fmt.Sprintf("update users age+1 (%d rows)", rows), rows, sqUpdateTook, rimeUpdateTook)

	// -------------------------------------------------------------
	// Phase 4: Point Query by Primary Key
	// -------------------------------------------------------------
	// SQLite Point Query
	start = time.Now()
	pstmt, err := sqDB.Prepare("SELECT name, email, age FROM users WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	var sqName, sqEmail string
	var sqAge int
	for i := 0; i < pointIters; i++ {
		id := benchLCGID(i, rows)
		if err := pstmt.QueryRow(id).Scan(&sqName, &sqEmail, &sqAge); err != nil {
			t.Fatalf("sqlite point query: %v", err)
		}
	}
	pstmt.Close()
	sqPointTook := time.Since(start)

	// Rime Point Query
	start = time.Now()
	for i := 0; i < pointIters; i++ {
		id := benchLCGID(i, rows)
		u, err := rUsers.Get(id)
		if err != nil || u == nil {
			t.Fatalf("rime point query id=%d: %v", id, err)
		}
	}
	rimePointTook := time.Since(start)
	record(fmt.Sprintf("point select by PK (%d lookups)", pointIters), pointIters, sqPointTook, rimePointTook)

	// -------------------------------------------------------------
	// Phase 5: Full Table Scan with Aggregate (COUNT + AVG(age))
	// -------------------------------------------------------------
	// SQLite Scan
	start = time.Now()
	for i := 0; i < scanIters; i++ {
		var cnt int
		var avg float64
		if err := sqDB.QueryRow("SELECT COUNT(*), AVG(age) FROM users").Scan(&cnt, &avg); err != nil {
			t.Fatalf("sqlite scan: %v", err)
		}
		if cnt != rows {
			t.Fatalf("sqlite scan count=%d, want %d", cnt, rows)
		}
	}
	sqScanTook := time.Since(start)

	// Rime Scan
	ageField := rime.OF[BenchUser, int](rUsers, "Age")
	start = time.Now()
	for i := 0; i < scanIters; i++ {
		res, err := rUsers.Where().Aggregate(rime.Count[BenchUser](), rime.AvgOf(ageField))
		if err != nil {
			t.Fatalf("rime scan: %v", err)
		}
		if res[0].(int) != rows {
			t.Fatalf("rime scan count=%v, want %d", res[0], rows)
		}
	}
	rimeScanTook := time.Since(start)
	record(fmt.Sprintf("full scan COUNT+AVG (%dx)", scanIters), scanIters, sqScanTook, rimeScanTook)

	// Consume all rows and columns separately from aggregate execution.
	sqFullScan, rimeFullScan, scannedRows := compareFullTableScan(t, sqDB, rUsers, uList, 1)
	record("full table scan (all columns)", scannedRows, sqFullScan, rimeFullScan)

	// -------------------------------------------------------------
	// Phase 6: Simple Join Point Lookup
	// -------------------------------------------------------------
	// SQLite Join Point
	start = time.Now()
	jstmt, err := sqDB.Prepare(`SELECT u.name, o.amount FROM users u JOIN orders o ON o.user_id = u.id WHERE u.id = ?`)
	if err != nil {
		t.Fatal(err)
	}
	var jName string
	var jAmt float64
	for i := 0; i < joinIters; i++ {
		id := benchLCGID(i, rows)
		if err := jstmt.QueryRow(id).Scan(&jName, &jAmt); err != nil {
			t.Fatalf("sqlite join point: %v", err)
		}
	}
	jstmt.Close()
	sqJoinPointTook := time.Since(start)

	// Rime Join Point
	orderUserField := rime.F[BenchOrder, int](rOrders, "UserID")
	start = time.Now()
	for i := 0; i < joinIters; i++ {
		id := benchLCGID(i, rows)
		u, err := rUsers.Get(id)
		if err != nil || u == nil {
			t.Fatalf("rime join point user lookup id=%d: %v", id, err)
		}
		ords, err := rOrders.Where(orderUserField.Eq(id)).Limit(1).Find()
		if err != nil || len(ords) == 0 {
			t.Fatalf("rime join point order lookup user=%d: %v", id, err)
		}
	}
	rimeJoinPointTook := time.Since(start)
	record(fmt.Sprintf("join point lookup (%d lookups)", joinIters), joinIters, sqJoinPointTook, rimeJoinPointTook)

	// -------------------------------------------------------------
	// Phase 7: Full Join Aggregate (COUNT(*) + SUM(o.amount))
	// -------------------------------------------------------------
	// SQLite Full Join
	start = time.Now()
	for i := 0; i < 3; i++ {
		var cnt int
		var sum float64
		if err := sqDB.QueryRow(`SELECT COUNT(*), SUM(o.amount) FROM users u JOIN orders o ON o.user_id = u.id`).Scan(&cnt, &sum); err != nil {
			t.Fatalf("sqlite full join: %v", err)
		}
		if cnt != rows {
			t.Fatalf("sqlite full join count=%d, want %d", cnt, rows)
		}
	}
	sqJoinFullTook := time.Since(start)

	// Rime Full Join via InnerJoinOn
	userIDField := rime.F[BenchUser, int](rUsers, "ID")
	start = time.Now()
	for i := 0; i < 3; i++ {
		joined, err := rime.InnerJoinOn(rUsers.In(nil), userIDField, rOrders.In(nil), orderUserField)
		if err != nil {
			t.Fatalf("rime full join: %v", err)
		}
		if len(joined) != rows {
			t.Fatalf("rime full join len=%d, want %d", len(joined), rows)
		}
	}
	rimeJoinFullTook := time.Since(start)
	record("join full InnerJoinOn (3x)", 3, sqJoinFullTook, rimeJoinFullTook)

	// -------------------------------------------------------------
	// Phase 8: Point Delete by PK
	// -------------------------------------------------------------
	// SQLite Point Delete
	start = time.Now()
	dstmt, err := sqDB.Prepare("DELETE FROM orders WHERE id = ?")
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= deleteIters; i++ {
		if _, err := dstmt.Exec(i); err != nil {
			t.Fatalf("sqlite point delete: %v", err)
		}
	}
	dstmt.Close()
	sqPointDelTook := time.Since(start)

	// Rime Point Delete
	start = time.Now()
	for i := 1; i <= deleteIters; i++ {
		if err := rOrders.Delete(i); err != nil {
			t.Fatalf("rime point delete: %v", err)
		}
	}
	rimePointDelTook := time.Since(start)
	record(fmt.Sprintf("point delete (%d rows)", deleteIters), deleteIters, sqPointDelTook, rimePointDelTook)

	// -------------------------------------------------------------
	// Phase 9: Bulk Delete Remaining Orders & Users
	// -------------------------------------------------------------
	// SQLite Bulk Delete
	start = time.Now()
	if _, err := sqDB.Exec("DELETE FROM orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqDB.Exec("DELETE FROM users"); err != nil {
		t.Fatal(err)
	}
	sqBulkDelTook := time.Since(start)

	// Rime Bulk Delete
	start = time.Now()
	delOrderKeys := make([]any, rows-deleteIters)
	for i := deleteIters + 1; i <= rows; i++ {
		delOrderKeys[i-(deleteIters+1)] = i
	}
	delUserKeys := make([]any, rows)
	for i := 1; i <= rows; i++ {
		delUserKeys[i-1] = i
	}
	if err := rOrders.DeleteMany(delOrderKeys); err != nil {
		t.Fatalf("rime bulk delete orders: %v", err)
	}
	if err := rUsers.DeleteMany(delUserKeys); err != nil {
		t.Fatalf("rime bulk delete users: %v", err)
	}
	rimeBulkDelTook := time.Since(start)
	record("bulk delete remaining tables", rows*2-deleteIters, sqBulkDelTook, rimeBulkDelTook)

	// -------------------------------------------------------------
	// Output Comparative Report
	// -------------------------------------------------------------
	wPhase := 38
	wDur := 10
	wRate := 14
	wSpeedup := 10

	t.Logf("\n=== Comparative Benchmark: Rime vs SQLite :memory: (rows=%d, CPUs=%d, Go=%s) ===",
		rows, runtime.NumCPU(), runtime.Version())
	t.Logf("Matching secondary indexes: %t (set RIME_BENCH_MATCH_INDEXES=1); full join and bulk delete use different operations", os.Getenv("RIME_BENCH_MATCH_INDEXES") == "1")

	hdr := fmt.Sprintf("%-*s | %-*s %-*s | %-*s %-*s | %-*s",
		wPhase, "Phase",
		wDur, "SQLite (s)", wRate, "SQLite (ops/s)",
		wDur, "Rime (s)", wRate, "Rime (ops/s)",
		wSpeedup, "Speedup")
	div := strings.Repeat("-", len(hdr))

	t.Log(hdr)
	t.Log(div)

	for _, m := range metrics {
		sqRate := float64(m.ops) / m.sqliteDur.Seconds()
		rRate := float64(m.ops) / m.rimeDur.Seconds()
		speedup := rRate / sqRate
		speedupStr := fmt.Sprintf("%.2fx", speedup)
		if speedup >= 1.0 {
			speedupStr = fmt.Sprintf("+%.2fx", speedup)
		}

		t.Logf("%-*s | %*.3fs %*.0f/s | %*.3fs %*.0f/s | %*s",
			wPhase, m.phase,
			wDur-1, m.sqliteDur.Seconds(), wRate-2, sqRate,
			wDur-1, m.rimeDur.Seconds(), wRate-2, rRate,
			wSpeedup, speedupStr)
	}
	t.Log(div)
}

// ---------------------------------------------------------------------
// Standard Micro-Benchmarks (go test -bench BenchmarkCompare_)
// ---------------------------------------------------------------------

func BenchmarkCompare_BulkInsert_SQLite(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := openSQLiteBenchDB(b)
		uList := make([]*BenchUser, 10000)
		for j := 1; j <= 10000; j++ {
			id, name, email, age := benchUserRow(j)
			uList[j-1] = &BenchUser{ID: id, Name: name, Email: email, Age: age}
		}
		b.StartTimer()

		tx, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		stmt, err := tx.Prepare("INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)")
		if err != nil {
			b.Fatal(err)
		}
		for _, u := range uList {
			if _, err := stmt.Exec(u.ID, u.Name, u.Email, u.Age); err != nil {
				b.Fatal(err)
			}
		}
		stmt.Close()
		tx.Commit()

		b.StopTimer()
		db.Close()
		b.StartTimer()
	}
}

func BenchmarkCompare_BulkInsert_Rime(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		rdb, rUsers, _ := openRimeBenchDB(b)
		uList := make([]*BenchUser, 10000)
		for j := 1; j <= 10000; j++ {
			id, name, email, age := benchUserRow(j)
			uList[j-1] = &BenchUser{ID: id, Name: name, Email: email, Age: age}
		}
		b.StartTimer()

		if err := rUsers.UpsertMany(uList); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		rdb.Close()
		b.StartTimer()
	}
}

func BenchmarkCompare_PointSelect_SQLite(b *testing.B) {
	db := openSQLiteBenchDB(b)
	defer db.Close()
	tx, err := db.Begin()
	if err != nil {
		b.Fatal(err)
	}
	stmt, err := tx.Prepare("INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	const n = 50000
	for j := 1; j <= n; j++ {
		id, name, email, age := benchUserRow(j)
		if _, err := stmt.Exec(id, name, email, age); err != nil {
			b.Fatal(err)
		}
	}
	stmt.Close()
	tx.Commit()

	pstmt, err := db.Prepare("SELECT name, email, age FROM users WHERE id = ?")
	if err != nil {
		b.Fatal(err)
	}
	defer pstmt.Close()

	var name, email string
	var age int

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := benchLCGID(i, n)
		if err := pstmt.QueryRow(id).Scan(&name, &email, &age); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompare_PointSelect_Rime(b *testing.B) {
	rdb, rUsers, _ := openRimeBenchDB(b)
	defer rdb.Close()
	const n = 50000
	uList := make([]*BenchUser, n)
	for j := 1; j <= n; j++ {
		id, name, email, age := benchUserRow(j)
		uList[j-1] = &BenchUser{ID: id, Name: name, Email: email, Age: age}
	}
	if err := rUsers.UpsertMany(uList); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := benchLCGID(i, n)
		u, err := rUsers.Get(id)
		if err != nil || u == nil {
			b.Fatalf("rime get: %v", err)
		}
	}
}

func BenchmarkCompare_PointUpdate_SQLite(b *testing.B) {
	db := openSQLiteBenchDB(b)
	defer db.Close()
	const n = 20000
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)")
	for j := 1; j <= n; j++ {
		id, name, email, age := benchUserRow(j)
		stmt.Exec(id, name, email, age)
	}
	stmt.Close()
	tx.Commit()

	upd, err := db.Prepare("UPDATE users SET age = age + 1 WHERE id = ?")
	if err != nil {
		b.Fatal(err)
	}
	defer upd.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := benchLCGID(i, n)
		if _, err := upd.Exec(id); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompare_PointUpdate_Rime(b *testing.B) {
	rdb, rUsers, _ := openRimeBenchDB(b)
	defer rdb.Close()
	const n = 20000
	uList := make([]*BenchUser, n)
	for j := 1; j <= n; j++ {
		id, name, email, age := benchUserRow(j)
		uList[j-1] = &BenchUser{ID: id, Name: name, Email: email, Age: age}
	}
	rUsers.UpsertMany(uList)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := benchLCGID(i, n)
		err := rUsers.Update(id, func(u *BenchUser) error {
			u.Age++
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompare_PointDelete_SQLite(b *testing.B) {
	db := openSQLiteBenchDB(b)
	defer db.Close()
	const n = 50000
	tx, _ := db.Begin()
	stmt, _ := tx.Prepare("INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?)")
	for j := 1; j <= n; j++ {
		id, name, email, age := benchUserRow(j)
		stmt.Exec(id, name, email, age)
	}
	stmt.Close()
	tx.Commit()

	del, err := db.Prepare("DELETE FROM users WHERE id = ?")
	if err != nil {
		b.Fatal(err)
	}
	defer del.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := (i % n) + 1
		if _, err := del.Exec(id); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompare_PointDelete_Rime(b *testing.B) {
	rdb, rUsers, _ := openRimeBenchDB(b)
	defer rdb.Close()
	const n = 50000
	uList := make([]*BenchUser, n)
	for j := 1; j <= n; j++ {
		id, name, email, age := benchUserRow(j)
		uList[j-1] = &BenchUser{ID: id, Name: name, Email: email, Age: age}
	}
	rUsers.UpsertMany(uList)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := (i % n) + 1
		// SQLite treats deleting a missing row as a silent no-op; tolerate
		// the same case here so wrapped iterations measure the same mix.
		if err := rUsers.Delete(id); err != nil && !errors.Is(err, rime.ErrNotFound) {
			b.Fatal(err)
		}
	}
}
