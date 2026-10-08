package rime_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

// TestCompareRimeVsSQLiteConcurrent compares clients writing to ONE database.
// Both engines use identical rows, indexes, transaction sizes and key schedules.
// SQLite's shared memory database has one connection per writer; IMMEDIATE
// transactions acquire the writer lock before executing any statements.
func TestCompareRimeVsSQLiteConcurrent(t *testing.T) {
	t.Setenv("RIME_BENCH_MATCH_INDEXES", "1")
	rows := getBenchEnvInt("RIME_BENCH_ROWS", 100000)
	users := make([]*BenchUser, rows)
	for i := range users {
		id, name, email, age := benchUserRow(i + 1)
		users[i] = &BenchUser{ID: id, Name: name, Email: email, Age: age}
	}
	for _, writers := range []int{1, 4} {
		for _, batch := range []int{1, 256} {
			t.Run(fmt.Sprintf("writers=%d/batch=%d", writers, batch), func(t *testing.T) {
				dsn := fmt.Sprintf("file:rime-compare-%d?mode=memory&cache=shared&_txlock=immediate&_busy_timeout=5000&_synchronous=off&_journal_mode=memory&_foreign_keys=off", time.Now().UnixNano())
				sq := openSQLiteBenchDSN(t, dsn, writers)
				defer sq.Close()
				warmSQLiteWriters(t, sq, writers)
				rdb, ru, _ := openRimeBenchDB(t)
				defer rdb.Close()
				t.Logf("rows=%d, writers=%d, batch=%d, matching secondary indexes; one shared database per engine", rows, writers, batch)
				for _, phase := range []string{"insert", "update", "contended-update", "delete"} {
					if phase == "contended-update" && batch != 1 {
						continue // isolate single-key contention from transaction size
					}
					query := map[string]string{
						"insert":           "INSERT INTO users(id,name,email,age) VALUES(?,?,?,?)",
						"update":           "UPDATE users SET age=age+1 WHERE id=?",
						"contended-update": "UPDATE users SET age=age+1 WHERE id=?",
						"delete":           "DELETE FROM users WHERE id=?",
					}[phase]
					stmt, err := sq.Prepare(query)
					if err != nil {
						t.Fatal(err)
					}
					before := sq.Stats()
					sqlDur, sqlRetries, sqlCommits := runConcurrentWrites(t, rows, writers, batch, func(ctx context.Context, start, end int) error {
						tx, err := sq.BeginTx(ctx, nil)
						if err != nil {
							return err
						}
						defer tx.Rollback()
						local := tx.StmtContext(ctx, stmt)
						defer local.Close()
						for i := start; i < end; i++ {
							id := i + 1
							if phase == "contended-update" {
								id = i%min(rows, 64) + 1
							}
							var result sql.Result
							if phase == "insert" {
								u := users[i]
								result, err = local.ExecContext(ctx, u.ID, u.Name, u.Email, u.Age)
							} else {
								result, err = local.ExecContext(ctx, id)
							}
							if err != nil {
								return err
							}
							n, err := result.RowsAffected()
							if err != nil {
								return err
							}
							if n != 1 {
								return fmt.Errorf("%s key %d affected %d rows", phase, id, n)
							}
						}
						return tx.Commit()
					})
					stmt.Close()
					after := sq.Stats()
					rimeDur, rimeRetries, rimeCommits := runConcurrentWrites(t, rows, writers, batch, func(ctx context.Context, start, end int) error {
						return rdb.WriteTxContext(ctx, func(tx *rime.Tx) error {
							for i := start; i < end; i++ {
								id := i + 1
								if phase == "contended-update" {
									id = i%min(rows, 64) + 1
								}
								var err error
								switch phase {
								case "insert":
									err = ru.In(tx).Insert(users[i])
								case "delete":
									err = ru.In(tx).Delete(id)
								default:
									err = ru.In(tx).Update(id, func(u *BenchUser) error { u.Age++; return nil })
								}
								if err != nil {
									return err
								}
							}
							return nil
						})
					})
					if sqlCommits != rimeCommits {
						t.Fatalf("commit counts: SQLite=%d RIME=%d", sqlCommits, rimeCommits)
					}
					t.Logf("%-17s SQLite %.3fs %.0f rows/s retries=%d pool_waits=%d; RIME %.3fs %.0f rows/s conflicts=%d; commits=%d speedup=%.2fx", phase, sqlDur.Seconds(), float64(rows)/sqlDur.Seconds(), sqlRetries, after.WaitCount-before.WaitCount, rimeDur.Seconds(), float64(rows)/rimeDur.Seconds(), rimeRetries, rimeCommits, sqlDur.Seconds()/rimeDur.Seconds())
					verifyConcurrentWrites(t, sq, ru, users, phase, batch == 1)
					if phase == "update" {
						sqlScan, rimeScan, scannedRows := compareFullTableScan(t, sq, ru, users, 1)
						t.Logf("full table scan   SQLite %.3fs %.0f rows/s; RIME %.3fs %.0f rows/s; rows visited=%d speedup=%.2fx (after writers complete)", sqlScan.Seconds(), float64(scannedRows)/sqlScan.Seconds(), rimeScan.Seconds(), float64(scannedRows)/rimeScan.Seconds(), scannedRows, sqlScan.Seconds()/rimeScan.Seconds())
					}
					rdb.GC() // outside timers; don't let earlier phase history bias later phases
				}
			})
		}
	}
}

// Open every connection outside the timer and check that all see the schema
// of the same memory database. Holding them together prevents pool reuse from
// accidentally warming just one connection repeatedly.
func warmSQLiteWriters(t *testing.T, db *sql.DB, writers int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var connections []*sql.Conn
	defer func() {
		for _, conn := range connections {
			conn.Close()
		}
	}()
	for i := 0; i < writers; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
		var count int
		if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count); err != nil || count != 0 {
			t.Fatalf("writer connection %d: rows=%d error=%v", i, count, err)
		}
	}
}

// Prepare all goroutines before starting the clock. Static disjoint partitions
// have the same transaction boundaries in both engines. Only successful writes
// count toward throughput; lock/conflict retries are inside the timed interval.
func runConcurrentWrites(t *testing.T, rows, writers, batch int, write func(context.Context, int, int) error) (time.Duration, int64, int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	startSignal := make(chan struct{})
	errs := make(chan error, writers)
	var ready, done sync.WaitGroup
	var retries, commits atomic.Int64
	ready.Add(writers)
	done.Add(writers)
	for worker := 0; worker < writers; worker++ {
		go func(worker int) {
			defer done.Done()
			ready.Done()
			<-startSignal
			last := (worker + 1) * rows / writers
			for first := worker * rows / writers; first < last; first += batch {
				for {
					if err := ctx.Err(); err != nil {
						errs <- err
						return
					}
					err := write(ctx, first, min(first+batch, last))
					if err == nil {
						commits.Add(1)
						break
					}
					// Shared-cache SQLite reports SQLITE_LOCKED immediately rather
					// than waiting on busy_timeout. Retry the whole rolled-back tx.
					if !errors.Is(err, rime.ErrConflict) && !strings.Contains(err.Error(), "database is locked") && !strings.Contains(err.Error(), "database table is locked") {
						errs <- err
						cancel()
						return
					}
					retries.Add(1)
					timer := time.NewTimer(50 * time.Microsecond)
					select {
					case <-timer.C:
					case <-ctx.Done():
						timer.Stop()
						errs <- ctx.Err()
						return
					}
				}
			}
		}(worker)
	}
	ready.Wait()
	start := time.Now()
	close(startSignal)
	done.Wait()
	duration := time.Since(start)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent writes: %v", err)
	}
	return duration, retries.Load(), commits.Load()
}

// Compare every row with the expected state, including exact hotspot increment
// counts, so lost updates, missing rows and silent partial batches fail the run.
func verifyConcurrentWrites(t *testing.T, sq *sql.DB, ru *rime.Table[BenchUser], initial []*BenchUser, phase string, hasHotspot bool) {
	t.Helper()
	rr, err := ru.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	sr, err := sq.Query("SELECT id,name,email,age FROM users ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer sr.Close()
	byID := make(map[int]*BenchUser, len(rr))
	ageCounts := make(map[int]int)
	for _, u := range rr {
		if byID[u.ID] != nil {
			t.Fatalf("duplicate RIME key %d", u.ID)
		}
		byID[u.ID] = u
		ageCounts[u.Age]++
	}
	expected := len(initial)
	if phase == "delete" {
		expected = 0
	}
	if len(rr) != expected {
		t.Fatalf("RIME rows=%d want=%d", len(rr), expected)
	}
	seen := 0
	for sr.Next() {
		var u BenchUser
		if err := sr.Scan(&u.ID, &u.Name, &u.Email, &u.Age); err != nil {
			t.Fatal(err)
		}
		if u.ID != seen+1 || u.ID > len(initial) {
			t.Fatalf("unexpected SQLite key %d", u.ID)
		}
		want := *initial[u.ID-1]
		if phase != "insert" {
			want.Age++
		}
		if phase == "contended-update" && hasHotspot && u.ID <= min(len(initial), 64) {
			want.Age += (len(initial)-u.ID)/min(len(initial), 64) + 1
		}
		if u != want || byID[u.ID] == nil || *byID[u.ID] != want {
			t.Fatalf("key=%d SQLite=%+v RIME=%+v want=%+v", u.ID, u, byID[u.ID], want)
		}
		seen++
	}
	if err := sr.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != expected {
		t.Fatalf("SQLite rows=%d want=%d", seen, expected)
	}
	// Exercise live ordered-index entries as well as the record scan, including
	// complete removal after deletion and changed buckets after hotspot updates.
	ages := []int{0, 18, 19, 40, 77, 78}
	if first := byID[1]; first != nil {
		ages = append(ages, first.Age)
	}
	field := rime.OF[BenchUser, int](ru, "Age")
	for _, age := range ages {
		count, err := ru.Where(field.Eq(age)).Count()
		if err != nil || count != ageCounts[age] {
			t.Fatalf("ordered age=%d count=%d want=%d error=%v", age, count, ageCounts[age], err)
		}
	}
}
