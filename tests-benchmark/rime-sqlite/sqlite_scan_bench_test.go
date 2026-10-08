package rime_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

// compareFullTableScan consumes every column of every user, without filtering,
// sorting or SQL aggregates. Writers have completed before this read phase.
// SQLite returns rows through database/sql; RIME returns immutable pointers.
// Count and an order-independent checksum validate each complete traversal.
func compareFullTableScan(t *testing.T, sq *sql.DB, users *rime.Table[BenchUser], initial []*BenchUser, ageDelta int) (time.Duration, time.Duration, int) {
	t.Helper()
	iterations := getBenchEnvInt("RIME_BENCH_SCAN_ITERS", 5)
	var want int64
	for _, u := range initial {
		want += scanUserChecksum(u) + int64(ageDelta)
	}
	stmt, err := sq.Prepare("SELECT id, name, email, age FROM users")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	start := time.Now()
	for i := 0; i < iterations; i++ {
		result, err := stmt.Query()
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		var sum int64
		for result.Next() {
			var u BenchUser
			if err := result.Scan(&u.ID, &u.Name, &u.Email, &u.Age); err != nil {
				result.Close()
				t.Fatal(err)
			}
			count++
			sum += scanUserChecksum(&u)
		}
		err = result.Err()
		closeErr := result.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("sqlite full scan: iteration error=%v close error=%v", err, closeErr)
		}
		if count != len(initial) || sum != want {
			t.Fatalf("sqlite full scan: rows=%d checksum=%d want rows=%d checksum=%d", count, sum, len(initial), want)
		}
	}
	sqlDuration := time.Since(start)
	start = time.Now()
	for i := 0; i < iterations; i++ {
		result, err := users.Where().Find()
		if err != nil {
			t.Fatal(err)
		}
		var sum int64
		for _, u := range result {
			sum += scanUserChecksum(u)
		}
		if len(result) != len(initial) || sum != want {
			t.Fatalf("rime full scan: rows=%d checksum=%d want rows=%d checksum=%d", len(result), sum, len(initial), want)
		}
	}
	return sqlDuration, time.Since(start), iterations * len(initial)
}

func scanUserChecksum(u *BenchUser) int64 {
	return int64(u.ID) + int64(u.Age) + int64(len(u.Name)) + int64(len(u.Email))
}
