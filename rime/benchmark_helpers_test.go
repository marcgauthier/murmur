package rime_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

type BenchUser struct {
	ID    int    `rime:"primary"`
	Name  string `rime:"index"`
	Email string `rime:"index"`
	Age   int    `rime:"ordered"`
}

func benchUserRow(i int) (id int, name, email string, age int) {
	return i, fmt.Sprintf("user-%06d", i), fmt.Sprintf("user-%06d@example.com", i), 18 + (i % 60)
}

func getBenchEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
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

type BenchOrder struct {
	ID     int     `rime:"primary"`
	UserID int     `rime:"index"`
	Amount float64 `rime:"ordered"`
	Note   string
}

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
					if !errors.Is(err, rime.ErrConflict) && !strings.Contains(err.Error(), "locked") {
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
