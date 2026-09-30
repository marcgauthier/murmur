package sqlengine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
)

func TestEngineBeginContextTimeoutUnderBlockedRead(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// Seed one row
	tx, err := e.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id := ids.NewRowID()
	if _, err := tx.Exec(`INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`, id[:], "test", "123"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Open standalone read and leave open
	readCtx, readCancel := context.WithCancel(context.Background())
	defer readCancel()
	rows, err := e.Query(readCtx, `SELECT name, phone FROM contacts WHERE id = ?`, id[:])
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	// Writer with short timeout should fail without hanging forever
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = e.Begin(timeoutCtx)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected Begin to time out while read is held open, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Begin took %v, want ~50ms", elapsed)
	}

	// When read context is canceled, read lock should auto-release
	readCancel()
	// Allow background callback to run
	time.Sleep(10 * time.Millisecond)

	// Now a new write transaction should succeed promptly
	tx2, err := e.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin after read cancellation failed: %v", err)
	}
	id2 := ids.NewRowID()
	if _, err := tx2.Exec(`INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`, id2[:], "test2", "456"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestEngineConcurrentCloseUnderBlockedRead(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 8)
	if err != nil {
		t.Fatal(err)
	}

	// Open read
	rows, err := e.Query(ctx, `SELECT count(*) FROM contacts`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	// Engine Close must not deadlock even if read is still open
	done := make(chan error, 1)
	go func() {
		done <- e.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked behind open read Rows")
	}
}

func TestEngineLockCancelQueueStress(t *testing.T) {
	ctx := context.Background()
	e, err := Open(testRegistry(t), nil, nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	// Hold read lock
	rows, err := e.Query(ctx, `SELECT count(*) FROM contacts`)
	if err != nil {
		t.Fatal(err)
	}

	// Spin up 20 writers that each cancel after 10-30ms
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(context.Background(), time.Duration(10+idx%20)*time.Millisecond)
			defer cancel()
			_, _ = e.Begin(cctx)
		}(i)
	}
	wg.Wait()

	// Release reader
	_ = rows.Close()

	// Ensure writer can cleanly acquire lock now
	tx, err := e.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin failed after queue drain: %v", err)
	}
	_ = tx.Rollback()
}
