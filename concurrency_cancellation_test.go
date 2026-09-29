package replicateddb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/ids"
)

// TestCanceledQueryUnblocksWriter verifies that canceling a read query context
// releases the read lock, unblocking pending writers.
func TestCanceledQueryUnblocksWriter(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(context.Background(), testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	id := ids.NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)", id[:], "alice", "111"); err != nil {
		t.Fatal(err)
	}

	readCtx, readCancel := context.WithCancel(context.Background())
	rows, err := db.QueryContext(readCtx, "SELECT name, phone FROM contacts WHERE id = ?", id[:])
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	writeDone := make(chan error, 1)
	go func() {
		id2 := ids.NewRowID()
		_, err := db.ExecContext(context.Background(), "INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)", id2[:], "bob", "222")
		writeDone <- err
	}()

	// Ensure writer is waiting behind the open reader
	select {
	case err := <-writeDone:
		t.Fatalf("writer completed before reader was canceled/closed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Cancel the reader's context
	readCancel()

	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("writer failed after read cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer did not complete after reader context was canceled")
	}
}

// TestPartiallyConsumedRowsTimeout verifies that a writer with a context timeout
// does not wedge when blocked behind partially consumed rows.
func TestPartiallyConsumedRowsTimeout(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(context.Background(), testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		id := ids.NewRowID()
		if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)", id[:], fmt.Sprintf("user-%d", i), "555"); err != nil {
			t.Fatal(err)
		}
	}

	// Read only 1 of the 5 rows
	rows, err := db.QueryContext(ctx, "SELECT id, name FROM contacts")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	// Attempt write with 50ms timeout
	writeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	idNew := ids.NewRowID()
	start := time.Now()
	_, err = db.ExecContext(writeCtx, "INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)", idNew[:], "timed-out", "000")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected write to time out while reader holds unclosed rows")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(writeCtx.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got: %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("write took %v, want ~50ms", elapsed)
	}

	// Close the partially consumed rows
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	// Subsequent write should succeed immediately
	if _, err := db.ExecContext(context.Background(), "INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)", idNew[:], "succeeded", "000"); err != nil {
		t.Fatalf("write after rows.Close failed: %v", err)
	}
}

// TestAbandonedTransactionAutoRollback verifies that abandoned transactions
// automatically rollback when their context is canceled.
func TestAbandonedTransactionAutoRollback(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(context.Background(), testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	txCtx, txCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	tx, err := db.BeginTx(txCtx, nil)
	if err != nil {
		t.Fatal(err)
	}
	id := ids.NewRowID()
	if _, err := tx.ExecContext(txCtx, "INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)", id[:], "abandoned", "999"); err != nil {
		t.Fatal(err)
	}
	// Do NOT call tx.Commit() or tx.Rollback().
	// Wait for tx context to expire.
	txCancel()
	time.Sleep(20 * time.Millisecond)

	// A new transaction should succeed without being blocked by the abandoned one.
	ctx := context.Background()
	id2 := ids.NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)", id2[:], "fresh", "111"); err != nil {
		t.Fatalf("write after abandoned transaction failed: %v", err)
	}

	// Verify that the abandoned row was NOT committed
	var count int
	row := db.QueryRowContext(ctx, "SELECT count(*) FROM contacts WHERE name = ?", "abandoned")
	if err := row.Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("abandoned transaction changes were committed (count = %d, want 0)", count)
	}
}

// TestConcurrentCloseUnderActiveReadsAndWrites verifies that db.Close() exits
// promptly and cleanly without hanging when readers, writers, and abandoned txs are active.
func TestConcurrentCloseUnderActiveReadsAndWrites(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(context.Background(), testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		id := ids.NewRowID()
		_, _ = db.ExecContext(ctx, "INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)", id[:], fmt.Sprintf("user-%d", i), "123")
	}

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Start readers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				rows, err := db.QueryContext(context.Background(), "SELECT id, name FROM contacts")
				if err == nil {
					for rows.Next() {
						if stop.Load() {
							break
						}
					}
					_ = rows.Close()
				}
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	// Start writers
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(widx int) {
			defer wg.Done()
			for !stop.Load() {
				id := ids.NewRowID()
				_, _ = db.ExecContext(context.Background(), "INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)", id[:], "writer", "000")
				time.Sleep(5 * time.Millisecond)
			}
		}(i)
	}

	// Start abandoned transactions
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				cctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				tx, err := db.BeginTx(cctx, nil)
				if err == nil {
					id := ids.NewRowID()
					_, _ = tx.ExecContext(cctx, "INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)", id[:], "ab", "1")
				}
				cancel()
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	// Let concurrency run for a bit
	time.Sleep(100 * time.Millisecond)

	// Close database while operations are in flight
	closeDone := make(chan error, 1)
	go func() {
		closeDone <- db.Close()
	}()

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("db.Close() error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("db.Close() hung under concurrent load")
	}

	stop.Store(true)
	wg.Wait()
}

// TestReplicationUnderBlockedReads verifies that remote batch applies succeed
// without deadlocks during concurrent standalone reads.
func TestReplicationUnderBlockedReads(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(context.Background(), testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	id := ids.NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)", id[:], "init", "000"); err != nil {
		t.Fatal(err)
	}

	// Open read cursor
	rows, err := db.QueryContext(ctx, "SELECT id, name FROM contacts")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected row")
	}

	// Apply remote mutation batch
	remoteRowID := ids.NewRowID()
	batch := remoteBatchCells(db, NewNodeID(), 1, db.store.ClockNow()+100, "contacts", remoteRowID, map[string]codec.Value{
		"id":    codec.Blob(remoteRowID[:]),
		"name":  codec.Text("remote-name"),
		"phone": codec.Text("remote-phone"),
	})

	applyDone := make(chan error, 1)
	go func() {
		applyDone <- db.ApplyRemote(context.Background(), batch)
	}()

	// Closing read cursor allows remote materialization to complete
	_ = rows.Close()

	select {
	case err := <-applyDone:
		if err != nil {
			t.Fatalf("ApplyRemote failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ApplyRemote hung under blocked read")
	}

	waitForRemoteMaterialization(t, db)

	// Verify remote row is visible
	var name string
	row := db.QueryRowContext(ctx, "SELECT name FROM contacts WHERE id = ?", remoteRowID[:])
	if err := row.Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "remote-name" {
		t.Fatalf("name = %q, want remote-name", name)
	}
}

// TestKeyRotationUnderConcurrentQueries verifies key rotation while queries
// and canceled transactions are occurring.
func TestKeyRotationUnderConcurrentQueries(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(context.Background(), testConfig(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		id := ids.NewRowID()
		_, _ = db.ExecContext(ctx, "INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)", id[:], fmt.Sprintf("user-%d", i), "111")
	}

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Background readers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			cctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			rows, err := db.QueryContext(cctx, "SELECT id, name FROM contacts")
			if err == nil {
				for rows.Next() {
				}
				_ = rows.Close()
			}
			cancel()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Rotate data key
	if err := db.RotateDataKey(context.Background()); err != nil {
		t.Fatalf("RotateDataKey failed: %v", err)
	}

	// Rotate storage key
	newKey := append([]byte(nil), testKey...)
	newKey[0] ^= 0xff
	if err := db.RotateStorageKey(context.Background(), KeyMaterial{
		ID:        "new-test-key",
		Algorithm: string(AES256GCM),
		Key:       newKey,
	}); err != nil {
		t.Fatalf("RotateStorageKey failed: %v", err)
	}

	// Maintenance rewrite
	if err := db.RewriteEncryptedFiles(context.Background()); err != nil {
		t.Fatalf("RewriteEncryptedFiles failed: %v", err)
	}

	stop.Store(true)
	wg.Wait()

	// Verify database is functional and data is intact
	var count int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM contacts").Scan(&count); err != nil {
		t.Fatalf("query after rotation failed: %v", err)
	}
	if count != 10 {
		t.Fatalf("count = %d, want 10", count)
	}
}
