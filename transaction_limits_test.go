package replicateddb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/nomadsql/replicateddb/codec"
)

func TestConfigMaxTransactionBytesValidation(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)

	// Negative MaxTransactionBytes
	cfg.MaxTransactionBytes = -1
	if err := cfg.validate(); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("expected ErrUnsupportedSchema on negative MaxTransactionBytes, got %v", err)
	}

	// Zero MaxTransactionBytes before withDefaults
	cfg.MaxTransactionBytes = 0
	cfg.withDefaults()
	if cfg.MaxTransactionBytes != 64<<20 {
		t.Fatalf("expected default 64 MiB MaxTransactionBytes, got %d", cfg.MaxTransactionBytes)
	}

	// MaxReplicatedValueBytes larger than MaxTransactionBytes
	cfg.MaxTransactionBytes = 1000
	cfg.MaxReplicatedValueBytes = 2000
	if err := cfg.validate(); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("expected ErrUnsupportedSchema when MaxReplicatedValueBytes > MaxTransactionBytes, got %v", err)
	}
}

func TestLocalTransactionMaxTransactionBytesEnforced(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	// Allow one encoded contact while rejecting the multi-row transaction.
	cfg.MaxTransactionBytes = 400
	cfg.MaxReplicatedValueBytes = 300

	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// 1. Transaction within limit: 1 contact
	id1 := NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)", id1[:], "Alice", 10); err != nil {
		t.Fatalf("Insert 1 within limit failed: %v", err)
	}

	// Verify Alice exists
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM contacts").Scan(&count); err != nil || count != 1 {
		t.Fatalf("expected 1 contact, got %d (err: %v)", count, err)
	}

	// 2. Transaction exceeding MaxTransactionBytes:
	// A transaction with multiple rows exceeding the 400-byte limit
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	for i := 0; i < 5; i++ {
		rowID := NewRowID()
		// each row will have ~50-80 bytes encoded
		if _, err := tx.ExecContext(ctx, "INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)", rowID[:], fmt.Sprintf("long_contact_name_entry_%d", i), i*10); err != nil {
			t.Fatalf("tx.Exec %d: %v", i, err)
		}
	}
	err = tx.Commit()
	if err == nil {
		t.Fatal("expected oversize transaction to fail commit")
	}
	if !errors.Is(err, ErrTransactionTooLarge) && !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("expected ErrTransactionTooLarge or ErrBatchTooLarge, got %v", err)
	}

	// Verify SQL state was rolled back cleanly and still has only 1 row
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM contacts").Scan(&count); err != nil || count != 1 {
		t.Fatalf("expected count 1 after rolled back oversize transaction, got %d (err: %v)", count, err)
	}

	// 3. Single large string within value limit but combined transaction exceeding MaxTransactionBytes
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	id2 := NewRowID()
	largeName := strings.Repeat("x", 250)
	if _, err := tx2.ExecContext(ctx, "INSERT INTO contacts (id, name, phone, score) VALUES (?, ?, ?, ?)", id2[:], largeName, strings.Repeat("y", 100), 20); err != nil {
		t.Fatalf("tx2.Exec: %v", err)
	}
	err = tx2.Commit()
	if err == nil {
		t.Fatal("expected combined transaction size > 350 bytes to fail")
	}
	if !errors.Is(err, ErrTransactionTooLarge) {
		t.Fatalf("expected ErrTransactionTooLarge, got %v", err)
	}

	// Verify database is still clean and operational
	id3 := NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)", id3[:], "Bob", 30); err != nil {
		t.Fatalf("subsequent small insert failed: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM contacts").Scan(&count); err != nil || count != 2 {
		t.Fatalf("expected count 2, got %d (err: %v)", count, err)
	}
}

func TestStoreCommitRemoteEnforcesMaxTransactionBytes(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.MaxTransactionBytes = 200
	cfg.MaxReplicatedValueBytes = 150

	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	peer := NewNodeID()
	rowID := NewRowID()

	// Create remote batch exceeding 200 bytes
	batch := remoteBatchCells(db, peer, 1, 1000<<16, "contacts", rowID, map[string]codec.Value{
		"id":    codec.Blob(rowID[:]),
		"name":  codec.Text(strings.Repeat("a", 150)),
		"phone": codec.Text(strings.Repeat("b", 150)),
	})

	err = db.ApplyRemote(ctx, batch)
	if err == nil {
		t.Fatal("expected ApplyRemote to reject batch exceeding MaxTransactionBytes")
	}
}
