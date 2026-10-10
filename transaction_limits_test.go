package murmur

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/codec"
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

func TestTypedTransactionMaxTransactionBytesEnforced(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	// Allow one encoded record while rejecting multi-row batches.
	cfg.MaxTransactionBytes = 600
	cfg.MaxReplicatedValueBytes = 300

	db, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}

	// One record commits within the configured encoded transaction limit.
	first := &facadeRecord{ID: NewRowID(), Name: "Alice"}
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		return table.Insert(tx, first)
	}); err != nil {
		t.Fatalf("insert within limit: %v", err)
	}

	// Five individually valid records exceed the transaction total.
	err = db.WriteTxContext(context.Background(), func(tx *Tx) error {
		for i := 0; i < 5; i++ {
			value := &facadeRecord{ID: NewRowID(), Name: fmt.Sprintf("entry-%d", i)}
			if err := table.Insert(tx, value); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected oversized transaction to fail")
	}
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("expected ErrBatchTooLarge, got %v", err)
	}

	rows, err := table.Where().Find()
	if err != nil || len(rows) != 1 || rows[0].ID != first.ID {
		t.Fatalf("rows after rejected batch = %#v, %v; want only first row", rows, err)
	}

	// Each 250-byte field fits the value limit, but the pair exceeds the
	// transaction limit and must leave the committed state unchanged.
	err = db.WriteTxContext(context.Background(), func(tx *Tx) error {
		for i := 0; i < 2; i++ {
			value := &facadeRecord{ID: NewRowID(), Name: strings.Repeat("x", 250)}
			if err := table.Insert(tx, value); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected combined transaction size to fail")
	}
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("expected ErrBatchTooLarge, got %v", err)
	}

	rows, err = table.Where().Find()
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows after oversized-value batch = %d, %v; want 1", len(rows), err)
	}
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		return table.Insert(tx, &facadeRecord{ID: NewRowID(), Name: "Bob"})
	}); err != nil {
		t.Fatalf("subsequent small insert: %v", err)
	}
	rows, err = table.Where().Find()
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows after subsequent write = %d, %v; want 2", len(rows), err)
	}
}

func TestStoreCommitRemoteEnforcesMaxTransactionBytes(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	cfg.MaxTransactionBytes = 200
	cfg.MaxReplicatedValueBytes = 150

	db, err := openSignedFixture(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	peer := NewNodeID()
	rowID := NewRowID()

	// Create remote batch exceeding 200 bytes
	batch := remoteBatchCells(db, peer, 1, 1000<<16, "records", rowID, map[string]codec.Value{
		"ID":   codec.Blob(rowID[:]),
		"Name": codec.Text(strings.Repeat("a", 150)),
	})

	err = applyRemoteFixture(db, ctx, batch)
	if err == nil {
		t.Fatal("expected ApplyRemote to reject batch exceeding MaxTransactionBytes")
	}
}
