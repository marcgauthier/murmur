package replicateddb

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/nomadsql/replicateddb/ids"
)

func TestDurabilityConfigValidation(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.withDefaults()
	cfg.Durability = DurabilityConfig{Mode: DurabilitySynchronous}
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected DurabilitySynchronous to be valid, got: %v", err)
	}

	cfg.Durability.Mode = DurabilityAsync
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected DurabilityAsync to be valid, got: %v", err)
	}

	cfg.Durability.Mode = DurabilityMode(99)
	if err := cfg.validate(); err == nil {
		t.Fatal("expected invalid DurabilityMode to fail validation")
	}
}

func TestDurabilityAsyncTransactionsAndSync(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node1")
	cfg := testConfig(dir)
	cfg.Durability = DurabilityConfig{Mode: DurabilityAsync}

	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	if db.DurabilityMode() != DurabilityAsync {
		t.Fatalf("expected DurabilityAsync, got %v", db.DurabilityMode())
	}

	rowID := ids.NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name) VALUES (?, ?)", rowID[:], "async_contact"); err != nil {
		t.Fatalf("ExecContext failed: %v", err)
	}

	// Verify query inside active database
	var name string
	row := db.QueryRowContext(ctx, "SELECT name FROM contacts WHERE id = ?", rowID[:])
	if err := row.Scan(&name); err != nil || name != "async_contact" {
		t.Fatalf("QueryRowContext failed: name=%q err=%v", name, err)
	}

	// Call explicit Sync to disk
	if err := db.Sync(ctx); err != nil {
		t.Fatalf("db.Sync failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("db.Close failed: %v", err)
	}

	// Reopen with DurabilityAsync and verify state is intact
	db2, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	var name2 string
	row2 := db2.QueryRowContext(ctx, "SELECT name FROM contacts WHERE id = ?", rowID[:])
	if err := row2.Scan(&name2); err != nil || name2 != "async_contact" {
		t.Fatalf("reopened query failed: name=%q err=%v", name2, err)
	}
}
