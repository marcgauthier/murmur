package replicateddb

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
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
	cfg.Durability.SyncInterval = time.Second
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected async periodic sync to be valid, got: %v", err)
	}
	cfg.Durability.Mode = DurabilitySynchronous
	if err := cfg.validate(); err == nil {
		t.Fatal("expected periodic sync with synchronous mode to fail validation")
	}
	cfg.Durability.Mode = DurabilityAsync
	cfg.Durability.SyncInterval = -time.Second
	if err := cfg.validate(); err == nil {
		t.Fatal("expected negative sync interval to fail validation")
	}
	cfg.Durability.SyncInterval = 0

	cfg.Durability.Mode = DurabilityMode(99)
	if err := cfg.validate(); err == nil {
		t.Fatal("expected invalid DurabilityMode to fail validation")
	}
}

func TestDurabilityPeriodicSync(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(filepath.Join(t.TempDir(), "node1"))
	cfg.Durability = DurabilityConfig{Mode: DurabilityAsync, SyncInterval: 20 * time.Millisecond}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	id := ids.NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name) VALUES (?, ?)", id[:], "periodic"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for db.Metrics().PeriodicSyncs == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := db.Metrics(); got.PeriodicSyncs == 0 || got.PeriodicSyncFailures != 0 {
		_ = db.Close()
		t.Fatalf("periodic sync result: syncs=%d failures=%d", got.PeriodicSyncs, got.PeriodicSyncFailures)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM contacts WHERE id = ?", id[:]).Scan(&name); err != nil || name != "periodic" {
		t.Fatalf("reopened row: name=%q err=%v", name, err)
	}
}

func TestDurabilityPeriodicSyncFailureFailsClosed(t *testing.T) {
	ctx := context.Background()
	fsys := &failFS{FS: vfs.Default}
	cfg := testConfig(t.TempDir())
	cfg.Pebble.BaseFS = fsys
	cfg.Durability = DurabilityConfig{Mode: DurabilityAsync, SyncInterval: 20 * time.Millisecond}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		fsys.armed.Store(false)
		_ = db.Close()
	}()
	id := ids.NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name) VALUES (?, ?)", id[:], "before failure"); err != nil {
		t.Fatal(err)
	}
	fsys.armed.Store(true)
	deadline := time.Now().Add(2 * time.Second)
	for db.Metrics().PeriodicSyncFailures == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if db.Metrics().PeriodicSyncFailures == 0 {
		t.Fatal("periodic sync failure was not reported")
	}
	if state := db.Status().State; state != StateFailed {
		t.Fatalf("state = %s after sync failure, want failed", state)
	}
	afterID := ids.NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name) VALUES (?, ?)", afterID[:], "after failure"); err == nil {
		t.Fatal("write succeeded after periodic sync failure")
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
