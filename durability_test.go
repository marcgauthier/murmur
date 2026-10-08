package murmur

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/spool"
)

func TestDurabilityConfigValidation(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.withDefaults()
	cfg.Durability = DurabilityConfig{Mode: DurabilitySynchronous}
	cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected DurabilitySynchronous to be valid, got: %v", err)
	}

	cfg.Durability = DurabilityConfig{Mode: DurabilityAsync}
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected DurabilityAsync to be valid, got: %v", err)
	}
	cfg.Durability.SyncInterval = time.Second
	if err := cfg.validate(); err != nil {
		t.Fatalf("expected async periodic sync to be valid, got: %v", err)
	}
	cfg.Durability = DurabilityConfig{Mode: DurabilitySynchronous, SyncInterval: time.Second}
	if err := cfg.validate(); err == nil {
		t.Fatal("expected periodic sync with synchronous mode to fail validation")
	}
	cfg.Durability = DurabilityConfig{Mode: DurabilityAsync, SyncInterval: -time.Second}
	if err := cfg.validate(); err == nil {
		t.Fatal("expected negative sync interval to fail validation")
	}
	cfg.Durability.SyncInterval = 0

	cfg.Durability.Mode = DurabilityMode(99)
	if err := cfg.validate(); err == nil {
		t.Fatal("expected invalid DurabilityMode to fail validation")
	}
}

func durabilityTypedConfig(t *testing.T, path string) Config {
	t.Helper()
	cfg := testConfig(path)
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	return cfg
}

func TestDurabilityPeriodicSync(t *testing.T) {
	ctx := context.Background()
	cfg := durabilityTypedConfig(t, filepath.Join(t.TempDir(), "node1"))
	cfg.Durability = DurabilityConfig{Mode: DurabilityAsync, SyncInterval: 20 * time.Millisecond}
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := TableOf[facadeRecord](db, "records")
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	id := ids.NewRowID()
	if err := insertRecord(ctx, db, table, &facadeRecord{ID: id, Name: "periodic"}); err != nil {
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
	db, err = openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err = TableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	got, err := table.Get(id)
	if err != nil || got.Name != "periodic" {
		t.Fatalf("reopened row: %+v err=%v", got, err)
	}
}

func TestDurabilityPeriodicSyncFailureFailsClosed(t *testing.T) {
	ctx := context.Background()
	var armed atomic.Bool
	faults := &spool.FaultHooks{
		SegmentSync: func() error {
			if armed.Load() {
				return syscall.ENOSPC
			}
			return nil
		},
	}
	cfg := durabilityTypedConfig(t, t.TempDir())
	cfg.Spool.Faults = faults
	cfg.Durability = DurabilityConfig{Mode: DurabilityAsync, SyncInterval: 20 * time.Millisecond}
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		armed.Store(false)
		_ = db.Close()
	}()
	table, err := TableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	if err := insertRecord(ctx, db, table, &facadeRecord{ID: ids.NewRowID(), Name: "before failure"}); err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
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
	if err := insertRecord(ctx, db, table, &facadeRecord{ID: ids.NewRowID(), Name: "after failure"}); err == nil {
		t.Fatal("write succeeded after periodic sync failure")
	}
}

func TestDurabilityAsyncTransactionsAndSync(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node1")
	cfg := durabilityTypedConfig(t, dir)
	cfg.Durability = DurabilityConfig{Mode: DurabilityAsync}

	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	table, err := TableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatalf("TableOf failed: %v", err)
	}

	if db.DurabilityMode() != DurabilityAsync {
		t.Fatalf("expected DurabilityAsync, got %v", db.DurabilityMode())
	}

	rowID := ids.NewRowID()
	if err := insertRecord(ctx, db, table, &facadeRecord{ID: rowID, Name: "async_contact"}); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Verify query inside active database
	got, err := table.Get(rowID)
	if err != nil || got.Name != "async_contact" {
		t.Fatalf("Get failed: %+v err=%v", got, err)
	}

	// Call explicit Sync to disk
	if err := db.Sync(ctx); err != nil {
		t.Fatalf("db.Sync failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("db.Close failed: %v", err)
	}

	// Reopen with DurabilityAsync and verify state is intact
	db2, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db2.Close()

	table2, err := TableOf[facadeRecord](db2, "records")
	if err != nil {
		t.Fatalf("TableOf failed: %v", err)
	}
	got2, err := table2.Get(rowID)
	if err != nil || got2.Name != "async_contact" {
		t.Fatalf("reopened query failed: %+v err=%v", got2, err)
	}
}
