package murmur

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
)

func TestDurabilityMaxUnsyncedBytesValidation(t *testing.T) {
	cfg := groupTestConfig(t)
	cfg.withDefaults()
	// Defaults: synchronous mode with no asynchronous triggers, so
	// synchronous group commit handles durability.
	if cfg.Durability.Mode != DurabilitySynchronous {
		t.Fatalf("default durability mode = %v", cfg.Durability.Mode)
	}
	if cfg.Durability.SyncInterval != 0 || cfg.Durability.MaxUnsyncedBytes != 0 {
		t.Fatalf("default async triggers = %v/%d, want 0/0",
			cfg.Durability.SyncInterval, cfg.Durability.MaxUnsyncedBytes)
	}

	cfg.Durability.Mode = DurabilityAsync
	cfg.Durability.MaxUnsyncedBytes = 10 << 20
	if err := cfg.validate(); err != nil {
		t.Fatalf("async size trigger rejected: %v", err)
	}
	cfg.Durability.SyncInterval = time.Second
	if err := cfg.validate(); err != nil {
		t.Fatalf("async time+size triggers rejected: %v", err)
	}
	cfg.Durability.MaxUnsyncedBytes = -1
	if err := cfg.validate(); err == nil {
		t.Fatal("negative MaxUnsyncedBytes accepted")
	}
	cfg.Durability.MaxUnsyncedBytes = 10 << 20
	cfg.Durability.Mode = DurabilitySynchronous
	if err := cfg.validate(); err == nil {
		t.Fatal("size trigger with synchronous mode accepted")
	}
}

// TestDurabilitySizeTriggeredSync proves the byte threshold alone drives
// durability: with no time interval, enough writes trigger a sync (visible
// in PeriodicSyncs before Close), and every acknowledged row survives a
// reopen.
func TestDurabilitySizeTriggeredSync(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "node1")
	cfg := groupTestConfig(t)
	cfg.Path = dir
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	cfg.Durability = DurabilityConfig{Mode: DurabilityAsync, MaxUnsyncedBytes: 4096}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := TableOf[facadeRecord](db, "records")
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	const total = 30
	for i := 0; i < total; i++ {
		tx, err := db.BeginTx(ctx)
		if err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		if err := table.Insert(tx, &facadeRecord{ID: ids.NewRowID(), Name: fmt.Sprintf("size-sync-%d", i)}); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for db.Metrics().PeriodicSyncs == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := db.Metrics(); got.PeriodicSyncs == 0 || got.PeriodicSyncFailures != 0 {
		_ = db.Close()
		t.Fatalf("size-triggered sync result: syncs=%d failures=%d",
			got.PeriodicSyncs, got.PeriodicSyncFailures)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err = TableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	count, err := table.Where().Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != total {
		t.Fatalf("durable rows = %d; acknowledged inserts = %d", count, total)
	}
}

// TestDurabilityTimeTriggerWithSizeSet proves the interval trigger still
// fires when a (never-reached) size threshold is also configured.
func TestDurabilityTimeTriggerWithSizeSet(t *testing.T) {
	ctx := context.Background()
	cfg := groupTestConfig(t)
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	cfg.Durability = DurabilityConfig{
		Mode: DurabilityAsync, SyncInterval: 20 * time.Millisecond, MaxUnsyncedBytes: 1 << 40,
	}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err := TableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(tx, &facadeRecord{ID: ids.NewRowID(), Name: "time-wins"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for db.Metrics().PeriodicSyncs == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := db.Metrics(); got.PeriodicSyncs == 0 || got.PeriodicSyncFailures != 0 {
		t.Fatalf("interval sync with size set: syncs=%d failures=%d",
			got.PeriodicSyncs, got.PeriodicSyncFailures)
	}
}
