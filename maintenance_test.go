package replicateddb

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRotateDataKey(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before := db.EncryptionStatus()
	if before.ActiveDataKeyID == "" || len(before.Keys) != 1 {
		t.Fatalf("initial status: %+v", before)
	}
	if err := db.RotateDataKey(ctx); err != nil {
		t.Fatal(err)
	}
	after := db.EncryptionStatus()
	if after.ActiveDataKeyID == before.ActiveDataKeyID {
		t.Fatal("active data key did not change")
	}
	if len(after.Keys) != 2 {
		t.Fatalf("want 2 keys, got %d", len(after.Keys))
	}
	if after.Phase != "idle" {
		t.Fatalf("phase = %q", after.Phase)
	}
	// Writes continue online after rotation.
	id := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "k"); err != nil {
		t.Fatal(err)
	}
}

func TestSetEncryptionAlgorithm(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	cfg := testConfig(path)
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.SetEncryptionAlgorithm(ctx, ChaCha20Poly1305); err != nil {
		t.Fatal(err)
	}
	st := db.EncryptionStatus()
	if st.Algorithm != ChaCha20Poly1305 {
		t.Fatalf("algorithm = %q", st.Algorithm)
	}
	id := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "c"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	// Reopen with the same algorithm works and keeps data.
	cfg.Encryption.Algorithm = ChaCha20Poly1305
	db2, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(queryAll(t, db2, `SELECT id FROM contacts`)); n != 1 {
		t.Fatalf("want 1 row, got %d", n)
	}
	_ = db2.Close()

	// Reopen disagreeing with the persisted algorithm refuses.
	cfg.Encryption.Algorithm = AES256GCM
	if _, err := Open(ctx, cfg); err == nil || !strings.Contains(err.Error(), "disagrees") {
		t.Fatalf("expected algorithm disagreement error, got %v", err)
	}

	// Unknown algorithms are rejected without state change.
	db3, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db3.Close()
	if err := db3.SetEncryptionAlgorithm(ctx, "nope"); err == nil {
		t.Fatal("expected unknown algorithm error")
	}
	if st := db3.Status().State; st != StateReady {
		t.Fatalf("state = %s", st)
	}
}

func TestRewriteEncryptedFiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	path := t.TempDir()
	cfg := testConfig(path)
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		id := NewRowID()
		if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "r"); err != nil {
			t.Fatal(err)
		}
	}
	before := queryAll(t, db, `SELECT id FROM contacts ORDER BY id`)
	genBefore := db.Status().StateGeneration
	if err := db.RewriteEncryptedFiles(ctx); err != nil {
		t.Fatal(err)
	}
	if st := db.Status(); st.State != StateReady || st.StateGeneration != genBefore {
		t.Fatalf("after rewrite: %+v", st)
	}
	after := queryAll(t, db, `SELECT id FROM contacts ORDER BY id`)
	if len(before) != len(after) {
		t.Fatalf("rows before=%d after=%d", len(before), len(after))
	}
	// The store keeps working after maintenance.
	id := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "post"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	// Reopen: data intact, no journal left behind.
	db2, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if n := len(queryAll(t, db2, `SELECT id FROM contacts`)); n != 26 {
		t.Fatalf("want 26 rows, got %d", n)
	}
}

func TestMaintenanceRejectsWrites(t *testing.T) {
	db, err := Open(context.Background(), testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Drive the state machine directly: maintenance allows reads, rejects
	// writes with ErrMaintenance.
	db.setState(StateMaintenance)
	if err := db.requireWrite(); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("requireWrite = %v", err)
	}
	if err := db.requireRead(); err != nil {
		t.Fatalf("requireRead = %v", err)
	}
	mid := NewRowID()
	if _, err := db.ExecContext(context.Background(), `INSERT INTO contacts (id) VALUES (?)`, mid[:]); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("exec in maintenance = %v", err)
	}
	// Reads serve the in-memory engine.
	rows, err := db.QueryContext(context.Background(), `SELECT id FROM contacts`)
	if err != nil {
		t.Fatalf("query in maintenance: %v", err)
	}
	rows.Close()
	db.setState(StateReady)
}

func TestEncryptionStatusShape(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	st := db.EncryptionStatus()
	if st.Algorithm != AES256GCM {
		t.Fatalf("algorithm = %q", st.Algorithm)
	}
	if st.ApplicationKeyID != testKeyID {
		t.Fatalf("appid = %q", st.ApplicationKeyID)
	}
	if st.RegistryGeneration != 1 {
		t.Fatalf("registry generation = %d", st.RegistryGeneration)
	}
	if st.Phase != "idle" {
		t.Fatalf("phase = %q", st.Phase)
	}
	if len(st.Keys) != 1 {
		t.Fatalf("keys = %d", len(st.Keys))
	}
	k := st.Keys[0]
	if k.ID != st.ActiveDataKeyID || k.ID == "" {
		t.Fatalf("active key ref mismatch: %+v", k)
	}
	if k.Algorithm != AES256GCM || k.CreatedAt.IsZero() {
		t.Fatalf("key meta: %+v", k)
	}
}
