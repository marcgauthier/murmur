package murmur

import (
	"context"
	"errors"
	"testing"
	"time"
)

func maintenanceTypedConfig(t *testing.T, path string) Config {
	t.Helper()
	cfg := testConfig(path)
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	return cfg
}

func TestRotateDataKey(t *testing.T) {
	ctx := context.Background()
	db, err := openSignedFixture(ctx, maintenanceTypedConfig(t, t.TempDir()))
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
	table, err := TableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	if err := insertRecord(ctx, db, table, &facadeRecord{ID: NewRowID(), Name: "k"}); err != nil {
		t.Fatal(err)
	}
}

func TestSetEncryptionAlgorithm(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	cfg := testConfig(path)
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.SetEncryptionAlgorithm(ctx, AES256GCM); err != nil {
		t.Fatal(err)
	}
	st := db.EncryptionStatus()
	if st.Algorithm != AES256GCM {
		t.Fatalf("algorithm = %q", st.Algorithm)
	}

	// Unsupported algorithms are rejected without state change.
	for _, alg := range []EncryptionAlgorithm{ChaCha20Poly1305, "nope"} {
		if err := db.SetEncryptionAlgorithm(ctx, alg); err == nil {
			t.Fatalf("expected unsupported algorithm error for %s", alg)
		}
	}
	if st := db.Status().State; st != StateReady {
		t.Fatalf("state = %s", st)
	}
}

func TestRewriteEncryptedFiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	path := t.TempDir()
	cfg := maintenanceTypedConfig(t, path)
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := TableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		if err := insertRecord(ctx, db, table, &facadeRecord{ID: NewRowID(), Name: "r"}); err != nil {
			t.Fatal(err)
		}
	}
	before, err := table.Where().Count()
	if err != nil {
		t.Fatal(err)
	}
	genBefore := db.Status().StateGeneration
	if err := db.RewriteEncryptedFiles(ctx); err != nil {
		t.Fatal(err)
	}
	if st := db.Status(); st.State != StateReady || st.StateGeneration != genBefore {
		t.Fatalf("after rewrite: %+v", st)
	}
	after, err := table.Where().Count()
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("rows before=%d after=%d", before, after)
	}
	// The store keeps working after maintenance.
	if err := insertRecord(ctx, db, table, &facadeRecord{ID: NewRowID(), Name: "post"}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	// Reopen: data intact, no journal left behind.
	db2, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	table2, err := TableOf[facadeRecord](db2, "records")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := table2.Where().Count(); err != nil || n != 26 {
		t.Fatalf("want 26 rows, got %d, %v", n, err)
	}
}

func TestMaintenanceRejectsWrites(t *testing.T) {
	db, err := openSignedFixture(context.Background(), maintenanceTypedConfig(t, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	table, err := TableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	// Drive the state machine directly: maintenance allows reads, rejects
	// writes with ErrMaintenance.
	db.setState(StateMaintenance)
	if err := db.requireWrite(); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("requireWrite = %v", err)
	}
	if err := db.requireRead(); err != nil {
		t.Fatalf("requireRead = %v", err)
	}
	if err := insertRecord(context.Background(), db, table, &facadeRecord{ID: NewRowID(), Name: "mid"}); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("exec in maintenance = %v", err)
	}
	// Reads serve the in-memory materializer.
	if _, err := table.Where().Count(); err != nil {
		t.Fatalf("query in maintenance: %v", err)
	}
	db.setState(StateReady)
}

func TestEncryptionStatusShape(t *testing.T) {
	ctx := context.Background()
	db, err := openSignedFixture(ctx, testConfig(t.TempDir()))
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
	if st.RegistryGeneration < 1 {
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
