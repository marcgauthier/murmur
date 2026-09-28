package backup

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRestoreWritesIntent(t *testing.T) {
	ctx := context.Background()
	db := setupMockDB(t)
	backupDir := filepath.Join(t.TempDir(), "backups")
	localDest, err := NewLocalDestination(backupDir)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := CreateBackup(ctx, db, Config{Destination: localDest, Compression: "gzip"})
	if err != nil {
		t.Fatal(err)
	}
	backups, err := localDest.ListBackups(ctx, db.dbID)
	if err != nil || len(backups) != 1 {
		t.Fatalf("list = %v, %v", backups, err)
	}

	targetDir := filepath.Join(t.TempDir(), "restored")
	if _, err := Restore(ctx, RestoreConfig{
		Source:      localDest,
		BackupName:  backups[0].Name,
		TargetPath:  targetDir,
		FreshNodeID: testFreshNodeID,
	}); err != nil {
		t.Fatal(err)
	}
	intent, err := ReadRestoreIntent(targetDir)
	if err != nil {
		t.Fatal(err)
	}
	if intent == nil {
		t.Fatal("no intent written")
	}
	if intent.Mode != string(RestoreClone) || intent.BackupID != meta.BackupID ||
		intent.SourceNodeID != db.nodeID || intent.SourceDBID != db.dbID ||
		intent.FreshNodeID != testFreshNodeID || intent.BackupName != backups[0].Name {
		t.Fatalf("unexpected intent: %+v", intent)
	}
	if intent.CreatedAt.IsZero() {
		t.Fatal("intent has no timestamp")
	}
}

func TestRestoreRequiresFreshIdentity(t *testing.T) {
	ctx := context.Background()
	db := setupMockDB(t)
	backupDir := filepath.Join(t.TempDir(), "backups")
	localDest, err := NewLocalDestination(backupDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateBackup(ctx, db, Config{Destination: localDest}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		fresh string
	}{
		{"missing", ""},
		{"invalid", "not-a-uuid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Restore(ctx, RestoreConfig{
				Source:      localDest,
				TargetPath:  filepath.Join(t.TempDir(), "restored"),
				FreshNodeID: tc.fresh,
			})
			if !errors.Is(err, ErrRestoreIdentityRequired) {
				t.Fatalf("err = %v, want ErrRestoreIdentityRequired", err)
			}
		})
	}
}

func TestRestoreRejectsSourceReuse(t *testing.T) {
	ctx := context.Background()
	db := setupMockDB(t)
	db.nodeID = testFreshNodeID // backup sourced from the "fresh" identity
	backupDir := filepath.Join(t.TempDir(), "backups")
	localDest, err := NewLocalDestination(backupDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateBackup(ctx, db, Config{Destination: localDest}); err != nil {
		t.Fatal(err)
	}
	_, err = Restore(ctx, RestoreConfig{
		Source:      localDest,
		TargetPath:  filepath.Join(t.TempDir(), "restored"),
		FreshNodeID: testFreshNodeID,
	})
	if !errors.Is(err, ErrRestoreIdentityReuse) {
		t.Fatalf("err = %v, want ErrRestoreIdentityReuse", err)
	}
}

func TestRestoreReseed(t *testing.T) {
	ctx := context.Background()
	db := setupMockDB(t)
	backupDir := filepath.Join(t.TempDir(), "backups")
	localDest, err := NewLocalDestination(backupDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateBackup(ctx, db, Config{Destination: localDest}); err != nil {
		t.Fatal(err)
	}
	const newDBID = "223e4567-e89b-12d3-a456-426614174001"
	targetDir := filepath.Join(t.TempDir(), "restored")
	if _, err := Restore(ctx, RestoreConfig{
		Source:      localDest,
		TargetPath:  targetDir,
		Mode:        RestoreReseed,
		FreshNodeID: testFreshNodeID,
		NewDBID:     newDBID,
	}); err != nil {
		t.Fatalf("reseed restore: %v", err)
	}
	intent, err := ReadRestoreIntent(targetDir)
	if err != nil {
		t.Fatal(err)
	}
	if intent.Mode != string(RestoreReseed) || intent.NewDBID != newDBID ||
		intent.FreshNodeID != testFreshNodeID {
		t.Fatalf("unexpected reseed intent: %+v", intent)
	}

	// Missing or reused reseed DBID fails closed.
	for _, tc := range []struct {
		name string
		dbid string
	}{
		{"missing", ""},
		{"invalid", "not-a-uuid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Restore(ctx, RestoreConfig{
				Source:      localDest,
				TargetPath:  filepath.Join(t.TempDir(), "restored"),
				Mode:        RestoreReseed,
				FreshNodeID: testFreshNodeID,
				NewDBID:     tc.dbid,
			})
			if !errors.Is(err, ErrRestoreIdentityRequired) {
				t.Fatalf("err = %v, want ErrRestoreIdentityRequired", err)
			}
		})
	}
	db2 := setupMockDB(t)
	db2.dbID = newDBID // backup already at the "new" identity
	backupDir2 := filepath.Join(t.TempDir(), "backups")
	localDest2, err := NewLocalDestination(backupDir2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateBackup(ctx, db2, Config{Destination: localDest2}); err != nil {
		t.Fatal(err)
	}
	_, err = Restore(ctx, RestoreConfig{
		Source:      localDest2,
		TargetPath:  filepath.Join(t.TempDir(), "restored"),
		Mode:        RestoreReseed,
		FreshNodeID: testFreshNodeID,
		NewDBID:     newDBID,
	})
	if !errors.Is(err, ErrRestoreIdentityReuse) {
		t.Fatalf("reseed-reuse err = %v, want ErrRestoreIdentityReuse", err)
	}
}

func TestRestoreBogusModeFailsClosed(t *testing.T) {
	ctx := context.Background()
	_, err := Restore(ctx, RestoreConfig{
		Mode:        RestoreMode("bogus"),
		FreshNodeID: testFreshNodeID,
		TargetPath:  filepath.Join(t.TempDir(), "restored"),
	})
	if !errors.Is(err, ErrRestoreIntentInvalid) {
		t.Fatalf("bogus mode err = %v, want ErrRestoreIntentInvalid", err)
	}
	// NewDBID is meaningless for clone restores (DBID is preserved).
	db := setupMockDB(t)
	backupDir := filepath.Join(t.TempDir(), "backups")
	localDest, err := NewLocalDestination(backupDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateBackup(ctx, db, Config{Destination: localDest}); err != nil {
		t.Fatal(err)
	}
	_, err = Restore(ctx, RestoreConfig{
		Source:      localDest,
		TargetPath:  filepath.Join(t.TempDir(), "restored"),
		FreshNodeID: testFreshNodeID,
		NewDBID:     "db-new",
	})
	if err == nil {
		t.Fatal("clone with NewDBID succeeded, want error")
	}
}

func TestRestoreIntentRoundTrip(t *testing.T) {
	dir := t.TempDir()
	intent, err := ReadRestoreIntent(dir)
	if err != nil || intent != nil {
		t.Fatalf("missing intent = (%v, %v), want (nil, nil)", intent, err)
	}
	want := &RestoreIntent{
		Version:      restoreIntentVersion,
		Mode:         string(RestoreClone),
		BackupID:     "b1",
		BackupName:   "n1",
		SourceNodeID: "s1",
		SourceDBID:   "d1",
		FreshNodeID:  "f1",
	}
	if err := WriteRestoreIntent(dir, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadRestoreIntent(dir)
	if err != nil {
		t.Fatal(err)
	}
	if *got != *want {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
	// Corrupt and version-skewed intents fail closed.
	if err := os.WriteFile(filepath.Join(dir, RestoreIntentFileName), []byte("{nope"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRestoreIntent(dir); !errors.Is(err, ErrRestoreIntentInvalid) {
		t.Fatalf("corrupt intent err = %v", err)
	}
	want.Version = 999
	if err := WriteRestoreIntent(dir, want); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRestoreIntent(dir); !errors.Is(err, ErrRestoreIntentInvalid) {
		t.Fatalf("version-skewed intent err = %v", err)
	}
}
