package backup

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func setupFilesMockDB(t *testing.T) *mockDB {
	t.Helper()
	m := setupMockDB(t)
	filesDir := filepath.Join(t.TempDir(), "files")
	objects := filepath.Join(filesDir, "objects")
	if err := os.MkdirAll(objects, 0o700); err != nil {
		t.Fatal(err)
	}
	// Two uniform gen-1 objects plus residue that must never enter the
	// archive.
	obj1 := strings.Repeat("a", 64) + ".spfo"
	obj2 := strings.Repeat("b", 64) + ".spfo"
	if err := os.WriteFile(filepath.Join(objects, obj1), []byte("ciphertext-one"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objects, obj2), []byte("ciphertext-two"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objects, ".stage-tmp"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objects, "tmp_rot_x"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(filesDir, "staging"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filesDir, "staging", "partial.part"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	m.filesDir = filesDir
	return m
}

func backupToLocal(t *testing.T, ctx context.Context, src SourceDB, cfg Config) (Metadata, *LocalDestination) {
	t.Helper()
	dest, err := NewLocalDestination(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg.Destination = dest
	meta, err := CreateBackup(ctx, src, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return *meta, dest
}

func TestBackupFilesInclusive(t *testing.T) {
	ctx := context.Background()
	m := setupFilesMockDB(t)
	meta, dest := backupToLocal(t, ctx, m, Config{Compression: "gzip", IncludeFiles: true})
	if meta.FilesMode != FilesModeObjects {
		t.Fatalf("files mode %q, want %q", meta.FilesMode, FilesModeObjects)
	}
	if meta.FilesObjects != 2 || meta.FilesBytes != int64(len("ciphertext-one")+len("ciphertext-two")) {
		t.Fatalf("files manifest %+v", meta)
	}
	backups, err := dest.ListBackups(ctx, m.dbID)
	if err != nil || len(backups) != 1 {
		t.Fatalf("list %+v err=%v", backups, err)
	}
	target := t.TempDir()
	restored, err := Restore(ctx, RestoreConfig{
		Source:      dest,
		BackupName:  backups[0].Name,
		TargetPath:  target,
		FreshNodeID: testFreshNodeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if restored.FilesRestoredObjects != 2 || restored.FilesSkipped {
		t.Fatalf("restore report %+v", restored)
	}
	for _, name := range []string{strings.Repeat("a", 64) + ".spfo", strings.Repeat("b", 64) + ".spfo"} {
		if _, err := os.Stat(filepath.Join(target, "files", "objects", name)); err != nil {
			t.Fatalf("restored object %s: %v", name, err)
		}
	}
	// Residue never enters the archive.
	for _, name := range []string{".stage-tmp", "tmp_rot_x", "staging"} {
		if _, err := os.Stat(filepath.Join(target, "files", "objects", name)); !os.IsNotExist(err) {
			t.Fatalf("residue %s restored", name)
		}
	}
	if _, err := os.Stat(filepath.Join(target, "files", "staging")); !os.IsNotExist(err) {
		t.Fatal("staging dir restored")
	}
}

func TestBackupFilesMetadataOnly(t *testing.T) {
	ctx := context.Background()
	m := setupFilesMockDB(t)
	meta, dest := backupToLocal(t, ctx, m, Config{Compression: "gzip"})
	if meta.FilesMode != "" || meta.FilesObjects != 0 {
		t.Fatalf("files manifest %+v, want metadata-only", meta)
	}
	backups, err := dest.ListBackups(ctx, m.dbID)
	if err != nil || len(backups) != 1 {
		t.Fatalf("list %+v err=%v", backups, err)
	}
	target := t.TempDir()
	restored, err := Restore(ctx, RestoreConfig{
		Source:      dest,
		BackupName:  backups[0].Name,
		TargetPath:  target,
		FreshNodeID: testFreshNodeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if restored.FilesRestoredObjects != 0 || restored.FilesSkipped {
		t.Fatalf("restore report %+v", restored)
	}
	entries, err := os.ReadDir(filepath.Join(target, "files", "objects"))
	if err == nil && len(entries) != 0 {
		t.Fatalf("%d objects restored from metadata-only backup", len(entries))
	}
}

func TestBackupFilesSkipOnRestore(t *testing.T) {
	ctx := context.Background()
	m := setupFilesMockDB(t)
	_, dest := backupToLocal(t, ctx, m, Config{Compression: "gzip", IncludeFiles: true})
	backups, err := dest.ListBackups(ctx, m.dbID)
	if err != nil || len(backups) != 1 {
		t.Fatalf("list %+v err=%v", backups, err)
	}
	target := t.TempDir()
	restored, err := Restore(ctx, RestoreConfig{
		Source:      dest,
		BackupName:  backups[0].Name,
		TargetPath:  target,
		FreshNodeID: testFreshNodeID,
		SkipFiles:   true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if restored.FilesMode != FilesModeObjects || !restored.FilesSkipped || restored.FilesRestoredObjects != 0 {
		t.Fatalf("restore report %+v", restored)
	}
}

func TestBackupFilesWithoutStore(t *testing.T) {
	ctx := context.Background()
	m := setupMockDB(t) // no filesDir
	dest, err := NewLocalDestination(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateBackup(ctx, m, Config{Destination: dest, IncludeFiles: true}); err == nil {
		t.Fatal("object-inclusive backup without a file store succeeded")
	}
}

func TestBackupFilesMixedGenerations(t *testing.T) {
	ctx := context.Background()
	m := setupMockDB(t)
	filesDir := filepath.Join(t.TempDir(), "files")
	objects := filepath.Join(filesDir, "objects")
	if err := os.MkdirAll(objects, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objects, strings.Repeat("a", 64)+".spfo"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(objects, strings.Repeat("b", 64)+".g2.spfo"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	m.filesDir = filesDir
	dest, err := NewLocalDestination(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// A live rotation mid-snapshot must fail loudly, never ship mixed.
	if _, err := CreateBackup(ctx, m, Config{Destination: dest, IncludeFiles: true}); err == nil {
		t.Fatal("backup of mixed generations succeeded")
	}
}

func TestSplitObjectName(t *testing.T) {
	digest := strings.Repeat("c", 64)
	for _, tc := range []struct {
		name string
		gen  uint32
		ok   bool
	}{
		{digest + ".spfo", 1, true},
		{digest + ".g2.spfo", 2, true},
		{digest + ".g12.spfo", 12, true},
		{digest + ".g1.spfo", 0, false},
		{digest + ".g0.spfo", 0, false},
		{digest + ".x2.spfo", 0, false},
		{digest + ".spfo.bak", 0, false},
		{"short.spfo", 0, false},
		{strings.Repeat("z", 64) + ".spfo", 0, false},
		{".stage-x", 0, false},
		{"tmp_rot_x", 0, false},
		{generationFileName(), 1, false},
	} {
		_, gen, ok := splitObjectName(tc.name)
		if ok != tc.ok || (ok && gen != tc.gen) {
			t.Fatalf("%s: gen=%d ok=%v", tc.name, gen, ok)
		}
	}
}

// generationFileName keeps the marker filename consistent with the snapshot.
func generationFileName() string { return "generation" }
