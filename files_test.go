package murmur

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/schema"
)

var testObjectKey = bytes.Repeat([]byte{0x71}, 32)

func fileTestConfig(path string) Config {
	cfg := testConfig(path)
	cfg.Files.Enabled = true
	cfg.Files.ObjectKey = append([]byte(nil), testObjectKey...)
	return cfg
}

func uploadBytes(t *testing.T, db *DB, name string, data []byte) FileInfo {
	t.Helper()
	info, err := db.UploadFile(context.Background(), name, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("upload %q: %v", name, err)
	}
	return info
}

func TestFilesDisabled(t *testing.T) {
	ctx := context.Background()
	db, err := openSignedFixture(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.UploadFile(ctx, "a", bytes.NewReader([]byte("x"))); !errors.Is(err, ErrFilesDisabled) {
		t.Fatalf("upload without Files.Enabled: got %v, want ErrFilesDisabled", err)
	}
	if _, err := db.FileStatus(ctx, "a"); !errors.Is(err, ErrFilesDisabled) {
		t.Fatalf("status without Files.Enabled: got %v, want ErrFilesDisabled", err)
	}
	if _, err := db.OpenFile(ctx, "a"); !errors.Is(err, ErrFilesDisabled) {
		t.Fatalf("open without Files.Enabled: got %v, want ErrFilesDisabled", err)
	}
	if _, err := db.ListFiles(ctx, "", 0); !errors.Is(err, ErrFilesDisabled) {
		t.Fatalf("list without Files.Enabled: got %v, want ErrFilesDisabled", err)
	}
	if err := db.DeleteFile(ctx, "a"); !errors.Is(err, ErrFilesDisabled) {
		t.Fatalf("delete without Files.Enabled: got %v, want ErrFilesDisabled", err)
	}
}

func TestFilesEnabledRequiresKey(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Files.Enabled = true
	if _, err := openSignedFixture(context.Background(), cfg); err == nil {
		t.Fatal("open with Files.Enabled and no key succeeded")
	}
}

func TestFileUploadRoundTrip(t *testing.T) {
	ctx := context.Background()
	db, err := openSignedFixture(ctx, fileTestConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	data := make([]byte, 200_000) // multi-chunk (64 KiB chunks)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	wantDigest := sha256.Sum256(data)

	info := uploadBytes(t, db, "docs/report.bin", data)
	if info.Size != int64(len(data)) {
		t.Fatalf("size = %d, want %d", info.Size, len(data))
	}
	if info.Digest != wantDigest {
		t.Fatal("digest mismatch")
	}
	if info.Chunks != 4 {
		t.Fatalf("chunks = %d, want 4", info.Chunks)
	}

	st, err := db.FileStatus(ctx, "docs/report.bin")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Exists || st.Deleted || !st.Available {
		t.Fatalf("status = %+v, want exists+available", st)
	}
	if st.Size != int64(len(data)) || st.Digest != wantDigest {
		t.Fatal("status metadata mismatch")
	}

	r, err := db.OpenFile(ctx, "docs/report.bin")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	cerr := r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if cerr != nil {
		t.Fatal(cerr)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("streamed bytes differ from upload")
	}
}

func TestTypedFileMetadataPersistsWithoutSQLMaterializer(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	cfg.Files.Enabled = true
	cfg.Files.ObjectKey = append([]byte(nil), testObjectKey...)
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("typed file payload")
	info, err := db.UploadFile(ctx, "typed/report.txt", bytes.NewReader(want))
	if err != nil {
		t.Fatalf("upload through typed database: %v", err)
	}
	status, err := db.FileStatus(ctx, "typed/report.txt")
	if err != nil || !status.Exists || !status.Available || status.Digest != info.Digest || status.Size != int64(len(want)) {
		t.Fatalf("typed file status = %+v, %v", status, err)
	}
	reader, err := db.OpenFile(ctx, "typed/report.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("typed file contents = %q, %v", got, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen typed file database: %v", err)
	}
	defer db.Close()
	status, err = db.FileStatus(ctx, "typed/report.txt")
	if err != nil || !status.Exists || !status.Available || status.Digest != info.Digest {
		t.Fatalf("typed file status after reopen = %+v, %v", status, err)
	}
	if err := db.DeleteFile(ctx, "typed/report.txt"); err != nil {
		t.Fatalf("delete typed file: %v", err)
	}
	status, err = db.FileStatus(ctx, "typed/report.txt")
	if err != nil || !status.Deleted {
		t.Fatalf("typed deleted file status = %+v, %v", status, err)
	}
}

func TestFileListSearch(t *testing.T) {
	ctx := context.Background()
	db, err := openSignedFixture(ctx, fileTestConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	uploadBytes(t, db, "docs/a.txt", []byte("a"))
	uploadBytes(t, db, "docs/b.txt", []byte("bb"))
	uploadBytes(t, db, "img/c.png", []byte("ccc"))

	all, err := db.ListFiles(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 || all[0].Name != "docs/a.txt" || all[1].Name != "docs/b.txt" || all[2].Name != "img/c.png" {
		t.Fatalf("list all = %v, want 3 sorted", all)
	}
	docs, err := db.ListFiles(ctx, "docs/", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 {
		t.Fatalf("list prefix = %d files, want 2", len(docs))
	}
	one, err := db.ListFiles(ctx, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 {
		t.Fatalf("list limit 1 = %d files, want 1", len(one))
	}
	found, err := db.SearchFiles(ctx, ".txt", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 2 {
		t.Fatalf("search = %d files, want 2", len(found))
	}
	if _, err := db.SearchFiles(ctx, ".txt", -1); err != nil {
		t.Fatalf("negative limit: %v", err)
	}
}

func TestFileDeleteTombstone(t *testing.T) {
	ctx := context.Background()
	db, err := openSignedFixture(ctx, fileTestConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	uploadBytes(t, db, "drop/me", []byte("bye"))
	if err := db.DeleteFile(ctx, "drop/me"); err != nil {
		t.Fatal(err)
	}
	st, err := db.FileStatus(ctx, "drop/me")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Exists || !st.Deleted {
		t.Fatalf("after delete: %+v, want exists+deleted", st)
	}
	if _, err := db.OpenFile(ctx, "drop/me"); !errors.Is(err, ErrFileUnavailable) {
		t.Fatalf("open deleted: got %v, want ErrFileUnavailable", err)
	}
	listed, err := db.ListFiles(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 0 {
		t.Fatalf("list after delete = %d files, want 0", len(listed))
	}
	// Idempotent second delete.
	if err := db.DeleteFile(ctx, "drop/me"); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	// Re-upload resurrects the tombstoned row.
	uploadBytes(t, db, "drop/me", []byte("back"))
	st, err = db.FileStatus(ctx, "drop/me")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Exists || st.Deleted || !st.Available {
		t.Fatalf("after re-upload: %+v, want live", st)
	}
	r, err := db.OpenFile(ctx, "drop/me")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	_ = r.Close()
	if string(got) != "back" {
		t.Fatalf("resurrected bytes = %q, want %q", got, "back")
	}
}

func TestFileUnavailableStatus(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	db, err := openSignedFixture(ctx, fileTestConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	info := uploadBytes(t, db, "ghost/data", []byte("here then gone"))
	// Simulate bytes that never arrived locally: metadata stays, the
	// content-addressed object is removed out of band.
	objPath := filepath.Join(path, "files", "objects", info.Digest.String()+".spfo")
	if err := os.Remove(objPath); err != nil {
		t.Fatal(err)
	}
	st, err := db.FileStatus(ctx, "ghost/data")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Exists || st.Deleted || st.Available {
		t.Fatalf("status = %+v, want exists, live, unavailable", st)
	}
	if _, err := db.OpenFile(ctx, "ghost/data"); !errors.Is(err, ErrFileUnavailable) {
		t.Fatalf("open missing-bytes: got %v, want ErrFileUnavailable", err)
	}
	// Unknown names report absent, not an error.
	st, err = db.FileStatus(ctx, "never/existed")
	if err != nil {
		t.Fatal(err)
	}
	if st.Exists {
		t.Fatalf("unknown name status = %+v, want absent", st)
	}
	if _, err := db.OpenFile(ctx, "never/existed"); !errors.Is(err, ErrFileUnavailable) {
		t.Fatalf("open unknown: got %v, want ErrFileUnavailable", err)
	}
}

func TestFileRestartPersistence(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	cfg := fileTestConfig(path)
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("durable across restart")
	uploadBytes(t, db, "keep/me", data)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db2, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	st, err := db2.FileStatus(ctx, "keep/me")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Exists || st.Deleted || !st.Available {
		t.Fatalf("after restart: %+v, want live+available", st)
	}
	r, err := db2.OpenFile(ctx, "keep/me")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	_ = r.Close()
	if !bytes.Equal(got, data) {
		t.Fatal("bytes changed across restart")
	}
}

func TestFilePublishBeforeAck(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	db, err := openSignedFixture(ctx, fileTestConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// The moment UploadFile returns (the commit acknowledgement), the
	// object must already be durable on disk: peers notified after this
	// point can fetch bytes from this node.
	info := uploadBytes(t, db, "early/bird", []byte("published"))
	objPath := filepath.Join(path, "files", "objects", info.Digest.String()+".spfo")
	fi, err := os.Stat(objPath)
	if err != nil {
		t.Fatalf("object missing right after acknowledged upload: %v", err)
	}
	if fi.Size() == 0 {
		t.Fatal("object is empty right after acknowledged upload")
	}
}

func TestFileTooLarge(t *testing.T) {
	ctx := context.Background()
	cfg := fileTestConfig(t.TempDir())
	cfg.Files.MaxFileBytes = 16
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.UploadFile(ctx, "big", bytes.NewReader(make([]byte, 100))); !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("oversize upload: got %v, want ErrFileTooLarge", err)
	}
	st, err := db.FileStatus(ctx, "big")
	if err != nil {
		t.Fatal(err)
	}
	if st.Exists {
		t.Fatal("rejected upload left committed metadata")
	}
	// The capped stream may leave a stray object; GC reclaims it.
	removed, err := db.FilesGC(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 {
		t.Fatalf("GC removed %d objects, want 1 stray", len(removed))
	}
	entries, err := os.ReadDir(filepath.Join(db.cfg.Path, "files", "objects"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("%d objects remain after GC, want 0", len(entries))
	}
}

func TestFilesGCReclaimsDeleted(t *testing.T) {
	ctx := context.Background()
	db, err := openSignedFixture(ctx, fileTestConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	keep := uploadBytes(t, db, "keep", []byte("live"))
	gone := uploadBytes(t, db, "gone", []byte("dead"))
	if err := db.DeleteFile(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	removed, err := db.FilesGC(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 1 || removed[0] != gone.Digest {
		t.Fatalf("GC removed %v, want only the deleted digest", removed)
	}
	st, err := db.FileStatus(ctx, "keep")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Available || st.Digest != keep.Digest {
		t.Fatalf("live file after GC: %+v", st)
	}
}

func TestFileReservedTableCollision(t *testing.T) {
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = append(cfg.Schema.Tables, schema.TableSchema{
		Name:    fileTableName,
		Columns: []schema.ColumnSchema{{Name: "id", Type: schema.ColBlob}},
	})
	cfg.Files.Enabled = true
	cfg.Files.ObjectKey = append([]byte(nil), testObjectKey...)
	if _, err := openSignedFixture(context.Background(), cfg); err == nil {
		t.Fatal("open with app table on the reserved file name succeeded")
	}
}

func TestFileObjectKeyRotation(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	cfg := fileTestConfig(path)
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	uploadBytes(t, db, "rot/a", []byte("first"))
	uploadBytes(t, db, "rot/b", make([]byte, 100_000))

	newKey := bytes.Repeat([]byte{0x55}, 32)
	var progressed int
	if err := db.RotateFileObjectKey(ctx, newKey, func(done, total int) { progressed = done }); err != nil {
		t.Fatal(err)
	}
	if progressed != 2 {
		t.Fatalf("progress reached %d objects, want 2", progressed)
	}
	gen, err := db.FileObjectKeyGeneration()
	if err != nil || gen != 2 {
		t.Fatalf("generation %d err=%v, want 2", gen, err)
	}
	r, err := db.OpenFile(ctx, "rot/a")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	_ = r.Close()
	if string(got) != "first" {
		t.Fatal("rotated bytes differ")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen under the new key: everything readable.
	cfg.Files.ObjectKey = append([]byte(nil), newKey...)
	db2, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	gen, err = db2.FileObjectKeyGeneration()
	if err != nil || gen != 2 {
		t.Fatalf("reopened generation %d err=%v, want 2", gen, err)
	}
	st, err := db2.FileStatus(ctx, "rot/b")
	if err != nil || !st.Available {
		t.Fatalf("reopened status %+v err=%v", st, err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
	// The retired key opens (single generation on disk) but cannot decrypt.
	cfg.Files.ObjectKey = append([]byte(nil), testObjectKey...)
	db3, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db3.Close()
	r, err = db3.OpenFile(ctx, "rot/a")
	if err != nil {
		t.Fatalf("open with retired key: %v", err)
	}
	_, rerr := io.ReadAll(r)
	_ = r.Close()
	if rerr == nil {
		t.Fatal("read with retired key succeeded")
	}
}

func fileBackupSource(t *testing.T, ctx context.Context, dir string) (*DB, Config, map[string][]byte) {
	t.Helper()
	cfg := fileTestConfig(dir)
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	payloads := map[string][]byte{
		"backup/one": []byte("first payload"),
		"backup/two": bytes.Repeat([]byte{0xab}, 100_000),
	}
	for name, data := range payloads {
		uploadBytes(t, db, name, data)
	}
	cfg.DBID = db.DBID()
	return db, cfg, payloads
}

func fileBackupDest(t *testing.T) backup.Destination {
	t.Helper()
	dest, err := backup.NewLocalDestination(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dest
}

func fileRestoreFresh(t *testing.T) NodeID {
	t.Helper()
	return NewNodeID()
}

func TestFileBackupRestoreInclusive(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, cfg, payloads := fileBackupSource(t, ctx, filepath.Join(dir, "primary"))
	dest := fileBackupDest(t)

	meta, err := db.Backup(ctx, backup.Config{Destination: dest, Compression: "gzip", IncludeFiles: true})
	if err != nil {
		t.Fatal(err)
	}
	if meta.FilesMode != backup.FilesModeObjects || meta.FilesObjects != 2 {
		t.Fatalf("backup manifest %+v, want 2 objects", meta)
	}
	backups, err := dest.ListBackups(ctx, cfg.DBID.String())
	if err != nil || len(backups) != 1 {
		t.Fatalf("list %+v err=%v", backups, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	restoredDir := filepath.Join(dir, "restored")
	freshNode := fileRestoreFresh(t)
	report, err := Restore(ctx, backup.RestoreConfig{
		Source: dest, BackupName: backups[0].Name, TargetPath: restoredDir,
		ExpectedDBID: cfg.DBID.String(), FreshNodeID: freshNode.String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.FilesRestoredObjects != 2 || report.FilesSkipped {
		t.Fatalf("restore report %+v", report)
	}
	cfg2 := fileTestConfig(restoredDir)
	cfg2.NodeID = freshNode
	db2, err := openSignedFixture(ctx, cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	for name, want := range payloads {
		st, err := db2.FileStatus(ctx, name)
		if err != nil || !st.Available {
			t.Fatalf("%s status %+v err=%v, want available", name, st, err)
		}
		r, err := db2.OpenFile(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := io.ReadAll(r)
		_ = r.Close()
		if !bytes.Equal(got, want) {
			t.Fatalf("%s bytes differ after restore", name)
		}
	}
}

func TestFileBackupRestoreMetadataOnly(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, cfg, payloads := fileBackupSource(t, ctx, filepath.Join(dir, "primary"))
	dest := fileBackupDest(t)

	meta, err := db.Backup(ctx, backup.Config{Destination: dest, Compression: "gzip"})
	if err != nil {
		t.Fatal(err)
	}
	if meta.FilesMode != "" || meta.FilesObjects != 0 {
		t.Fatalf("backup manifest %+v, want metadata-only", meta)
	}
	backups, err := dest.ListBackups(ctx, cfg.DBID.String())
	if err != nil || len(backups) != 1 {
		t.Fatalf("list %+v err=%v", backups, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	restoredDir := filepath.Join(dir, "restored")
	freshNode := fileRestoreFresh(t)
	report, err := Restore(ctx, backup.RestoreConfig{
		Source: dest, BackupName: backups[0].Name, TargetPath: restoredDir,
		ExpectedDBID: cfg.DBID.String(), FreshNodeID: freshNode.String(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.FilesRestoredObjects != 0 || report.FilesSkipped {
		t.Fatalf("restore report %+v", report)
	}
	cfg2 := fileTestConfig(restoredDir)
	cfg2.NodeID = freshNode
	db2, err := openSignedFixture(ctx, cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	// Metadata restores; payloads stay pending until mesh fetch repairs them.
	for name := range payloads {
		st, err := db2.FileStatus(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		if !st.Exists || st.Deleted || st.Available {
			t.Fatalf("%s status %+v, want exists+pending", name, st)
		}
		if _, err := db2.OpenFile(ctx, name); !errors.Is(err, ErrFileUnavailable) {
			t.Fatalf("%s open: got %v, want ErrFileUnavailable", name, err)
		}
	}
}

func TestFileBackupRestoreSkipFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, cfg, _ := fileBackupSource(t, ctx, filepath.Join(dir, "primary"))
	dest := fileBackupDest(t)

	if _, err := db.Backup(ctx, backup.Config{Destination: dest, Compression: "gzip", IncludeFiles: true}); err != nil {
		t.Fatal(err)
	}
	backups, err := dest.ListBackups(ctx, cfg.DBID.String())
	if err != nil || len(backups) != 1 {
		t.Fatalf("list %+v err=%v", backups, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := Restore(ctx, backup.RestoreConfig{
		Source: dest, BackupName: backups[0].Name, TargetPath: filepath.Join(dir, "restored"),
		ExpectedDBID: cfg.DBID.String(), FreshNodeID: fileRestoreFresh(t).String(), SkipFiles: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.FilesMode != backup.FilesModeObjects || !report.FilesSkipped || report.FilesRestoredObjects != 0 {
		t.Fatalf("restore report %+v", report)
	}
}
