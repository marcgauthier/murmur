package crypto

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// writeSourceFile writes content through fs and syncs it durable.
func writeSourceFile(t *testing.T, fs *EncryptedFS, path string, content []byte) {
	t.Helper()
	f, err := fs.Create(path, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(content); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func readLogicalFile(t *testing.T, fs *EncryptedFS, path string) []byte {
	t.Helper()
	f, err := fs.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out, err := io.ReadAll(fileReader{f: f})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// setupRebindStore creates keys/ + data/ with two containers and a LOCK
// file under the source DBID, and returns the dirs and identities.
func setupRebindStore(t *testing.T) (keysDir, dataDir string, prov *MapProvider, source, target [16]byte) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	keysDir = filepath.Join(dir, "keys")
	dataDir = filepath.Join(dir, "data")
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		t.Fatal(err)
	}
	prov = testProvider()
	source = testDBID(t)
	target = testDBID(t)
	reg, err := OpenRegistry(keysDir, prov, source)
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	srcFS, err := NewEncryptedFS(FSOptions{Base: vfs.Default, Registry: reg, DBID: source})
	if err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, srcFS, filepath.Join(dataDir, "000001.log"), []byte("wal-bytes-1"))
	writeSourceFile(t, srcFS, filepath.Join(dataDir, "000002.sst"), []byte("sstable-bytes-2"))
	if err := os.WriteFile(filepath.Join(dataDir, "LOCK"), []byte("lock"), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = ctx
	return keysDir, dataDir, prov, source, target
}

func openRebound(t *testing.T, keysDir string, prov *MapProvider, dbID [16]byte) *Registry {
	t.Helper()
	reg, err := OpenRegistry(keysDir, prov, dbID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reg.Close)
	return reg
}

// TestRebindStoreRoundTrip proves files and registry move to the new DBID
// with logical content preserved and the source identity locked out.
func TestRebindStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	keysDir, dataDir, prov, source, target := setupRebindStore(t)
	if err := RebindStore(ctx, RebindOptions{
		RegDir: keysDir, Roots: []string{dataDir}, Provider: prov,
		SourceDBID: source, NewDBID: target,
	}); err != nil {
		t.Fatal(err)
	}
	// Registry opens under the new DBID, no longer under the source.
	newReg := openRebound(t, keysDir, prov, target)
	if _, err := OpenRegistry(keysDir, prov, source); err == nil {
		t.Fatal("rebound registry still opens under source DBID")
	}
	newFS, err := NewEncryptedFS(FSOptions{Base: vfs.Default, Registry: newReg, DBID: target})
	if err != nil {
		t.Fatal(err)
	}
	if got := readLogicalFile(t, newFS, filepath.Join(dataDir, "000001.log")); string(got) != "wal-bytes-1" {
		t.Fatalf("rebound file 1 = %q", got)
	}
	if got := readLogicalFile(t, newFS, filepath.Join(dataDir, "000002.sst")); string(got) != "sstable-bytes-2" {
		t.Fatalf("rebound file 2 = %q", got)
	}
	// The source view fails closed on rebound containers.
	srcReg := openRebound(t, keysDir, prov, target) // keys only; FS binds source below
	srcFS, err := NewEncryptedFS(FSOptions{Base: vfs.Default, Registry: srcReg, DBID: source})
	if err != nil {
		t.Fatal(err)
	}
	if f, err := srcFS.Open(filepath.Join(dataDir, "000001.log")); err == nil {
		f.Close()
		t.Fatal("rebound file still opens under source DBID")
	}
	// Plaintext metadata is untouched.
	if raw, err := os.ReadFile(filepath.Join(dataDir, "LOCK")); err != nil || string(raw) != "lock" {
		t.Fatalf("LOCK = %q, %v", raw, err)
	}
}

// TestRebindStoreIdempotent proves a second run (crash retry after
// success) is a no-op with content intact.
func TestRebindStoreIdempotent(t *testing.T) {
	ctx := context.Background()
	keysDir, dataDir, prov, source, target := setupRebindStore(t)
	opt := RebindOptions{
		RegDir: keysDir, Roots: []string{dataDir}, Provider: prov,
		SourceDBID: source, NewDBID: target,
	}
	if err := RebindStore(ctx, opt); err != nil {
		t.Fatal(err)
	}
	if err := RebindStore(ctx, opt); err != nil {
		t.Fatal(err)
	}
	newReg := openRebound(t, keysDir, prov, target)
	newFS, err := NewEncryptedFS(FSOptions{Base: vfs.Default, Registry: newReg, DBID: target})
	if err != nil {
		t.Fatal(err)
	}
	if got := readLogicalFile(t, newFS, filepath.Join(dataDir, "000002.sst")); string(got) != "sstable-bytes-2" {
		t.Fatalf("file after re-rebind = %q", got)
	}
}

// TestRebindResumesPartialFiles simulates a crash after the first file
// rebound but before the registry persisted, proving the next run
// converges: the rebound file is skipped, the rest rebind, and the
// registry moves last.
func TestRebindResumesPartialFiles(t *testing.T) {
	ctx := context.Background()
	keysDir, dataDir, prov, source, target := setupRebindStore(t)
	srcReg, err := OpenRegistry(keysDir, prov, source)
	if err != nil {
		t.Fatal(err)
	}
	srcFS, err := NewEncryptedFS(FSOptions{Base: vfs.Default, Registry: srcReg, DBID: source})
	if err != nil {
		t.Fatal(err)
	}
	dstFS, err := NewEncryptedFS(FSOptions{Base: vfs.Default, Registry: srcReg, DBID: target})
	if err != nil {
		t.Fatal(err)
	}
	// Crash after exactly one file: registry still source-bound.
	if err := rebindFile(vfs.Default, srcFS, dstFS, filepath.Join(dataDir, "000001.log")); err != nil {
		t.Fatal(err)
	}
	srcReg.Close()
	if _, err := OpenRegistry(keysDir, prov, target); err == nil {
		t.Fatal("registry moved before files completed")
	}
	// Retry converges.
	if err := RebindStore(ctx, RebindOptions{
		RegDir: keysDir, Roots: []string{dataDir}, Provider: prov,
		SourceDBID: source, NewDBID: target,
	}); err != nil {
		t.Fatal(err)
	}
	newReg := openRebound(t, keysDir, prov, target)
	newFS, err := NewEncryptedFS(FSOptions{Base: vfs.Default, Registry: newReg, DBID: target})
	if err != nil {
		t.Fatal(err)
	}
	if got := readLogicalFile(t, newFS, filepath.Join(dataDir, "000001.log")); string(got) != "wal-bytes-1" {
		t.Fatalf("file 1 = %q", got)
	}
	if got := readLogicalFile(t, newFS, filepath.Join(dataDir, "000002.sst")); string(got) != "sstable-bytes-2" {
		t.Fatalf("file 2 = %q", got)
	}
}

// TestRebindStoreMissingRegistry proves a store with nothing DBID-bound
// is a no-op.
func TestRebindStoreMissingRegistry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	if err := RebindStore(ctx, RebindOptions{
		RegDir: filepath.Join(dir, "keys"), Roots: []string{filepath.Join(dir, "data")},
		Provider: testProvider(), SourceDBID: testDBID(t), NewDBID: testDBID(t),
	}); err != nil {
		t.Fatal(err)
	}
}

// TestRebindStoreCorruptFailsClosed proves garbage fails closed instead
// of rebinding.
func TestRebindStoreCorruptFailsClosed(t *testing.T) {
	ctx := context.Background()
	keysDir, dataDir, prov, source, target := setupRebindStore(t)
	if err := os.WriteFile(filepath.Join(keysDir, RegistryFileName), []byte("garbage-not-a-registry"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := RebindStore(ctx, RebindOptions{
		RegDir: keysDir, Roots: []string{dataDir}, Provider: prov,
		SourceDBID: source, NewDBID: target,
	})
	if err == nil {
		t.Fatal("corrupt registry rebound without error")
	}
}
