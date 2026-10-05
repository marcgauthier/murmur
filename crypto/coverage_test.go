package crypto

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// TestDiscardLoggerPins the silent logger used by default.
func TestDiscardLoggerPins(t *testing.T) {
	var l discardLogger
	l.Debug("d")
	l.Info("i")
	l.Warn("w")
	l.Error("e")
}

// TestProviderLookup pins ID-scoped key lookup for env and file providers.
func TestProviderLookup(t *testing.T) {
	ctx := context.Background()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MURMUR_TEST_LOOKUP_KEY", hex.EncodeToString(key))
	env := &EnvProvider{Var: "MURMUR_TEST_LOOKUP_KEY", ID: "k1"}
	for _, id := range []string{"", "k1"} {
		if _, err := env.Lookup(ctx, id); err != nil {
			t.Fatalf("env lookup %q: %v", id, err)
		}
	}
	if _, err := env.Lookup(ctx, "other"); err == nil {
		t.Fatal("env lookup of unknown ID succeeded")
	}
	path := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(path, []byte(hex.EncodeToString(key)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	file := &FileProvider{Path: path, ID: "k1"}
	for _, id := range []string{"", "k1"} {
		if _, err := file.Lookup(ctx, id); err != nil {
			t.Fatalf("file lookup %q: %v", id, err)
		}
	}
	if _, err := file.Lookup(ctx, "other"); err == nil {
		t.Fatal("file lookup of unknown ID succeeded")
	}
}

// TestEncryptedFSPassthrough covers the vfs.FS surface forwarded to the
// base filesystem plus WAL-recycle reuse.
func TestEncryptedFSPassthrough(t *testing.T) {
	efs, _ := testFS(t, vfs.NewMem())
	if efs.Unwrap() == nil {
		t.Fatal("Unwrap is nil")
	}
	writeFileSync(t, efs, "/a.sst", []byte("payload"))
	if err := efs.Link("/a.sst", "/b.sst"); err != nil {
		t.Fatalf("Link: %v", err)
	}
	if got := readFileAll(t, efs, "/b.sst"); string(got) != "payload" {
		t.Fatalf("linked read = %q", got)
	}
	if err := efs.RemoveAll("/a.sst"); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if err := efs.RemoveAll("/missing"); err != nil {
		t.Fatalf("RemoveAll(missing): %v", err)
	}
	var cat vfs.DiskWriteCategory
	f, err := efs.ReuseForWrite("/old.log", "/new.log", cat)
	if err != nil {
		t.Fatalf("ReuseForWrite: %v", err)
	}
	if _, err := f.Write([]byte("wal")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readFileAll(t, efs, "/new.log"); string(got) != "wal" {
		t.Fatalf("recycled read = %q", got)
	}
}

// TestRegistryReplaceProvider proves the key provider swaps (nil ignored).
func TestRegistryReplaceProvider(t *testing.T) {
	dir := t.TempDir()
	reg, err := OpenRegistry(dir, testProvider(), testDBID(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reg.Close)
	reg.ReplaceProvider(nil)
	reg.ReplaceProvider(testProvider())
}
