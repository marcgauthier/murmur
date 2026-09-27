package crypto

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

type pebbleDiscardLogger struct{}

func (pebbleDiscardLogger) Infof(string, ...interface{})    {}
func (pebbleDiscardLogger) Warningf(string, ...interface{}) {}
func (pebbleDiscardLogger) Errorf(string, ...interface{})   {}
func (pebbleDiscardLogger) Fatalf(string, ...interface{})   {}

// TestPebbleOverEncryptedFS is the integration proof: a real Pebble database
// runs on the encrypted VFS (flushes, WALs, manifest, reopen).
func TestPebbleOverEncryptedFS(t *testing.T) {
	dir := t.TempDir()
	regDir := filepath.Join(dir, "keys")
	reg, err := OpenRegistry(regDir, testProvider(), testDBID(t))
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	var dbID [16]byte
	copy(dbID[:], reg.dbID[:])
	efs, err := NewEncryptedFS(FSOptions{Base: vfs.Default, Registry: reg, DBID: dbID})
	if err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(dir, "data")
	walDir := filepath.Join(dir, "wal")

	open := func() *pebble.DB {
		t.Helper()
		opt := &pebble.Options{
			FS:           efs,
			Logger:       pebbleDiscardLogger{},
			CacheSize:    4 << 20,
			MemTableSize: 1 << 20,
			WALDir:       walDir,
		}
		opt.EnsureDefaults()
		db, err := pebble.Open(dataDir, opt)
		if err != nil {
			t.Fatal(err)
		}
		return db
	}

	db := open()
	const n = 5000
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%06d", i))
		v := []byte(fmt.Sprintf("value-%06d-padding-padding", i))
		if err := db.Set(k, v, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// No plaintext anywhere in the data or WAL dirs.
	marker := []byte("value-000123-padding")
	for _, root := range []string{dataDir, walDir} {
		filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if bytes.Contains(raw, marker) || bytes.Contains(raw, []byte("key-000123")) {
				t.Errorf("plaintext in %s", path)
			}
			return nil
		})
	}

	db = open()
	defer db.Close()
	for i := 0; i < n; i += 499 {
		k := []byte(fmt.Sprintf("key-%06d", i))
		v, closer, err := db.Get(k)
		if err != nil {
			t.Fatalf("get %s: %v", k, err)
		}
		if !bytes.Equal(v, []byte(fmt.Sprintf("value-%06d-padding-padding", i))) {
			t.Fatalf("value mismatch for %s", k)
		}
		closer.Close()
	}
	// Full iteration.
	it, err := db.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for it.First(); it.Valid(); it.Next() {
		count++
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	it.Close()
	if count != n {
		t.Fatalf("iterated %d, want %d", count, n)
	}
	t.Logf("fs stats: %+v", efs.Stats())
}

// TestPebbleEncryptedCorruption fails closed when ciphertext is damaged.
func TestPebbleEncryptedCorruption(t *testing.T) {
	setup := func(t *testing.T) (string, string, string) {
		dir := t.TempDir()
		regDir := filepath.Join(dir, "keys")
		reg, err := OpenRegistry(regDir, testProvider(), testDBID(t))
		if err != nil {
			t.Fatal(err)
		}
		defer reg.Close()
		var dbID [16]byte
		copy(dbID[:], reg.dbID[:])
		efs, err := NewEncryptedFS(FSOptions{Base: vfs.Default, Registry: reg, DBID: dbID})
		if err != nil {
			t.Fatal(err)
		}
		dataDir := filepath.Join(dir, "data")
		opt := &pebble.Options{FS: efs, Logger: pebbleDiscardLogger{}}
		opt.EnsureDefaults()
		db, err := pebble.Open(dataDir, opt)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2000; i++ {
			if err := db.Set([]byte(fmt.Sprintf("k%05d", i)), []byte(fmt.Sprintf("v%05d", i)), pebble.Sync); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		// Persist dbID for reopen.
		if err := os.WriteFile(filepath.Join(dir, "dbid"), dbID[:], 0o600); err != nil {
			t.Fatal(err)
		}
		return dir, regDir, dataDir
	}
	reopen := func(t *testing.T, dir, regDir, dataDir string) error {
		dbIDRaw, err := os.ReadFile(filepath.Join(dir, "dbid"))
		if err != nil {
			t.Fatal(err)
		}
		var dbID [16]byte
		copy(dbID[:], dbIDRaw)
		reg, err := OpenRegistry(regDir, testProvider(), dbID)
		if err != nil {
			return err
		}
		defer reg.Close()
		efs, err := NewEncryptedFS(FSOptions{Base: vfs.Default, Registry: reg, DBID: dbID})
		if err != nil {
			return err
		}
		opt := &pebble.Options{FS: efs, Logger: pebbleDiscardLogger{}}
		opt.EnsureDefaults()
		db, err := pebble.Open(dataDir, opt)
		if err != nil {
			return err
		}
		defer db.Close()
		// Force reads across all sstables.
		it, err := db.NewIter(nil)
		if err != nil {
			return err
		}
		defer it.Close()
		for it.First(); it.Valid(); it.Next() {
		}
		return it.Error()
	}

	t.Run("manifest", func(t *testing.T) {
		dir, regDir, dataDir := setup(t)
		manifests, _ := filepath.Glob(filepath.Join(dataDir, "MANIFEST-*"))
		if len(manifests) == 0 {
			t.Fatal("no manifest")
		}
		for _, m := range manifests {
			raw, _ := os.ReadFile(m)
			raw[len(raw)/2] ^= 0x01
			os.WriteFile(m, raw, 0o600)
		}
		if err := reopen(t, dir, regDir, dataDir); err == nil {
			t.Fatal("expected open failure on corrupt manifest")
		} else {
			t.Logf("got: %v", err)
		}
	})

	t.Run("sstable", func(t *testing.T) {
		dir, regDir, dataDir := setup(t)
		ssts, _ := filepath.Glob(filepath.Join(dataDir, "*.sst"))
		if len(ssts) == 0 {
			t.Fatal("no sstables")
		}
		raw, _ := os.ReadFile(ssts[0])
		raw[len(raw)-100] ^= 0x01
		os.WriteFile(ssts[0], raw, 0o600)
		if err := reopen(t, dir, regDir, dataDir); err == nil {
			t.Fatal("expected failure on corrupt sstable")
		} else {
			t.Logf("got: %v", err)
		}
	})

	t.Run("wrong-key", func(t *testing.T) {
		dir, regDir, _ := setup(t)
		dbIDRaw, _ := os.ReadFile(filepath.Join(dir, "dbid"))
		var dbID [16]byte
		copy(dbID[:], dbIDRaw)
		wrong := &MapProvider{Keys: map[string][]byte{"s1": bytesRepeat(0xee, 32)}, CurrentID: "s1"}
		if _, err := OpenRegistry(regDir, wrong, dbID); !errors.Is(err, ErrAuth) {
			t.Fatalf("expected auth error, got %v", err)
		}
	})
}
