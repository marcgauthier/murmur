package objectstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func installTestStores(t *testing.T) (*Store, *Store, string) {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	root1 := t.TempDir()
	root2 := t.TempDir()
	s1, err := New(root1, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s1.Close() })
	s2, err := New(root2, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	return s1, s2, root1
}

func stageCopy(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestInstallVerifiedRoundTrip(t *testing.T) {
	ctx := context.Background()
	s1, s2, root1 := installTestStores(t)
	data := make([]byte, 200_000)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	info, err := s1.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	staged := filepath.Join(t.TempDir(), "obj.part")
	stageCopy(t, s1.objectPath(info.Digest), staged)

	got, err := s2.InstallVerified(ctx, staged, info.Digest, info.Length)
	if err != nil {
		t.Fatal(err)
	}
	if got != info {
		t.Fatalf("installed %+v, want %+v", got, info)
	}
	if _, err := os.Stat(staged); !os.IsNotExist(err) {
		t.Fatal("staged file survives successful install")
	}
	if !s2.Has(info.Digest) {
		t.Fatal("installed object not present")
	}
	rinfo, err := s2.Read(ctx, info.Digest, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if rinfo != info {
		t.Fatalf("read back %+v, want %+v", rinfo, info)
	}
	_ = root1
}

func TestInstallVerifiedCorrupt(t *testing.T) {
	ctx := context.Background()
	s1, s2, _ := installTestStores(t)
	info, err := s1.Put(ctx, bytes.NewReader([]byte("genuine bytes here")))
	if err != nil {
		t.Fatal(err)
	}
	staged := filepath.Join(t.TempDir(), "obj.part")
	stageCopy(t, s1.objectPath(info.Digest), staged)
	// Tamper one container byte in the middle.
	raw, err := os.ReadFile(staged)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xff
	if err := os.WriteFile(staged, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s2.InstallVerified(ctx, staged, info.Digest, info.Length); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("corrupt install: got %v, want ErrInvalidObject", err)
	}
	if s2.Has(info.Digest) {
		t.Fatal("corrupt install published an object")
	}
	if _, err := os.Stat(staged); err != nil {
		t.Fatal("failed install must leave staging for the caller")
	}
}

func TestInstallVerifiedDuplicate(t *testing.T) {
	ctx := context.Background()
	s1, s2, _ := installTestStores(t)
	info, err := s1.Put(ctx, bytes.NewReader([]byte("shared content")))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		staged := filepath.Join(t.TempDir(), "obj.part")
		stageCopy(t, s1.objectPath(info.Digest), staged)
		got, err := s2.InstallVerified(ctx, staged, info.Digest, info.Length)
		if err != nil {
			t.Fatalf("install %d: %v", i, err)
		}
		if got != info {
			t.Fatalf("install %d: %+v, want %+v", i, got, info)
		}
	}
}

func TestInstallVerifiedWrongExpectations(t *testing.T) {
	ctx := context.Background()
	s1, s2, _ := installTestStores(t)
	info, err := s1.Put(ctx, bytes.NewReader([]byte("twelve bytes")))
	if err != nil {
		t.Fatal(err)
	}
	staged := filepath.Join(t.TempDir(), "obj.part")
	stageCopy(t, s1.objectPath(info.Digest), staged)
	if _, err := s2.InstallVerified(ctx, staged, info.Digest, info.Length+1); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("wrong length: got %v, want ErrInvalidObject", err)
	}
	var other Digest
	other[0] = info.Digest[0] ^ 0xff
	if _, err := s2.InstallVerified(ctx, staged, other, info.Length); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("wrong digest: got %v, want ErrInvalidObject", err)
	}
	if s2.Has(info.Digest) || s2.Has(other) {
		t.Fatal("failed install published an object")
	}
}

func TestServeRanges(t *testing.T) {
	ctx := context.Background()
	s1, _, _ := installTestStores(t)
	data := make([]byte, 100_000)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	info, err := s1.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	obj, err := s1.Serve(info.Digest)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	raw, err := io.ReadAll(io.NewSectionReader(obj, 0, obj.Size()))
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(s1.objectPath(info.Digest))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, want) {
		t.Fatal("served bytes differ from the container")
	}
	var missing Digest
	missing[0] = 0xab
	if _, err := s1.Serve(missing); !os.IsNotExist(err) {
		t.Fatalf("serve missing: got %v, want not-exist", err)
	}
}
