package crypto

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestStaticProvider(t *testing.T) {
	for _, n := range []int{16, 24, 32} {
		key := make([]byte, n)
		if _, err := rand.Read(key); err != nil {
			t.Fatal(err)
		}
		p := Static(key)
		p.ID = "k1"
		mat, err := p.Current(context.Background())
		if err != nil {
			t.Fatalf("len %d: %v", n, err)
		}
		if len(mat.Key) != n {
			t.Fatalf("len = %d", len(mat.Key))
		}
		if _, err := p.Lookup(context.Background(), "k1"); err != nil {
			t.Fatalf("len %d lookup: %v", n, err)
		}
		if _, err := p.Lookup(context.Background(), "other"); err == nil {
			t.Fatalf("len %d: expected unknown-id error", n)
		}
	}
	if _, err := Static([]byte("short")).Current(context.Background()); err == nil {
		t.Fatal("expected length error")
	}
}

func TestMapProvider(t *testing.T) {
	k1 := bytesRepeat(0x01, 32)
	k2 := bytesRepeat(0x02, 16)
	p := &MapProvider{Keys: map[string][]byte{"a": k1, "b": k2}, CurrentID: "a"}
	mat, err := p.Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if mat.ID != "a" {
		t.Fatalf("id = %q", mat.ID)
	}
	p.CurrentID = "b"
	if mat, err := p.Lookup(context.Background(), "a"); err != nil || len(mat.Key) != 32 {
		t.Fatalf("lookup a: %v", err)
	}
	if _, err := p.Lookup(context.Background(), "missing"); err == nil {
		t.Fatal("expected unknown-id error")
	}
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func TestEnvProvider(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MURMUR_TEST_KEY", hex.EncodeToString(key))
	mat, err := (&EnvProvider{Var: "MURMUR_TEST_KEY"}).Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(mat.Key) != hex.EncodeToString(key) {
		t.Fatal("key mismatch")
	}
	if _, err := (&EnvProvider{Var: "MURMUR_TEST_KEY_MISSING"}).Current(context.Background()); err == nil {
		t.Fatal("expected missing-var error")
	}
}

func TestFileProvider(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(path, []byte(hex.EncodeToString(key)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mat, err := (&FileProvider{Path: path}).Current(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(mat.Key) != hex.EncodeToString(key) {
		t.Fatal("key mismatch")
	}
}

func TestDecodeKeyMaterialLengths(t *testing.T) {
	for _, n := range []int{16, 24, 32} {
		raw := bytesRepeat(0x9a, n)
		if got, err := decodeKeyMaterial(raw, AlgorithmUnknown); err != nil || len(got) != n {
			t.Fatalf("raw %d: %v", n, err)
		}
		hexed := []byte(hex.EncodeToString(raw))
		if got, err := decodeKeyMaterial(hexed, AlgorithmUnknown); err != nil || len(got) != n {
			t.Fatalf("hex %d: %v", n, err)
		}
	}
	if _, err := decodeKeyMaterial([]byte("short"), AlgorithmUnknown); err == nil {
		t.Fatal("expected length error")
	}
}

func TestAlgorithmAliases(t *testing.T) {
	if id, err := ParseAlgorithm("AES-GCM-256"); err != nil || id != AlgorithmAES256GCM {
		t.Fatalf("alias: %v %v", id, err)
	}
	if id, err := ParseAlgorithm("ChaCha"); err != nil || id != AlgorithmChaCha20Poly1305 {
		t.Fatalf("alias: %v %v", id, err)
	}
}

func TestProviderAlgorithmLengths(t *testing.T) {
	p := &StaticProvider{ID: "k", Algorithm: AlgorithmAES128GCM, Key: bytesRepeat(0x01, 16)}
	if _, err := p.Current(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Key = bytesRepeat(0x01, 32)
	if _, err := p.Current(context.Background()); err == nil {
		t.Fatal("expected length mismatch for AES-128-GCM")
	}
	t.Setenv("MURMUR_TEST_KEY16", "00112233445566778899aabbccddeeff")
	m16, err := (&EnvProvider{Var: "MURMUR_TEST_KEY16", Algorithm: AlgorithmAEGIS128L}).Current(context.Background())
	if err != nil || len(m16.Key) != 16 || m16.Algorithm != AlgorithmAEGIS128L.String() {
		t.Fatalf("env16: %v", err)
	}
	if _, err := decodeKeyMaterial([]byte("001122"), AlgorithmAES256GCM); err == nil {
		t.Fatal("expected required-length error")
	}
}

func TestZero(t *testing.T) {
	b := []byte{1, 2, 3}
	Zero(b)
	for _, v := range b {
		if v != 0 {
			t.Fatal("not zeroed")
		}
	}
}
