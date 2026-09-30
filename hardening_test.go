package replicateddb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/marcgauthier/murmur/crypto"
)

// TestOpenBadPathFailsCleanly proves Open surfaces storage errors instead of
// panicking or wedging (a regular file where the data directory belongs).
func TestOpenBadPathFailsCleanly(t *testing.T) {
	ctx := context.Background()
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(filepath.Join(blocker, "data"))
	if _, err := Open(ctx, cfg); err == nil {
		t.Fatal("expected Open to fail on an impossible path")
	}
}

// TestOpenMissingKeyFailsCleanly proves a failing key provider surfaces as
// ErrEncryptionKey.
func TestOpenMissingKeyFailsCleanly(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.Encryption = EncryptionConfig{KeyID: "x", Provider: &failProvider{}}
	if _, err := Open(ctx, cfg); err == nil {
		t.Fatal("expected Open to fail with a failing provider")
	} else if !errors.Is(err, ErrEncryptionKey) {
		t.Fatalf("expected ErrEncryptionKey, got %v", err)
	}
}

type failProvider struct{}

func (failProvider) Current(context.Context) (crypto.KeyMaterial, error) {
	return crypto.KeyMaterial{}, errors.New("no key for you")
}

func (failProvider) Lookup(context.Context, string) (crypto.KeyMaterial, error) {
	return crypto.KeyMaterial{}, errors.New("no key for you")
}
