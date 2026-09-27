// Package crypto provides storage-key providers for at-rest encryption.
//
// The database package depends only on KeyProvider; operators choose how
// keys are sourced (static config, environment, files, Vault/KMS/TPM via
// out-of-tree implementations).
package crypto

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// Accepted storage-key lengths: 16, 24, or 32 bytes. The registry wrap key
// is always HKDF-derived at 32 bytes regardless of input length.
var acceptedKeyLengths = []int{16, 24, 32}

// KeyMaterial is one storage key. Algorithm is the canonical algorithm name
// identifying the input material ("" means unspecified: 16, 24, or 32 bytes
// are accepted); when set, the key length must match the algorithm exactly.
// A plain string keeps the application-facing alias assignment-compatible
// with EncryptionAlgorithm constants.
type KeyMaterial struct {
	ID        string
	Algorithm string
	Key       []byte
}

// KeyProvider supplies storage keys. Current returns the key used for new
// wraps; Lookup retrieves a named historical key (for registries wrapped
// before a storage-key rotation).
type KeyProvider interface {
	Current(ctx context.Context) (KeyMaterial, error)
	Lookup(ctx context.Context, id string) (KeyMaterial, error)
}

// checkKeyLength validates storage-key material length.
func checkKeyLength(key []byte) error {
	for _, n := range acceptedKeyLengths {
		if len(key) == n {
			return nil
		}
	}
	return fmt.Errorf("key must be 16, 24, or 32 bytes, got %d", len(key))
}

// StaticProvider returns a fixed key (tests, simple deployments).
type StaticProvider struct {
	ID        string
	Algorithm AlgorithmID
	Key       []byte
}

// Static returns a provider for key (copied).
func Static(key []byte) *StaticProvider {
	cp := make([]byte, len(key))
	copy(cp, key)
	return &StaticProvider{Key: cp}
}

// Current implements KeyProvider.
func (p *StaticProvider) Current(context.Context) (KeyMaterial, error) {
	if err := checkKeyLengthFor(p.Key, p.Algorithm); err != nil {
		return KeyMaterial{}, fmt.Errorf("crypto: static %w", err)
	}
	cp := make([]byte, len(p.Key))
	copy(cp, p.Key)
	return KeyMaterial{ID: p.ID, Algorithm: algName(p.Algorithm), Key: cp}, nil
}

// Lookup implements KeyProvider. A single-key provider serves any id that
// matches (or is empty, treated as "current").
func (p *StaticProvider) Lookup(ctx context.Context, id string) (KeyMaterial, error) {
	if id != "" && id != p.ID {
		return KeyMaterial{}, fmt.Errorf("crypto: static provider has no key %q", id)
	}
	return p.Current(ctx)
}

// EnvProvider reads a hex- or raw-encoded key from an environment variable.
// Hex (32/48/64 chars) is preferred; otherwise the value must be exactly
// 16, 24, or 32 bytes.
type EnvProvider struct {
	Var       string
	ID        string
	Algorithm AlgorithmID
}

// Current implements KeyProvider.
func (p *EnvProvider) Current(context.Context) (KeyMaterial, error) {
	v, ok := os.LookupEnv(p.Var)
	if !ok {
		return KeyMaterial{}, fmt.Errorf("crypto: env var %s not set", p.Var)
	}
	key, err := decodeKeyMaterial([]byte(strings.TrimSpace(v)), p.Algorithm)
	if err != nil {
		return KeyMaterial{}, fmt.Errorf("crypto: env var %s: %w", p.Var, err)
	}
	return KeyMaterial{ID: p.ID, Algorithm: algName(p.Algorithm), Key: key}, nil
}

// Lookup implements KeyProvider.
func (p *EnvProvider) Lookup(ctx context.Context, id string) (KeyMaterial, error) {
	if id != "" && id != p.ID {
		return KeyMaterial{}, fmt.Errorf("crypto: env provider has no key %q", id)
	}
	return p.Current(ctx)
}

// FileProvider reads the key from a file (hex or raw, whitespace trimmed).
type FileProvider struct {
	Path      string
	ID        string
	Algorithm AlgorithmID
}

// Current implements KeyProvider.
func (p *FileProvider) Current(context.Context) (KeyMaterial, error) {
	raw, err := os.ReadFile(p.Path)
	if err != nil {
		return KeyMaterial{}, fmt.Errorf("crypto: read key file: %w", err)
	}
	key, err := decodeKeyMaterial([]byte(strings.TrimSpace(string(raw))), p.Algorithm)
	if err != nil {
		return KeyMaterial{}, fmt.Errorf("crypto: key file %s: %w", p.Path, err)
	}
	return KeyMaterial{ID: p.ID, Algorithm: algName(p.Algorithm), Key: key}, nil
}

// Lookup implements KeyProvider.
func (p *FileProvider) Lookup(ctx context.Context, id string) (KeyMaterial, error) {
	if id != "" && id != p.ID {
		return KeyMaterial{}, fmt.Errorf("crypto: file provider has no key %q", id)
	}
	return p.Current(ctx)
}

// MapProvider serves several named keys (tests, rotation scenarios).
type MapProvider struct {
	Keys      map[string][]byte
	CurrentID string
	Algorithm AlgorithmID
}

// Current implements KeyProvider.
func (p *MapProvider) Current(context.Context) (KeyMaterial, error) {
	return p.Lookup(context.Background(), p.CurrentID)
}

// Lookup implements KeyProvider.
func (p *MapProvider) Lookup(_ context.Context, id string) (KeyMaterial, error) {
	if id == "" {
		id = p.CurrentID
	}
	key, ok := p.Keys[id]
	if !ok {
		return KeyMaterial{}, fmt.Errorf("crypto: no key %q", id)
	}
	if err := checkKeyLengthFor(key, p.Algorithm); err != nil {
		return KeyMaterial{}, fmt.Errorf("crypto: key %q: %w", id, err)
	}
	cp := make([]byte, len(key))
	copy(cp, key)
	return KeyMaterial{ID: id, Algorithm: algName(p.Algorithm), Key: cp}, nil
}

// algName renders an algorithm for KeyMaterial ("" when unspecified).
func algName(alg AlgorithmID) string {
	if alg == AlgorithmUnknown {
		return ""
	}
	return alg.String()
}

// checkKeyLengthFor validates length against an algorithm when set,
// else against the accepted raw lengths.
func checkKeyLengthFor(key []byte, alg AlgorithmID) error {
	if alg != AlgorithmUnknown {
		want, err := alg.KeySize()
		if err != nil {
			return err
		}
		if len(key) != want {
			return fmt.Errorf("key must be %d bytes for %s, got %d", want, alg, len(key))
		}
		return nil
	}
	return checkKeyLength(key)
}

func decodeKeyMaterial(v []byte, alg AlgorithmID) ([]byte, error) {
	lengths := acceptedKeyLengths
	if alg != AlgorithmUnknown {
		n, err := alg.KeySize()
		if err != nil {
			return nil, err
		}
		lengths = []int{n}
	}
	for _, n := range lengths {
		if len(v) == 2*n {
			hexed := make([]byte, n)
			if _, err := hex.Decode(hexed, v); err == nil {
				return hexed, nil
			}
		}
		if len(v) == n {
			cp := make([]byte, n)
			copy(cp, v)
			return cp, nil
		}
	}
	if alg != AlgorithmUnknown {
		n, _ := alg.KeySize()
		return nil, fmt.Errorf("key material must be %d raw bytes (or %d hex chars) for %s, got %d", n, 2*n, alg, len(v))
	}
	return nil, fmt.Errorf("key material must be 16, 24, or 32 raw bytes (or 32/48/64 hex chars), got %d", len(v))
}

// Zero overwrites b. Note Go gives no guarantee that secret bytes were never
// copied by the runtime; this is best-effort hygiene.
func Zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
