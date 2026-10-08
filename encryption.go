package murmur

import (
	"fmt"
	"time"

	"github.com/marcgauthier/murmur/crypto"
)

// EncryptionAlgorithm names an at-rest AEAD. Canonical values double as the
// on-disk algorithm identifiers; parsing additionally accepts the aliases
// "AES-GCM-256" (AES-256-GCM) and "ChaCha" (ChaCha20-Poly1305).
type EncryptionAlgorithm string

const (
	AES128GCM         EncryptionAlgorithm = "AES-128-GCM"
	AES192GCM         EncryptionAlgorithm = "AES-192-GCM"
	AES256GCM         EncryptionAlgorithm = "AES-256-GCM"
	ChaCha20Poly1305  EncryptionAlgorithm = "ChaCha20-Poly1305"
	XChaCha20Poly1305 EncryptionAlgorithm = "XChaCha20-Poly1305"
)

// KeyMaterial and KeyProvider are the application-facing key interfaces
// (aliased from crypto so callers implement one set of types). Material
// carries the algorithm identifying the input key bytes; providers attach it
// per key.
type (
	KeyMaterial = crypto.KeyMaterial
	KeyProvider = crypto.KeyProvider
)

// EncryptionConfig configures mandatory at-rest encryption.
type EncryptionConfig struct {
	// Algorithm is the write cipher for newly created files.
	// Default AES256GCM for newly created files (empty resolves to it).
	Algorithm EncryptionAlgorithm
	// KeyAlgorithm identifies direct wrapping material; defaults to Algorithm.
	KeyAlgorithm EncryptionAlgorithm
	// Key is application wrapping material; exclusive with Provider.
	// It is copied into package-owned memory on Open.
	Key []byte
	// KeyID names the wrapping key; always required.
	KeyID string
	// Provider supplies wrapping keys (current + historical lookup).
	Provider KeyProvider
	// DataKeyRotation is the active data-key lifetime; rotate on open and
	// before creating a file when the active key exceeds it. The package
	// constructor defaults it to 10 days; it must be positive.
	DataKeyRotation time.Duration
}

// EncryptionStatus describes at-rest encryption state (no key bytes).
type EncryptionStatus struct {
	Algorithm             EncryptionAlgorithm
	ApplicationKeyID      string
	ActiveDataKeyID       string
	RegistryGeneration    uint64
	Keys                  []KeyReferenceStatus
	Phase                 string // idle, rotating, rewriting, or recovering
	FilesDone, FilesTotal uint64
	BytesDone, BytesTotal uint64
}

// KeyReferenceStatus counts references pinning one data key.
type KeyReferenceStatus struct {
	ID                                           string
	Algorithm                                    EncryptionAlgorithm
	CreatedAt                                    time.Time
	LiveFiles, OpenHandles, Checkpoints, Backups uint64
}

// parseAlgorithm resolves canonical names and aliases.
func parseAlgorithm(a EncryptionAlgorithm) (crypto.AlgorithmID, error) {
	return crypto.ParseAlgorithm(string(a))
}

// mustParseAlgorithm converts a validated algorithm (panics on unknown).
func mustParseAlgorithm(a EncryptionAlgorithm) crypto.AlgorithmID {
	id, err := parseAlgorithm(a)
	if err != nil {
		panic(err)
	}
	return id
}

// algorithmString renders a crypto id canonically.
func algorithmString(id crypto.AlgorithmID) EncryptionAlgorithm {
	return EncryptionAlgorithm(id.String())
}

// validate checks the encryption configuration (after defaults).
func (c EncryptionConfig) validate() error {
	hasKey := len(c.Key) > 0
	hasProvider := c.Provider != nil
	if hasKey == hasProvider {
		return fmt.Errorf("murmur: exactly one of Encryption.Key or Encryption.Provider is required")
	}
	if c.KeyID == "" {
		return fmt.Errorf("murmur: Encryption.KeyID is required")
	}
	alg := c.Algorithm
	if alg == "" {
		alg = AES256GCM
	}
	if alg != AES256GCM {
		return fmt.Errorf("murmur: Encryption.Algorithm %q unsupported (only %s is supported)", alg, AES256GCM)
	}
	if hasKey && len(c.Key) != 32 {
		return fmt.Errorf("murmur: Encryption.Key must be 32 bytes, got %d", len(c.Key))
	}
	if c.DataKeyRotation <= 0 {
		return fmt.Errorf("murmur: Encryption.DataKeyRotation must be positive")
	}
	return nil
}

// writeAlgorithm resolves the configured write cipher id.
func (c EncryptionConfig) writeAlgorithm() crypto.AlgorithmID {
	return crypto.DefaultAlgorithm
}

// keyAlgorithm resolves the direct-material algorithm id.
func (c EncryptionConfig) keyAlgorithm() crypto.AlgorithmID {
	return crypto.DefaultAlgorithm
}

// storageProvider builds the key provider: the configured
// provider, or a synthetic single-key provider over copied direct material.
func (c EncryptionConfig) storageProvider() crypto.KeyProvider {
	if c.Provider != nil {
		return c.Provider
	}
	key := append([]byte(nil), c.Key...)
	return &crypto.MapProvider{
		Keys:      map[string][]byte{c.KeyID: key},
		CurrentID: c.KeyID,
		Algorithm: crypto.DefaultAlgorithm,
	}
}

// checkOpenKeyMaterial validates open-time storage-key material: 32 bytes.
func checkOpenKeyMaterial(mat KeyMaterial) error {
	if len(mat.Key) != 32 {
		return fmt.Errorf("storage key must be 32 bytes, got %d", len(mat.Key))
	}
	return nil
}

// validateKeyMaterial checks rotation input: nonempty new ID, supported
// algorithm, matching length.
func validateKeyMaterial(currentID string, m KeyMaterial) error {
	if m.ID == "" {
		return fmt.Errorf("murmur: rotation requires a new application-key ID")
	}
	if m.ID == currentID {
		return fmt.Errorf("murmur: rotation requires a new application-key ID (got current %q)", currentID)
	}
	if m.Algorithm != string(AES256GCM) && m.Algorithm != "AES-GCM-256" {
		return fmt.Errorf("murmur: rotation algorithm must be %s (got %q)", AES256GCM, m.Algorithm)
	}
	if len(m.Key) != 32 {
		return fmt.Errorf("murmur: rotation key must be 32 bytes, got %d", len(m.Key))
	}
	return nil
}
