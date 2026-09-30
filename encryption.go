package replicateddb

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
	AEGIS128L         EncryptionAlgorithm = "AEGIS-128L"
	AEGIS256          EncryptionAlgorithm = "AEGIS-256"
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
		return fmt.Errorf("replicateddb: exactly one of Encryption.Key or Encryption.Provider is required")
	}
	if c.KeyID == "" {
		return fmt.Errorf("replicateddb: Encryption.KeyID is required")
	}
	alg := c.Algorithm
	if alg == "" {
		alg = AES256GCM
	}
	if _, err := parseAlgorithm(alg); err != nil {
		return fmt.Errorf("replicateddb: Encryption.Algorithm: %w", err)
	}
	keyAlg := c.KeyAlgorithm
	if keyAlg == "" {
		keyAlg = alg
	}
	keyAlgID, err := parseAlgorithm(keyAlg)
	if err != nil {
		return fmt.Errorf("replicateddb: Encryption.KeyAlgorithm: %w", err)
	}
	if hasKey {
		want, err := keyAlgID.KeySize()
		if err != nil {
			return err
		}
		if len(c.Key) != want {
			return fmt.Errorf("replicateddb: Encryption.Key must be %d bytes for %s, got %d",
				want, keyAlg, len(c.Key))
		}
	}
	if c.DataKeyRotation <= 0 {
		return fmt.Errorf("replicateddb: Encryption.DataKeyRotation must be positive")
	}
	return nil
}

// writeAlgorithm resolves the configured write cipher id.
func (c EncryptionConfig) writeAlgorithm() crypto.AlgorithmID {
	if c.Algorithm == "" {
		return crypto.DefaultAlgorithm
	}
	return mustParseAlgorithm(c.Algorithm)
}

// keyAlgorithm resolves the direct-material algorithm id.
func (c EncryptionConfig) keyAlgorithm() crypto.AlgorithmID {
	if c.KeyAlgorithm != "" {
		return mustParseAlgorithm(c.KeyAlgorithm)
	}
	return c.writeAlgorithm()
}

// storageProvider builds the registry's key provider: the configured
// provider, or a synthetic single-key provider over copied direct material.
func (c EncryptionConfig) storageProvider() crypto.KeyProvider {
	if c.Provider != nil {
		return c.Provider
	}
	key := append([]byte(nil), c.Key...)
	return &crypto.MapProvider{
		Keys:      map[string][]byte{c.KeyID: key},
		CurrentID: c.KeyID,
		Algorithm: c.keyAlgorithm(),
	}
}

// checkOpenKeyMaterial validates open-time storage-key material: an
// explicit algorithm requires exact length, unspecified ("") accepts the
// raw 16/24/32-byte lengths (the registry wrap key is always HKDF-derived
// at 32 bytes regardless of input length).
func checkOpenKeyMaterial(mat KeyMaterial) error {
	if mat.Algorithm == "" {
		for _, n := range []int{16, 24, 32} {
			if len(mat.Key) == n {
				return nil
			}
		}
		return fmt.Errorf("storage key must be 16, 24, or 32 bytes, got %d", len(mat.Key))
	}
	algID, err := crypto.ParseAlgorithm(mat.Algorithm)
	if err != nil {
		return err
	}
	want, err := algID.KeySize()
	if err != nil {
		return err
	}
	if len(mat.Key) != want {
		return fmt.Errorf("storage key must be %d bytes for %s, got %d",
			want, mat.Algorithm, len(mat.Key))
	}
	return nil
}

// validateKeyMaterial checks rotation input: nonempty new ID, supported
// algorithm, matching length.
func validateKeyMaterial(currentID string, m KeyMaterial) error {
	if m.ID == "" {
		return fmt.Errorf("replicateddb: rotation requires a new application-key ID")
	}
	if m.ID == currentID {
		return fmt.Errorf("replicateddb: rotation requires a new application-key ID (got current %q)", currentID)
	}
	if m.Algorithm == "" {
		return fmt.Errorf("replicateddb: rotation requires an explicit supported algorithm")
	}
	algID, err := crypto.ParseAlgorithm(m.Algorithm)
	if err != nil {
		return fmt.Errorf("replicateddb: rotation algorithm: %w", err)
	}
	want, err := algID.KeySize()
	if err != nil {
		return fmt.Errorf("replicateddb: rotation algorithm: %w", err)
	}
	if len(m.Key) != want {
		return fmt.Errorf("replicateddb: rotation key must be %d bytes for %s, got %d",
			want, m.Algorithm, len(m.Key))
	}
	return nil
}
