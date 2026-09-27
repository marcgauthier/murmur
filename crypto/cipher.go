// Package crypto provides at-rest encryption for the durable state:
// AEAD cipher adapters, storage-key providers, the authenticated key
// registry, the encrypted container format, and the encrypted VFS.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"fmt"
	"io"
	"runtime"

	"github.com/ericlagergren/aegis"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/sys/cpu"
)

// AlgorithmID identifies one of the seven supported AEADs.
type AlgorithmID uint16

const (
	AlgorithmUnknown AlgorithmID = iota
	AlgorithmAES128GCM
	AlgorithmAES192GCM
	AlgorithmAES256GCM
	AlgorithmChaCha20Poly1305
	AlgorithmXChaCha20Poly1305
	AlgorithmAEGIS128L
	AlgorithmAEGIS256
)

// DefaultAlgorithm is AES-256-GCM.
const DefaultAlgorithm = AlgorithmAES256GCM

// canonical names, also used by Config.SetEncryptionAlgorithm / status.
var algorithmNames = map[AlgorithmID]string{
	AlgorithmAES128GCM:        "AES-128-GCM",
	AlgorithmAES192GCM:        "AES-192-GCM",
	AlgorithmAES256GCM:        "AES-256-GCM",
	AlgorithmChaCha20Poly1305: "ChaCha20-Poly1305",
	AlgorithmXChaCha20Poly1305: "XChaCha20-Poly1305",
	AlgorithmAEGIS128L:        "AEGIS-128L",
	AlgorithmAEGIS256:         "AEGIS-256",
}

// String returns the canonical algorithm name.
func (a AlgorithmID) String() string {
	if s, ok := algorithmNames[a]; ok {
		return s
	}
	return fmt.Sprintf("unknown-algorithm(%d)", uint16(a))
}

// ParseAlgorithm parses a canonical algorithm name. It accepts the
// documented aliases AES-GCM-256 (AES-256-GCM) and ChaCha
// (ChaCha20-Poly1305).
func ParseAlgorithm(s string) (AlgorithmID, error) {
	for id, name := range algorithmNames {
		if name == s {
			return id, nil
		}
	}
	switch s {
	case "AES-GCM-256":
		return AlgorithmAES256GCM, nil
	case "ChaCha":
		return AlgorithmChaCha20Poly1305, nil
	}
	return AlgorithmUnknown, fmt.Errorf("crypto: unknown algorithm %q", s)
}

// KeySize returns the required key length in bytes.
func (a AlgorithmID) KeySize() (int, error) {
	switch a {
	case AlgorithmAES128GCM, AlgorithmAEGIS128L:
		return 16, nil
	case AlgorithmAES192GCM:
		return 24, nil
	case AlgorithmAES256GCM, AlgorithmChaCha20Poly1305,
		AlgorithmXChaCha20Poly1305, AlgorithmAEGIS256:
		return 32, nil
	}
	return 0, fmt.Errorf("crypto: unknown algorithm %q", a.String())
}

// mustKeySize panics on unknown algorithms (internal use after validation).
func (a AlgorithmID) mustKeySize() int {
	n, err := a.KeySize()
	if err != nil {
		panic(err)
	}
	return n
}

// MaxNonceSize bounds every supported nonce (AEGIS-256 uses 32).
const MaxNonceSize = 32

// NewAEAD builds the AEAD for alg from a full-size key.
func (a AlgorithmID) NewAEAD(key []byte) (cipher.AEAD, error) {
	want, err := a.KeySize()
	if err != nil {
		return nil, err
	}
	if len(key) != want {
		return nil, fmt.Errorf("crypto: %s key must be %d bytes, got %d", a, want, len(key))
	}
	switch a {
	case AlgorithmAES128GCM, AlgorithmAES192GCM, AlgorithmAES256GCM:
		block, err := aes.NewCipher(key)
		if err != nil {
			return nil, err
		}
		return cipher.NewGCM(block)
	case AlgorithmChaCha20Poly1305:
		return chacha20poly1305.New(key)
	case AlgorithmXChaCha20Poly1305:
		return chacha20poly1305.NewX(key)
	case AlgorithmAEGIS128L, AlgorithmAEGIS256:
		return aegis.New(key)
	}
	return nil, fmt.Errorf("crypto: unknown algorithm %q", a.String())
}

// AEGISHardwareAccelerated reports whether the AEGIS implementation can use
// hardware acceleration on this machine, mirroring the dependency's own
// selection (amd64 with AES+SSE4.1, arm64 with AES/SHA3, or 32-bit ARM with
// AES+NEON).
func AEGISHardwareAccelerated() bool {
	switch runtime.GOARCH {
	case "amd64":
		return cpu.X86.HasAES && cpu.X86.HasSSE41
	case "arm64":
		return cpu.ARM64.HasAES
	case "arm":
		return cpu.ARM.HasAES && cpu.ARM.HasNEON
	}
	return false
}

// AEGISBackend describes which AEGIS code path is active.
func AEGISBackend() string {
	if AEGISHardwareAccelerated() {
		return "asm(" + runtime.GOARCH + ")"
	}
	return "generic(" + runtime.GOARCH + ")"
}

// DeriveSubkey derives a per-algorithm subkey from a 256-bit master key with
// HKDF-SHA256. info must bind every authenticated attribute (algorithm, key
// id, epoch, file id, ...); salt should be a fresh random value per epoch.
func DeriveSubkey(master []byte, alg AlgorithmID, salt, info []byte) ([]byte, error) {
	if len(master) != 32 {
		return nil, fmt.Errorf("crypto: subkey master must be 32 bytes, got %d", len(master))
	}
	out := make([]byte, alg.mustKeySize())
	if _, err := io.ReadFull(hkdf.New(sha256.New, master, salt, info), out); err != nil {
		return nil, fmt.Errorf("crypto: hkdf: %w", err)
	}
	return out, nil
}

// deriveKEK derives the 32-byte registry wrap key from storage-key material
// of any accepted length. dbID binds the wrap to one database.
func deriveKEK(storageKey, dbID, storageKeyID []byte) ([]byte, error) {
	switch len(storageKey) {
	case 16, 24, 32:
	default:
		return nil, fmt.Errorf("crypto: storage key must be 16, 24, or 32 bytes, got %d", len(storageKey))
	}
	info := make([]byte, 0, 32+len(storageKeyID))
	info = append(info, []byte("nomadsql/registry-kek/v1")...)
	info = append(info, storageKeyID...)
	salt := make([]byte, 0, 16+len(dbID))
	salt = append(salt, []byte("nomadsql/kek-salt")...)
	salt = append(salt, dbID...)
	out := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha256.New, storageKey, salt, info), out); err != nil {
		return nil, fmt.Errorf("crypto: hkdf: %w", err)
	}
	return out, nil
}
