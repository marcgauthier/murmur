package bridge

import (
	"crypto/ed25519"
	"fmt"

	"golang.org/x/crypto/curve25519"
)

// Binary key-material encoding for multi-process deployments: private
// bytes on disk, public parts re-derived on load. Signer material is the
// 64-byte Ed25519 private key; recipient material is the 32-byte X25519
// private scalar.
const (
	SignerKeyBinarySize    = ed25519.PrivateKeySize
	RecipientKeyBinarySize = keyIDSize
)

// MarshalBinary exports the signer's private key material.
func (k *SignerKey) MarshalBinary() []byte {
	out := make([]byte, ed25519.PrivateKeySize)
	copy(out, k.priv)
	return out
}

// ParseSignerKey rebuilds a signer from MarshalBinary output.
func ParseSignerKey(raw []byte) (*SignerKey, error) {
	if len(raw) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("bridge: signer key must be %d bytes, got %d", ed25519.PrivateKeySize, len(raw))
	}
	priv := ed25519.PrivateKey(append([]byte(nil), raw...))
	pub := priv.Public().(ed25519.PublicKey)
	var id [keyIDSize]byte
	copy(id[:], pub)
	return &SignerKey{ID: id, pub: append(ed25519.PublicKey(nil), pub...), priv: priv}, nil
}

// MarshalBinary exports the recipient's private key material.
func (k *RecipientKey) MarshalBinary() []byte {
	out := make([]byte, keyIDSize)
	copy(out, k.priv[:])
	return out
}

// ParseRecipientKey rebuilds a recipient from MarshalBinary output.
func ParseRecipientKey(raw []byte) (*RecipientKey, error) {
	if len(raw) != keyIDSize {
		return nil, fmt.Errorf("bridge: recipient key must be %d bytes, got %d", keyIDSize, len(raw))
	}
	var priv [keyIDSize]byte
	copy(priv[:], raw)
	pub, err := curve25519.X25519(priv[:], curve25519.Basepoint)
	if err != nil {
		return nil, fmt.Errorf("bridge: derive recipient public key: %w", err)
	}
	var k RecipientKey
	k.priv = priv
	copy(k.pub[:], pub)
	k.ID = k.pub
	return &k, nil
}
