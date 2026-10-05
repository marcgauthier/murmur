// Package origin authenticates immutable replication transaction identities.
package origin

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"sync"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

// KeyRegistry contains administrator-provisioned origin public keys. Adding a
// key never replaces a binding. Registry updates are safe during replication;
// discovery and replication handlers must never add keys to this registry.
type KeyRegistry struct {
	mu   sync.RWMutex
	keys map[ids.NodeID]ed25519.PublicKey
}

func NewKeyRegistry(keys map[ids.NodeID]ed25519.PublicKey) (*KeyRegistry, error) {
	r := &KeyRegistry{keys: make(map[ids.NodeID]ed25519.PublicKey)}
	for id, key := range keys {
		if err := r.Add(id, key); err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (r *KeyRegistry) Add(id ids.NodeID, key ed25519.PublicKey) error {
	if r == nil || id.IsZero() || len(key) != ed25519.PublicKeySize {
		return fmt.Errorf("origin: invalid node/public key binding")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.keys[id]; ok && !bytes.Equal(old, key) {
		return fmt.Errorf("origin: signing key cannot change under NodeID %s", id)
	}
	if r.keys == nil {
		r.keys = make(map[ids.NodeID]ed25519.PublicKey)
	}
	r.keys[id] = append(ed25519.PublicKey(nil), key...)
	return nil
}

func (r *KeyRegistry) Lookup(id ids.NodeID) (ed25519.PublicKey, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	key, ok := r.keys[id]
	return append(ed25519.PublicKey(nil), key...), ok
}

// Config supplies a private key and an explicit registry for this database.
// Use separate registries when different databases authorize different nodes.
type Config struct {
	PrivateKey  ed25519.PrivateKey
	TrustedKeys *KeyRegistry
}

func (c Config) Validate(local ids.NodeID) error {
	if len(c.PrivateKey) != ed25519.PrivateKeySize {
		return fmt.Errorf("origin: Ed25519 private key is required (64 bytes)")
	}
	derived := ed25519.NewKeyFromSeed(c.PrivateKey[:ed25519.SeedSize])
	if !bytes.Equal(derived, c.PrivateKey) {
		return fmt.Errorf("origin: inconsistent Ed25519 private key")
	}
	key, ok := c.TrustedKeys.Lookup(local)
	if !ok || !bytes.Equal(key, derived[ed25519.SeedSize:]) {
		return fmt.Errorf("origin: local NodeID must be bound to its signing public key")
	}
	return nil
}

func (c Config) Verify(batch *codec.MutationBatch, dbid ids.DBID) error {
	if batch == nil {
		return codec.ErrOriginUnsigned
	}
	key, ok := c.TrustedKeys.Lookup(batch.OriginNode)
	if !ok {
		return codec.ErrOriginUnknown
	}
	return codec.VerifyOrigin(batch, dbid, key)
}

func (c Config) VerifyChunk(chunk *codec.TransactionChunk, dbid ids.DBID) error {
	if chunk == nil {
		return codec.ErrOriginUnsigned
	}
	key, ok := c.TrustedKeys.Lookup(chunk.Origin)
	if !ok {
		return codec.ErrOriginUnknown
	}
	return codec.VerifyOriginIdentity(chunk.OriginBatch(), dbid, key)
}
