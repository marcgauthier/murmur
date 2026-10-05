// Package demoidentity provisions ephemeral signing identities for examples.
// Production applications persist each private key separately and distribute
// public bindings through an administrator-controlled registry.
package demoidentity

import (
	"crypto/ed25519"
	"crypto/rand"
	"sync"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/origin"
)

var mu sync.Mutex
var keys = make(map[murmur.NodeID]ed25519.PrivateKey)
var registry, _ = origin.NewKeyRegistry(nil)

func Configure(cfg murmur.Config) murmur.Config {
	mu.Lock()
	defer mu.Unlock()
	key, ok := keys[cfg.NodeID]
	if !ok {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			panic(err)
		}
		keys[cfg.NodeID] = key
	}
	key = keys[cfg.NodeID]
	if err := registry.Add(cfg.NodeID, key.Public().(ed25519.PublicKey)); err != nil {
		panic(err)
	}
	cfg.OriginSigning = murmur.OriginSigningConfig{PrivateKey: key, TrustedKeys: registry}
	// These examples deliberately trust their explicitly configured peers for snapshots.
	for _, peer := range cfg.Replication.Peers {
		cfg.Replication.TrustedSnapshotSources = append(cfg.Replication.TrustedSnapshotSources, peer.NodeID)
	}
	return cfg
}
