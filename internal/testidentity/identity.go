// Package testidentity supplies reproducible keys for test fixtures only.
// Its deterministic keys must never be used for real databases.
package testidentity

import (
	"crypto/ed25519"
	"crypto/sha256"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/origin"
)

var Registry, _ = origin.NewKeyRegistry(nil)
var DBID = ids.DBID{0xd1, 0x51}

func Key(id ids.NodeID) ed25519.PrivateKey {
	seed := sha256.Sum256(append([]byte("murmur-test-only-origin"), id[:]...))
	key := ed25519.NewKeyFromSeed(seed[:])
	if !id.IsZero() {
		if err := Registry.Add(id, key.Public().(ed25519.PublicKey)); err != nil {
			panic(err)
		}
	}
	return key
}
func Config(id ids.NodeID) origin.Config {
	return origin.Config{PrivateKey: Key(id), TrustedKeys: Registry}
}
func Sign(b *codec.MutationBatch, dbid ids.DBID) *codec.MutationBatch {
	if dbid.IsZero() {
		dbid = DBID
	}
	_ = codec.SignOrigin(b, dbid, Key(b.OriginNode))
	return b
}
