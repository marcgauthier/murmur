package origin

import (
	"crypto/ed25519"
	"crypto/rand"
	"github.com/marcgauthier/murmur/ids"
	"testing"
)

func TestRegistryRequiresExplicitStableBindings(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	node := ids.NewNodeID()
	registry, _ := NewKeyRegistry(nil)
	cfg := Config{PrivateKey: key, TrustedKeys: registry}
	if cfg.Validate(node) == nil {
		t.Fatal("missing local binding accepted")
	}
	if err := registry.Add(node, pub); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(node); err != nil {
		t.Fatal(err)
	}
	pub2, _, _ := ed25519.GenerateKey(rand.Reader)
	if registry.Add(node, pub2) == nil {
		t.Fatal("key replacement accepted")
	}
	copyPub, _ := registry.Lookup(node)
	copyPub[0]++
	if err := cfg.Validate(node); err != nil {
		t.Fatal("lookup exposed mutable key")
	}
	bad := append(ed25519.PrivateKey(nil), key...)
	bad[63]++
	cfg.PrivateKey = bad
	if cfg.Validate(node) == nil {
		t.Fatal("inconsistent key accepted")
	}
}
