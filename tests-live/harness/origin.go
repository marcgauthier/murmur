package harness

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/origin"
)

// OriginSigning retains historical bindings when an offline restore adopts a
// fresh writer. Keys are explicitly fixture-provisioned, never learned by peers.
func (c *Cluster) OriginSigning(node db.NodeID) db.OriginSigningConfig {
	c.originMu.Lock()
	defer c.originMu.Unlock()
	for _, n := range c.Nodes {
		c.originKeys[n.NodeID] = n.OriginKey
	}
	key, ok := c.originKeys[node]
	if !ok {
		_, key, _ = ed25519.GenerateKey(nil)
		c.originKeys[node] = key
	}
	registry, _ := origin.NewKeyRegistry(nil)
	for id, key := range c.originKeys {
		if err := registry.Add(id, key.Public().(ed25519.PublicKey)); err != nil {
			c.T.Fatal(err)
		}
	}
	return db.OriginSigningConfig{PrivateKey: key, TrustedKeys: registry}
}

// ProvisionOrigin is an explicit administrative fixture action. Persist the
// public binding in every node config, then authorize it on running nodes.
func (c *Cluster) ProvisionOrigin(node db.NodeID, key ed25519.PrivateKey) {
	c.T.Helper()
	c.originMu.Lock()
	c.originKeys[node] = key
	c.originMu.Unlock()
	pub := hex.EncodeToString(key.Public().(ed25519.PublicKey))
	for _, n := range c.Nodes {
		raw, err := os.ReadFile(n.ConfigFile)
		if err != nil {
			c.T.Fatal(err)
		}
		var cfg map[string]any
		if err := json.Unmarshal(raw, &cfg); err != nil {
			c.T.Fatal(err)
		}
		keys, _ := cfg["origin_public_keys"].(map[string]any)
		if keys == nil {
			keys = make(map[string]any)
		}
		keys[node.String()] = pub
		cfg["origin_public_keys"] = keys
		raw, err = json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			c.T.Fatal(err)
		}
		if err := os.WriteFile(n.ConfigFile, raw, 0600); err != nil {
			c.T.Fatal(err)
		}
		if n.Process == nil {
			continue
		}
		body, _ := json.Marshal(map[string]string{"node_id": node.String(), "public_key": pub})
		resp, err := liveHTTPClient.Post("https://"+n.APIAddr+"/v1/admin/authorize_origin", "application/json", bytes.NewReader(body))
		if err != nil {
			c.T.Fatal(err)
		}
		raw, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			c.T.Fatal(fmt.Sprintf("authorize origin: %d %s", resp.StatusCode, raw))
		}
	}
}
