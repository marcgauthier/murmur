package cmd

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/origin"
	"github.com/marcgauthier/murmur/schema"
)

// openLocalDB creates a standard murmur.Config and opens the database.
func openLocalDB(ctx context.Context, target string, globalOpts GlobalOptions, readOnly bool) (*murmur.DB, error) {
	if err := os.MkdirAll(target, 0o700); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	// 1. Node Identity persistence
	nodeIDFile := filepath.Join(target, "node.id")
	var nodeID ids.NodeID
	if intent, err := backup.ReadRestoreIntent(target); err == nil && intent != nil {
		if parsed, err := ids.ParseNodeID(intent.FreshNodeID); err == nil {
			nodeID = parsed
			_ = os.WriteFile(nodeIDFile, []byte(nodeID.String()), 0o600)
		}
	}
	if nodeID == (ids.NodeID{}) {
		if data, err := os.ReadFile(nodeIDFile); err == nil && len(strings.TrimSpace(string(data))) > 0 {
			if parsed, err := ids.ParseNodeID(strings.TrimSpace(string(data))); err == nil {
				nodeID = parsed
			}
		}
	}
	if nodeID == (ids.NodeID{}) {
		nodeID = ids.NewNodeID()
		_ = os.WriteFile(nodeIDFile, []byte(nodeID.String()), 0o600)
	}

	// 2. Schema persistence
	schemaFile := filepath.Join(target, "schema.json")
	schemaCfg := murmur.SchemaConfig{
		Version: 1,
		Tables: []schema.TableSchema{
			{
				Name: "_murmur_init",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "created_at", Type: schema.ColText, Nullable: true},
				},
			},
		},
	}
	if data, err := os.ReadFile(schemaFile); err == nil && len(data) > 0 {
		var loaded murmur.SchemaConfig
		if err := json.Unmarshal(data, &loaded); err == nil && loaded.Version > 0 && len(loaded.Tables) > 0 {
			schemaCfg = loaded
		}
	} else {
		data, _ := json.MarshalIndent(schemaCfg, "", "  ")
		_ = os.WriteFile(schemaFile, data, 0o600)
	}

	cfg := murmur.Config{
		Path:   target,
		NodeID: nodeID,
		DBID:   ids.DBID{}, // Zero DBID binds to the database directory
		Pebble: murmur.DefaultPebbleConfig(),
		Schema: schemaCfg,
	}

	// 3. Encryption Key
	var encKey []byte
	keyID := "cli-key"

	if globalOpts.Passphrase != "" {
		h := sha256.Sum256([]byte(globalOpts.Passphrase))
		encKey = h[:]
		keyID = "passphrase-key"
	} else if globalOpts.KeyHex != "" {
		rawKey, err := hex.DecodeString(globalOpts.KeyHex)
		if err != nil {
			return nil, fmt.Errorf("invalid key-hex: %w", err)
		}
		if len(rawKey) != 32 {
			return nil, fmt.Errorf("key-hex must be 32 bytes (64 hex characters)")
		}
		encKey = rawKey
		keyID = "hex-key"
	} else {
		encKey = []byte("0123456789abcdef0123456789abcdef")
	}

	cfg.Encryption = murmur.EncryptionConfig{
		Key:   encKey,
		KeyID: keyID,
	}

	// 4. Origin Signing Identity
	var signingKey ed25519.PrivateKey
	keyPath := filepath.Join(target, "origin.key")
	if data, err := os.ReadFile(keyPath); err == nil && len(data) == ed25519.PrivateKeySize {
		signingKey = ed25519.PrivateKey(data)
	} else {
		_, newKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("generate signing key: %w", err)
		}
		signingKey = newKey
		_ = os.WriteFile(keyPath, signingKey, 0o600)
	}

	registry, err := origin.NewKeyRegistry(nil)
	if err != nil {
		return nil, fmt.Errorf("create key registry: %w", err)
	}
	_ = registry.Add(nodeID, signingKey.Public().(ed25519.PublicKey))

	cfg.OriginSigning = murmur.OriginSigningConfig{
		PrivateKey:  signingKey,
		TrustedKeys: registry,
	}

	return murmur.Open(ctx, cfg)
}
