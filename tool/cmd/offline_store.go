package cmd

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/origin"
	"github.com/marcgauthier/murmur/spool"
	"github.com/marcgauthier/murmur/state"
)

// openOfflineStore opens the authoritative encrypted state without constructing
// either the legacy engine or a RIME materializer. It is for inspection
// commands that need durable metadata only.
func openOfflineStore(dataDir string, opts GlobalOptions) (*state.Store, error) {
	return openOfflineStoreInternal(dataDir, opts, false)
}

// openOfflineStoreForInit creates only the encrypted Spool state and node
// identity. The application binds its Go schema on its first typed Open.
func openOfflineStoreForInit(dataDir string, opts GlobalOptions) (*state.Store, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	nodePath := filepath.Join(dataDir, "node.id")
	if _, err := os.Stat(nodePath); os.IsNotExist(err) {
		if err := os.WriteFile(nodePath, []byte(ids.NewNodeID().String()), 0o600); err != nil {
			return nil, fmt.Errorf("write node identity: %w", err)
		} else if err := os.Chmod(nodePath, 0o600); err != nil {
			return nil, fmt.Errorf("secure node identity: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("inspect node identity: %w", err)
	}
	originPath := filepath.Join(dataDir, "origin.key")
	if _, err := os.Stat(originPath); os.IsNotExist(err) {
		_, privateKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("generate origin signing key: %w", err)
		}
		if err := os.WriteFile(originPath, privateKey, 0o600); err != nil {
			clear(privateKey)
			return nil, fmt.Errorf("write origin signing key: %w", err)
		}
		clear(privateKey)
	} else if err != nil {
		return nil, fmt.Errorf("inspect origin signing key: %w", err)
	}
	return openOfflineStoreInternal(dataDir, opts, true)
}

func openOfflineStoreInternal(dataDir string, opts GlobalOptions, allowUninitialized bool) (*state.Store, error) {
	nodeBytes, err := os.ReadFile(filepath.Join(dataDir, "node.id"))
	if err != nil {
		return nil, fmt.Errorf("read node identity: %w", err)
	}
	nodeID, err := ids.ParseNodeID(strings.TrimSpace(string(nodeBytes)))
	if err != nil {
		return nil, err
	}
	privateKey, err := os.ReadFile(filepath.Join(dataDir, "origin.key"))
	if err != nil {
		return nil, fmt.Errorf("read origin signing key: %w", err)
	}
	defer clear(privateKey)
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("origin signing key has %d bytes; want %d", len(privateKey), ed25519.PrivateKeySize)
	}
	trusted, err := origin.NewKeyRegistry(nil)
	if err != nil {
		return nil, err
	}
	key := ed25519.PrivateKey(append([]byte(nil), privateKey...))
	defer clear(key)
	if err := trusted.Add(nodeID, key.Public().(ed25519.PublicKey)); err != nil {
		return nil, err
	}

	var masterKey []byte
	keyID := "cli-key"
	switch {
	case opts.Passphrase != "":
		h := sha256.Sum256([]byte(opts.Passphrase))
		masterKey = h[:]
		keyID = "passphrase-key"
	case opts.KeyHex != "":
		masterKey, err = hex.DecodeString(opts.KeyHex)
		if err != nil {
			return nil, fmt.Errorf("invalid key-hex: %w", err)
		}
		if len(masterKey) != 32 {
			return nil, fmt.Errorf("key-hex must be 32 bytes (64 hex characters)")
		}
		keyID = "hex-key"
	default:
		masterKey = []byte("0123456789abcdef0123456789abcdef")
	}
	defer clear(masterKey)

	dataPath := filepath.Join(dataDir, "data")
	if _, err := os.Stat(filepath.Join(dataPath, "manifest")); err != nil && !allowUninitialized {
		return nil, fmt.Errorf("read spool manifest: %w", err)
	}
	spoolOptions := spool.DefaultOptions(dataPath)
	spoolOptions.MasterKey = append([]byte(nil), masterKey...)
	spoolOptions.WrappingKeyID = keyID
	store, err := state.Open(dataPath, nodeID, ids.DBID{}, state.Options{
		OriginSigning: origin.Config{PrivateKey: key, TrustedKeys: trusted},
		Spool:         spoolOptions,
		Limits:        codec.DefaultLimits(),
	})
	if err != nil {
		return nil, fmt.Errorf("open encrypted durable state: %w", err)
	}
	return store, nil
}
