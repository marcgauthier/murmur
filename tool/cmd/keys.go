package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcgauthier/murmur/crypto"
	"github.com/marcgauthier/murmur/tool/format"
)

type KeysCommand struct{}

func (c *KeysCommand) Name() string        { return "keys" }
func (c *KeysCommand) Description() string { return "Inspect key registry metadata and test key decryption" }
func (c *KeysCommand) Usage() string {
	return "murmur keys <data-dir> [--passphrase=...] [--key-hex=...]"
}

func init() {
	Register(&KeysCommand{})
}

type keyRegistryReport struct {
	RegistryPath string         `json:"registry_path"`
	Exists       bool           `json:"exists"`
	Magic        string         `json:"magic,omitempty"`
	Version      uint16         `json:"version,omitempty"`
	StorageKeyID string         `json:"storage_key_id,omitempty"`
	SealedBytes  uint32         `json:"sealed_bytes,omitempty"`
	Unlocked     bool           `json:"unlocked"`
	DBID         string         `json:"db_id,omitempty"`
	Generation   uint64         `json:"generation,omitempty"`
	KeyCount     int            `json:"key_count,omitempty"`
	Keys         []keyEntryInfo `json:"keys,omitempty"`
	Pins         []string       `json:"pins,omitempty"`
	Error        string         `json:"error,omitempty"`
}

type keyEntryInfo struct {
	KeyID      string `json:"key_id"`
	Algorithm  string `json:"algorithm"`
	Generation uint64 `json:"generation"`
	CreatedAt  string `json:"created_at"`
	State      string `json:"state"`
}

func (c *KeysCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var dataDir string
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") && dataDir == "" {
			dataDir = arg
		}
	}

	if dataDir == "" {
		return fmt.Errorf("missing <data-dir>. Usage: %s", c.Usage())
	}

	regPath := filepath.Join(dataDir, "keys", "KEYREGISTRY")
	if _, err := os.Stat(regPath); err != nil {
		regPath = filepath.Join(dataDir, "KEYREGISTRY")
		if _, err := os.Stat(regPath); err != nil {
			return fmt.Errorf("KEYREGISTRY not found in %s or %s/keys", dataDir, dataDir)
		}
	}

	raw, err := os.ReadFile(regPath)
	if err != nil {
		return fmt.Errorf("read KEYREGISTRY file: %w", err)
	}

	report := keyRegistryReport{
		RegistryPath: regPath,
		Exists:       true,
	}

	if len(raw) < 8 {
		return fmt.Errorf("KEYREGISTRY file is truncated (%d bytes)", len(raw))
	}

	report.Magic = string(raw[0:4])
	report.Version = binary.LittleEndian.Uint16(raw[4:6])

	offset := 6
	if len(raw) >= offset+2 {
		skIDLen := int(binary.LittleEndian.Uint16(raw[offset : offset+2]))
		offset += 2
		if len(raw) >= offset+skIDLen {
			report.StorageKeyID = string(raw[offset : offset+skIDLen])
			offset += skIDLen
		}
	}

	offset += 12
	if len(raw) >= offset+4 {
		report.SealedBytes = binary.LittleEndian.Uint32(raw[offset : offset+4])
	}

	// Try unlocking if passphrase or key is provided, or try default CLI key
	var provider crypto.KeyProvider
	keyID := report.StorageKeyID
	if keyID == "" {
		keyID = "cli-key"
	}
	if globalOpts.Passphrase != "" {
		h := sha256.Sum256([]byte(globalOpts.Passphrase))
		provider = &crypto.StaticProvider{ID: keyID, Key: h[:]}
	} else if globalOpts.KeyHex != "" {
		keyBytes, err := hex.DecodeString(globalOpts.KeyHex)
		if err == nil && len(keyBytes) == 32 {
			provider = &crypto.StaticProvider{ID: keyID, Key: keyBytes}
		}
	} else {
		provider = &crypto.StaticProvider{ID: keyID, Key: []byte("0123456789abcdef0123456789abcdef")}
	}

	if provider != nil {
		keysDir := filepath.Dir(regPath)
		var dummyDBID [16]byte
		reg, err := crypto.OpenRegistry(keysDir, provider, dummyDBID)
		if err != nil {
			report.Error = fmt.Sprintf("Failed to unlock registry with provided credentials: %v", err)
		} else {
			defer reg.Close()
			report.Unlocked = true
			report.Generation = reg.Generation()
			report.KeyCount = reg.KeyCount()
			for _, k := range reg.Keys() {
				stateStr := "active"
				if k.State == crypto.KeyExpired {
					stateStr = "expired"
				}
				report.Keys = append(report.Keys, keyEntryInfo{
					KeyID:      hex.EncodeToString(k.ID[:]),
					Algorithm:  k.Alg.String(),
					Generation: k.Generation,
					CreatedAt:  k.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
					State:      stateStr,
				})
			}
			for _, pin := range reg.Pins() {
				report.Pins = append(report.Pins, fmt.Sprintf("%s (%s)", pin.Path, pin.Kind))
			}
		}
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, report, true)
	}

	fmt.Fprintln(stdout, "=== Key Registry Metadata ===")
	format.RenderKV(stdout, [][2]string{
		{"Registry Path", report.RegistryPath},
		{"Magic Header", report.Magic},
		{"Version", fmt.Sprintf("%d", report.Version)},
		{"Storage Key ID", report.StorageKeyID},
		{"Sealed Payload Size", fmt.Sprintf("%d bytes", report.SealedBytes)},
		{"Unlocked", fmt.Sprintf("%t", report.Unlocked)},
	})

	if report.Error != "" {
		fmt.Fprintf(stderr, "\nWarning: %s\n", report.Error)
	}

	if report.Unlocked {
		fmt.Fprintf(stdout, "\nGeneration: %d | Total Keys: %d\n", report.Generation, len(report.Keys))
		if len(report.Keys) > 0 {
			var headers = []string{"Key ID", "Algorithm", "Generation", "Created At", "State"}
			var rows [][]string
			for _, k := range report.Keys {
				rows = append(rows, []string{k.KeyID, k.Algorithm, fmt.Sprintf("%d", k.Generation), k.CreatedAt, k.State})
			}
			format.RenderTable(stdout, headers, rows, globalOpts.Markdown)
		}
		if len(report.Pins) > 0 {
			fmt.Fprintf(stdout, "\nPinned Checkpoints / Backups:\n")
			for _, pin := range report.Pins {
				fmt.Fprintf(stdout, "  - %s\n", pin)
			}
		}
	} else if provider == nil {
		fmt.Fprintf(stdout, "\n(Passphrase or --key-hex required to unlock and inspect individual data keys)\n")
	}

	return nil
}
