package cmd

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcgauthier/murmur/spool"
	"github.com/marcgauthier/murmur/tool/format"
)

type KeysCommand struct{}

func (c *KeysCommand) Name() string        { return "keys" }
func (c *KeysCommand) Description() string { return "Inspect the encrypted Spool key inventory" }
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

	candidates := []string{
		filepath.Join(dataDir, "data", "keys.enc"),
		filepath.Join(dataDir, "keys.enc"),
		filepath.Join(dataDir, "keys", "KEYREGISTRY"),
		filepath.Join(dataDir, "KEYREGISTRY"),
	}
	var regPath string
	for _, cand := range candidates {
		if _, err := os.Stat(cand); err == nil {
			regPath = cand
			break
		}
	}
	if regPath == "" {
		return fmt.Errorf("keyring not found in %s or %s/data", dataDir, dataDir)
	}

	raw, err := os.ReadFile(regPath)
	if err != nil {
		return fmt.Errorf("read keyring file: %w", err)
	}

	if len(raw) < 8 {
		return fmt.Errorf("keyring file is truncated (%d bytes)", len(raw))
	}

	report := keyRegistryReport{
		RegistryPath: regPath,
		Exists:       true,
		Magic:        string(raw[0:4]),
		Version:      binary.LittleEndian.Uint16(raw[4:6]),
		SealedBytes:  uint32(len(raw)),
	}

	// Try reading Spool key hint if in Spool data dir
	dataPath := filepath.Dir(regPath)
	if hint, err := spool.ReadKeyHint(dataPath); err == nil {
		report.StorageKeyID = hint.WrappingKeyID
		report.Generation = hint.KeyringSeq
	}

	// Unlock the keyring through authoritative Spool state. Key inspection
	// needs no query engine or RIME materializer.
	if err := ctx.Err(); err != nil {
		return err
	}
	store, err := openOfflineStore(dataDir, globalOpts)
	if err != nil {
		report.Error = fmt.Sprintf("Failed to unlock registry with provided credentials: %v", err)
	} else {
		defer store.Close()
		inv := store.KeyInventory()
		report.Unlocked = true
		report.StorageKeyID = inv.WrappingKeyID
		report.Generation = inv.ManifestGeneration
		report.KeyCount = len(inv.DataKeys)
		for _, k := range inv.DataKeys {
			stateStr := "active"
			if k.Status != spool.KeyStatusActive {
				stateStr = "inactive"
			}
			report.Keys = append(report.Keys, keyEntryInfo{
				KeyID:      fmt.Sprintf("%08x", k.ID),
				Algorithm:  "AES-256-GCM",
				Generation: inv.ManifestGeneration,
				CreatedAt:  k.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"),
				State:      stateStr,
			})
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
	} else {
		fmt.Fprintf(stdout, "\n(Passphrase or --key-hex required to unlock and inspect individual data keys)\n")
	}

	return nil
}
