package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/marcgauthier/murmur/tool/format"
)

type InitCommand struct{}

func (c *InitCommand) Name() string        { return "init" }
func (c *InitCommand) Description() string { return "Initialize a new encrypted database directory" }
func (c *InitCommand) Usage() string {
	return "murmur init <data-dir> [--passphrase=P] [--key-hex=K]"
}

func init() {
	Register(&InitCommand{})
}

func (c *InitCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var dataDir string
	for _, arg := range args {
		if strings.HasPrefix(arg, "--seed=") || arg == "--seed" {
			return fmt.Errorf("SQL seed files are no longer supported; define Go table types in your application")
		}
		if !strings.HasPrefix(arg, "-") {
			dataDir = arg
		}
	}

	if dataDir == "" {
		return fmt.Errorf("missing <data-dir>. Usage: %s", c.Usage())
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	store, err := openOfflineStoreForInit(dataDir, globalOpts)
	if err != nil {
		return fmt.Errorf("init failed to open encrypted Spool: %w", err)
	}
	nodeID, dbID := store.NodeID(), store.DBID()
	keyInventory := store.KeyInventory()
	if err := store.Close(); err != nil {
		return fmt.Errorf("close initialized Spool: %w", err)
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, map[string]any{
			"initialized": true,
			"data_dir":    dataDir,
			"node_id":     nodeID.String(),
			"db_id":       dbID.String(),
			"encrypted":   keyInventory.Encrypted,
		}, true)
	}

	fmt.Fprintf(stdout, "Initialized new Murmur database successfully.\n")
	format.RenderKV(stdout, [][2]string{
		{"Data Directory", dataDir},
		{"Node ID", nodeID.String()},
		{"Cluster DB ID", dbID.String()},
		{"Encrypted", fmt.Sprintf("%t", keyInventory.Encrypted)},
		{"Schema", "Bound by the application's Go table definitions on first open"},
	})
	return nil
}
