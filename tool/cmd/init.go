package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/marcgauthier/murmur/tool/format"
)

type InitCommand struct{}

func (c *InitCommand) Name() string        { return "init" }
func (c *InitCommand) Description() string { return "Initialize a new encrypted database directory" }
func (c *InitCommand) Usage() string {
	return "murmur init <data-dir> [--passphrase=P] [--key-hex=K] [--seed=schema.sql]"
}

func init() {
	Register(&InitCommand{})
}

func (c *InitCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var dataDir string
	var seedFile string

	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--seed="):
			seedFile = strings.TrimPrefix(arg, "--seed=")
		case !strings.HasPrefix(arg, "-"):
			dataDir = arg
		}
	}

	if dataDir == "" {
		return fmt.Errorf("missing <data-dir>. Usage: %s", c.Usage())
	}

	db, err := openLocalDB(ctx, dataDir, globalOpts, false)
	if err != nil {
		return fmt.Errorf("init failed to open database: %w", err)
	}

	if seedFile != "" {
		seedSQL, err := os.ReadFile(seedFile)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("read seed file: %w", err)
		}
		if _, err := handleDDLOrExec(ctx, db, string(seedSQL)); err != nil {
			_ = db.Close()
			return fmt.Errorf("execute seed SQL: %w", err)
		}
	}

	status := db.Status()
	_ = db.Close()

	if globalOpts.JSON {
		return format.RenderJSON(stdout, map[string]any{
			"initialized": true,
			"data_dir":    dataDir,
			"node_id":     status.NodeID.String(),
			"db_id":       status.DBID.String(),
			"encrypted":   globalOpts.Passphrase != "" || globalOpts.KeyHex != "",
			"seed_loaded": seedFile != "",
		}, true)
	}

	fmt.Fprintf(stdout, "Initialized new Murmur database successfully.\n")
	format.RenderKV(stdout, [][2]string{
		{"Data Directory", dataDir},
		{"Node ID", status.NodeID.String()},
		{"Cluster DB ID", status.DBID.String()},
		{"Encrypted", fmt.Sprintf("%t", globalOpts.Passphrase != "" || globalOpts.KeyHex != "")},
		{"Seed Schema", seedFile},
	})
	return nil
}
