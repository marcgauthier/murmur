package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcgauthier/murmur/tool/format"
)

type RepairCommand struct{}

func (c *RepairCommand) Name() string { return "repair" }
func (c *RepairCommand) Description() string {
	return "Validate durable storage and clean restore intents"
}
func (c *RepairCommand) Usage() string {
	return "murmur repair <data-dir> [--clear-intent] [--dry-run]"
}

func init() {
	Register(&RepairCommand{})
}

type repairResult struct {
	DataDir              string `json:"data_dir"`
	DurableStateChecked  bool   `json:"durable_state_checked"`
	RestoreIntentCleaned bool   `json:"restore_intent_cleaned"`
	Success              bool   `json:"success"`
	Message              string `json:"message"`
}

func (c *RepairCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var dataDir string
	clearIntent := false
	dryRun := false

	for _, arg := range args {
		switch {
		case arg == "--rebuild-materializer" || arg == "--force":
			return fmt.Errorf("the CLI cannot rebuild a RIME materializer without the application's Go table definitions; start the application with its typed configuration")
		case arg == "--clear-intent":
			clearIntent = true
		case arg == "--dry-run":
			dryRun = true
		case !strings.HasPrefix(arg, "-") && dataDir == "":
			dataDir = arg
		}
	}

	if dataDir == "" {
		return fmt.Errorf("missing <data-dir>. Usage: %s", c.Usage())
	}

	res := repairResult{
		DataDir: dataDir,
		Success: true,
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	intentFile := filepath.Join(dataDir, "RESTORE_INTENT")
	_, intentErr := os.Stat(intentFile)
	intentPending := intentErr == nil
	if intentErr != nil && !os.IsNotExist(intentErr) {
		return fmt.Errorf("inspect RESTORE_INTENT: %w", intentErr)
	}
	if clearIntent && intentPending && !dryRun {
		if err := os.Remove(intentFile); err != nil {
			return fmt.Errorf("remove RESTORE_INTENT: %w", err)
		}
		res.RestoreIntentCleaned = true
	}
	if !dryRun && !(intentPending && !clearIntent) {
		store, err := openOfflineStore(dataDir, globalOpts)
		if err != nil {
			return fmt.Errorf("durable state check failed: %w", err)
		}
		if _, err := store.LoadSchemaManifest(); err != nil {
			_ = store.Close()
			return fmt.Errorf("durable schema manifest check failed: %w", err)
		}
		if _, err := store.StateGeneration(); err != nil {
			_ = store.Close()
			return fmt.Errorf("durable state generation check failed: %w", err)
		}
		if err := store.Close(); err != nil {
			return fmt.Errorf("close durable state: %w", err)
		}
		res.DurableStateChecked = true
	}

	res.Message = "Durable state checked; RIME materialization is rebuilt when the application opens with its Go schema"
	if intentPending && !clearIntent {
		res.Message = "Restore intent remains pending; use --clear-intent only after confirming recovery state"
	}
	if clearIntent && intentPending && res.RestoreIntentCleaned {
		res.Message = "Restore intent cleared and durable state checked"
	}
	if dryRun {
		res.Message = "Dry run completed: no modifications made or durable state opened"
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, res, true)
	}

	fmt.Fprintf(stdout, "%s for %s\n", res.Message, dataDir)
	format.RenderKV(stdout, [][2]string{
		{"Durable State Checked", fmt.Sprintf("%t", res.DurableStateChecked)},
		{"Stale Restore Intent Cleaned", fmt.Sprintf("%t", res.RestoreIntentCleaned)},
		{"Status", "REPAIRED"},
	})

	return nil
}
