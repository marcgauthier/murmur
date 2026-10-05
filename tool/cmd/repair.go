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

func (c *RepairCommand) Name() string        { return "repair" }
func (c *RepairCommand) Description() string { return "Repair storage, clean stale intents, and rebuild SQLite" }
func (c *RepairCommand) Usage() string {
	return "murmur repair <data-dir> [--rebuild-sqlite] [--clear-intent] [--dry-run]"
}

func init() {
	Register(&RepairCommand{})
}

type repairResult struct {
	DataDir              string `json:"data_dir"`
	SQLiteRebuilt        bool   `json:"sqlite_rebuilt"`
	RestoreIntentCleaned bool   `json:"restore_intent_cleaned"`
	Success              bool   `json:"success"`
	Message              string `json:"message"`
}

func (c *RepairCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var dataDir string
	rebuildSQLite := true
	clearIntent := false
	dryRun := false

	for _, arg := range args {
		switch {
		case arg == "--rebuild-sqlite":
			rebuildSQLite = true
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

	if clearIntent {
		intentFile := filepath.Join(dataDir, "RESTORE_INTENT")
		if _, err := os.Stat(intentFile); err == nil {
			if !dryRun {
				if err := os.Remove(intentFile); err != nil {
					return fmt.Errorf("remove RESTORE_INTENT: %w", err)
				}
			}
			res.RestoreIntentCleaned = true
		}
	}

	if rebuildSQLite {
		sqliteFile := filepath.Join(dataDir, "murmur_sqlite.db")
		if _, err := os.Stat(sqliteFile); err == nil && !dryRun {
			if err := os.Remove(sqliteFile); err != nil {
				return fmt.Errorf("remove damaged SQLite file %s: %w", sqliteFile, err)
			}
			_ = os.Remove(sqliteFile + "-wal")
			_ = os.Remove(sqliteFile + "-shm")
		}

		if !dryRun {
			db, err := openLocalDB(ctx, dataDir, globalOpts, false)
			if err != nil {
				return fmt.Errorf("rebuild failed during database open: %w", err)
			}
			_ = db.Close()
			res.SQLiteRebuilt = true
		}
	}

	res.Message = "Repair operations completed successfully"
	if dryRun {
		res.Message = "Dry run completed: no modifications made"
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, res, true)
	}

	fmt.Fprintf(stdout, "%s for %s\n", res.Message, dataDir)
	format.RenderKV(stdout, [][2]string{
		{"SQLite Materialization Rebuilt", fmt.Sprintf("%t", res.SQLiteRebuilt)},
		{"Stale Restore Intent Cleaned", fmt.Sprintf("%t", res.RestoreIntentCleaned)},
		{"Status", "REPAIRED"},
	})

	return nil
}
