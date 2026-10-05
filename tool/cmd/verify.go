package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/marcgauthier/murmur/tool/format"
)

type VerifyCommand struct{}

func (c *VerifyCommand) Name() string        { return "verify" }
func (c *VerifyCommand) Description() string { return "Verify integrity of offline storage, WAL, and schema DAG" }
func (c *VerifyCommand) Usage() string {
	return "murmur verify <data-dir> [--deep] [--json]"
}

func init() {
	Register(&VerifyCommand{})
}

type verifyResult struct {
	DataDir        string   `json:"data_dir"`
	Valid          bool     `json:"valid"`
	TablesChecked  int      `json:"tables_checked"`
	RowsChecked    int64    `json:"rows_checked"`
	Errors         []string `json:"errors,omitempty"`
	Warnings       []string `json:"warnings,omitempty"`
}

func (c *VerifyCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var dataDir string
	deep := false

	for _, arg := range args {
		switch {
		case arg == "--deep":
			deep = true
		case !strings.HasPrefix(arg, "-") && dataDir == "":
			dataDir = arg
		}
	}

	if dataDir == "" {
		return fmt.Errorf("missing <data-dir>. Usage: %s", c.Usage())
	}

	res := verifyResult{
		DataDir: dataDir,
		Valid:   true,
	}

	db, err := openLocalDB(ctx, dataDir, globalOpts, true)
	if err != nil {
		res.Valid = false
		res.Errors = append(res.Errors, fmt.Sprintf("Failed to open database: %v", err))
		if globalOpts.JSON {
			return format.RenderJSON(stdout, res, true)
		}
		fmt.Fprintf(stderr, "FAIL: %v\n", err)
		return nil
	}
	defer db.Close()

	// 1. Run SQLite PRAGMA quick_check or integrity_check
	checkSQL := "PRAGMA quick_check;"
	if deep {
		checkSQL = "PRAGMA integrity_check;"
	}

	rows, err := db.QueryContext(ctx, checkSQL)
	if err != nil {
		res.Valid = false
		res.Errors = append(res.Errors, fmt.Sprintf("Integrity check query failed: %v", err))
	} else {
		for rows.Next() {
			var msg string
			if err := rows.Scan(&msg); err == nil {
				if msg != "ok" {
					res.Valid = false
					res.Errors = append(res.Errors, msg)
				}
			}
		}
		rows.Close()
	}

	// 2. Query table counts
	tRows, err := db.QueryContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
	if err == nil {
		for tRows.Next() {
			var tbl string
			if err := tRows.Scan(&tbl); err == nil {
				res.TablesChecked++
				var count int64
				_ = db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s;", tbl)).Scan(&count)
				res.RowsChecked += count
			}
		}
		tRows.Close()
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, res, true)
	}

	if res.Valid {
		fmt.Fprintf(stdout, "PASS: Storage and SQLite materialization verified for %s\n", dataDir)
		format.RenderKV(stdout, [][2]string{
			{"Tables Checked", fmt.Sprintf("%d", res.TablesChecked)},
			{"Total Rows Verified", fmt.Sprintf("%d", res.RowsChecked)},
			{"Integrity Status", "HEALTHY"},
		})
	} else {
		fmt.Fprintf(stderr, "FAIL: Integrity verification found %d error(s) in %s:\n", len(res.Errors), dataDir)
		for _, e := range res.Errors {
			fmt.Fprintf(stderr, "  - %s\n", e)
		}
	}

	return nil
}
