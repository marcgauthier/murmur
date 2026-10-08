package cmd

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/marcgauthier/murmur/tool/format"
)

type VerifyCommand struct{}

func (c *VerifyCommand) Name() string { return "verify" }
func (c *VerifyCommand) Description() string {
	return "Verify integrity of offline storage, WAL, and schema DAG"
}
func (c *VerifyCommand) Usage() string {
	return "murmur verify <data-dir> [--deep] [--json]"
}

func init() {
	Register(&VerifyCommand{})
}

type verifyResult struct {
	DataDir             string   `json:"data_dir"`
	Valid               bool     `json:"valid"`
	TablesChecked       int      `json:"tables_checked"`
	StateGeneration     uint64   `json:"state_generation"`
	MaterializerChecked bool     `json:"materializer_checked"`
	Deep                bool     `json:"deep"`
	Errors              []string `json:"errors,omitempty"`
	Warnings            []string `json:"warnings,omitempty"`
}

func (c *VerifyCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var dataDir string
	deep := false

	for _, arg := range args {
		switch {
		case arg == "--deep" || arg == "--full":
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
		Deep:    deep,
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	store, err := openOfflineStore(dataDir, globalOpts)
	if err != nil {
		res.Valid = false
		res.Errors = append(res.Errors, fmt.Sprintf("Failed to open durable state: %v", err))
		if globalOpts.JSON {
			return format.RenderJSON(stdout, res, true)
		}
		fmt.Fprintf(stderr, "FAIL: %v\n", err)
		return nil
	}
	defer store.Close()

	// Opening Spool replays and authenticates its durable records. Read the
	// manifest from the same authoritative state; the CLI has no application
	// Go types with which to reconstruct a RIME materializer.
	manifest, err := store.LoadSchemaManifest()
	if err != nil {
		res.Valid = false
		res.Errors = append(res.Errors, fmt.Sprintf("Schema manifest verification failed: %v", err))
	} else if manifest == nil {
		res.Warnings = append(res.Warnings, "Schema is not bound yet; the first application Open will persist its Go table definitions")
	} else {
		res.TablesChecked = len(manifest.Tables)
	}
	res.StateGeneration, err = store.StateGeneration()
	if err != nil {
		res.Valid = false
		res.Errors = append(res.Errors, fmt.Sprintf("State generation read failed: %v", err))
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, res, true)
	}

	if res.Valid {
		fmt.Fprintf(stdout, "PASS: Durable Spool state verified for %s\n", dataDir)
		integrityStatus := "HEALTHY"
		if len(res.Warnings) > 0 {
			integrityStatus = "DEGRADED"
		}
		format.RenderKV(stdout, [][2]string{
			{"Tables Checked", fmt.Sprintf("%d", res.TablesChecked)},
			{"State Generation", fmt.Sprintf("%d", res.StateGeneration)},
			{"Schema Status", map[bool]string{true: "bound", false: "not yet bound"}[res.Warnings == nil]},
			{"Materializer Check", "not run (application schema unavailable)"},
			{"Integrity Status", integrityStatus},
		})
		for _, warning := range res.Warnings {
			fmt.Fprintf(stderr, "WARN: %s\n", warning)
		}
	} else {
		fmt.Fprintf(stderr, "FAIL: Integrity verification found %d error(s) in %s:\n", len(res.Errors), dataDir)
		for _, e := range res.Errors {
			fmt.Fprintf(stderr, "  - %s\n", e)
		}
	}

	return nil
}
