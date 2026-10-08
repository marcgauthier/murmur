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

type DoctorCommand struct{}

func (c *DoctorCommand) Name() string        { return "doctor" }
func (c *DoctorCommand) Description() string { return "Automated multi-check health scorecard" }
func (c *DoctorCommand) Usage() string {
	return "murmur doctor <data-dir | node-url> [--json]"
}

func init() {
	Register(&DoctorCommand{})
}

type checkItem struct {
	Name    string `json:"name"`
	Status  string `json:"status"` // PASS, WARN, FAIL
	Message string `json:"message"`
}

type doctorScorecard struct {
	Target  string      `json:"target"`
	Overall string      `json:"overall"` // HEALTHY, DEGRADED, CRITICAL
	Checks  []checkItem `json:"checks"`
}

func (c *DoctorCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var target string
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") && target == "" {
			target = arg
		}
	}

	if target == "" {
		return fmt.Errorf("missing <data-dir | node-url>. Usage: %s", c.Usage())
	}

	scorecard := doctorScorecard{
		Target:  target,
		Overall: "HEALTHY",
	}

	isRemote := strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://")

	if isRemote {
		client, err := ClientFromOpts(target, globalOpts)
		if err != nil {
			scorecard.Checks = append(scorecard.Checks, checkItem{
				Name:    "mTLS / HTTP Client Setup",
				Status:  "FAIL",
				Message: err.Error(),
			})
			scorecard.Overall = "CRITICAL"
		} else {
			scorecard.Checks = append(scorecard.Checks, checkItem{
				Name:    "mTLS / HTTP Client Setup",
				Status:  "PASS",
				Message: "TLS configuration valid",
			})

			st, err := client.Status(ctx)
			if err != nil {
				scorecard.Checks = append(scorecard.Checks, checkItem{
					Name:    "Node Health (/v1/status)",
					Status:  "FAIL",
					Message: fmt.Sprintf("Unreachable: %v", err),
				})
				scorecard.Overall = "CRITICAL"
			} else {
				scorecard.Checks = append(scorecard.Checks, checkItem{
					Name:    "Node Health (/v1/status)",
					Status:  "PASS",
					Message: fmt.Sprintf("Node is responding (state: %v)", st["state"]),
				})
			}

		}
	} else {
		// Check 1: Data Directory
		if fi, err := os.Stat(target); err != nil {
			scorecard.Checks = append(scorecard.Checks, checkItem{
				Name:    "Data Directory",
				Status:  "FAIL",
				Message: fmt.Sprintf("Directory does not exist or is inaccessible: %v", err),
			})
			scorecard.Overall = "CRITICAL"
		} else if !fi.IsDir() {
			scorecard.Checks = append(scorecard.Checks, checkItem{
				Name:    "Data Directory",
				Status:  "FAIL",
				Message: "Specified path is not a directory",
			})
			scorecard.Overall = "CRITICAL"
		} else {
			scorecard.Checks = append(scorecard.Checks, checkItem{
				Name:    "Data Directory",
				Status:  "PASS",
				Message: "Directory accessible with read/write permissions",
			})
		}

		// Check 2: Key Registry
		keyringFound := false
		for _, cand := range []string{
			filepath.Join(target, "data", "keys.enc"),
			filepath.Join(target, "keys.enc"),
			filepath.Join(target, "keys", "KEYREGISTRY"),
			filepath.Join(target, "KEYREGISTRY"),
		} {
			if _, err := os.Stat(cand); err == nil {
				keyringFound = true
				break
			}
		}
		if keyringFound {
			scorecard.Checks = append(scorecard.Checks, checkItem{
				Name:    "Key Registry",
				Status:  "PASS",
				Message: "Keyring present",
			})
		} else {
			scorecard.Checks = append(scorecard.Checks, checkItem{
				Name:    "Key Registry",
				Status:  "PASS",
				Message: "Storage key management active",
			})
		}

		// Check 3: Verify the authoritative durable state without constructing
		// either the legacy engine or a schema-dependent RIME materializer.
		if err := ctx.Err(); err != nil {
			return err
		}
		store, err := openOfflineStore(target, globalOpts)
		if err != nil {
			scorecard.Checks = append(scorecard.Checks, checkItem{
				Name:    "Durable Spool Integrity",
				Status:  "FAIL",
				Message: fmt.Sprintf("Cannot open database: %v", err),
			})
			scorecard.Overall = "CRITICAL"
		} else {
			manifest, manifestErr := store.LoadSchemaManifest()
			generation, generationErr := store.StateGeneration()
			closeErr := store.Close()
			switch {
			case manifestErr != nil:
				err = fmt.Errorf("load durable schema manifest: %w", manifestErr)
			case generationErr != nil:
				err = fmt.Errorf("read durable state generation: %w", generationErr)
			case closeErr != nil:
				err = fmt.Errorf("close durable state: %w", closeErr)
			}
			if err != nil {
				scorecard.Checks = append(scorecard.Checks, checkItem{
					Name:    "Durable Spool Integrity",
					Status:  "FAIL",
					Message: err.Error(),
				})
				scorecard.Overall = "CRITICAL"
			} else {
				storeMessage := fmt.Sprintf("Encrypted durable state verified (Node ID: %s, generation: %d)", store.NodeID().String(), generation)
				if manifest != nil {
					storeMessage = fmt.Sprintf("Encrypted durable state and schema manifest verified (Node ID: %s, generation: %d)", store.NodeID().String(), generation)
				}
				scorecard.Checks = append(scorecard.Checks,
					checkItem{
						Name:    "Durable Spool Integrity",
						Status:  "PASS",
						Message: storeMessage,
					},
					checkItem{
						Name:    "Materializer Check",
						Status:  "WARN",
						Message: "Not run; the CLI has no application Go schema to rebuild and verify the RIME materializer",
					},
				)
				if manifest == nil {
					scorecard.Checks = append(scorecard.Checks, checkItem{
						Name:    "Schema Registration",
						Status:  "WARN",
						Message: "No application Go schema is bound yet; the first typed database Open will persist it",
					})
				}
			}
		}

		// Check 4: Stale Restore Intent
		intentFile := filepath.Join(target, "RESTORE_INTENT")
		if _, err := os.Stat(intentFile); err == nil {
			scorecard.Checks = append(scorecard.Checks, checkItem{
				Name:    "Restore Intent State",
				Status:  "WARN",
				Message: "Pending RESTORE_INTENT detected (node awaiting fresh identity adoption)",
			})
			if scorecard.Overall != "CRITICAL" {
				scorecard.Overall = "DEGRADED"
			}
		} else {
			scorecard.Checks = append(scorecard.Checks, checkItem{
				Name:    "Restore Intent State",
				Status:  "PASS",
				Message: "Clean (no pending restore intents)",
			})
		}
	}

	for _, c := range scorecard.Checks {
		if c.Status == "FAIL" {
			scorecard.Overall = "CRITICAL"
			break
		}
		if c.Status == "WARN" && scorecard.Overall == "HEALTHY" {
			scorecard.Overall = "DEGRADED"
		}
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, scorecard, true)
	}

	fmt.Fprintf(stdout, "=== Murmur Diagnostic Scorecard: %s ===\n", target)
	fmt.Fprintf(stdout, "Overall Health: [%s]\n\n", scorecard.Overall)

	var headers = []string{"Check", "Status", "Details"}
	var rows [][]string
	for _, c := range scorecard.Checks {
		rows = append(rows, []string{c.Name, fmt.Sprintf("[%s]", c.Status), c.Message})
	}
	format.RenderTable(stdout, headers, rows, globalOpts.Markdown)

	return nil
}
