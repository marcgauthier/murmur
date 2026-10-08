package cmd

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/marcgauthier/murmur/tool/format"
)

type InspectCommand struct{}

func (c *InspectCommand) Name() string { return "inspect" }
func (c *InspectCommand) Description() string {
	return "Inspect offline database metadata, watermarks, and schema"
}
func (c *InspectCommand) Usage() string {
	return "murmur inspect <data-dir> [--json]"
}

func init() {
	Register(&InspectCommand{})
}

type inspectionReport struct {
	DataDir          string `json:"data_dir"`
	NodeID           string `json:"node_id"`
	DBID             string `json:"db_id"`
	State            string `json:"state"`
	StateGeneration  uint64 `json:"state_generation"`
	HLC              uint64 `json:"hlc_watermark"`
	LocalSeq         uint64 `json:"local_sequence"`
	SchemaEpoch      uint64 `json:"schema_epoch"`
	SchemaHash       string `json:"schema_hash"`
	KeyRegistryPath  string `json:"key_registry_path,omitempty"`
	StorageFiles     int    `json:"storage_files"`
	StorageDiskBytes int64  `json:"storage_disk_bytes"`
	TableCount       int    `json:"table_count"`
}

func (c *InspectCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var dataDir string
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-") && dataDir == "" {
			dataDir = arg
		}
	}

	if dataDir == "" {
		return fmt.Errorf("missing <data-dir>. Usage: %s", c.Usage())
	}

	if _, err := os.Stat(dataDir); err != nil {
		return fmt.Errorf("data directory does not exist: %w", err)
	}

	var diskBytes int64
	var fileCount int
	_ = filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			fileCount++
			diskBytes += info.Size()
		}
		return nil
	})

	report := inspectionReport{
		DataDir:          dataDir,
		StorageFiles:     fileCount,
		StorageDiskBytes: diskBytes,
	}

	keysDir := filepath.Join(dataDir, "keys")
	keyRegistryFile := filepath.Join(keysDir, "KEYREGISTRY")
	if _, err := os.Stat(keyRegistryFile); err == nil {
		report.KeyRegistryPath = keyRegistryFile
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	store, err := openOfflineStore(dataDir, globalOpts)
	if err != nil {
		return fmt.Errorf("open durable state for inspection: %w", err)
	}
	defer store.Close()

	report.NodeID = store.NodeID().String()
	report.DBID = store.DBID().String()
	report.State = "durable-state-open"
	report.StateGeneration, err = store.StateGeneration()
	if err != nil {
		return fmt.Errorf("read durable state generation: %w", err)
	}
	report.HLC = store.ClockMax()
	report.LocalSeq, err = store.LocalSeq()
	if err != nil {
		return fmt.Errorf("read local sequence: %w", err)
	}

	// Table definitions come from the durable schema manifest, not from a
	// query-engine catalog. This keeps inspection independent of the current
	// materializer implementation.
	manifest, err := store.LoadSchemaManifest()
	if err != nil {
		return fmt.Errorf("read schema manifest for inspection: %w", err)
	}
	if manifest != nil {
		report.SchemaEpoch = manifest.Version
		report.SchemaHash = hex.EncodeToString(manifest.Hash[:])
		report.TableCount = len(manifest.Tables)
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, report, true)
	}

	fmt.Fprintln(stdout, "=== Murmur Storage Inspection ===")
	format.RenderKV(stdout, [][2]string{
		{"Data Directory", report.DataDir},
		{"Node ID", report.NodeID},
		{"Cluster DB ID", report.DBID},
		{"Node State", report.State},
		{"State Generation", fmt.Sprintf("%d", report.StateGeneration)},
		{"HLC Watermark", fmt.Sprintf("%d", report.HLC)},
		{"Local Sequence", fmt.Sprintf("%d", report.LocalSeq)},
		{"Schema Epoch", fmt.Sprintf("%d", report.SchemaEpoch)},
		{"Schema Hash", report.SchemaHash},
		{"Key Registry", report.KeyRegistryPath},
		{"User Tables", fmt.Sprintf("%d", report.TableCount)},
		{"Storage Disk Size", fmt.Sprintf("%d files (%d bytes / %.2f MB)", report.StorageFiles, diskBytes, float64(diskBytes)/(1024*1024))},
	})

	return nil
}
