package cmd

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/tool/format"
)

type BackupCommand struct{}

func (c *BackupCommand) Name() string        { return "backup" }
func (c *BackupCommand) Description() string { return "Create, inspect, verify, and restore backups" }
func (c *BackupCommand) Usage() string {
	return `murmur backup create  <data-dir> <dest.tar.gz>
  murmur backup info    <archive.tar.gz>
  murmur backup verify  <archive.tar.gz>
  murmur backup restore <archive.tar.gz> <dest-dir> [--fresh-node-id=ID] [--mode=clone|reseed]`
}

func init() {
	Register(&BackupCommand{})
}

func (c *BackupCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("missing backup action (create, info, verify, restore). Usage:\n%s", c.Usage())
	}

	action := args[0]
	rest := args[1:]

	switch action {
	case "create":
		return c.runCreate(ctx, globalOpts, rest, stdout, stderr)
	case "info":
		return c.runInfo(ctx, globalOpts, rest, stdout, stderr)
	case "verify":
		return c.runVerify(ctx, globalOpts, rest, stdout, stderr)
	case "restore":
		return c.runRestore(ctx, globalOpts, rest, stdout, stderr)
	default:
		return fmt.Errorf("unknown backup action %q. Usage:\n%s", action, c.Usage())
	}
}

func (c *BackupCommand) runCreate(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: murmur backup create <data-dir> <dest.tar.gz>")
	}
	dataDir := args[0]
	archivePath := args[1]

	db, err := openLocalDB(ctx, dataDir, globalOpts, false)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	destDir := filepath.Dir(archivePath)
	destName := filepath.Base(archivePath)
	dest, err := backup.NewLocalDestination(destDir)
	if err != nil {
		return fmt.Errorf("create backup destination: %w", err)
	}

	start := time.Now()
	meta, err := backup.CreateBackup(ctx, db, backup.Config{
		Destination: dest,
	})
	if err != nil {
		return fmt.Errorf("backup failed: %w", err)
	}
	elapsed := time.Since(start)

	createdFilename := fmt.Sprintf("nomadsql-backup-%s-%s-%s.tar.gz", meta.DBID, meta.CreatedAt.Format("20060102-150405"), meta.BackupID[:8])
	createdPath := filepath.Join(destDir, createdFilename)
	if _, err := os.Stat(createdPath); err == nil && createdPath != archivePath {
		_ = os.Rename(createdPath, archivePath)
	}

	if schemaData, err := os.ReadFile(filepath.Join(dataDir, "schema.json")); err == nil && len(schemaData) > 0 {
		_ = appendFileToTarGz(archivePath, "schema.json", schemaData)
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, map[string]any{
			"success":     true,
			"archive":     destName,
			"db_id":       meta.DBID,
			"node_id":     meta.NodeID,
			"files_count": meta.DataFilesCount,
			"size_bytes":  meta.TotalBytes,
			"elapsed_ms":  elapsed.Milliseconds(),
		}, true)
	}

	fmt.Fprintf(stdout, "Backup created successfully in %s\n", elapsed)
	format.RenderKV(stdout, [][2]string{
		{"Archive File", archivePath},
		{"Cluster DB ID", meta.DBID},
		{"Source Node ID", meta.NodeID},
		{"Files Archived", fmt.Sprintf("%d", meta.DataFilesCount)},
		{"Archive Size", fmt.Sprintf("%d bytes (%.2f MB)", meta.TotalBytes, float64(meta.TotalBytes)/(1024*1024))},
	})
	return nil
}

func (c *BackupCommand) runInfo(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: murmur backup info <archive.tar.gz>")
	}
	archivePath := args[0]

	meta, err := readArchiveMetadata(archivePath)
	if err != nil {
		return fmt.Errorf("read backup metadata: %w", err)
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, meta, true)
	}

	fmt.Fprintln(stdout, "=== Murmur Backup Archive Info ===")
	format.RenderKV(stdout, [][2]string{
		{"Archive File", archivePath},
		{"Cluster DB ID", meta.DBID},
		{"Source Node ID", meta.NodeID},
		{"Creation Time", meta.CreatedAt.UTC().Format("2006-01-02T15:04:05Z")},
		{"Schema Epoch", fmt.Sprintf("%d", meta.SchemaEpoch)},
		{"Schema Hash", meta.SchemaHash},
		{"Files Archived", fmt.Sprintf("%d", meta.DataFilesCount)},
		{"Archive Size", fmt.Sprintf("%d bytes", meta.TotalBytes)},
	})
	return nil
}

func (c *BackupCommand) runVerify(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: murmur backup verify <archive.tar.gz>")
	}
	archivePath := args[0]

	f, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}
	defer f.Close()

	hasher := sha256.New()
	gzr, err := gzip.NewReader(io.TeeReader(f, hasher))
	if err != nil {
		return fmt.Errorf("open gzip stream: %w", err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	var meta *backup.Metadata
	fileCount := 0
	var totalBytes int64

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("corrupt tar archive: %w", err)
		}
		fileCount++
		totalBytes += hdr.Size

		if hdr.Name == "backup-metadata.json" {
			meta = &backup.Metadata{}
			if err := json.NewDecoder(tr).Decode(meta); err != nil {
				return fmt.Errorf("corrupt backup-metadata.json: %w", err)
			}
		} else {
			if _, err := io.Copy(io.Discard, tr); err != nil {
				return fmt.Errorf("read file %s: %w", hdr.Name, err)
			}
		}
	}

	if meta == nil {
		return fmt.Errorf("missing backup-metadata.json in archive")
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, map[string]any{
			"valid":           true,
			"archive":         archivePath,
			"entries_checked": fileCount,
			"unpacked_bytes":  totalBytes,
			"db_id":           meta.DBID,
			"source_node":     meta.NodeID,
		}, true)
	}

	fmt.Fprintf(stdout, "PASS: Backup archive %s verified successfully\n", archivePath)
	format.RenderKV(stdout, [][2]string{
		{"Entries Checked", fmt.Sprintf("%d", fileCount)},
		{"Total Payload Size", fmt.Sprintf("%d bytes", totalBytes)},
		{"Cluster DB ID", meta.DBID},
		{"Source Node ID", meta.NodeID},
		{"Integrity Status", "VALID"},
	})
	return nil
}

func (c *BackupCommand) runRestore(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	var archivePath, destDir string
	var freshNodeID string
	modeStr := "clone"

	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--fresh-node-id="):
			freshNodeID = strings.TrimPrefix(arg, "--fresh-node-id=")
		case strings.HasPrefix(arg, "--mode="):
			modeStr = strings.TrimPrefix(arg, "--mode=")
		case !strings.HasPrefix(arg, "-"):
			if archivePath == "" {
				archivePath = arg
			} else if destDir == "" {
				destDir = arg
			}
		}
	}

	if archivePath == "" || destDir == "" {
		return fmt.Errorf("usage: murmur backup restore <archive.tar.gz> <dest-dir> [--fresh-node-id=ID]")
	}

	if freshNodeID == "" {
		freshNodeID = ids.NewNodeID().String()
	}

	restoreMode := backup.RestoreClone
	if modeStr == "reseed" {
		restoreMode = backup.RestoreReseed
	}

	destDirSource := filepath.Dir(archivePath)
	destName := filepath.Base(archivePath)
	srcDest, err := backup.NewLocalDestination(destDirSource)
	if err != nil {
		return fmt.Errorf("open backup source destination: %w", err)
	}

	start := time.Now()
	meta, err := backup.Restore(ctx, backup.RestoreConfig{
		Source:      srcDest,
		BackupName:  destName,
		TargetPath:  destDir,
		Mode:        restoreMode,
		FreshNodeID: freshNodeID,
	})
	if err != nil {
		return fmt.Errorf("restore failed: %w", err)
	}
	elapsed := time.Since(start)

	if schemaBytes, err := readFileFromTarGz(archivePath, "schema.json"); err == nil && len(schemaBytes) > 0 {
		_ = os.WriteFile(filepath.Join(destDir, "schema.json"), schemaBytes, 0o600)
	}

	if globalOpts.JSON {
		return format.RenderJSON(stdout, map[string]any{
			"success":       true,
			"target_path":   destDir,
			"fresh_node_id": freshNodeID,
			"mode":          modeStr,
			"source_db":     meta.DBID,
			"elapsed_ms":    elapsed.Milliseconds(),
		}, true)
	}

	fmt.Fprintf(stdout, "Database restored successfully in %s\n", elapsed)
	format.RenderKV(stdout, [][2]string{
		{"Destination Directory", destDir},
		{"Fresh Node ID", freshNodeID},
		{"Restore Mode", modeStr},
		{"Source Cluster DB ID", meta.DBID},
	})
	return nil
}

func appendFileToTarGz(tarGzPath, entryName string, data []byte) error {
	tmpPath := tarGzPath + ".tmp"
	inF, err := os.Open(tarGzPath)
	if err != nil {
		return err
	}
	defer inF.Close()

	outF, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer outF.Close()

	gzr, err := gzip.NewReader(inF)
	if err != nil {
		return err
	}
	defer gzr.Close()

	gzw := gzip.NewWriter(outF)
	defer gzw.Close()

	tr := tar.NewReader(gzr)
	tw := tar.NewWriter(gzw)
	defer tw.Close()

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if _, err := io.Copy(tw, tr); err != nil {
			return err
		}
	}

	hdr := &tar.Header{
		Name:     entryName,
		Mode:     0o600,
		Size:     int64(len(data)),
		ModTime:  time.Now(),
		Typeflag: tar.TypeReg,
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	if _, err := tw.Write(data); err != nil {
		return err
	}

	if err := tw.Close(); err != nil {
		return err
	}
	if err := gzw.Close(); err != nil {
		return err
	}
	if err := outF.Close(); err != nil {
		return err
	}
	inF.Close()

	return os.Rename(tmpPath, tarGzPath)
}

func readFileFromTarGz(tarGzPath, entryName string) ([]byte, error) {
	f, err := os.Open(tarGzPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if hdr.Name == entryName {
			return io.ReadAll(tr)
		}
	}
	return nil, os.ErrNotExist
}

func readArchiveMetadata(archivePath string) (*backup.Metadata, error) {
	f, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gzr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		if hdr.Name == "backup-metadata.json" {
			var meta backup.Metadata
			if err := json.NewDecoder(tr).Decode(&meta); err != nil {
				return nil, err
			}
			return &meta, nil
		}
	}
	return nil, fmt.Errorf("backup-metadata.json not found in archive")
}
