package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Restore unpacks a backup bundle from cfg.Source into cfg.TargetPath and cfg.KeysPath.
// After Restore completes, the database is opened via replicateddb.Open using the master key.
func Restore(ctx context.Context, cfg RestoreConfig) (*Metadata, error) {
	cfg.withDefaults()
	log := cfg.Logger
	start := time.Now()

	if cfg.Source == nil {
		return nil, fmt.Errorf("backup: restore source cannot be nil")
	}
	if cfg.TargetPath == "" {
		return nil, fmt.Errorf("backup: restore TargetPath cannot be empty")
	}
	if cfg.KeysPath == "" {
		cfg.KeysPath = filepath.Join(cfg.TargetPath, "keys")
	}
	dataDir := filepath.Join(cfg.TargetPath, "data")

	// 1. Check target directories
	for _, dir := range []string{dataDir, cfg.KeysPath} {
		if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
			if !cfg.Overwrite {
				return nil, fmt.Errorf("%w: %s contains existing files", ErrRestoreTargetNotEmpty, dir)
			}
			if err := os.RemoveAll(dir); err != nil {
				return nil, fmt.Errorf("backup: clear existing dir %s: %w", dir, err)
			}
		}
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("backup: create dir %s: %w", dir, err)
		}
	}

	// 2. Select backup name
	backupName := cfg.BackupName
	if backupName == "" {
		list, err := cfg.Source.ListBackups(ctx, cfg.ExpectedDBID)
		if err != nil {
			return nil, fmt.Errorf("backup: list backups: %w", err)
		}
		if len(list) == 0 {
			return nil, fmt.Errorf("%w: no backups found for db %q", ErrDestinationNotFound, cfg.ExpectedDBID)
		}
		backupName = list[0].Name
	}

	log.Info("backup: starting restore",
		"backup_name", backupName,
		"source", cfg.Source.Type(),
		"target_path", cfg.TargetPath,
	)

	// 3. Open backup stream
	rc, err := cfg.Source.ReadBackup(ctx, backupName)
	if err != nil {
		return nil, fmt.Errorf("backup: open stream: %w", err)
	}
	defer rc.Close()

	// 4. Wrap with gzip reader
	gr, err := gzip.NewReader(rc)
	if err != nil {
		return nil, fmt.Errorf("backup: gzip decompression: %w", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)

	var meta *Metadata
	var restoredFiles int
	var restoredBytes int64

	// 5. Unpack tar entries
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("backup: read tar entry: %w", err)
		}

		cleanName := filepath.Clean(hdr.Name)
		if strings.HasPrefix(cleanName, "..") || filepath.IsAbs(cleanName) {
			return nil, fmt.Errorf("%w: illegal file path %q", ErrCorruptBackup, hdr.Name)
		}

		// A. Parse backup-metadata.json
		if cleanName == "backup-metadata.json" {
			meta = &Metadata{}
			if err := json.NewDecoder(tr).Decode(meta); err != nil {
				return nil, fmt.Errorf("%w: decode metadata: %v", ErrInvalidMetadata, err)
			}
			if cfg.ExpectedDBID != "" && meta.DBID != cfg.ExpectedDBID {
				return nil, fmt.Errorf("%w: got %s, want %s", ErrDBIDMismatch, meta.DBID, cfg.ExpectedDBID)
			}
			continue
		}

		// B. Route keys/* to KeysPath and data/* to TargetPath/data
		var destFile string
		if strings.HasPrefix(cleanName, "keys/") {
			rel := strings.TrimPrefix(cleanName, "keys/")
			destFile = filepath.Join(cfg.KeysPath, rel)
		} else if strings.HasPrefix(cleanName, "data/") {
			rel := strings.TrimPrefix(cleanName, "data/")
			destFile = filepath.Join(dataDir, rel)
		} else {
			// Skip unknown top-level files
			continue
		}

		if hdr.Typeflag == tar.TypeDir {
			if err := os.MkdirAll(destFile, 0o700); err != nil {
				return nil, fmt.Errorf("backup: mkdir %s: %w", destFile, err)
			}
			continue
		}

		if hdr.Typeflag == tar.TypeReg {
			if err := os.MkdirAll(filepath.Dir(destFile), 0o700); err != nil {
				return nil, fmt.Errorf("backup: ensure parent dir %s: %w", destFile, err)
			}
			out, err := os.OpenFile(destFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
			if err != nil {
				return nil, fmt.Errorf("backup: create destination file %s: %w", destFile, err)
			}
			n, err := io.Copy(out, tr)
			if err != nil {
				out.Close()
				return nil, fmt.Errorf("backup: extract file %s: %w", destFile, err)
			}
			if err := out.Sync(); err != nil {
				out.Close()
				return nil, fmt.Errorf("backup: sync file %s: %w", destFile, err)
			}
			out.Close()
			restoredFiles++
			restoredBytes += n
		}
	}

	if meta == nil {
		return nil, fmt.Errorf("%w: backup-metadata.json missing from archive", ErrInvalidMetadata)
	}

	// 6. Verify restored essentials
	regFile := filepath.Join(cfg.KeysPath, "KEYREGISTRY")
	if _, err := os.Stat(regFile); err != nil {
		// Key registry might not exist if unencrypted, which is fine
		log.Debug("backup: KEYREGISTRY not found in restored keys dir (unencrypted DB)")
	}

	log.Info("backup: restore completed successfully",
		"backup_id", meta.BackupID,
		"db_id", meta.DBID,
		"files", restoredFiles,
		"bytes", restoredBytes,
		"duration", time.Since(start),
	)
	return meta, nil
}
