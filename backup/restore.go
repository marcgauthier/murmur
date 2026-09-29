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

	"github.com/marcgauthier/spedsql/ids"
)

// Restore unpacks a backup bundle from cfg.Source into cfg.TargetPath and cfg.KeysPath,
// then records a restore intent requiring a fresh writer identity.
// After Restore completes, the database is opened via replicateddb.Open using the
// master key and cfg.FreshNodeID; opening under any other NodeID is rejected.
func Restore(ctx context.Context, cfg RestoreConfig) (*Metadata, error) {
	cfg.withDefaults()
	log := cfg.Logger
	start := time.Now()

	var newDB ids.DBID
	switch cfg.Mode {
	case RestoreClone:
		if cfg.NewDBID != "" {
			return nil, fmt.Errorf("backup: NewDBID applies to reseed restores only")
		}
	case RestoreReseed:
		var err error
		newDB, err = ids.ParseDBID(cfg.NewDBID)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid reseed NewDBID %q: %v", ErrRestoreIdentityRequired, cfg.NewDBID, err)
		}
	default:
		return nil, fmt.Errorf("%w: unknown restore mode %q", ErrRestoreIntentInvalid, cfg.Mode)
	}
	freshNode, err := ids.ParseNodeID(cfg.FreshNodeID)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid FreshNodeID %q: %v", ErrRestoreIdentityRequired, cfg.FreshNodeID, err)
	}
	if cfg.Source == nil {
		return nil, fmt.Errorf("backup: restore source cannot be nil")
	}
	if cfg.TargetPath == "" {
		return nil, fmt.Errorf("backup: restore TargetPath cannot be empty")
	}
	if cfg.KeysPath == "" {
		cfg.KeysPath = filepath.Join(cfg.TargetPath, "keys")
	}
	if cfg.FilesPath == "" {
		cfg.FilesPath = filepath.Join(cfg.TargetPath, "files")
	}
	dataDir := filepath.Join(cfg.TargetPath, "data")

	// 1. Check target directories
	for _, dir := range []string{dataDir, cfg.KeysPath, cfg.FilesPath} {
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
	var restoredFileObjects int
	var restoredFileBytes int64
	var metaFilesSkipped bool

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
			// The fresh writer identity must differ from the backup's
			// source identity; same-identity rollback is rejected here
			// (and again at Open, which also replays this check against
			// the stored identity and historical origins).
			if sourceNode, serr := ids.ParseNodeID(meta.NodeID); serr == nil && sourceNode == freshNode {
				return nil, fmt.Errorf("%w: %q is the backup source identity", ErrRestoreIdentityReuse, meta.NodeID)
			}
			// A reseed must move to a genuinely new cluster identity.
			if cfg.Mode == RestoreReseed {
				if sourceDB, serr := ids.ParseDBID(meta.DBID); serr == nil && sourceDB == newDB {
					return nil, fmt.Errorf("%w: %q is the backup DBID", ErrRestoreIdentityReuse, meta.DBID)
				} else if meta.DBID == cfg.NewDBID {
					return nil, fmt.Errorf("%w: %q is the backup DBID", ErrRestoreIdentityReuse, meta.DBID)
				}
			}
			continue
		}

		// B. Route keys/* to KeysPath, data/* to TargetPath/data, and
		// files/* to FilesPath (unless the operator asked to skip them).
		var destFile string
		isObject := false
		if strings.HasPrefix(cleanName, "keys/") {
			rel := strings.TrimPrefix(cleanName, "keys/")
			destFile = filepath.Join(cfg.KeysPath, rel)
		} else if strings.HasPrefix(cleanName, "data/") {
			rel := strings.TrimPrefix(cleanName, "data/")
			destFile = filepath.Join(dataDir, rel)
		} else if strings.HasPrefix(cleanName, "files/") {
			if cfg.SkipFiles {
				metaFilesSkipped = true
				continue
			}
			rel := strings.TrimPrefix(cleanName, "files/")
			destFile = filepath.Join(cfg.FilesPath, rel)
			isObject = strings.HasPrefix(rel, "objects/") && hdr.Typeflag == tar.TypeReg
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
			if isObject {
				restoredFileObjects++
				restoredFileBytes += n
			}
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

	// 7. Persist the fresh-identity intent before reporting success.
	intent := &RestoreIntent{
		Version:      restoreIntentVersion,
		Mode:         string(cfg.Mode),
		BackupID:     meta.BackupID,
		BackupName:   backupName,
		SourceNodeID: meta.NodeID,
		SourceDBID:   meta.DBID,
		FreshNodeID:  freshNode.String(),
		CreatedAt:    time.Now().UTC(),
	}
	if cfg.Mode == RestoreReseed {
		intent.NewDBID = newDB.String()
	}
	if err := WriteRestoreIntent(cfg.TargetPath, intent); err != nil {
		return nil, err
	}

	// Report file-object coverage on the returned manifest (never inside
	// the archive): metadata-only restores leave payloads pending until
	// mesh fetch repairs them, and the operator must configure the same
	// object key the backup was taken with for restored bytes to decrypt.
	meta.FilesRestoredObjects = restoredFileObjects
	meta.FilesRestoredBytes = restoredFileBytes
	meta.FilesSkipped = metaFilesSkipped
	if meta.FilesMode == FilesModeObjects && restoredFileObjects < meta.FilesObjects {
		log.Warn("backup: restore is missing file payloads",
			"declared", meta.FilesObjects, "restored", restoredFileObjects)
	}
	if meta.FilesMode != FilesModeObjects {
		log.Info("backup: metadata-only restore; file payloads pending mesh repair",
			"backup_id", meta.BackupID)
	}
	log.Info("backup: restore completed successfully",
		"backup_id", meta.BackupID,
		"db_id", meta.DBID,
		"files", restoredFiles,
		"bytes", restoredBytes,
		"file_objects", restoredFileObjects,
		"duration", time.Since(start),
	)
	return meta, nil
}

// WriteRestoreIntent durably records intent in dir (atomically via rename).
func WriteRestoreIntent(dir string, intent *RestoreIntent) error {
	raw, err := json.MarshalIndent(intent, "", "  ")
	if err != nil {
		return fmt.Errorf("backup: encode restore intent: %w", err)
	}
	tmp, err := os.CreateTemp(dir, RestoreIntentFileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("backup: create restore intent: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after successful rename
	if _, err := tmp.Write(append(raw, '\n')); err != nil {
		tmp.Close()
		return fmt.Errorf("backup: write restore intent: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("backup: sync restore intent: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("backup: close restore intent: %w", err)
	}
	final := filepath.Join(dir, RestoreIntentFileName)
	if err := os.Rename(tmpName, final); err != nil {
		return fmt.Errorf("backup: publish restore intent: %w", err)
	}
	if dfd, err := os.Open(dir); err == nil {
		_ = dfd.Sync()
		_ = dfd.Close()
	}
	return nil
}

// ReadRestoreIntent loads the intent from dir. It returns (nil, nil) when no
// intent file is present (ordinary database, no restore pending).
func ReadRestoreIntent(dir string) (*RestoreIntent, error) {
	raw, err := os.ReadFile(filepath.Join(dir, RestoreIntentFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("backup: read restore intent: %w", err)
	}
	intent := &RestoreIntent{}
	if err := json.Unmarshal(raw, intent); err != nil {
		return nil, fmt.Errorf("%w: decode: %v", ErrRestoreIntentInvalid, err)
	}
	if intent.Version != restoreIntentVersion {
		return nil, fmt.Errorf("%w: version %d (want %d)", ErrRestoreIntentInvalid, intent.Version, restoreIntentVersion)
	}
	if intent.Mode == "" {
		intent.Mode = string(RestoreClone)
	}
	if intent.Mode != string(RestoreClone) && intent.Mode != string(RestoreReseed) {
		return nil, fmt.Errorf("%w: unknown mode %q", ErrRestoreIntentInvalid, intent.Mode)
	}
	if intent.SourceNodeID == "" || intent.FreshNodeID == "" {
		return nil, fmt.Errorf("%w: missing node identities", ErrRestoreIntentInvalid)
	}
	if intent.Mode == string(RestoreReseed) && intent.NewDBID == "" {
		return nil, fmt.Errorf("%w: reseed intent missing new DBID", ErrRestoreIntentInvalid)
	}
	return intent, nil
}
