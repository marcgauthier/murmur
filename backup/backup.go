package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// SourceDB defines the interface that DB/state must satisfy to produce an online backup.
type SourceDB interface {
	// Checkpoint snapshots Pebble's live state via hard-links into stagingDataDir.
	Checkpoint(stagingDataDir string) error
	// Pin registers the checkpoint directory in KEYREGISTRY to pin needed data keys.
	Pin(ctx context.Context, path, kind string) error
	// Unpin removes the pin from KEYREGISTRY when the backup finishes or fails.
	Unpin(ctx context.Context, path string) error
	// KeysDir returns the directory containing KEYREGISTRY.
	KeysDir() string
	// ClusterID returns the database cluster identifier.
	ClusterID() string
	// LocalNodeID returns the local node identifier.
	LocalNodeID() string
	// SchemaInfo returns current epoch, version, and hash.
	SchemaInfo() (epoch uint64, version uint64, hash string)
	// DataDir returns the root data directory.
	DataDir() string
	// FilesDir returns the file-object root directory, or "" when the
	// source runs without file storage.
	FilesDir() string
}

// CreateBackup executes the 4-step online zero-downtime backup pipeline:
// 1. Instant hard-link checkpoint (<100ms) + key pinning.
// 2. Pure ciphertext streaming tar.gz archive generation (zero heap inflation).
// 3. Offsite destination transfer (Local, HTTPS/S3, or FTP).
// 4. Instant cleanup (<10ms) unlinking hard links and unpinning keys.
func CreateBackup(ctx context.Context, src SourceDB, cfg Config) (*Metadata, error) {
	cfg.withDefaults()
	log := cfg.Logger
	start := time.Now()

	if cfg.Destination == nil {
		return nil, fmt.Errorf("backup: destination cannot be nil")
	}

	// 1. Establish staging directory on the SAME mount as DataDir so hard-links succeed
	stagingBase := cfg.StagingDir
	if stagingBase == "" {
		stagingBase = src.DataDir()
	}
	stagingDir := filepath.Join(stagingBase, fmt.Sprintf(".backup-%d", time.Now().UnixNano()))
	if err := os.MkdirAll(stagingDir, 0o700); err != nil {
		return nil, fmt.Errorf("backup: create staging dir %s: %w", stagingDir, err)
	}
	defer func() {
		_ = os.RemoveAll(stagingDir)
	}()

	// 2. Pin keys in registry
	if err := src.Pin(ctx, stagingDir, "checkpoint"); err != nil {
		return nil, fmt.Errorf("backup: pin registry: %w", err)
	}
	defer func() {
		_ = src.Unpin(context.Background(), stagingDir)
	}()

	stagingData := filepath.Join(stagingDir, "data")
	stagingKeys := filepath.Join(stagingDir, "keys")

	// 3. Instant hard-link Pebble checkpoint (<100ms)
	ckStart := time.Now()
	if err := src.Checkpoint(stagingData); err != nil {
		return nil, fmt.Errorf("backup: pebble checkpoint: %w", err)
	}
	log.Debug("backup: pebble checkpoint created", "elapsed", time.Since(ckStart))

	// 4. Snapshot KEYREGISTRY (already encrypted with storage key)
	if err := os.MkdirAll(stagingKeys, 0o700); err != nil {
		return nil, fmt.Errorf("backup: mkdir staging keys: %w", err)
	}
	srcReg := filepath.Join(src.KeysDir(), "KEYREGISTRY")
	if _, err := os.Stat(srcReg); err == nil {
		if err := copyRawFile(srcReg, filepath.Join(stagingKeys, "KEYREGISTRY")); err != nil {
			return nil, fmt.Errorf("backup: copy key registry: %w", err)
		}
	}
	// Copy KEYREGISTRY.bak if present
	srcRegBak := filepath.Join(src.KeysDir(), "KEYREGISTRY.bak")
	if _, err := os.Stat(srcRegBak); err == nil {
		_ = copyRawFile(srcRegBak, filepath.Join(stagingKeys, "KEYREGISTRY.bak"))
	}

	// 4b. Snapshot file objects when object-inclusive coverage is asked.
	// File metadata always rides inside the Pebble checkpoint; this step
	// only adds object bytes plus the key-generation marker.
	var filesMode string
	var filesObjects int
	var filesBytes int64
	filesStaged := false
	if cfg.IncludeFiles {
		filesDir := src.FilesDir()
		if filesDir == "" {
			return nil, fmt.Errorf("backup: object-inclusive backup requested but the source has no file store")
		}
		stagingFiles := filepath.Join(stagingDir, "files")
		count, total, err := snapshotFiles(filesDir, stagingFiles, log)
		if err != nil {
			return nil, err
		}
		filesMode, filesObjects, filesBytes = FilesModeObjects, count, total
		filesStaged = true
		log.Debug("backup: files snapshot staged", "objects", count, "bytes", total)
	}

	// 5. Gather file metrics and build metadata
	var dataFilesCount int
	var totalBytes int64
	_ = filepath.Walk(stagingDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			dataFilesCount++
			totalBytes += info.Size()
		}
		return nil
	})

	epoch, ver, hash := src.SchemaInfo()
	meta := &Metadata{
		Version:        1,
		BackupID:       newBackupID(),
		DBID:           src.ClusterID(),
		NodeID:         src.LocalNodeID(),
		SchemaVersion:  ver,
		SchemaEpoch:    epoch,
		SchemaHash:     hash,
		CreatedAt:      time.Now().UTC(),
		DataFilesCount: dataFilesCount,
		TotalBytes:     totalBytes,
		PebbleFormat:   1,
		Compression:    cfg.Compression,
		FilesMode:      filesMode,
		FilesObjects:   filesObjects,
		FilesBytes:     filesBytes,
	}

	filename := fmt.Sprintf("nomadsql-backup-%s-%s-%s.tar.gz", meta.DBID, meta.CreatedAt.Format("20060102-150405"), meta.BackupID[:8])
	log.Info("backup: streaming archive",
		"filename", filename,
		"files", meta.DataFilesCount,
		"bytes", meta.TotalBytes,
		"destination", cfg.Destination.Type(),
	)

	// 6. Streaming Pipeline: io.Pipe() -> tar.Writer -> gzip.Writer -> Destination
	pr, pw := io.Pipe()
	archiveErrCh := make(chan error, 1)

	go func() {
		var archiveErr error
		defer func() {
			if archiveErr != nil {
				pw.CloseWithError(archiveErr)
			} else {
				pw.Close()
			}
			archiveErrCh <- archiveErr
		}()

		var tw *tar.Writer
		var gw *gzip.Writer

		if cfg.Compression == "gzip" {
			gw = gzip.NewWriter(pw)
			tw = tar.NewWriter(gw)
		} else {
			tw = tar.NewWriter(pw)
		}

		// A. Write backup-metadata.json first
		metaBytes, err := json.MarshalIndent(meta, "", "  ")
		if err != nil {
			archiveErr = err
			return
		}
		hdr := &tar.Header{
			Name:     "backup-metadata.json",
			Mode:     0o644,
			Size:     int64(len(metaBytes)),
			ModTime:  meta.CreatedAt,
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(hdr); err != nil {
			archiveErr = err
			return
		}
		if _, err := tw.Write(metaBytes); err != nil {
			archiveErr = err
			return
		}

		// B. Write keys/, data/ (and files/ when staged) files
		subs := []string{"keys", "data"}
		if filesStaged {
			subs = append(subs, "files")
		}
		for _, sub := range subs {
			subDir := filepath.Join(stagingDir, sub)
			err := filepath.Walk(subDir, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				rel, err := filepath.Rel(stagingDir, path)
				if err != nil {
					return err
				}
				if rel == "." {
					return nil
				}
				if info.IsDir() {
					return tw.WriteHeader(&tar.Header{
						Name:     rel + "/",
						Mode:     0o755,
						Typeflag: tar.TypeDir,
						ModTime:  info.ModTime(),
					})
				}
				fileHdr := &tar.Header{
					Name:     rel,
					Mode:     0o600,
					Size:     info.Size(),
					ModTime:  info.ModTime(),
					Typeflag: tar.TypeReg,
				}
				if err := tw.WriteHeader(fileHdr); err != nil {
					return err
				}
				f, err := os.Open(path)
				if err != nil {
					return err
				}
				defer f.Close()
				_, err = io.Copy(tw, f)
				return err
			})
			if err != nil {
				archiveErr = err
				return
			}
		}

		if err := tw.Close(); err != nil {
			archiveErr = err
			return
		}
		if gw != nil {
			if err := gw.Close(); err != nil {
				archiveErr = err
				return
			}
		}
	}()

	// 7. Ship to destination
	transferErr := cfg.Destination.WriteBackup(ctx, filename, pr, -1)
	if transferErr != nil {
		pr.CloseWithError(transferErr)
		return nil, fmt.Errorf("backup: ship to %s failed: %w", cfg.Destination.Type(), transferErr)
	}

	if archiveErr := <-archiveErrCh; archiveErr != nil {
		return nil, fmt.Errorf("backup: stream archive: %w", archiveErr)
	}

	log.Info("backup: completed successfully",
		"backup_id", meta.BackupID,
		"filename", filename,
		"destination", cfg.Destination.Type(),
		"duration", time.Since(start),
	)
	return meta, nil
}

func copyRawFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Sync()
}

func newBackupID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
