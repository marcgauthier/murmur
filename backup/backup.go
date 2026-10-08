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
	// HoldCommits excludes state commits and returns a release function.
	HoldCommits() func()
	// Checkpoint snapshots Spool's live state into stagingDataDir and returns a release func.
	Checkpoint(ctx context.Context, stagingDataDir string) (release func() error, err error)
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
// 1. Instant hard-link Spool checkpoint (<100ms) with state metadata cut.
// 2. Pure ciphertext streaming tar.gz archive generation (zero heap inflation).
// 3. Offsite destination transfer (Local, HTTPS/S3, or FTP).
// 4. Instant cleanup (<10ms) unlinking hard links and releasing checkpoint handle.
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

	stagingData := filepath.Join(stagingDir, "data")

	// 2. Instant hard-link Spool checkpoint under commit exclusion
	ckStart := time.Now()
	releaseCommits := src.HoldCommits()
	epoch, ver, hash := src.SchemaInfo()
	clusterID := src.ClusterID()
	nodeID := src.LocalNodeID()
	cpRelease, err := src.Checkpoint(ctx, stagingData)
	releaseCommits()
	if err != nil {
		return nil, fmt.Errorf("backup: spool checkpoint: %w", err)
	}
	if cpRelease != nil {
		defer func() { _ = cpRelease() }()
	}
	log.Debug("backup: spool checkpoint created", "elapsed", time.Since(ckStart))

	// 3. Snapshot file objects when object-inclusive coverage is asked.
	// File metadata always rides inside the Spool checkpoint; this step
	// only adds object bytes plus the key-generation marker.
	var filesMode string
	var filesObjects int
	var filesBytes int64
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
		log.Debug("backup: files snapshot staged", "objects", count, "bytes", total)
	}

	// 4. Gather file metrics and build metadata
	var dataFilesCount int
	var totalBytes int64
	_ = filepath.Walk(stagingDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			dataFilesCount++
			totalBytes += info.Size()
		}
		return nil
	})

	meta := &Metadata{
		Version:        2,
		Backend:        "spool",
		StorageFormat:  5,
		BackupID:       newBackupID(),
		DBID:           clusterID,
		NodeID:         nodeID,
		SchemaVersion:  ver,
		SchemaEpoch:    epoch,
		SchemaHash:     hash,
		CreatedAt:      time.Now().UTC(),
		DataFilesCount: dataFilesCount,
		TotalBytes:     totalBytes,
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

		// B. Write data/ (and files/ when staged) files
		subs := []string{"data"}
		if filesMode != "" {
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
