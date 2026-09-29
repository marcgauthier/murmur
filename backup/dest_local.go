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
	"sort"
	"strings"
	"time"
)

// LocalDestination implements Destination for local directory storage.
type LocalDestination struct {
	Dir string
}

// NewLocalDestination creates a local destination rooted at dir.
func NewLocalDestination(dir string) (*LocalDestination, error) {
	if dir == "" {
		return nil, fmt.Errorf("backup: local destination dir cannot be empty")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("backup: create local destination dir %s: %w", dir, err)
	}
	return &LocalDestination{Dir: dir}, nil
}

func (d *LocalDestination) Type() string { return "local" }

// WriteBackup streams the backup archive to a temporary file, fsyncs, and atomically renames.
func (d *LocalDestination) WriteBackup(ctx context.Context, name string, r io.Reader, _ int64) error {
	if err := os.MkdirAll(d.Dir, 0o700); err != nil {
		return fmt.Errorf("backup: ensure dir %s: %w", d.Dir, err)
	}
	finalPath := filepath.Join(d.Dir, name)
	tmpPath := finalPath + fmt.Sprintf(".tmp-%d", time.Now().UnixNano())

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("backup: create tmp file %s: %w", tmpPath, err)
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmpPath)
	}()

	// Buffer pipe reads with a cancelable context watcher
	buf := make([]byte, 128*1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, rErr := r.Read(buf)
		if n > 0 {
			if _, wErr := f.Write(buf[:n]); wErr != nil {
				return fmt.Errorf("backup: write local file %s: %w", tmpPath, wErr)
			}
		}
		if rErr == io.EOF {
			break
		}
		if rErr != nil {
			return fmt.Errorf("backup: read stream: %w", rErr)
		}
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("backup: sync file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("backup: close file: %w", err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("backup: rename %s to %s: %w", tmpPath, finalPath, err)
	}

	// Sync parent directory to persist directory entry
	if df, err := os.Open(d.Dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

// ReadBackup returns an open ReadCloser for the named backup file.
func (d *LocalDestination) ReadBackup(_ context.Context, name string) (io.ReadCloser, error) {
	p := filepath.Join(d.Dir, name)
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrDestinationNotFound, name)
		}
		return nil, fmt.Errorf("backup: open %s: %w", p, err)
	}
	return f, nil
}

// ListBackups enumerates all backup archives matching the given dbID.
func (d *LocalDestination) ListBackups(_ context.Context, dbID string) ([]BackupInfo, error) {
	entries, err := os.ReadDir(d.Dir)
	if err != nil {
		return nil, fmt.Errorf("backup: list dir %s: %w", d.Dir, err)
	}

	var results []BackupInfo
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tar.gz") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		meta, err := readLocalArchiveMetadata(filepath.Join(d.Dir, entry.Name()))
		if err != nil {
			// Fallback: check if filename contains dbID
			if dbID != "" && !strings.Contains(entry.Name(), dbID) {
				continue
			}
			results = append(results, BackupInfo{
				Name:      entry.Name(),
				DBID:      dbID,
				CreatedAt: info.ModTime(),
				SizeBytes: info.Size(),
			})
			continue
		}
		if dbID != "" && meta.DBID != dbID {
			continue
		}
		results = append(results, BackupInfo{
			Name:      entry.Name(),
			DBID:      meta.DBID,
			CreatedAt: meta.CreatedAt,
			SizeBytes: info.Size(),
		})
	}

	// Sort newest first
	sort.Slice(results, func(i, j int) bool {
		return results[i].CreatedAt.After(results[j].CreatedAt)
	})
	return results, nil
}

// DeleteBackup deletes a backup archive from the local directory.
func (d *LocalDestination) DeleteBackup(_ context.Context, name string) error {
	p := filepath.Join(d.Dir, name)
	if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("backup: remove %s: %w", p, err)
	}
	return nil
}

// readLocalArchiveMetadata fast-reads backup-metadata.json from the head of the tar.gz.
func readLocalArchiveMetadata(archivePath string) (*Metadata, error) {
	f, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err != nil {
			break
		}
		if hdr.Name == "backup-metadata.json" || strings.HasSuffix(hdr.Name, "/backup-metadata.json") {
			var meta Metadata
			if err := json.NewDecoder(tr).Decode(&meta); err != nil {
				return nil, err
			}
			return &meta, nil
		}
	}
	return nil, ErrInvalidMetadata
}
