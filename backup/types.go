package backup

import (
	"context"
	"errors"
	"io"
	"time"
)

// Common errors returned by backup and restore operations.
var (
	ErrBackupInProgress     = errors.New("backup: operation already in progress")
	ErrRestoreTargetNotEmpty = errors.New("backup: restore target directory is not empty")
	ErrCorruptBackup        = errors.New("backup: archive is corrupt or invalid")
	ErrInvalidMetadata      = errors.New("backup: metadata is missing or invalid")
	ErrDestinationNotFound  = errors.New("backup: requested backup not found on destination")
	ErrDBIDMismatch         = errors.New("backup: backup DBID does not match expected database")
	ErrUnauthenticated      = errors.New("backup: authentication failed for remote destination")
)

// Metadata stores the manifest descriptor inside the backup archive (backup-metadata.json).
type Metadata struct {
	Version         int       `json:"version"`
	BackupID        string    `json:"backup_id"`
	DBID            string    `json:"db_id"`
	NodeID          string    `json:"node_id"`
	SchemaVersion   uint64    `json:"schema_version"`
	SchemaEpoch     uint64    `json:"schema_epoch"`
	SchemaHash      string    `json:"schema_hash"`
	CreatedAt       time.Time `json:"created_at"`
	DataFilesCount  int       `json:"data_files_count"`
	TotalBytes      int64     `json:"total_bytes"`
	PebbleFormat    uint64    `json:"pebble_format"`
	Compression     string    `json:"compression"` // "gzip" or "none"
}

// BackupInfo summarizes an available backup returned by ListBackups.
type BackupInfo struct {
	Name      string    `json:"name"`
	DBID      string    `json:"db_id"`
	CreatedAt time.Time `json:"created_at"`
	SizeBytes int64     `json:"size_bytes"`
}

// Destination represents a storage backend for writing, reading, and managing backups.
type Destination interface {
	// WriteBackup streams the backup archive to the destination.
	WriteBackup(ctx context.Context, name string, r io.Reader, sizeHint int64) error
	// ReadBackup retrieves a backup stream for restoration.
	ReadBackup(ctx context.Context, name string) (io.ReadCloser, error)
	// ListBackups returns all available backups for the given DBID (or all if dbID is empty).
	ListBackups(ctx context.Context, dbID string) ([]BackupInfo, error)
	// DeleteBackup removes a backup file (used by retention policy).
	DeleteBackup(ctx context.Context, name string) error
	// Type returns the provider type ("local", "https", "s3", "ftp").
	Type() string
}

// Logger interface mirrors the root Logger to avoid cyclic imports.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Debug(string, ...any) {}
func (discardLogger) Info(string, ...any)  {}
func (discardLogger) Warn(string, ...any)  {}
func (discardLogger) Error(string, ...any) {}

// Config configures a backup operation.
type Config struct {
	// Destination is the target storage for the backup.
	Destination Destination
	// Compression selects compression algorithm: "gzip" (default) or "none".
	Compression string
	// StagingDir optionally overrides the temporary checkpoint directory.
	// When empty, a directory inside the database's parent volume is used
	// to guarantee hard-link support.
	StagingDir string
	// Logger for progress and debug messages.
	Logger Logger
}

func (c *Config) withDefaults() {
	if c.Compression == "" {
		c.Compression = "gzip"
	}
	if c.Logger == nil {
		c.Logger = discardLogger{}
	}
}

// RestoreConfig configures a restore operation.
type RestoreConfig struct {
	// Source is the storage destination to read the backup from.
	Source Destination
	// BackupName is the specific backup file name to restore. If empty, the latest is used.
	BackupName string
	// TargetPath is the directory where the restored Pebble data directory will be placed.
	TargetPath string
	// KeysPath is the directory where the restored KEYREGISTRY will be placed.
	KeysPath string
	// ExpectedDBID validates that the backup matches this DBID (optional).
	ExpectedDBID string
	// Overwrite allows restoring into an existing non-empty directory.
	Overwrite bool
	// Logger for progress and debug messages.
	Logger Logger
}

func (c *RestoreConfig) withDefaults() {
	if c.Logger == nil {
		c.Logger = discardLogger{}
	}
}
