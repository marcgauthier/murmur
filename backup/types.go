package backup

import (
	"context"
	"errors"
	"io"
	"time"
)

// Common errors returned by backup and restore operations.
var (
	ErrBackupInProgress      = errors.New("backup: operation already in progress")
	ErrRestoreTargetNotEmpty = errors.New("backup: restore target directory is not empty")
	ErrCorruptBackup         = errors.New("backup: archive is corrupt or invalid")
	ErrInvalidMetadata       = errors.New("backup: metadata is missing or invalid")
	ErrDestinationNotFound   = errors.New("backup: requested backup not found on destination")
	ErrDBIDMismatch          = errors.New("backup: backup DBID does not match expected database")
	ErrUnauthenticated       = errors.New("backup: authentication failed for remote destination")
	// ErrRestoreIdentityRequired indicates a restore without its mandatory
	// fresh writer NodeID (or, for reseeds, new DBID).
	ErrRestoreIdentityRequired = errors.New("backup: restore requires a fresh writer NodeID")
	// ErrRestoreIdentityReuse indicates a fresh NodeID that collides with
	// the backup's source identity.
	ErrRestoreIdentityReuse = errors.New("backup: fresh NodeID must differ from the backup source identity")
	// ErrRestoreIntentInvalid indicates a corrupt or incompatible
	// restore-intent file.
	ErrRestoreIntentInvalid = errors.New("backup: restore intent is missing or invalid")
)

// Metadata stores the manifest descriptor inside the backup archive (backup-metadata.json).
type Metadata struct {
	Version        int       `json:"version"`
	BackupID       string    `json:"backup_id"`
	DBID           string    `json:"db_id"`
	NodeID         string    `json:"node_id"`
	SchemaVersion  uint64    `json:"schema_version"`
	SchemaEpoch    uint64    `json:"schema_epoch"`
	SchemaHash     string    `json:"schema_hash"`
	CreatedAt      time.Time `json:"created_at"`
	DataFilesCount int       `json:"data_files_count"`
	TotalBytes     int64     `json:"total_bytes"`
	PebbleFormat   uint64    `json:"pebble_format"`
	Compression    string    `json:"compression"` // "gzip" or "none"
	// FilesMode declares file-object coverage: "objects" when the archive
	// carries files/objects, "" (metadata-only) otherwise. File metadata
	// always rides inside the Pebble checkpoint; only object bytes vary.
	FilesMode string `json:"files_mode,omitempty"`
	// FilesObjects and FilesBytes declare the archived object payload.
	FilesObjects int   `json:"files_objects,omitempty"`
	FilesBytes   int64 `json:"files_bytes,omitempty"`
	// FilesRestoredObjects/Bytes and FilesSkipped report what a restore
	// actually unpacked. They are set on the Metadata returned by
	// Restore, never inside the archive manifest.
	FilesRestoredObjects int   `json:"files_restored_objects,omitempty"`
	FilesRestoredBytes   int64 `json:"files_restored_bytes,omitempty"`
	FilesSkipped         bool  `json:"files_skipped,omitempty"`
}

// FilesModeObjects marks object-inclusive backups in Metadata.FilesMode.
const FilesModeObjects = "objects"

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
	// IncludeFiles packs files/objects plus the key-generation marker
	// alongside the Pebble checkpoint (object-inclusive backup). False
	// leaves object bytes out (metadata-only): file metadata still
	// restores from the checkpoint, and missing payloads repair through
	// mesh fetch. The object key itself never enters the archive: the
	// operator must configure the same key on restore.
	IncludeFiles bool
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

// RestoreMode selects the restore identity policy.
type RestoreMode string

const (
	// RestoreClone restores a backup that rejoins its original cluster
	// under a fresh writer NodeID. It is the default (zero value "").
	RestoreClone RestoreMode = "clone"
	// RestoreReseed restores a baseline for a coordinated new-DBID
	// cluster: every node restores under a fresh NodeID and the same new
	// DBID. Nodes with the previous DBID cannot join the reseeded cluster.
	RestoreReseed RestoreMode = "reseed"
)

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
	// FilesPath is the directory where restored file objects land.
	// Empty defaults to TargetPath/files.
	FilesPath string
	// SkipFiles leaves archived file objects behind (metadata-only
	// restore even from an object-inclusive backup). Restored metadata
	// references missing payloads until mesh fetch repairs them.
	SkipFiles bool
	// ExpectedDBID validates that the backup matches this DBID (optional).
	ExpectedDBID string
	// Overwrite allows restoring into an existing non-empty directory.
	Overwrite bool
	// Mode selects the restore identity policy. Empty defaults to
	// RestoreClone.
	Mode RestoreMode
	// FreshNodeID is the new writer NodeID (UUID text) the restored data
	// must be opened with. Required for RestoreClone; it must differ from
	// the backup's source identity. Opening the restored data under any
	// other NodeID (including the backup's original) is rejected, so old
	// origin sequences can never be reused.
	FreshNodeID string
	// NewDBID carries the replacement cluster identity for RestoreReseed
	// (required; must differ from the backup's DBID). It must be empty for
	// RestoreClone (the original DBID is preserved).
	NewDBID string
	// Logger for progress and debug messages.
	Logger Logger
}

func (c *RestoreConfig) withDefaults() {
	if c.Logger == nil {
		c.Logger = discardLogger{}
	}
	if c.Mode == "" {
		c.Mode = RestoreClone
	}
}

// RestoreIntentFileName is the restore-intent file written into the restore
// target directory (alongside data/ and keys/). It durably records the
// fresh-identity requirement until the first Open adopts it.
const RestoreIntentFileName = "restore-intent.json"

// restoreIntentVersion is the intent encoding version.
const restoreIntentVersion = 1

// RestoreIntent is the persisted fresh-identity requirement for restored
// data. Restore writes it; Open consumes it during identity adoption and
// removes the file only after the adoption is durable (plus a successful
// rebuild), so a crash can never clear the requirement prematurely. A reopen
// with an already-satisfied intent is a no-op that finishes the cleanup.
type RestoreIntent struct {
	Version      int       `json:"version"`
	Mode         string    `json:"mode"`
	BackupID     string    `json:"backup_id"`
	BackupName   string    `json:"backup_name"`
	SourceNodeID string    `json:"source_node_id"`
	SourceDBID   string    `json:"source_db_id"`
	FreshNodeID  string    `json:"fresh_node_id"`
	NewDBID      string    `json:"new_db_id,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}
