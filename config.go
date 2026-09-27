package replicateddb

import (
	"fmt"
	"time"

	"github.com/nomadsql/replicateddb/backup"
	"github.com/nomadsql/replicateddb/schema"
)

// QueryStoreMode selects how the SQL materialization is stored.
type QueryStoreMode int

const (
	// QueryStoreMemory keeps the query database memory-resident (primary target).
	QueryStoreMemory QueryStoreMode = iota
	// QueryStoreMMap is a future disposable mmap-backed mode. Not implemented yet.
	QueryStoreMMap
)

// QueryStoreConfig configures the SQL materialization.
type QueryStoreConfig struct {
	Mode QueryStoreMode
}

// DurabilityMode selects the acknowledgement contract for local writes.
type DurabilityMode int

const (
	// DurabilitySynchronous acknowledges only after the durable commit. Default.
	DurabilitySynchronous DurabilityMode = iota
	// DurabilityAsync is reserved for a future explicitly-configured mode.
	DurabilityAsync
)

// DurabilityConfig configures the durability contract.
type DurabilityConfig struct {
	Mode DurabilityMode
}

// BackupScheduleConfig configures the automated online backup worker.
type BackupScheduleConfig struct {
	// Enabled activates the periodic worker.
	Enabled bool
	// Interval is the duration between automatic backups (e.g. 24 * time.Hour).
	Interval time.Duration
	// Destination is the target storage backend (Local, HTTPS/S3, or FTP).
	Destination backup.Destination
	// Compression: "gzip" (default) or "none".
	Compression string
	// RetentionDays prunes backups older than this number of days (0 disables).
	RetentionDays int
	// MaxBackups retains at most this many latest backups (0 disables).
	MaxBackups int
}

// CacheConfig sizes the caches.
type CacheConfig struct {
	StatementCacheEntries int
	// BlockCacheBytes sizes Pebble's unified block cache (alias for Pebble.CacheBytes).
	// Default 16 MiB.
	BlockCacheBytes int64
}

// CompressionAlgorithm selects Pebble block compression.
type CompressionAlgorithm string

const (
	CompressionZstd   CompressionAlgorithm = "zstd"
	CompressionSnappy CompressionAlgorithm = "snappy"
	CompressionNone   CompressionAlgorithm = "none"
)

// CompressionConfig configures Pebble block compression.
type CompressionConfig struct {
	// Algorithm selects the codec. Default zstd.
	Algorithm CompressionAlgorithm
	// ZstdLevel selects the Zstd level. Default 3; Pebble supports only 3.
	ZstdLevel int
}

// PebbleConfig configures the durable Pebble store. It is required; use
// DefaultPebbleConfig for standard settings. A missing/invalid storage
// budget is an error rather than silently allocating an unbounded cache.
type PebbleConfig struct {
	// CacheBytes sizes Pebble's unified block cache. Required, positive.
	CacheBytes int64
	// MemTableBytes caps one memtable. Default 4MiB.
	MemTableBytes uint64
	// MemTableCount caps live memtables. Default 2.
	MemTableCount int
	// MaxOpenFiles caps open file handles. Default 1000.
	MaxOpenFiles int
	// MaxConcurrentCompactions caps background compactions. Default 1.
	MaxConcurrentCompactions int
	// Compression selects block compression. Default zstd level 3.
	Compression CompressionConfig
}

// DefaultPebbleConfig returns standard Pebble settings (16MiB cache, 4MiB
// memtables x2, 1000 open files, 1 concurrent compaction, Zstd level 3).
func DefaultPebbleConfig() PebbleConfig {
	return PebbleConfig{
		CacheBytes:               16 << 20,
		MemTableBytes:            4 << 20,
		MemTableCount:            2,
		MaxOpenFiles:             1000,
		MaxConcurrentCompactions: 1,
		Compression:              CompressionConfig{Algorithm: CompressionZstd, ZstdLevel: 3},
	}
}

// SchemaConfig carries the application-owned replicated schema.
type SchemaConfig struct {
	// Version is the schema epoch. All nodes at an epoch must share one hash.
	Version uint64
	// Tables are the replicated tables. IDs may be zero for deterministic derivation.
	Tables []schema.TableSchema
	// DDL, when provided, is executed verbatim to create tables instead of
	// generated DDL. Table/column names must still match Tables.
	DDL []string
	// LocalDDL holds local-only objects (indexes, views, FTS) applied after
	// replicated tables on every open/rebuild. Never replicated.
	LocalDDL []string
}

// ReplicationConfig configures QUIC replication. A zero ListenAddr disables
// replication (single-node mode); peers may still be added later.
type ReplicationConfig struct {
	ListenAddr string
	// TLS holds node certificate material. Required when replication is used.
	TLS *TLSCredential
	// Peers are statically configured peers.
	Peers []Peer
	// AllowedPeers, when non-empty, restricts which NodeIDs may connect.
	AllowedPeers []NodeID
	// MaxBatchBytes caps one replication frame payload.
	MaxBatchBytes int
	// MaxBatchMutations caps mutations pulled per send round.
	MaxBatchMutations int
	// SendInterval is the worst-case delay before new batches are pushed.
	SendInterval time.Duration
	// DialInterval is the reconnect backoff for disconnected peers.
	DialInterval time.Duration
	// AckInterval is how often durable receive watermarks are advertised.
	AckInterval time.Duration
	// MinLogRetention keeps origin-log entries newer than this even when
	// fully acknowledged.
	MinLogRetention time.Duration
	// MaxOfflineLogRetention bounds how long one peer's backlog pins log GC.
	// Peers offline longer must snapshot-resync.
	MaxOfflineLogRetention time.Duration
	// MinRetainedBatches keeps at least this many recent batches per origin.
	MinRetainedBatches uint64
	// SnapshotChunkCells bounds cells per snapshot chunk.
	SnapshotChunkCells int
}

// TLSCredential carries this node's certificate material for mTLS.
type TLSCredential struct {
	// CertPEM / KeyPEM is the node certificate and private key (PEM).
	CertPEM []byte
	KeyPEM  []byte
	// CAPEM is the trusted cluster CA (PEM). Peers presenting certs outside
	// this CA are rejected.
	CAPEM []byte
}

// Peer is a replication peer address.
type Peer struct {
	NodeID NodeID
	Addrs  []string
}

// Config is the complete DB configuration.
type Config struct {
	// Path is the Pebble data directory (content files plus key registry).
	Path string
	// NodeID is this node's identity. Required.
	NodeID NodeID
	// DBID identifies the cluster. Zero means "load or create".
	DBID DBID

	QueryStore  QueryStoreConfig
	Pebble      PebbleConfig
	Encryption  EncryptionConfig
	Replication ReplicationConfig
	Cache       CacheConfig
	Schema      SchemaConfig
	Durability  DurabilityConfig
	Backup      BackupScheduleConfig

	// MaxReplicatedValueBytes caps one replicated cell value. Default 16 MiB.
	MaxReplicatedValueBytes int
	// MaxBatchMutations caps cells+deletes in one local transaction.
	// Default 100_000.
	MaxBatchMutations int

	// Logger receives package logs. Nil means discard.
	Logger Logger
}

func (c *Config) withDefaults() {
	if c.Cache.StatementCacheEntries == 0 {
		c.Cache.StatementCacheEntries = 256
	}
	p := &c.Pebble
	if c.Cache.BlockCacheBytes > 0 {
		p.CacheBytes = c.Cache.BlockCacheBytes
	} else if p.CacheBytes == 0 {
		p.CacheBytes = 16 << 20
	}
	if c.Cache.BlockCacheBytes == 0 {
		c.Cache.BlockCacheBytes = p.CacheBytes
	}
	if p.MemTableBytes == 0 {
		p.MemTableBytes = 4 << 20
	}
	if p.MemTableCount == 0 {
		p.MemTableCount = 2
	}
	if p.MaxOpenFiles == 0 {
		p.MaxOpenFiles = 1000
	}
	if p.MaxConcurrentCompactions == 0 {
		p.MaxConcurrentCompactions = 1
	}
	if p.Compression.Algorithm == "" {
		p.Compression.Algorithm = CompressionZstd
	}
	if p.Compression.ZstdLevel == 0 {
		p.Compression.ZstdLevel = 3
	}
	if c.MaxReplicatedValueBytes == 0 {
		c.MaxReplicatedValueBytes = 16 << 20
	}
	if c.MaxBatchMutations == 0 {
		c.MaxBatchMutations = 100_000
	}
	if c.Encryption.DataKeyRotation == 0 {
		c.Encryption.DataKeyRotation = 10 * 24 * time.Hour
	}
	r := &c.Replication
	if r.MaxBatchBytes == 0 {
		r.MaxBatchBytes = 1 << 20
	}
	if r.MaxBatchMutations == 0 {
		r.MaxBatchMutations = 10_000
	}
	if r.SendInterval == 0 {
		r.SendInterval = 20 * time.Millisecond
	}
	if r.DialInterval == 0 {
		r.DialInterval = 5 * time.Second
	}
	if r.AckInterval == 0 {
		r.AckInterval = time.Second
	}
	if r.MinLogRetention == 0 {
		r.MinLogRetention = time.Hour
	}
	if r.MaxOfflineLogRetention == 0 {
		r.MaxOfflineLogRetention = 7 * 24 * time.Hour
	}
	if r.MinRetainedBatches == 0 {
		r.MinRetainedBatches = 1_000
	}
	if r.SnapshotChunkCells == 0 {
		r.SnapshotChunkCells = 2_000
	}
	if c.Logger == nil {
		c.Logger = DiscardLogger{}
	}
}

func (c *Config) validate() error {
	if c.Path == "" {
		return fmt.Errorf("replicateddb: Path is required: %w", ErrUnsupportedSchema)
	}
	if c.NodeID.IsZero() {
		return fmt.Errorf("replicateddb: NodeID is required: %w", ErrUnsupportedSchema)
	}
	if c.Durability.Mode != DurabilitySynchronous {
		return fmt.Errorf("replicateddb: only synchronous durability is implemented: %w", ErrUnsupportedSchema)
	}
	if c.QueryStore.Mode != QueryStoreMemory {
		return fmt.Errorf("replicateddb: only QueryStoreMemory is implemented: %w", ErrUnsupportedSchema)
	}
	if len(c.Schema.Tables) == 0 {
		return fmt.Errorf("replicateddb: at least one replicated table is required: %w", ErrUnsupportedSchema)
	}
	if _, err := schema.BuildRegistry(c.Schema.Version, c.Schema.Tables); err != nil {
		return err
	}
	if err := c.Pebble.validate(); err != nil {
		return err
	}
	if err := c.Encryption.validate(); err != nil {
		return err
	}
	return nil
}

func (p PebbleConfig) validate() error {
	if p.CacheBytes <= 0 {
		return fmt.Errorf("replicateddb: Pebble.CacheBytes is required and must be positive (use DefaultPebbleConfig)")
	}
	if p.MemTableBytes == 0 {
		return fmt.Errorf("replicateddb: Pebble.MemTableBytes must be positive")
	}
	if p.MemTableCount <= 0 {
		return fmt.Errorf("replicateddb: Pebble.MemTableCount must be positive")
	}
	if p.MaxOpenFiles <= 0 {
		return fmt.Errorf("replicateddb: Pebble.MaxOpenFiles must be positive")
	}
	if p.MaxConcurrentCompactions <= 0 {
		return fmt.Errorf("replicateddb: Pebble.MaxConcurrentCompactions must be positive")
	}
	switch p.Compression.Algorithm {
	case CompressionZstd, CompressionSnappy, CompressionNone:
	default:
		return fmt.Errorf("replicateddb: unknown Pebble compression %q", p.Compression.Algorithm)
	}
	if p.Compression.Algorithm == CompressionZstd && p.Compression.ZstdLevel != 3 {
		return fmt.Errorf("replicateddb: Pebble Zstd supports only level 3")
	}
	return nil
}
