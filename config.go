package replicateddb

import (
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/replication"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/transport"
)

// QueryStoreConfig configures the SQL materialization.
type QueryStoreConfig struct {
	// RemoteApplyInterval batches durable remote changes into one SQLite
	// transaction. Zero selects one second. A negative value is invalid.
	RemoteApplyInterval time.Duration
	// RemoteApplyMaxTransactions flushes when this many received transactions
	// are queued, even before RemoteApplyInterval. Zero selects 1,000.
	RemoteApplyMaxTransactions int
}

// DurabilityMode selects the acknowledgement contract for local writes.
type DurabilityMode int

const (
	// DurabilitySynchronous acknowledges only after the durable commit (fsync). Default.
	DurabilitySynchronous DurabilityMode = iota
	// DurabilityAsync acknowledges once mutations are committed in memory/WAL without
	// waiting for synchronous disk sync. Weaker durability contract: in an ungraceful crash
	// or power-loss scenario, transactions acknowledged since the last sync may be lost.
	DurabilityAsync
)

// DurabilityConfig configures the durability contract.
type DurabilityConfig struct {
	Mode DurabilityMode
	// SyncInterval periodically makes asynchronous commits durable. It is only
	// valid with DurabilityAsync. Zero leaves synchronization to db.Sync.
	// One second limits the usual unsynced window to about one second;
	// a blocked or failed sync can extend it.
	SyncInterval time.Duration
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
	// IncludeFiles packs file objects into scheduled backups
	// (object-inclusive). False leaves object bytes out (metadata-only).
	IncludeFiles bool
}

// FilesConfig configures replicated file metadata with node-local
// content-addressed object storage. Metadata (name, digest, size)
// replicates through the normal durable log under a reserved table;
// object bytes stay on the node that uploaded them until a transfer
// protocol fetches them (see architecture/file-replication.md).
type FilesConfig struct {
	// Enabled activates the file APIs and the local object store.
	Enabled bool
	// ObjectKey is the independent 256-bit object-storage key (32 bytes,
	// required when Enabled). Every node that stores objects needs its
	// key configured; key loss makes local objects unreadable while
	// replicated metadata is unaffected. Rotation must install the same
	// new key on every node: mixed generations fail mesh fetches closed.
	ObjectKey []byte
	// PrevObjectKey is the previous generation's key (32 bytes). It is
	// only needed to recover an interrupted rotation (objects spanning
	// generations); otherwise it is accepted and ignored. Remove it once
	// every node reports the new generation.
	PrevObjectKey []byte
	// MaxFileBytes caps one uploaded file. Zero selects the default
	// (4 GiB). Negative disables uploads.
	MaxFileBytes int64
	// FetchAddr is the listen address of the node-local fetch endpoint
	// serving object bytes to peers (e.g. "127.0.0.1:7844"). Empty
	// disables serving; the node can still fetch from peers. Serving
	// requires replication TLS credentials. Fetching peers must run the
	// same ObjectKey: receivers verify containers with the local key.
	FetchAddr string
	// FetchPeers statically lists fetch sources (NodeID plus fetch
	// endpoint addresses). Empty disables background and on-demand
	// fetching; the node serves (when FetchAddr is set) but never pulls.
	FetchPeers []Peer
	// FetchInterval is the background scan period for missing objects.
	// Zero selects 30 seconds. Negative disables the background worker
	// (FetchFile still works on demand).
	FetchInterval time.Duration
	// MaxConcurrentFetches bounds simultaneous object fetches. Zero
	// selects 2.
	MaxConcurrentFetches int
	// MaxStagingBytes caps the total fetch staging directory. Zero
	// selects 1 GiB.
	MaxStagingBytes int64
	// FetchTimeout bounds one file fetch across all sources. Zero
	// selects 5 minutes.
	FetchTimeout time.Duration
	// MaxFetchConns bounds concurrent serving connections. Zero
	// selects 16.
	MaxFetchConns int
}

// DefaultMaxFileBytes is the default per-file upload cap.
const DefaultMaxFileBytes = 4 << 30

// SubscriptionConfig configures reactive query subscriptions.
type SubscriptionConfig struct {
	// MaxSubscribers is the maximum number of concurrent active query subscriptions.
	// Defaults to 1024.
	MaxSubscribers int
	// EventBufferSize is the channel buffer size for each subscription.
	// Defaults to 64.
	EventBufferSize int
	// MaxRetainedEvents is the number of historical change events retained
	// in memory to support resumption from a cursor.
	// Defaults to 256.
	MaxRetainedEvents int
}

// CacheConfig sizes the caches.
type CacheConfig struct {
	StatementCacheEntries int
	// BlockCacheBytes sizes Pebble's unified block cache (alias for Pebble.CacheBytes).
	// Default 256 MiB.
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
	// ZstdLevel selects the Zstd level. Default 3; supported levels are
	// 3 (default), 9, and 12.
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
	// BaseFS is the filesystem under the encrypted VFS. Nil means
	// vfs.Default. Tests use it for fault injection (for example,
	// simulated disk-full failures).
	BaseFS vfs.FS
}

// DefaultPebbleConfig returns standard Pebble settings (256MiB cache, 4MiB
// memtables x2, 1000 open files, 1 concurrent compaction, Zstd level 3).
func DefaultPebbleConfig() PebbleConfig {
	return PebbleConfig{
		CacheBytes:               256 << 20,
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
	// AcceptRemoteSchema controls schema synchronization from peers during
	// replication (architecture/schema.md section 50). Nil (the default)
	// adopts compatible remote schemas and merges concurrent additive
	// branches automatically. Set an explicit false to refuse remote
	// schemas: mismatched peers exchange no data until this node is
	// explicitly upgraded with Migrate. A pointer distinguishes "unset"
	// from "strict" because the default adopts.
	AcceptRemoteSchema *bool
}

// acceptRemoteSchema resolves the schema-sync policy: default adopt.
func (c SchemaConfig) acceptRemoteSchema() bool {
	return c.AcceptRemoteSchema == nil || *c.AcceptRemoteSchema
}

// MembershipConfig configures SWIM dynamic discovery and failure detection over QUIC.
type MembershipConfig = replication.MembershipConfig

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
	// AllowedNetworks, when non-empty, additionally restricts peer network
	// addresses to the listed IPs/CIDRs (e.g. "192.0.2.0/24", "2001:db8::1").
	// It applies to inbound QUIC remotes and outbound resolved addresses in
	// addition to CA/NodeID/DBID authorization; absent means no address
	// filtering. Invalid entries fail Open. See
	// architecture/membership-and-transport.md ("IP and CIDR admission policy").
	AllowedNetworks []string
	// Membership configures SWIM dynamic discovery over QUIC.
	Membership MembershipConfig
	// Bootstrap is an optional list of QUIC seed addresses for dynamic discovery.
	Bootstrap []string
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
	// MaxSnapshotBytes bounds snapshot staging. Default 512 MiB.
	// Snapshots above the atomic merge threshold (state
	// DefaultSnapshotAtomicMergeBytes) merge chunk by chunk with durable
	// resume progress and publish watermarks/generation atomically last.
	MaxSnapshotBytes int64
	// SnapshotTransferTimeout bounds how long the source holds its consistent read cut.
	// Default 10 minutes.
	SnapshotTransferTimeout time.Duration
	// SnapshotRequestTimeout bounds how long a receiver waits for snapshot
	// progress before re-requesting. Default 30 seconds.
	SnapshotRequestTimeout time.Duration
	// Fanout is the number of active replication targets selected per push round.
	// Default 4.
	Fanout int
	// PeerRotationInterval is the interval at which one selected peer is rotated toward an off-subset member.
	// Default 30s.
	PeerRotationInterval time.Duration
	// AntiEntropyInterval is the interval at which jittered anti-entropy synchronizes with an off-subset peer.
	// Default 10s.
	AntiEntropyInterval time.Duration
	// MaxConcurrentRepairs bounds concurrent repair sessions.
	// Default 2.
	MaxConcurrentRepairs int
	// MaxReplicationSessions bounds concurrent replication sessions.
	// Default 32.
	MaxReplicationSessions int
	// MaxQUICConnections bounds the total physical QUIC connections maintained.
	// Must be at least MaxReplicationSessions + 8 (reserving 8 connections for membership).
	// Default 64.
	MaxQUICConnections int
	// Dissemination selects bounded gossip (the zero/default) or optional
	// Plumtree eager/lazy routing. All peers in the cluster must agree.
	Dissemination DisseminationMode
	// ProtocolVersionOverride/MinProtocolVersionOverride replace the
	// advertised handshake versions when nonzero, for interoperability
	// testing (peers must refuse unknown versions). Zero selects the
	// protocol constants. Never set in production.
	ProtocolVersionOverride    uint16
	MinProtocolVersionOverride uint16
}

// DisseminationMode selects how mutation batches are propagated.
type DisseminationMode string

const (
	DisseminationGossip   DisseminationMode = "gossip"
	DisseminationPlumtree DisseminationMode = "plumtree"
)

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
	Files       FilesConfig
	// Scheduling configures local/replication writer-time shares.
	// Zero resolves to the 90/10 defaults.
	Scheduling   WriterSchedulingConfig
	Subscription SubscriptionConfig

	// MaxReplicatedValueBytes caps one replicated cell value. Default 16 MiB.
	MaxReplicatedValueBytes int
	// MaxBatchMutations caps cells+deletes in one local transaction.
	// Default 100_000.
	MaxBatchMutations int
	// MaxTransactionBytes caps the total encoded canonical transaction size.
	// Default 64 MiB.
	MaxTransactionBytes int64

	// Logger receives package logs. Nil means discard.
	Logger Logger
}

func (c *Config) withDefaults() {
	if c.QueryStore.RemoteApplyInterval == 0 {
		c.QueryStore.RemoteApplyInterval = time.Second
	}
	if c.QueryStore.RemoteApplyMaxTransactions == 0 {
		c.QueryStore.RemoteApplyMaxTransactions = 1_000
	}
	if c.Cache.StatementCacheEntries == 0 {
		c.Cache.StatementCacheEntries = 256
	}
	p := &c.Pebble
	if c.Cache.BlockCacheBytes > 0 {
		p.CacheBytes = c.Cache.BlockCacheBytes
	} else if p.CacheBytes == 0 {
		p.CacheBytes = 256 << 20
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
	if c.MaxTransactionBytes == 0 {
		c.MaxTransactionBytes = 64 << 20
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
	if r.MaxSnapshotBytes == 0 {
		r.MaxSnapshotBytes = 512 << 20
	}
	if r.SnapshotTransferTimeout == 0 {
		r.SnapshotTransferTimeout = 10 * time.Minute
	}
	if r.SnapshotRequestTimeout == 0 {
		r.SnapshotRequestTimeout = 30 * time.Second
	}
	if r.Fanout == 0 {
		r.Fanout = 4
	}
	if r.PeerRotationInterval == 0 {
		r.PeerRotationInterval = 30 * time.Second
	}
	if r.AntiEntropyInterval == 0 {
		r.AntiEntropyInterval = 10 * time.Second
	}
	if r.MaxConcurrentRepairs == 0 {
		r.MaxConcurrentRepairs = 2
	}
	if r.MaxReplicationSessions == 0 {
		r.MaxReplicationSessions = 32
	}
	if r.MaxQUICConnections == 0 {
		r.MaxQUICConnections = 64
	}
	if len(r.Membership.Bootstrap) == 0 && len(r.Bootstrap) > 0 {
		r.Membership.Bootstrap = append([]string(nil), r.Bootstrap...)
	}
	if c.Subscription.MaxSubscribers == 0 {
		c.Subscription.MaxSubscribers = 1024
	}
	if c.Subscription.EventBufferSize == 0 {
		c.Subscription.EventBufferSize = 64
	}
	if c.Subscription.MaxRetainedEvents == 0 {
		c.Subscription.MaxRetainedEvents = 256
	}
	if c.Files.MaxFileBytes == 0 {
		c.Files.MaxFileBytes = DefaultMaxFileBytes
	}
	if c.Files.FetchInterval == 0 {
		c.Files.FetchInterval = 30 * time.Second
	}
	if c.Files.MaxConcurrentFetches <= 0 {
		c.Files.MaxConcurrentFetches = 2
	}
	if c.Files.MaxStagingBytes <= 0 {
		c.Files.MaxStagingBytes = 1 << 30
	}
	if c.Files.FetchTimeout <= 0 {
		c.Files.FetchTimeout = 5 * time.Minute
	}
	if c.Files.MaxFetchConns <= 0 {
		c.Files.MaxFetchConns = 16
	}
	if c.Logger == nil {
		c.Logger = DiscardLogger{}
	}
	c.Scheduling.withDefaults()
}

func (c *Config) validate() error {
	if c.Path == "" {
		return fmt.Errorf("replicateddb: Path is required: %w", ErrUnsupportedSchema)
	}
	if c.NodeID.IsZero() {
		return fmt.Errorf("replicateddb: NodeID is required: %w", ErrUnsupportedSchema)
	}
	if c.Durability.Mode != DurabilitySynchronous && c.Durability.Mode != DurabilityAsync {
		return fmt.Errorf("replicateddb: unsupported durability mode %d: %w", c.Durability.Mode, ErrUnsupportedSchema)
	}
	if c.Durability.SyncInterval < 0 || (c.Durability.SyncInterval > 0 && c.Durability.Mode != DurabilityAsync) {
		return fmt.Errorf("replicateddb: durability SyncInterval requires DurabilityAsync and must be non-negative: %w", ErrUnsupportedSchema)
	}
	if c.MaxTransactionBytes <= 0 {
		return fmt.Errorf("replicateddb: MaxTransactionBytes must be positive: %w", ErrUnsupportedSchema)
	}
	if int64(c.MaxReplicatedValueBytes) > c.MaxTransactionBytes {
		return fmt.Errorf("replicateddb: MaxReplicatedValueBytes (%d) cannot exceed MaxTransactionBytes (%d): %w", c.MaxReplicatedValueBytes, c.MaxTransactionBytes, ErrUnsupportedSchema)
	}
	if c.Subscription.MaxSubscribers < 0 || c.Subscription.EventBufferSize < 0 || c.Subscription.MaxRetainedEvents < 0 {
		return fmt.Errorf("replicateddb: subscription limits must be non-negative: %w", ErrUnsupportedSchema)
	}
	if c.QueryStore.RemoteApplyInterval <= 0 {
		return fmt.Errorf("replicateddb: query-store RemoteApplyInterval must be positive: %w", ErrUnsupportedSchema)
	}
	if c.QueryStore.RemoteApplyMaxTransactions <= 0 {
		return fmt.Errorf("replicateddb: query-store RemoteApplyMaxTransactions must be positive: %w", ErrUnsupportedSchema)
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
	if c.Replication.MaxSnapshotBytes <= 0 {
		return fmt.Errorf("replicateddb: Replication.MaxSnapshotBytes must be positive")
	}
	if c.Replication.SnapshotTransferTimeout <= 0 {
		return fmt.Errorf("replicateddb: Replication.SnapshotTransferTimeout must be positive")
	}
	if c.Replication.SnapshotRequestTimeout <= 0 {
		return fmt.Errorf("replicateddb: Replication.SnapshotRequestTimeout must be positive")
	}
	if c.Replication.Fanout <= 0 {
		return fmt.Errorf("replicateddb: Replication.Fanout must be positive")
	}
	if c.Replication.Dissemination != "" && c.Replication.Dissemination != DisseminationGossip && c.Replication.Dissemination != DisseminationPlumtree {
		return fmt.Errorf("replicateddb: unsupported replication dissemination mode %q", c.Replication.Dissemination)
	}
	if c.Replication.Dissemination == DisseminationPlumtree && c.Replication.Fanout < 2 {
		return fmt.Errorf("replicateddb: Replication.Fanout must be at least 2 for Plumtree")
	}
	if v, m := c.Replication.ProtocolVersionOverride, c.Replication.MinProtocolVersionOverride; v != 0 && m > v {
		return fmt.Errorf("replicateddb: Replication.MinProtocolVersionOverride must not exceed ProtocolVersionOverride")
	}
	if c.Replication.PeerRotationInterval <= 0 {
		return fmt.Errorf("replicateddb: Replication.PeerRotationInterval must be positive")
	}
	if c.Replication.AntiEntropyInterval <= 0 {
		return fmt.Errorf("replicateddb: Replication.AntiEntropyInterval must be positive")
	}
	if c.Replication.MaxConcurrentRepairs <= 0 {
		return fmt.Errorf("replicateddb: Replication.MaxConcurrentRepairs must be positive")
	}
	if c.Replication.MaxReplicationSessions <= 0 {
		return fmt.Errorf("replicateddb: Replication.MaxReplicationSessions must be positive")
	}
	if c.Replication.Fanout+c.Replication.MaxConcurrentRepairs > c.Replication.MaxReplicationSessions {
		return fmt.Errorf("replicateddb: Replication.Fanout + MaxConcurrentRepairs (%d) cannot exceed MaxReplicationSessions (%d)",
			c.Replication.Fanout+c.Replication.MaxConcurrentRepairs, c.Replication.MaxReplicationSessions)
	}
	if c.Replication.MaxQUICConnections < c.Replication.MaxReplicationSessions+8 {
		return fmt.Errorf("replicateddb: Replication.MaxQUICConnections (%d) must be at least MaxReplicationSessions + 8 (%d)",
			c.Replication.MaxQUICConnections, c.Replication.MaxReplicationSessions+8)
	}
	if _, err := transport.ParseAddressPolicy(c.Replication.AllowedNetworks); err != nil {
		return err
	}
	if err := c.Scheduling.validate(); err != nil {
		return err
	}
	if c.Files.Enabled && len(c.Files.ObjectKey) != 32 {
		return fmt.Errorf("replicateddb: Files.ObjectKey must be 32 bytes when Files.Enabled")
	}
	if len(c.Files.PrevObjectKey) != 0 && len(c.Files.PrevObjectKey) != 32 {
		return fmt.Errorf("replicateddb: Files.PrevObjectKey must be 32 bytes when set")
	}
	if c.Files.Enabled && (c.Files.FetchAddr != "" || len(c.Files.FetchPeers) > 0) && c.Replication.TLS == nil {
		return fmt.Errorf("replicateddb: Files fetch requires Replication.TLS credentials")
	}
	for i, p := range c.Files.FetchPeers {
		if p.NodeID.IsZero() {
			return fmt.Errorf("replicateddb: Files.FetchPeers[%d] needs a NodeID", i)
		}
		if len(p.Addrs) == 0 {
			return fmt.Errorf("replicateddb: Files.FetchPeers[%d] needs at least one address", i)
		}
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
	if p.Compression.Algorithm == CompressionZstd {
		switch p.Compression.ZstdLevel {
		case 3, 9, 12:
		default:
			return fmt.Errorf("replicateddb: Pebble Zstd supports only levels 3, 9, and 12 (got %d)", p.Compression.ZstdLevel)
		}
	}
	return nil
}
