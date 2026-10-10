package murmur

import (
	"fmt"
	"time"

	"github.com/marcgauthier/murmur/backup"
	"github.com/marcgauthier/murmur/compression"
	"github.com/marcgauthier/murmur/origin"
	"github.com/marcgauthier/murmur/replication"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/spool"
	"github.com/marcgauthier/murmur/transport"
)

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

// GroupCommitConfig configures synchronous group commit for managed typed
// writes.
type GroupCommitConfig struct {
	// MaxDelay bounds how long a group leader waits for concurrent
	// transactions before committing. Zero selects the one-millisecond
	// default in DurabilitySynchronous mode. A negative value disables
	// group commit (every transaction commits alone, as before). The
	// setting is ignored in DurabilityAsync mode.
	MaxDelay time.Duration
	// MaxTransactions caps transactions per group. Zero selects 64.
	MaxTransactions int
	// MaxBytes caps the total encoded size per group. Zero selects 4 MiB.
	MaxBytes int64
}

// DurabilityConfig configures the durability contract.
type DurabilityConfig struct {
	Mode DurabilityMode
	// SyncInterval periodically makes asynchronous commits durable. It is only
	// valid with DurabilityAsync. Zero leaves synchronization to db.Sync.
	// One second limits the usual unsynced window to about one second;
	// a blocked or failed sync can extend it.
	SyncInterval time.Duration
	// MaxUnsyncedBytes triggers a durability sync once approximately this
	// many bytes have been written without one. It is only valid with
	// DurabilityAsync; zero disables the size trigger. When both
	// SyncInterval and MaxUnsyncedBytes are set, whichever threshold is
	// reached first triggers the sync, bounding the loss window under
	// bursty load that a pure time interval would leave wide open.
	MaxUnsyncedBytes int64
	// GroupCommit configures synchronous group commit for managed typed writes.
	// The setting is ignored in DurabilityAsync mode, which already
	// acknowledges without waiting for fsync.
	GroupCommit GroupCommitConfig
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
	// endpoint addresses). Serving members discovered through SWIM
	// membership are additional sources; on a NodeID conflict the static
	// entry wins. Empty with no membership service disables background
	// and on-demand fetching; the node serves (when FetchAddr is set)
	// but never pulls.
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

// CompressionAlgorithm selects Spool block compression.
type CompressionAlgorithm string

const (
	CompressionNone CompressionAlgorithm = "none"
	// CompressionDeflate is the default: standard-library deflate at
	// its fastest level (no external dependency). Custom codecs are
	// selected through SpoolConfig.Codec.
	CompressionDeflate CompressionAlgorithm = "deflate"
)

// SpoolConfig configures the durable Spool store. It is required; use
// DefaultSpoolConfig for standard settings.
type SpoolConfig struct {
	// WriteShards sets the number of concurrent put buffers (power of two).
	// Default 128.
	WriteShards int
	// IndexShards sets the number of index shards (power of two).
	// Default 256.
	IndexShards int
	// TargetBlockBytes seals a block around this size. Default 4 MiB.
	TargetBlockBytes int
	// MaxBlockBytes is the hard cap for one block. Default 64 MiB.
	MaxBlockBytes int
	// MaxRecordsPerBlock caps records in one block. Default 10,000.
	MaxRecordsPerBlock int
	// MaxAtomicBatchBytes bounds one Commit group's plaintext bodies. Default 256 MiB.
	MaxAtomicBatchBytes int64
	// MaxSegmentSize rolls to a new segment past this size. Default 256 MiB.
	MaxSegmentSize int64
	// MaxPendingBytes caps buffered plus in-flight write bytes. Default 512 MiB.
	MaxPendingBytes int64
	// Workers bounds parallel block compression/encryption.
	// Default runtime.NumCPU clamped to [2,32].
	Workers int
	// CompactionThreshold in (0,1] marks sealed segments with live/total
	// records at or below it eligible for compaction. Default 0.20.
	CompactionThreshold float64
	// CompactionMinFreeBytes refuses rewrite passes while the store's
	// filesystem reports less free space. Default 0 (disabled).
	CompactionMinFreeBytes int64
	// TombProofThreshold gates deletion-marker garbage collection.
	// Default 1024.
	TombProofThreshold int
	// ReclaimInterval ticks background tombstone cleanup, dead-file
	// deletion and compaction. Default 30s. Negative disables the worker.
	ReclaimInterval time.Duration
	// Flush bounds the buffered flush thresholds.
	Flush spool.FlushPolicy
	// Compression selects the block-level codec. Default CompressionDeflate.
	Compression CompressionAlgorithm
	// Codec, when non-nil, is a custom compression codec (wire id
	// 128-255) used for Spool blocks and outgoing replication snapshot
	// chunks. It overrides Compression. Every node that must read
	// this node's data or snapshots has to register the same codec.
	Codec compression.Codec
	// Faults injects storage failures for testing.
	Faults *spool.FaultHooks
}

// DefaultSpoolConfig returns standard Spool settings.
func DefaultSpoolConfig() SpoolConfig {
	return SpoolConfig{
		WriteShards:         128,
		IndexShards:         256,
		TargetBlockBytes:    4 << 20,
		MaxBlockBytes:       64 << 20,
		MaxRecordsPerBlock:  10000,
		MaxAtomicBatchBytes: 256 << 20,
		MaxSegmentSize:      256 << 20,
		MaxPendingBytes:     512 << 20,
		CompactionThreshold: 0.20,
		TombProofThreshold:  1024,
		ReclaimInterval:     30 * time.Second,
		Compression:         CompressionDeflate,
	}
}

// SchemaConfig carries the application-owned replicated schema.
type SchemaConfig struct {
	// Version is the schema epoch. All nodes at an epoch must share one hash.
	Version uint64
	// Tables are the replicated tables. IDs may be zero for deterministic derivation.
	Tables []schema.TableSchema
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
	// TrustedSnapshotSources authorizes merged-state recovery independently
	// of transport and transaction-origin trust. Empty disables remote snapshots.
	TrustedSnapshotSources []NodeID
	ListenAddr             string
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
	// SnapshotAtomicMergeBytes chooses the snapshot atomic merge threshold.
	// Zero uses the state default; larger snapshots use resumable SST ingestion.
	SnapshotAtomicMergeBytes uint64
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
// OriginSigningConfig supplies a separate Ed25519 identity and explicit origin trust.
type OriginSigningConfig = origin.Config

type Config struct {
	OriginSigning           OriginSigningConfig
	originBaselineMigration bool
	mergePolicyMigration    bool
	// Path is the database directory (data/ contains Spool persistence).
	Path string
	// NodeID is this node's identity. Required.
	NodeID NodeID
	// DBID identifies the cluster. Zero means "load or create".
	DBID DBID

	// Codecs registers additional custom compression codecs (ids
	// 128-255) for reading Spool blocks and replication frames written
	// under them. Spool.Codec is registered automatically.
	Codecs []compression.Codec

	Spool       SpoolConfig
	Encryption  EncryptionConfig
	Replication ReplicationConfig
	Schema      SchemaConfig
	// Models registers named struct exemplars or compiled Model definitions.
	// Models and Tables are combined and validated before opening storage.
	Models []any
	// Tables supplies explicit or generic compiled definitions alongside Models.
	// Schema.Tables is internal metadata derived from those definitions.
	Tables     []TableDefinition
	Durability DurabilityConfig
	Backup     BackupScheduleConfig
	Files      FilesConfig
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
	// OnOpenProgress receives serialized startup snapshots from a dedicated
	// goroutine, including processed cells and a terminal event before Open
	// returns. It must return promptly and must not wait for Open to return.
	// No extra counting scan is performed. Nil disables reporting. Forward events to
	// your application's UI thread; callbacks do not run on the caller thread.
	OnOpenProgress func(OpenProgress) `json:"-"`
}

func (c *Config) withDefaults() {
	s := &c.Spool
	if s.WriteShards == 0 {
		s.WriteShards = 128
	}
	if s.IndexShards == 0 {
		s.IndexShards = 256
	}
	if s.TargetBlockBytes == 0 {
		s.TargetBlockBytes = 4 << 20
	}
	if s.MaxBlockBytes == 0 {
		s.MaxBlockBytes = 64 << 20
	}
	if s.MaxRecordsPerBlock == 0 {
		s.MaxRecordsPerBlock = 10000
	}
	if s.MaxAtomicBatchBytes == 0 {
		s.MaxAtomicBatchBytes = 256 << 20
	}
	if s.MaxSegmentSize == 0 {
		s.MaxSegmentSize = 256 << 20
	}
	if s.MaxPendingBytes == 0 {
		s.MaxPendingBytes = 512 << 20
	}
	if s.CompactionThreshold == 0 {
		s.CompactionThreshold = 0.20
	}
	if s.TombProofThreshold == 0 {
		s.TombProofThreshold = 1024
	}
	if s.ReclaimInterval == 0 {
		s.ReclaimInterval = 30 * time.Second
	}
	if s.Compression == "" {
		s.Compression = CompressionDeflate
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
	g := &c.Durability.GroupCommit
	if g.MaxDelay == 0 && c.Durability.Mode == DurabilitySynchronous {
		g.MaxDelay = time.Millisecond
	}
	if g.MaxTransactions == 0 {
		g.MaxTransactions = 64
	}
	if g.MaxBytes == 0 {
		g.MaxBytes = 4 << 20
	}
	c.Scheduling.withDefaults()
}

func (c *Config) validate() error {
	if err := c.OriginSigning.Validate(c.NodeID); err != nil {
		return err
	}
	if c.Path == "" {
		return fmt.Errorf("murmur: Path is required: %w", ErrUnsupportedSchema)
	}
	if c.NodeID.IsZero() {
		return fmt.Errorf("murmur: NodeID is required: %w", ErrUnsupportedSchema)
	}
	if c.Durability.Mode != DurabilitySynchronous && c.Durability.Mode != DurabilityAsync {
		return fmt.Errorf("murmur: unsupported durability mode %d: %w", c.Durability.Mode, ErrUnsupportedSchema)
	}
	if c.Durability.SyncInterval < 0 || (c.Durability.SyncInterval > 0 && c.Durability.Mode != DurabilityAsync) {
		return fmt.Errorf("murmur: durability SyncInterval requires DurabilityAsync and must be non-negative: %w", ErrUnsupportedSchema)
	}
	if c.Durability.MaxUnsyncedBytes < 0 || (c.Durability.MaxUnsyncedBytes > 0 && c.Durability.Mode != DurabilityAsync) {
		return fmt.Errorf("murmur: durability MaxUnsyncedBytes requires DurabilityAsync and must be non-negative: %w", ErrUnsupportedSchema)
	}
	g := c.Durability.GroupCommit
	// Group commit is inactive in asynchronous mode (acknowledgement never
	// waits for fsync there); its settings are validated but ignored.
	if g.MaxDelay > time.Second {
		return fmt.Errorf("murmur: durability GroupCommit MaxDelay must not exceed one second: %w", ErrUnsupportedSchema)
	}
	// Zero caps mean "use defaults" (withDefaults fills them before Open
	// validates); only explicit out-of-range values are rejected.
	if g.MaxTransactions < 0 || g.MaxTransactions > 512 {
		return fmt.Errorf("murmur: durability GroupCommit MaxTransactions must be within 0..512: %w", ErrUnsupportedSchema)
	}
	if g.MaxBytes < 0 || g.MaxBytes > 64<<20 {
		return fmt.Errorf("murmur: durability GroupCommit MaxBytes must be within 0..64MiB: %w", ErrUnsupportedSchema)
	}
	if c.MaxTransactionBytes <= 0 {
		return fmt.Errorf("murmur: MaxTransactionBytes must be positive: %w", ErrUnsupportedSchema)
	}
	if int64(c.MaxReplicatedValueBytes) > c.MaxTransactionBytes {
		return fmt.Errorf("murmur: MaxReplicatedValueBytes (%d) cannot exceed MaxTransactionBytes (%d): %w", c.MaxReplicatedValueBytes, c.MaxTransactionBytes, ErrUnsupportedSchema)
	}
	if c.Subscription.MaxSubscribers < 0 || c.Subscription.EventBufferSize < 0 || c.Subscription.MaxRetainedEvents < 0 {
		return fmt.Errorf("murmur: subscription limits must be non-negative: %w", ErrUnsupportedSchema)
	}
	if len(c.Schema.Tables) == 0 && len(c.Tables) == 0 {
		return fmt.Errorf("murmur: at least one replicated table is required: %w", ErrUnsupportedSchema)
	}
	if _, err := schema.BuildRegistry(c.Schema.Version, c.Schema.Tables); err != nil {
		return err
	}
	if err := c.Spool.validate(); err != nil {
		return err
	}
	if err := c.Encryption.validate(); err != nil {
		return err
	}
	if c.Replication.MaxSnapshotBytes <= 0 {
		return fmt.Errorf("murmur: Replication.MaxSnapshotBytes must be positive")
	}
	if c.Replication.SnapshotTransferTimeout <= 0 {
		return fmt.Errorf("murmur: Replication.SnapshotTransferTimeout must be positive")
	}
	if c.Replication.SnapshotRequestTimeout <= 0 {
		return fmt.Errorf("murmur: Replication.SnapshotRequestTimeout must be positive")
	}
	if c.Replication.Fanout <= 0 {
		return fmt.Errorf("murmur: Replication.Fanout must be positive")
	}
	if c.Replication.Dissemination != "" && c.Replication.Dissemination != DisseminationGossip && c.Replication.Dissemination != DisseminationPlumtree {
		return fmt.Errorf("murmur: unsupported replication dissemination mode %q", c.Replication.Dissemination)
	}
	if c.Replication.Dissemination == DisseminationPlumtree && c.Replication.Fanout < 2 {
		return fmt.Errorf("murmur: Replication.Fanout must be at least 2 for Plumtree")
	}
	if v, m := c.Replication.ProtocolVersionOverride, c.Replication.MinProtocolVersionOverride; v != 0 && m > v {
		return fmt.Errorf("murmur: Replication.MinProtocolVersionOverride must not exceed ProtocolVersionOverride")
	}
	if c.Replication.PeerRotationInterval <= 0 {
		return fmt.Errorf("murmur: Replication.PeerRotationInterval must be positive")
	}
	if c.Replication.AntiEntropyInterval <= 0 {
		return fmt.Errorf("murmur: Replication.AntiEntropyInterval must be positive")
	}
	if c.Replication.MaxConcurrentRepairs <= 0 {
		return fmt.Errorf("murmur: Replication.MaxConcurrentRepairs must be positive")
	}
	if c.Replication.MaxReplicationSessions <= 0 {
		return fmt.Errorf("murmur: Replication.MaxReplicationSessions must be positive")
	}
	if c.Replication.Fanout+c.Replication.MaxConcurrentRepairs > c.Replication.MaxReplicationSessions {
		return fmt.Errorf("murmur: Replication.Fanout + MaxConcurrentRepairs (%d) cannot exceed MaxReplicationSessions (%d)",
			c.Replication.Fanout+c.Replication.MaxConcurrentRepairs, c.Replication.MaxReplicationSessions)
	}
	if c.Replication.MaxQUICConnections < c.Replication.MaxReplicationSessions+8 {
		return fmt.Errorf("murmur: Replication.MaxQUICConnections (%d) must be at least MaxReplicationSessions + 8 (%d)",
			c.Replication.MaxQUICConnections, c.Replication.MaxReplicationSessions+8)
	}
	if _, err := transport.ParseAddressPolicy(c.Replication.AllowedNetworks); err != nil {
		return err
	}
	if err := c.Scheduling.validate(); err != nil {
		return err
	}
	if c.Files.Enabled && len(c.Files.ObjectKey) != 32 {
		return fmt.Errorf("murmur: Files.ObjectKey must be 32 bytes when Files.Enabled")
	}
	if len(c.Files.PrevObjectKey) != 0 && len(c.Files.PrevObjectKey) != 32 {
		return fmt.Errorf("murmur: Files.PrevObjectKey must be 32 bytes when set")
	}
	if c.Files.Enabled && (c.Files.FetchAddr != "" || len(c.Files.FetchPeers) > 0) && c.Replication.TLS == nil {
		return fmt.Errorf("murmur: Files fetch requires Replication.TLS credentials")
	}
	for i, p := range c.Files.FetchPeers {
		if p.NodeID.IsZero() {
			return fmt.Errorf("murmur: Files.FetchPeers[%d] needs a NodeID", i)
		}
		if len(p.Addrs) == 0 {
			return fmt.Errorf("murmur: Files.FetchPeers[%d] needs at least one address", i)
		}
	}
	return nil
}

func (s SpoolConfig) validate() error {
	if s.WriteShards <= 0 || (s.WriteShards&(s.WriteShards-1)) != 0 {
		return fmt.Errorf("murmur: Spool.WriteShards must be a positive power of two")
	}
	if s.IndexShards <= 0 || (s.IndexShards&(s.IndexShards-1)) != 0 {
		return fmt.Errorf("murmur: Spool.IndexShards must be a positive power of two")
	}
	if s.TargetBlockBytes <= 0 {
		return fmt.Errorf("murmur: Spool.TargetBlockBytes must be positive")
	}
	if s.MaxBlockBytes <= 0 || s.MaxBlockBytes < s.TargetBlockBytes {
		return fmt.Errorf("murmur: Spool.MaxBlockBytes must be positive and at least TargetBlockBytes")
	}
	if s.MaxRecordsPerBlock <= 0 {
		return fmt.Errorf("murmur: Spool.MaxRecordsPerBlock must be positive")
	}
	if s.MaxAtomicBatchBytes <= 0 {
		return fmt.Errorf("murmur: Spool.MaxAtomicBatchBytes must be positive")
	}
	if s.MaxSegmentSize <= 0 {
		return fmt.Errorf("murmur: Spool.MaxSegmentSize must be positive")
	}
	if s.MaxPendingBytes <= 0 {
		return fmt.Errorf("murmur: Spool.MaxPendingBytes must be positive")
	}
	if s.CompactionThreshold <= 0 || s.CompactionThreshold > 1 {
		return fmt.Errorf("murmur: Spool.CompactionThreshold must be within (0, 1]")
	}
	if s.TombProofThreshold <= 0 {
		return fmt.Errorf("murmur: Spool.TombProofThreshold must be positive")
	}
	switch s.Compression {
	case CompressionNone, CompressionDeflate:
	default:
		return fmt.Errorf("murmur: unsupported Spool compression %q", s.Compression)
	}
	return nil
}
