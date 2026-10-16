package murmur

import (
	"encoding/json"
	"fmt"
	"time"
)

type NodeID [16]byte
type Key [32]byte

// Bytes uses byte arrays in control JSON, matching Rust's serde representation.
type Bytes []byte

func (b Bytes) MarshalJSON() ([]byte, error) {
	if b == nil {
		return []byte("[]"), nil
	}
	values := make([]uint16, len(b))
	for i, n := range b {
		values[i] = uint16(n)
	}
	return json.Marshal(values)
}
func (b *Bytes) UnmarshalJSON(data []byte) error {
	var values []uint16
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	out := make([]byte, len(values))
	for i, n := range values {
		if n > 255 {
			return fmt.Errorf("invalid byte %d", n)
		}
		out[i] = byte(n)
	}
	*b = out
	return nil
}

type OpenMode string

const (
	Existing  OpenMode = "Existing"
	CreateNew OpenMode = "CreateNew"
)

type DistanceMetric string

const (
	L2     DistanceMetric = "L2"
	Cosine DistanceMetric = "Cosine"
	L1     DistanceMetric = "L1"
)

type MergePolicy string

const (
	LWW           MergePolicy = "Lww"
	CounterPolicy MergePolicy = "Counter"
	SetPolicy     MergePolicy = "Set"
	MinPolicy     MergePolicy = "Min"
	MaxPolicy     MergePolicy = "Max"
)

type WriterClass string

const (
	WriterLocal       WriterClass = "Local"
	WriterRemote      WriterClass = "Remote"
	WriterMaintenance WriterClass = "Maintenance"
)

const WriterClassCount = 3

func (c WriterClass) String() string {
	switch c {
	case WriterLocal:
		return "local"
	case WriterRemote:
		return "remote"
	case WriterMaintenance:
		return "maintenance"
	}
	return string(c)
}

type SchedulerConfig struct {
	LocalShare              uint32        `json:"local_share"`
	RemoteShare             uint32        `json:"remote_share"`
	MaxDebt                 time.Duration `json:"max_debt"`
	MaintenanceReserveEvery uint64        `json:"maintenance_reserve_every"`
}
type SearchIndex struct {
	Kind        string         `json:"kind"`
	Name        string         `json:"name"`
	SourceTable string         `json:"source_table"`
	Columns     []string       `json:"columns,omitempty"`
	Tokenizer   string         `json:"tokenizer,omitempty"`
	Column      string         `json:"column,omitempty"`
	Dimensions  uint32         `json:"dimensions,omitempty"`
	Distance    DistanceMetric `json:"distance,omitempty"`
}

func FTS5Index(name, source string, columns ...string) SearchIndex {
	var i SearchIndex
	if err := invoke(0, "helper_fts_defaults", map[string]any{"name": name, "source": source, "columns": nonNil(columns)}, &i); err != nil {
		panic(err)
	}
	return i
}
func VectorIndex(name, source, column string, dimensions uint32) SearchIndex {
	var i SearchIndex
	if err := invoke(0, "helper_vector_defaults", map[string]any{"name": name, "source": source, "column": column, "dimensions": dimensions}, &i); err != nil {
		panic(err)
	}
	return i
}

func (s SearchIndex) IndexName() string { return s.Name }
func (s SearchIndex) Source() string    { return s.SourceTable }

type Options struct {
	Mode             OpenMode           `json:"mode"`
	CaptureInterval  time.Duration      `json:"capture_interval"`
	CaptureBytes     uint64             `json:"capture_bytes"`
	QueueBytes       uint64             `json:"queue_bytes"`
	TransactionBytes uint64             `json:"transaction_bytes"`
	SegmentBytes     uint64             `json:"segment_bytes"`
	SpoolBlockBytes  uint64             `json:"spool_block_bytes"`
	SpoolCacheBytes  uint64             `json:"spool_cache_bytes"`
	SnapshotDeltas   uint64             `json:"snapshot_deltas"`
	SnapshotBytes    uint64             `json:"snapshot_bytes"`
	CompressionLevel int32              `json:"compression_level"`
	StorageWorkers   uint64             `json:"storage_workers"`
	DataKeyMaxAge    *time.Duration     `json:"data_key_max_age"`
	DatabaseID       *NodeID            `json:"database_id"`
	NodeID           *NodeID            `json:"node_id"`
	SearchIndexes    []SearchIndex      `json:"search_indexes"`
	SearchQueueBytes uint64             `json:"search_queue_bytes"`
	SearchBatchRows  uint64             `json:"search_batch_rows"`
	SearchInterval   time.Duration      `json:"search_interval"`
	Scheduler        SchedulerConfig    `json:"scheduler"`
	Replication      *ReplicationConfig `json:"replication"`
	OnOpenProgress   func(OpenProgress) `json:"-"`
	FaultHooks       *FaultHooks        `json:"-"`
}

func DefaultOptions() Options {
	var o Options
	if err := invoke(0, "defaults", nil, &o); err != nil {
		panic(err)
	}
	return o
}

type TableMapping struct {
	OldName string      `json:"old_name"`
	NewName string      `json:"new_name"`
	Columns [][2]string `json:"columns"`
}
type Migration struct {
	SQL      string         `json:"sql"`
	Mappings []TableMapping `json:"mappings"`
}

func NewMigration(sql string) Migration { return Migration{SQL: sql, Mappings: []TableMapping{}} }

type RestoreOptions struct {
	NewDatabaseID *NodeID `json:"new_database_id"`
	NewNodeID     *NodeID `json:"new_node_id"`
}
type OpenPhase string

const (
	Opening    OpenPhase = "Opening"
	Rebuilding OpenPhase = "Rebuilding"
	Indexing   OpenPhase = "Indexing"
	Finalizing OpenPhase = "Finalizing"
	Ready      OpenPhase = "Ready"
	Failed     OpenPhase = "Failed"
	Cancelled  OpenPhase = "Cancelled"
)

type SubscriptionCursor struct {
	DatabaseID NodeID `json:"database_id"`
	Epoch      uint64 `json:"epoch"`
	Sequence   uint64 `json:"sequence"`
}
type SubscriptionOptions struct {
	BufferSize    uint64              `json:"buffer_size"`
	EmitUnchanged bool                `json:"emit_unchanged"`
	ResumeFrom    *SubscriptionCursor `json:"resume_from"`
	Tables        []string            `json:"tables"`
}

func DefaultSubscriptionOptions() SubscriptionOptions {
	var o SubscriptionOptions
	if err := invoke(0, "helper_subscription_defaults", nil, &o); err != nil {
		panic(err)
	}
	return o
}

type ChangeType string

const (
	Added   ChangeType = "Added"
	Updated ChangeType = "Updated"
	Removed ChangeType = "Removed"
)

type Credentials struct {
	Chain      []Bytes `json:"chain"`
	PrivateKey Bytes   `json:"private_key"`
	Roots      []Bytes `json:"roots"`
}
type TrustEntry struct {
	Node NodeID `json:"node"`
	Key  Key    `json:"key"`
}
type PolicyEntry struct {
	Table  string      `json:"table"`
	Column string      `json:"column"`
	Policy MergePolicy `json:"policy"`
}
type Dissemination string

const (
	Gossip   Dissemination = "Gossip"
	Plumtree Dissemination = "Plumtree"
)

type ReplicationConfig struct {
	Bind                     string        `json:"bind"`
	Advertise                *string       `json:"advertise"`
	Credentials              Credentials   `json:"credentials"`
	SigningKey               Key           `json:"signing_key"`
	TrustedOrigins           []TrustEntry  `json:"trusted_origins"`
	AllowedPeers             []NodeID      `json:"allowed_peers"`
	AllowedNetworks          []string      `json:"allowed_networks"`
	TrustedSnapshotSources   []NodeID      `json:"trusted_snapshot_sources"`
	Bootstrap                []string      `json:"bootstrap"`
	Membership               bool          `json:"membership"`
	Dissemination            Dissemination `json:"dissemination"`
	AcceptRemoteSchema       bool          `json:"accept_remote_schema"`
	Master                   bool          `json:"master"`
	SchemaMaster             *NodeID       `json:"schema_master"`
	Fanout                   uint64        `json:"fanout"`
	Repairs                  uint64        `json:"repairs"`
	Sessions                 uint64        `json:"sessions"`
	Connections              uint64        `json:"connections"`
	QueueBytes               uint64        `json:"queue_bytes"`
	PeerQueueBytes           uint64        `json:"peer_queue_bytes"`
	StagingBytes             uint64        `json:"staging_bytes"`
	StagingTransfers         uint64        `json:"staging_transfers"`
	MaxTransactionBytes      uint64        `json:"max_transaction_bytes"`
	SnapshotBytes            uint64        `json:"snapshot_bytes"`
	SnapshotStagingBytes     uint64        `json:"snapshot_staging_bytes"`
	SnapshotTransferLifetime time.Duration `json:"snapshot_transfer_lifetime"`
	IncomingInterval         time.Duration `json:"incoming_interval"`
	BatchTransactions        uint64        `json:"batch_transactions"`
	BatchBytes               uint64        `json:"batch_bytes"`
	AdaptiveSync             bool          `json:"adaptive_sync"`
	AntiEntropyInterval      time.Duration `json:"anti_entropy_interval"`
	Retention                time.Duration `json:"retention"`
	RetentionInterval        time.Duration `json:"retention_interval"`
	MinimumRetained          uint64        `json:"minimum_retained"`
	MaximumOffline           time.Duration `json:"maximum_offline"`
	MergePolicies            []PolicyEntry `json:"merge_policies"`
	Files                    *FileConfig   `json:"files"`
	Bridge                   *BridgeConfig `json:"bridge"`
}

func NewReplicationConfig(bind string, credentials Credentials, signing Key) (ReplicationConfig, error) {
	var c ReplicationConfig
	err := invoke(0, "helper_replication_defaults", map[string]any{"bind": bind}, &c)
	c.Credentials = credentials
	c.SigningKey = signing
	return c, err
}
func (c ReplicationConfig) Validate() error {
	return invoke(0, "helper_validate_replication", map[string]any{"config": c}, nil)
}

type FileConfig struct {
	Directory         string        `json:"directory"`
	Key               Key           `json:"key"`
	PreviousKey       *Key          `json:"previous_key"`
	Generation        uint64        `json:"generation"`
	FetchBind         string        `json:"fetch_bind"`
	MaxFileBytes      uint64        `json:"max_file_bytes"`
	MaxStagingBytes   uint64        `json:"max_staging_bytes"`
	ConcurrentFetches uint64        `json:"concurrent_fetches"`
	FetchInterval     time.Duration `json:"fetch_interval"`
}

func NewFileConfig(directory string, key Key, bind string) (FileConfig, error) {
	var c FileConfig
	err := invoke(0, "helper_file_defaults", map[string]any{"directory": directory, "key": key, "fetch_bind": bind}, &c)
	return c, err
}
func (c FileConfig) Validate() error {
	return invoke(0, "helper_validate_files", map[string]any{"config": c}, nil)
}

type BridgeRole string

const (
	LowExporter  BridgeRole = "LowExporter"
	HighReceiver BridgeRole = "HighReceiver"
)

type BridgeConfig struct {
	Domain          NodeID       `json:"domain"`
	TrustedDomains  []NodeID     `json:"trusted_domains"`
	TrustedOrigins  []TrustEntry `json:"trusted_origins"`
	Role            BridgeRole   `json:"role"`
	Directory       string       `json:"directory"`
	Stream          string       `json:"stream"`
	Recipient       Key          `json:"recipient"`
	ReceiverSecret  *Key         `json:"receiver_secret"`
	Signer          *Key         `json:"signer"`
	TrustedSigners  []TrustEntry `json:"trusted_signers"`
	MaxBundleBytes  uint64       `json:"max_bundle_bytes"`
	MaxPayloadBytes uint64       `json:"max_payload_bytes"`
	StagingBytes    uint64       `json:"staging_bytes"`
}

func (c BridgeConfig) Validate() error {
	return invoke(0, "helper_validate_bridge", map[string]any{"config": c}, nil)
}

type FTPOptions struct {
	RootsPEM Bytes         `json:"roots_pem"`
	Timeout  time.Duration `json:"timeout"`
}
type FaultHooks struct{ Append, SegmentSync, ManifestPersist, KeyringPersist, IntentPersist, CompactionStage, CheckpointLink, Rename, DirSync, Delete func() error }
