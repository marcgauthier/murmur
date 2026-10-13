// Public data types mirror Murmur Rust fields. Control durations are nanoseconds.
package murmur

import "time"

type Version struct {
	Millis  uint64 `json:"millis"`
	Logical uint32 `json:"logical"`
	Node    NodeID `json:"node"`
}
type Column struct {
	Policy       MergePolicy `json:"policy"`
	ID           string      `json:"id"`
	Name         string      `json:"name"`
	PrimaryKey   uint64      `json:"primary_key"`
	DeclaredType string      `json:"declared_type"`
	NotNull      bool        `json:"not_null"`
}
type Table struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	SQL     string   `json:"sql"`
	Columns []Column `json:"columns"`
}
type Schema struct {
	TableReplication map[string]bool `json:"table_replication"`
	Tables           []Table         `json:"tables"`
	Indexes          []string        `json:"indexes"`
	History          Key             `json:"history"`
}
type Cell struct {
	Column  string  `json:"column"`
	Value   Value   `json:"value"`
	Version Version `json:"version"`
}
type RowState struct {
	Table      string  `json:"table"`
	Key        []Value `json:"key"`
	Generation Version `json:"generation"`
	Cells      []Cell  `json:"cells"`
}
type Tombstone struct {
	Table      string  `json:"table"`
	Key        Bytes   `json:"key"`
	Generation Version `json:"generation"`
	Version    Version `json:"version"`
}
type ReplicaState struct {
	DatabaseID NodeID      `json:"database_id"`
	Schema     Schema      `json:"schema"`
	Rows       []RowState  `json:"rows"`
	Tombstones []Tombstone `json:"tombstones"`
	Clock      Version     `json:"clock"`
}
type OpenProgress struct {
	Phase          OpenPhase     `json:"phase"`
	Elapsed        time.Duration `json:"elapsed"`
	PhaseElapsed   time.Duration `json:"phase_elapsed"`
	ProcessedItems uint64        `json:"processed_items"`
	CurrentTable   *string       `json:"current_table"`
	RowsInserted   uint64        `json:"rows_inserted"`
	RowsSkipped    uint64        `json:"rows_skipped"`
	Error          *string       `json:"error"`
}
type SqliteMetrics struct {
	MemoryUsedBytes       int64  `json:"memory_used_bytes"`
	MemoryHighwaterBytes  int64  `json:"memory_highwater_bytes"`
	PageSize              int64  `json:"page_size"`
	PageCount             int64  `json:"page_count"`
	DatabaseSizeBytes     int64  `json:"database_size_bytes"`
	FreelistPages         int64  `json:"freelist_pages"`
	SchemaVersion         int64  `json:"schema_version"`
	TableCount            uint64 `json:"table_count"`
	IndexCount            uint64 `json:"index_count"`
	TotalRows             uint64 `json:"total_rows"`
	SessionMemoryBytes    uint64 `json:"session_memory_bytes"`
	SessionChangesetBytes uint64 `json:"session_changeset_bytes"`
}
type SpoolMetrics struct {
	Keys               uint64  `json:"keys"`
	Segments           uint64  `json:"segments"`
	DiskBytes          uint64  `json:"disk_bytes"`
	LiveBytes          uint64  `json:"live_bytes"`
	LiveRecords        uint64  `json:"live_records"`
	DeadRecords        uint64  `json:"dead_records"`
	PendingRecords     uint64  `json:"pending_records"`
	PendingBytes       uint64  `json:"pending_bytes"`
	BlocksWritten      uint64  `json:"blocks_written"`
	BytesWritten       uint64  `json:"bytes_written"`
	CompressionRatio   float64 `json:"compression_ratio"`
	Compactions        uint64  `json:"compactions"`
	CompactionBytes    uint64  `json:"compaction_bytes"`
	Flushes            uint64  `json:"flushes"`
	FlushErrors        uint64  `json:"flush_errors"`
	Reclaims           uint64  `json:"reclaims"`
	ReclaimErrors      uint64  `json:"reclaim_errors"`
	Commits            uint64  `json:"commits"`
	Syncs              uint64  `json:"syncs"`
	CommitNanos        uint64  `json:"commit_nanos"`
	SyncNanos          uint64  `json:"sync_nanos"`
	StorageFailures    uint64  `json:"storage_failures"`
	IndexBytesEstimate uint64  `json:"index_bytes_estimate"`
}
type Status struct {
	SearchIndexes   []SearchIndexStatus `json:"search_indexes"`
	Committed       uint64              `json:"committed"`
	Durable         uint64              `json:"durable"`
	PendingBytes    uint64              `json:"pending_bytes"`
	StorageError    *string             `json:"storage_error"`
	CheckpointPause time.Duration       `json:"checkpoint_pause"`
	Closed          bool                `json:"closed"`
	OpenProgress    *OpenProgress       `json:"open_progress"`
	SQLite          *SqliteMetrics      `json:"sqlite"`
	Spool           *SpoolMetrics       `json:"spool"`
	Scheduler       *SchedulerSnapshot  `json:"scheduler"`
	Replication     *ReplicationStatus  `json:"replication"`
}
type SearchIndexStatus struct {
	Name        string  `json:"name"`
	Ready       bool    `json:"ready"`
	Rebuilding  bool    `json:"rebuilding"`
	PendingKeys uint64  `json:"pending_keys"`
	QueuedBytes uint64  `json:"queued_bytes"`
	Error       *string `json:"error"`
}
type SchedulerClassStats struct {
	Acquisitions     uint64 `json:"acquisitions"`
	Cancels          uint64 `json:"cancels"`
	WaitNanos        uint64 `json:"wait_nanos"`
	ServiceNanos     uint64 `json:"service_nanos"`
	DualServiceNanos uint64 `json:"dual_service_nanos"`
	Waiters          uint64 `json:"waiters"`
	OldestWaitNanos  uint64 `json:"oldest_wait_nanos"`
}
type SchedulerSnapshot struct {
	Local       SchedulerClassStats `json:"local"`
	Remote      SchedulerClassStats `json:"remote"`
	Maintenance SchedulerClassStats `json:"maintenance"`
	DebtNanos   uint64              `json:"debt_nanos"`
	Shares      [2]uint32           `json:"shares"`
}
type ReplicationMetrics struct {
	PushedTransactions   uint64 `json:"pushed_transactions"`
	ReceivedTransactions uint64 `json:"received_transactions"`
	AppliedTransactions  uint64 `json:"applied_transactions"`
	StagedTransactions   uint64 `json:"staged_transactions"`
	GossipRounds         uint64 `json:"gossip_rounds"`
	ProbesSent           uint64 `json:"probes_sent"`
	ProbesFailed         uint64 `json:"probes_failed"`
	AntiEntropyRuns      uint64 `json:"anti_entropy_runs"`
	SchemaPushes         uint64 `json:"schema_pushes"`
	SchemaAdoptions      uint64 `json:"schema_adoptions"`
	RetentionRuns        uint64 `json:"retention_runs"`
	RetentionPruned      uint64 `json:"retention_pruned"`
	QueueAvailableBytes  uint64 `json:"queue_available_bytes"`
	ChunkNeedSent        uint64 `json:"chunk_need_sent"`
	ChunkNeedServed      uint64 `json:"chunk_need_served"`
}
type PeerDiagnostics struct {
	NodeID            NodeID  `json:"node_id"`
	Address           *string `json:"address"`
	Connected         bool    `json:"connected"`
	LastSeenMillisAgo *uint64 `json:"last_seen_millis_ago"`
	SentWatermark     uint64  `json:"sent_watermark"`
	SchemaAgreed      bool    `json:"schema_agreed"`
	PushMode          bool    `json:"push_mode"`
}
type ReplicationStatus struct {
	Running             bool               `json:"running"`
	Address             *string            `json:"address"`
	Peers               uint64             `json:"peers"`
	Members             uint64             `json:"members"`
	StagedTransactions  uint64             `json:"staged_transactions"`
	AppliedTransactions uint64             `json:"applied_transactions"`
	LastError           *string            `json:"last_error"`
	Deferred            bool               `json:"deferred"`
	FileEndpoint        *string            `json:"file_endpoint"`
	Metrics             ReplicationMetrics `json:"metrics"`
	PeersDiagnostics    []PeerDiagnostics  `json:"peers_diagnostics"`
}
type OriginTransaction struct {
	Database      NodeID `json:"database"`
	Origin        NodeID `json:"origin"`
	Sequence      uint64 `json:"sequence"`
	ID            NodeID `json:"id"`
	Millis        uint64 `json:"millis"`
	Logical       uint32 `json:"logical"`
	Schema        Key    `json:"schema"`
	PayloadDigest Key    `json:"payload_digest"`
	Payload       Bytes  `json:"payload"`
	Signature     Bytes  `json:"signature"`
}
type SignedSchema struct {
	Database  NodeID `json:"database"`
	Signer    NodeID `json:"signer"`
	Sequence  uint64 `json:"sequence"`
	Schema    Bytes  `json:"schema"`
	Signature Bytes  `json:"signature"`
}
type Chunk struct {
	Proof    OriginTransaction `json:"proof"`
	Origin   NodeID            `json:"origin"`
	Sequence uint64            `json:"sequence"`
	ID       NodeID            `json:"id"`
	Index    uint32            `json:"index"`
	Count    uint32            `json:"count"`
	Total    uint64            `json:"total"`
	Digest   Key               `json:"digest"`
	Data     Bytes             `json:"data"`
}
type ChunkManifest struct {
	ID     NodeID `json:"id"`
	Count  uint32 `json:"count"`
	Total  uint64 `json:"total"`
	Digest Key    `json:"digest"`
}
type PeerProgress struct {
	Origin          NodeID `json:"origin"`
	Applied         uint64 `json:"applied"`
	Observed        uint64 `json:"observed"`
	RetainedFrom    uint64 `json:"retained_from"`
	RetainedThrough uint64 `json:"retained_through"`
}
type SnapshotManifest struct {
	Database  NodeID         `json:"database"`
	Donor     NodeID         `json:"donor"`
	Recipient NodeID         `json:"recipient"`
	Transfer  NodeID         `json:"transfer"`
	Expires   uint64         `json:"expires"`
	Bytes     uint64         `json:"bytes"`
	Digest    Key            `json:"digest"`
	Schema    Key            `json:"schema"`
	Progress  []PeerProgress `json:"progress"`
	Chunks    []Key          `json:"chunks"`
	Signature Bytes          `json:"signature"`
}
type SnapshotIncoming struct {
	Manifest SnapshotManifest `json:"manifest"`
	Deadline uint64           `json:"deadline"`
}
type BridgeOrigin struct {
	Node        NodeID `json:"node"`
	Sequence    uint64 `json:"sequence"`
	Transaction NodeID `json:"transaction"`
	Digest      Key    `json:"digest"`
}
type BridgeHeader struct {
	Origin         *BridgeOrigin `json:"origin"`
	SourceDatabase NodeID        `json:"source_database"`
	SourceNode     NodeID        `json:"source_node"`
	Stream         string        `json:"stream"`
	Sequence       uint64        `json:"sequence"`
	Bundle         NodeID        `json:"bundle"`
	Recipient      Key           `json:"recipient"`
	Ephemeral      Key           `json:"ephemeral"`
	Nonce          [24]byte      `json:"nonce"`
	Digest         Key           `json:"digest"`
}
type BridgeBundle struct {
	Version    uint16       `json:"version"`
	Header     BridgeHeader `json:"header"`
	Ciphertext Bytes        `json:"ciphertext"`
	Signature  Bytes        `json:"signature"`
}
type FileMetadata struct {
	Name       string `json:"name"`
	Digest     Key    `json:"digest"`
	Bytes      uint64 `json:"bytes"`
	Generation uint64 `json:"generation"`
	Millis     uint64 `json:"millis"`
	Node       NodeID `json:"node"`
	Deleted    bool   `json:"deleted"`
}
type FileKeyRotationStatus struct {
	CurrentGeneration                   uint64 `json:"current_generation"`
	OldGenerationObjects                uint64 `json:"old_generation_objects"`
	OldGenerationMetadataMissingLocally uint64 `json:"old_generation_metadata_missing_locally"`
	StagedTransfers                     uint64 `json:"staged_transfers"`
	PreviousKeyUnusedLocally            bool   `json:"previous_key_unused_locally"`
}
type DataKeyInfo struct {
	ID        uint32    `json:"id"`
	CreatedAt uint64    `json:"created_at"`
	Status    KeyStatus `json:"status"`
}
type KeyStatus string

const (
	ActiveKey  KeyStatus = "Active"
	RetiredKey KeyStatus = "Retired"
)

type KeyInventory struct {
	Encrypted          bool          `json:"encrypted"`
	WrappingKeyID      string        `json:"wrapping_key_id"`
	ActiveDataKeyID    uint32        `json:"active_data_key_id"`
	DataKeys           []DataKeyInfo `json:"data_keys"`
	KeyringSeq         uint64        `json:"keyring_seq"`
	ManifestGeneration uint64        `json:"manifest_generation"`
	MaintenancePhase   string        `json:"maintenance_phase"`
	MaintenanceDone    uint64        `json:"maintenance_done"`
	MaintenanceTotal   uint64        `json:"maintenance_total"`
}
type PeerRetention struct {
	Node           NodeID         `json:"node"`
	LastSeenMillis uint64         `json:"last_seen_millis"`
	Retired        bool           `json:"retired"`
	Acknowledged   []NodeSequence `json:"acknowledged"`
}
type NodeSequence struct {
	Node     NodeID
	Sequence uint64
}
type RetentionStatus struct {
	Peers                 []PeerRetention `json:"peers"`
	CollectedTransactions uint64          `json:"collected_transactions"`
	LastCollectionMillis  uint64          `json:"last_collection_millis"`
}
