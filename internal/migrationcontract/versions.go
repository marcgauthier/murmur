// Package migrationcontract records the on-disk and wire versions selected
// and implemented for the RIME cutover.
package migrationcontract

const (
	// StoreFormatVersion is the fresh Spool/state format for RIME records.
	StoreFormatVersion uint64 = 6
	// ReplicationProtocolVersion rejects peers that only understand SQL cells.
	ReplicationProtocolVersion uint16 = 6
	// MutationCodecVersion adds canonical rich-record field payloads.
	MutationCodecVersion uint16 = 4
	// SchemaManifestEncodingVersion adds recursive Go field descriptors.
	SchemaManifestEncodingVersion uint16 = 3
	// SnapshotManifestVersion binds snapshots to the rich-record schema format.
	SnapshotManifestVersion uint16 = 3
	// RecordValueEncodingVersion versions canonical individual field values.
	RecordValueEncodingVersion uint16 = 1
)
