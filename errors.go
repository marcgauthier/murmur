package replicateddb

import "errors"

// Typed errors. Wrap underlying errors with context while preserving
// errors.Is / errors.As.
var (
	// ErrClosed is returned when operating on a closed DB.
	ErrClosed = errors.New("replicateddb: closed")
	// ErrNotReady is returned when the DB has not finished opening/rebuilding.
	ErrNotReady = errors.New("replicateddb: not ready")
	// ErrMaterializerDirty indicates the query database diverged from durable
	// state and is being rebuilt; the caller should retry.
	ErrMaterializerDirty = errors.New("replicateddb: materializer dirty, retry")
	// ErrSchemaMismatch indicates an incompatible schema epoch or hash.
	ErrSchemaMismatch = errors.New("replicateddb: schema mismatch")
	// ErrProtocolMismatch indicates an incompatible replication protocol.
	ErrProtocolMismatch = errors.New("replicateddb: protocol mismatch")
	// ErrPeerNotAllowed indicates the peer failed authentication/authorization.
	ErrPeerNotAllowed = errors.New("replicateddb: peer not allowed")
	// ErrReplicationGap indicates a missing sequence range in an origin log.
	ErrReplicationGap = errors.New("replicateddb: replication gap")
	// ErrSnapshotRequired indicates the peer must resync from a snapshot.
	ErrSnapshotRequired = errors.New("replicateddb: snapshot required")
	// ErrEncryptionKey indicates a missing or invalid storage key.
	ErrEncryptionKey = errors.New("replicateddb: encryption key error")
	// ErrValueTooLarge indicates a value exceeding MaxReplicatedValueBytes.
	ErrValueTooLarge = errors.New("replicateddb: value too large")
	// ErrUnsupportedSchema indicates a schema the package cannot replicate.
	ErrUnsupportedSchema = errors.New("replicateddb: unsupported schema")
	// ErrAmbiguousCommit indicates durability succeeded but acknowledgement
	// was lost; retry with the same TxID is safe (idempotent).
	ErrAmbiguousCommit = errors.New("replicateddb: ambiguous commit")
	// ErrBatchTooLarge indicates a transaction that does not fit in one
	// durable commit; split it into smaller transactions.
	ErrBatchTooLarge = errors.New("replicateddb: batch too large")
	// ErrTxDone is returned when using a committed or rolled back Tx.
	ErrTxDone = errors.New("replicateddb: transaction done")
	// ErrMaintenance is returned for durable writes during maintenance
	// (explicit file rewrite). Reads against the in-memory materialization
	// stay available; retry writes after maintenance completes.
	ErrMaintenance = errors.New("replicateddb: maintenance in progress")
	// ErrUnsupportedStorageFormat indicates a data directory created by an
	// incompatible store (e.g. the legacy Badger layout). It is reported
	// before any file is created or modified; migrate explicitly.
	ErrUnsupportedStorageFormat = errors.New("replicateddb: unsupported storage format")
)
