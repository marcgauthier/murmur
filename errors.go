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
	// ErrPeerExcluded indicates the peer is locally retired / persistently excluded.
	ErrPeerExcluded = errors.New("replicateddb: peer is locally excluded")
	// ErrPeerNotFound indicates the requested peer is unknown.
	ErrPeerNotFound = errors.New("replicateddb: peer not found")
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
	// ErrTransactionTooLarge indicates a transaction exceeding MaxTransactionBytes.
	ErrTransactionTooLarge = errors.New("replicateddb: transaction too large")
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
	// ErrSubscriptionClosed is returned when operating on a closed subscription.
	ErrSubscriptionClosed = errors.New("replicateddb: subscription closed")
	// ErrSubscriptionReset indicates the subscription was reset due to slow consumer or rebuilt materializer.
	ErrSubscriptionReset = errors.New("replicateddb: subscription reset, resnapshot required")
	// ErrSubscriptionExpired indicates the requested resume cursor is no longer available in retained history.
	ErrSubscriptionExpired = errors.New("replicateddb: subscription cursor expired")
	// ErrMaxSubscribersReached is returned when the maximum number of active subscriptions has been reached.
	ErrMaxSubscribersReached = errors.New("replicateddb: max subscribers limit reached")
	// ErrReadOnlyRequired is returned when subscribing to a non-SELECT / mutating query.
	ErrReadOnlyRequired = errors.New("replicateddb: subscription query must be read-only")
	// ErrLocalIdentityMismatch indicates the replication certificate's
	// NodeID does not match the configured NodeID.
	ErrLocalIdentityMismatch = errors.New("replicateddb: replication certificate does not match configured NodeID")
	// ErrRestoreIdentity indicates restored data opened without its
	// required fresh writer identity (same-identity rollback is rejected).
	ErrRestoreIdentity = errors.New("replicateddb: restored data requires its fresh writer NodeID")
	// ErrConnectionLimitReached indicates the bounded QUIC connection pool limit has been reached.
	ErrConnectionLimitReached = errors.New("replicateddb: connection limit reached")
	// ErrSessionLimitReached indicates the bounded replication session limit has been reached.
	ErrSessionLimitReached = errors.New("replicateddb: replication session limit reached")
	// ErrFilesDisabled is returned by the file APIs when Files.Enabled is false.
	ErrFilesDisabled = errors.New("replicateddb: file storage is not enabled")
	// ErrFileUnavailable indicates the named file has no readable bytes on
	// this node: either no visible metadata exists, or the metadata is
	// known but the content-addressed object was never fetched here.
	ErrFileUnavailable = errors.New("replicateddb: file unavailable")
	// ErrFileTooLarge indicates an upload exceeding Files.MaxFileBytes.
	ErrFileTooLarge = errors.New("replicateddb: file too large")
)
