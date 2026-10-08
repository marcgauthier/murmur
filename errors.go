package murmur

import (
	"errors"
	"fmt"

	"github.com/marcgauthier/murmur/ids"
)

// Typed errors. Wrap underlying errors with context while preserving
// errors.Is / errors.As.
var (
	// ErrClosed is returned when operating on a closed DB.
	ErrClosed = errors.New("murmur: closed")
	// ErrNotReady is returned when the DB has not finished opening/rebuilding.
	ErrNotReady = errors.New("murmur: not ready")
	// ErrMaterializerDirty indicates the query database diverged from durable
	// state and is being rebuilt; the caller should retry.
	ErrMaterializerDirty = errors.New("murmur: materializer dirty, retry")
	// ErrSchemaMismatch indicates an incompatible schema epoch or hash.
	ErrSchemaMismatch = errors.New("murmur: schema mismatch")
	// ErrProtocolMismatch indicates an incompatible replication protocol.
	ErrProtocolMismatch = errors.New("murmur: protocol mismatch")
	// ErrPeerNotAllowed indicates the peer failed authentication/authorization.
	ErrPeerNotAllowed = errors.New("murmur: peer not allowed")
	// ErrPeerExcluded indicates the peer is locally retired / persistently excluded.
	ErrPeerExcluded = errors.New("murmur: peer is locally excluded")
	// ErrPeerNotFound indicates the requested peer is unknown.
	ErrPeerNotFound = errors.New("murmur: peer not found")
	// ErrReplicationGap indicates a missing sequence range in an origin log.
	ErrReplicationGap = errors.New("murmur: replication gap")
	// ErrSnapshotRequired indicates the peer must resync from a snapshot.
	ErrSnapshotRequired = errors.New("murmur: snapshot required")
	// ErrEncryptionKey indicates a missing or invalid storage key.
	ErrEncryptionKey = errors.New("murmur: encryption key error")
	// ErrValueTooLarge indicates a value exceeding MaxReplicatedValueBytes.
	ErrValueTooLarge = errors.New("murmur: value too large")
	// ErrUnsupportedSchema indicates a schema the package cannot replicate.
	ErrUnsupportedSchema = errors.New("murmur: unsupported schema")
	// ErrAmbiguousCommit indicates durability succeeded but acknowledgement
	// was lost; retry with the same TxID is safe (idempotent).
	ErrAmbiguousCommit = errors.New("murmur: ambiguous commit")
	// ErrCommitOutcomeUncertain indicates a durable write may or may not have
	// reached storage. Inspect CommitOutcomeUncertainError.TxID and resolve it
	// with HasTransactionReceipt after reopening the database.
	ErrCommitOutcomeUncertain = errors.New("murmur: commit outcome uncertain")
	// ErrBatchTooLarge indicates a transaction that does not fit in one
	// durable commit; split it into smaller transactions.
	ErrBatchTooLarge = errors.New("murmur: batch too large")
	// ErrTransactionTooLarge indicates a transaction exceeding MaxTransactionBytes.
	ErrTransactionTooLarge = errors.New("murmur: transaction too large")
	// ErrTxDone is returned when using a committed or rolled back Tx.
	ErrTxDone = errors.New("murmur: transaction done")
	// ErrMaintenance is returned for durable writes during maintenance
	// (explicit file rewrite). Reads against the in-memory materialization
	// stay available; retry writes after maintenance completes.
	ErrMaintenance = errors.New("murmur: maintenance in progress")
	// ErrUnsupportedStorageFormat indicates a data directory created by an
	// incompatible store (e.g. the legacy Badger layout). It is reported
	// before any file is created or modified; migrate explicitly.
	ErrUnsupportedStorageFormat = errors.New("murmur: unsupported storage format")
	// ErrSubscriptionReset indicates the subscription was reset due to slow consumer or rebuilt materializer.
	ErrSubscriptionReset = errors.New("murmur: subscription reset, resnapshot required")
	// ErrSubscriptionExpired indicates the requested resume cursor is no longer available in retained history.
	ErrSubscriptionExpired = errors.New("murmur: subscription cursor expired")
	// ErrMaxSubscribersReached is returned when the maximum number of active subscriptions has been reached.
	ErrMaxSubscribersReached = errors.New("murmur: max subscribers limit reached")
	// ErrReadOnlyRequired is returned when subscribing to a non-SELECT / mutating query.
	ErrReadOnlyRequired = errors.New("murmur: subscription query must be read-only")
	// ErrLocalIdentityMismatch indicates the replication certificate's
	// NodeID does not match the configured NodeID.
	ErrLocalIdentityMismatch = errors.New("murmur: replication certificate does not match configured NodeID")
	// ErrRestoreIdentity indicates restored data opened without its
	// required fresh writer identity (same-identity rollback is rejected).
	ErrRestoreIdentity = errors.New("murmur: restored data requires its fresh writer NodeID")
	// ErrConnectionLimitReached indicates the bounded QUIC connection pool limit has been reached.
	ErrConnectionLimitReached = errors.New("murmur: connection limit reached")
	// ErrSessionLimitReached indicates the bounded replication session limit has been reached.
	ErrSessionLimitReached = errors.New("murmur: replication session limit reached")
	// ErrFilesDisabled is returned by the file APIs when Files.Enabled is false.
	ErrFilesDisabled = errors.New("murmur: file storage is not enabled")
	// ErrFileUnavailable indicates the named file has no readable bytes on
	// this node: either no visible metadata exists, or the metadata is
	// known but the content-addressed object was never fetched here.
	ErrFileUnavailable = errors.New("murmur: file unavailable")
	// ErrFileTooLarge indicates an upload exceeding Files.MaxFileBytes.
	ErrFileTooLarge = errors.New("murmur: file too large")
)

// CommitOutcomeUncertainError preserves the transaction identity when a
// storage failure makes a commit's outcome ambiguous. After reopening, use
// DB.HasTransactionReceipt(TxID) to determine whether the transaction landed.
type CommitOutcomeUncertainError struct {
	TxID  ids.TxID
	Cause error
}

func (e *CommitOutcomeUncertainError) Error() string {
	if e == nil {
		return ErrCommitOutcomeUncertain.Error()
	}
	return fmt.Sprintf("%v (transaction %x): %v", ErrCommitOutcomeUncertain, e.TxID, e.Cause)
}

func (e *CommitOutcomeUncertainError) Unwrap() []error {
	if e == nil {
		return []error{ErrCommitOutcomeUncertain}
	}
	return []error{ErrCommitOutcomeUncertain, e.Cause}
}
