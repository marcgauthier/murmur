package rime

import (
	"errors"
	"fmt"
)

// BatchError identifies the input that made an atomic batch fail.
type BatchError struct {
	Operation string
	Index     int
	Err       error
}

func (e *BatchError) Error() string {
	return "rime: batch " + e.Operation + " item " + fmt.Sprint(e.Index) + ": " + e.Err.Error()
}
func (e *BatchError) Unwrap() error { return e.Err }

// Sentinel errors returned by RIME operations. Use errors.Is to match them.
var (
	// ErrBadView indicates an invalid name, query, builder, or source list.
	ErrBadView = errors.New("rime: invalid view definition")
	// ErrViewExists indicates a duplicate database-local view name.
	ErrViewExists = errors.New("rime: view already registered")
	// ErrViewClosed indicates an operation on an explicitly closed view.
	ErrViewClosed = errors.New("rime: view closed")
	// ErrViewDependency indicates a read of an undeclared source table.
	ErrViewDependency = errors.New("rime: undeclared view source")
	// ErrConflict is returned when an optimistic write transaction detects
	// that a record it read was concurrently modified.
	ErrConflict = errors.New("rime: write conflict")
	// ErrNotFound is returned when a record does not exist at the
	// transaction snapshot.
	ErrNotFound = errors.New("rime: record not found")
	// ErrAlreadyExists is returned by Insert when the primary key already
	// exists at the transaction snapshot.
	ErrAlreadyExists = errors.New("rime: record already exists")
	// ErrUnique is returned when a unique constraint is violated.
	ErrUnique = errors.New("rime: unique constraint violated")
	// ErrNotNull is returned when a NOT NULL constraint is violated.
	ErrNotNull = errors.New("rime: not null constraint violated")
	// ErrCheck is returned when a CHECK constraint is violated.
	ErrCheck = errors.New("rime: check constraint violated")
	// ErrForeignKey is returned when a foreign-key constraint is violated.
	ErrForeignKey = errors.New("rime: foreign key constraint violated")
	// ErrTxClosed is returned when operating on a closed transaction.
	ErrTxClosed = errors.New("rime: transaction closed")
	// ErrTxPrepared is returned when a staged transaction is locked by a
	// managed publication token.
	ErrTxPrepared = errors.New("rime: transaction has a prepared publication")
	// ErrManagedCommitRequired means PrepareCommit requires BeginTx.
	ErrManagedCommitRequired = errors.New("rime: managed publication requires BeginTx")
	// ErrTxReadOnly is returned when attempting a write on a read transaction.
	ErrTxReadOnly = errors.New("rime: transaction is read-only")
	// ErrLimitExceeded is returned when a configured safety limit is hit.
	ErrLimitExceeded = errors.New("rime: safety limit exceeded")
	// ErrTableExists is returned when registering a duplicate table name.
	ErrTableExists = errors.New("rime: table already registered")
	// ErrTableNotFound is returned when referencing an unknown table.
	ErrTableNotFound = errors.New("rime: table not found")
	// ErrNoPrimaryKey is returned when a registered struct has no primary key.
	ErrNoPrimaryKey = errors.New("rime: no primary key field")
	// ErrBadSchema is returned when a struct cannot be registered.
	ErrBadSchema = errors.New("rime: invalid schema")
	// ErrHookRejected is returned when a Before hook rejects an operation.
	// Hooks return it (or any other error) to abort; it propagates unwrapped.
	ErrHookRejected = errors.New("rime: hook rejected operation")
	// ErrTxDatabase is returned when a transaction from one database is used
	// with a table (or query) belonging to another.
	ErrTxDatabase = errors.New("rime: transaction belongs to another database")
	// ErrSnapshotUnavailable is returned when a read targets a snapshot whose
	// history was reclaimed by GC. The request fails explicitly instead of
	// silently serving incomplete data.
	ErrSnapshotUnavailable = errors.New("rime: snapshot history unavailable")
	// ErrDBClosed is returned when operating on a closed database.
	ErrDBClosed = errors.New("rime: database closed")
	// ErrDBFaulted is returned after managed publication panics following a
	// durable commit. Reopen the database from its durable source of truth.
	ErrDBFaulted = errors.New("rime: database faulted during publication")
)

// UncertainCommitError reports a managed publication that failed after its
// durable commit: the durable write may become visible after recovery while
// the in-memory publication is only partial. Commit is the RIME visibility
// generation the token was publishing; Cause is the recovered panic value.
// It matches ErrDBFaulted with errors.Is; use errors.As to read the commit.
type UncertainCommitError struct {
	Commit TxID
	Cause  any
}

func (e *UncertainCommitError) Error() string {
	return fmt.Sprintf("rime: uncertain commit outcome at generation %d: %v", uint64(e.Commit), e.Cause)
}

func (e *UncertainCommitError) Unwrap() error { return ErrDBFaulted }

// ConstraintError decorates a constraint failure with table/field context.
type ConstraintError struct {
	Table string
	Field string
	Value any
	Err   error
}

func (e *ConstraintError) Error() string {
	if e.Field != "" {
		return "rime: table " + e.Table + " field " + e.Field + ": " + e.Err.Error()
	}
	return "rime: table " + e.Table + ": " + e.Err.Error()
}

func (e *ConstraintError) Unwrap() error { return e.Err }
