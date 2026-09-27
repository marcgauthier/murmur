// Package replicateddb is an embedded, replicated, in-memory SQL engine.
//
// Applications link it directly; there is no standalone server. The query
// database (SQLite-compatible, memory resident) is a rebuildable
// materialization of the authoritative durable state in Pebble.
// Masterless multi-writer replication runs over QUIC with per-column
// last-writer-wins conflict resolution driven by a hybrid logical clock.
//
// Architecture:
//
//	Application SQL -> query engine -> TxDelta -> CRDT merge -> Pebble -> QUIC peers
package replicateddb

import "github.com/nomadsql/replicateddb/ids"

// Identity aliases so the public API reads naturally (replicateddb.NodeID)
// while subpackages share one definition without import cycles.
type (
	// NodeID identifies a cluster node.
	NodeID = ids.NodeID
	// DBID identifies a database/cluster.
	DBID = ids.DBID
	// TxID identifies one committed transaction.
	TxID = ids.TxID
	// RowID is the primary key of a replicated row.
	RowID = ids.RowID
)

// Re-exported constructors.
var (
	NewNodeID   = ids.NewNodeID
	NewDBID     = ids.NewDBID
	NewTxID     = ids.NewTxID
	NewRowID    = ids.NewRowID
	MustNodeID  = ids.MustNodeID
	ParseNodeID = ids.ParseNodeID
	ParseDBID   = ids.ParseDBID
)
