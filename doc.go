// Package murmur is an embedded database with encrypted durable state and
// masterless replication over QUIC. There is no standalone server. Applications
// define managed Go record tables with Config.Tables; Murmur commits typed
// changes to Spool before publishing them through RIME. SQL-only Schema.Tables
// configurations are no longer accepted by Open.
//
//	Application -> Murmur facade -> RIME / commit coordinator -> Spool -> QUIC peers
package murmur

import "github.com/marcgauthier/murmur/ids"

// Identity aliases so the public API reads naturally (murmur.NodeID)
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
