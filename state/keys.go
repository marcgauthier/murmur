// Package state implements the durable source of truth on Pebble.
//
// It stores current winning cell state, row tombstones, per-origin
// replication logs, watermarks, receipts, and system metadata. The SQL
// materialization can always be rebuilt from this state alone.
package state

import (
	"encoding/binary"

	"github.com/nomadsql/replicateddb/ids"
)

// Key prefixes (PLAN section 14).
const (
	prefixCell     byte = 0x01
	prefixTomb     byte = 0x02
	prefixLog      byte = 0x03
	prefixRecv     byte = 0x04
	prefixPeerAck  byte = 0x05
	prefixSchema   byte = 0x06
	prefixSys      byte = 0x07
	prefixReceipt  byte = 0x08
	prefixSnapshot byte = 0x09
)

const (
	sysLocalNode   = "local_node_id"
	sysLocalSeq    = "local_sequence"
	sysHLC         = "hlc"
	sysFormat      = "format_version"
	sysSchemaEpoch = "schema_epoch"
	sysSchemaHash  = "schema_hash"
	sysGeneration  = "state_generation"
	sysMaterial    = "materialized_generation"
	sysDBID        = "db_id"
)

// FormatVersion is the persistent Pebble format version. It is incompatible
// with the old Badger store (which used 1); old directories are rejected.
const FormatVersion uint64 = 2

// CellKey builds 01 | tableID:u32 | rowUUID:16 | columnID:u32.
func CellKey(tableID uint32, row ids.RowID, col uint32) []byte {
	k := make([]byte, 0, 25)
	k = append(k, prefixCell)
	k = binary.BigEndian.AppendUint32(k, tableID)
	k = append(k, row[:]...)
	return binary.BigEndian.AppendUint32(k, col)
}

// CellTablePrefix is the scan prefix for one table's cells.
func CellTablePrefix(tableID uint32) []byte {
	k := []byte{prefixCell}
	return binary.BigEndian.AppendUint32(k, tableID)
}

// CellRowPrefix is the scan prefix for one row's cells.
func CellRowPrefix(tableID uint32, row ids.RowID) []byte {
	k := []byte{prefixCell}
	k = binary.BigEndian.AppendUint32(k, tableID)
	return append(k, row[:]...)
}

// ParseCellKey splits a cell key.
func ParseCellKey(k []byte) (tableID uint32, row ids.RowID, col uint32, ok bool) {
	if len(k) != 25 || k[0] != prefixCell {
		return 0, ids.RowID{}, 0, false
	}
	tableID = binary.BigEndian.Uint32(k[1:5])
	copy(row[:], k[5:21])
	col = binary.BigEndian.Uint32(k[21:25])
	return tableID, row, col, true
}

// TombKey builds 02 | tableID:u32 | rowUUID:16.
func TombKey(tableID uint32, row ids.RowID) []byte {
	k := make([]byte, 0, 21)
	k = append(k, prefixTomb)
	k = binary.BigEndian.AppendUint32(k, tableID)
	return append(k, row[:]...)
}

// ParseTombKey splits a tombstone key.
func ParseTombKey(k []byte) (uint32, ids.RowID, bool) {
	if len(k) != 21 || k[0] != prefixTomb {
		return 0, ids.RowID{}, false
	}
	var row ids.RowID
	copy(row[:], k[5:21])
	return binary.BigEndian.Uint32(k[1:5]), row, true
}

// LogKey builds 03 | originNode:16 | sequence:u64.
func LogKey(origin ids.NodeID, seq uint64) []byte {
	k := make([]byte, 0, 25)
	k = append(k, prefixLog)
	k = append(k, origin[:]...)
	return binary.BigEndian.AppendUint64(k, seq)
}

// LogOriginPrefix is the scan prefix for one origin's log.
func LogOriginPrefix(origin ids.NodeID) []byte {
	k := []byte{prefixLog}
	return append(k, origin[:]...)
}

// ParseLogKey splits a log key.
func ParseLogKey(k []byte) (ids.NodeID, uint64, bool) {
	if len(k) != 25 || k[0] != prefixLog {
		return ids.NodeID{}, 0, false
	}
	var origin ids.NodeID
	copy(origin[:], k[1:17])
	return origin, binary.BigEndian.Uint64(k[17:25]), true
}

// RecvKey builds 04 | originNode:16.
func RecvKey(origin ids.NodeID) []byte {
	return append([]byte{prefixRecv}, origin[:]...)
}

// PeerAckKey builds 05 | peerNode:16 | originNode:16.
func PeerAckKey(peer, origin ids.NodeID) []byte {
	k := make([]byte, 0, 33)
	k = append(k, prefixPeerAck)
	k = append(k, peer[:]...)
	return append(k, origin[:]...)
}

// PeerAckPeerPrefix scans all origin acks of one peer.
func PeerAckPeerPrefix(peer ids.NodeID) []byte {
	return append([]byte{prefixPeerAck}, peer[:]...)
}

// SchemaKey builds 06 | name.
func SchemaKey(name string) []byte {
	k := []byte{prefixSchema}
	return append(k, name...)
}

// SysKey builds 07 | name.
func SysKey(name string) []byte {
	k := []byte{prefixSys}
	return append(k, name...)
}

// ReceiptKey builds 08 | TxID:16.
func ReceiptKey(tx ids.TxID) []byte {
	return append([]byte{prefixReceipt}, tx[:]...)
}

// SnapshotKey builds 09 | name.
func SnapshotKey(name string) []byte {
	k := []byte{prefixSnapshot}
	return append(k, name...)
}

func encodeU64(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

func decodeU64(b []byte) (uint64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(b), true
}
