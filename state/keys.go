// Package state implements the durable source of truth on Spool.
//
// It stores current winning cell state, row tombstones, per-origin
// replication logs, watermarks, receipts, and system metadata. The SQL
// materialization can always be rebuilt from this state alone.
package state

import (
	"encoding/binary"

	"github.com/marcgauthier/murmur/ids"
)

// Key prefixes (PLAN section 14).
const (
	prefixCell           byte = 0x01
	prefixTomb           byte = 0x02
	prefixLog            byte = 0x03
	prefixRecv           byte = 0x04
	prefixPeerAck        byte = 0x05
	prefixSchema         byte = 0x06
	prefixSys            byte = 0x07
	prefixReceipt        byte = 0x08
	prefixSnapshot       byte = 0x09
	prefixPeerExcluded   byte = 0x0a
	prefixMember         byte = 0x0b
	prefixBridgeProgress byte = 0x0c
	prefixTxnStage       byte = 0x0d
	prefixLocalCell      byte = 0x10
	prefixLocalReceipt   byte = 0x11
)

const (
	sysLocalNode   = "local_node_id"
	sysLocalSeq    = "local_sequence"
	sysHLC         = "hlc"
	sysFormat      = "format_version"
	sysMinReader   = "minimum_reader_version"
	sysMinWriter   = "minimum_writer_version"
	sysSchemaEpoch = "schema_epoch"
	sysSchemaHash  = "schema_hash"
	sysGeneration  = "state_generation"
	// sysRemotePrepare holds one complete remote transaction whose prepare
	// record is durable but whose final atomic apply has not committed yet.
	sysRemotePrepare = "remote_prepare"
	sysDBID          = "db_id"
	// sysRestoreMarker records the last restore adoption (JSON): the backup
	// and the retired source identity. It is written atomically with the
	// fresh identity swap so old origin sequences can never be reused.
	sysRestoreMarker = "restore_marker"
)

// FormatVersion marks the RIME record cutover. Format-5 SQL-era stores are
// deliberately rejected; applications must start a fresh directory.
const FormatVersion uint64 = 6

// MinFormatVersion is the oldest persistent format this binary still
// recognizes for explicit offline migration. Ordinary Open requires signed
// format 4; v1 (Badger) and future formats always fail closed.
const MinFormatVersion uint64 = 2

// MinReaderVersion and MinWriterVersion are the newest store minimum
// versions this binary satisfies (its own version): a store demanding
// a newer reader or writer needs a newer binary. Fresh stores record
// all three markers at FormatVersion; pre-marker stores default
// missing minima to their own stored format version on open.
const (
	MinReaderVersion uint64 = 6
	MinWriterVersion uint64 = 6
)

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

// LocalCellKey builds the durable node-local cell key. This namespace is
// intentionally distinct from replicated cells and is omitted from snapshots.
func LocalCellKey(tableID uint32, row ids.RowID, col uint32) []byte {
	k := make([]byte, 0, 25)
	k = append(k, prefixLocalCell)
	k = binary.BigEndian.AppendUint32(k, tableID)
	k = append(k, row[:]...)
	return binary.BigEndian.AppendUint32(k, col)
}

// LocalCellTablePrefix is the scan prefix for one node-local table.
func LocalCellTablePrefix(tableID uint32) []byte {
	k := []byte{prefixLocalCell}
	return binary.BigEndian.AppendUint32(k, tableID)
}

// ParseLocalCellKey splits a node-local cell key.
func ParseLocalCellKey(k []byte) (tableID uint32, row ids.RowID, col uint32, ok bool) {
	if len(k) != 25 || k[0] != prefixLocalCell {
		return 0, ids.RowID{}, 0, false
	}
	tableID = binary.BigEndian.Uint32(k[1:5])
	copy(row[:], k[5:21])
	col = binary.BigEndian.Uint32(k[21:25])
	return tableID, row, col, true
}

// LocalReceiptKey builds the receipt key for a node-local-only transaction.
func LocalReceiptKey(tx ids.TxID) []byte {
	return append([]byte{prefixLocalReceipt}, tx[:]...)
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

// PeerExcludedKey builds 0a | nodeID:16.
func PeerExcludedKey(node ids.NodeID) []byte {
	return append([]byte{prefixPeerExcluded}, node[:]...)
}

// PeerExcludedPrefix scans all excluded peers.
func PeerExcludedPrefix() []byte {
	return []byte{prefixPeerExcluded}
}

// ParsePeerExcludedKey splits a peer excluded key.
func ParsePeerExcludedKey(k []byte) (ids.NodeID, bool) {
	if len(k) != 17 || k[0] != prefixPeerExcluded {
		return ids.NodeID{}, false
	}
	var node ids.NodeID
	copy(node[:], k[1:17])
	return node, true
}

// MemberKey builds 0b | nodeID:16 (persisted admission record).
func MemberKey(node ids.NodeID) []byte {
	return append([]byte{prefixMember}, node[:]...)
}

// MemberPrefix scans all member admission records.
func MemberPrefix() []byte {
	return []byte{prefixMember}
}

// ParseMemberKey splits a member record key.
func ParseMemberKey(k []byte) (ids.NodeID, bool) {
	if len(k) != 17 || k[0] != prefixMember {
		return ids.NodeID{}, false
	}
	var node ids.NodeID
	copy(node[:], k[1:17])
	return node, true
}

// PeerAckPrefix scans all persisted peer acknowledgements.
func PeerAckPrefix() []byte {
	return []byte{prefixPeerAck}
}

// ParsePeerAckKey splits a peer acknowledgement key.
func ParsePeerAckKey(k []byte) (peer, origin ids.NodeID, ok bool) {
	if len(k) != 33 || k[0] != prefixPeerAck {
		return ids.NodeID{}, ids.NodeID{}, false
	}
	copy(peer[:], k[1:17])
	copy(origin[:], k[17:33])
	return peer, origin, true
}

// BridgeProgressKey builds 0c | stream.
func BridgeProgressKey(stream string) []byte {
	k := []byte{prefixBridgeProgress}
	return append(k, stream...)
}

// BridgeProgressPrefix scans all bridge stream progress entries.
func BridgeProgressPrefix() []byte {
	return []byte{prefixBridgeProgress}
}

// ParseBridgeProgressKey splits a bridge stream progress key.
func ParseBridgeProgressKey(k []byte) (string, bool) {
	if len(k) <= 1 || k[0] != prefixBridgeProgress {
		return "", false
	}
	return string(k[1:]), true
}

// TransactionStagePrefix selects resumable transaction chunk records.
func TransactionStagePrefix(tx ids.TxID) []byte {
	k := []byte{prefixTxnStage}
	return append(k, tx[:]...)
}

func transactionStageKey(tx ids.TxID, index uint32) []byte {
	k := TransactionStagePrefix(tx)
	return binary.BigEndian.AppendUint32(k, index)
}

func transactionStageMetaKey(tx ids.TxID) []byte { return transactionStageKey(tx, ^uint32(0)) }

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
