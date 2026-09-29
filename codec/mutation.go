package codec

import (
	"encoding/binary"
	"fmt"

	"github.com/marcgauthier/spedsql/crdt"
	"github.com/marcgauthier/spedsql/ids"
)

// CodecVersion versions the mutation/snapshot binary encoding.
const CodecVersion uint16 = 1

// MutationFlags qualifies a Mutation.
type MutationFlags uint32

const (
	// FlagTombstone marks a row delete. The value payload is ignored and
	// ColumnID must be ColumnTombstone.
	FlagTombstone MutationFlags = 1 << 0
)

// ColumnTombstone is the sentinel ColumnID for row-delete mutations.
const ColumnTombstone uint32 = 0xFFFFFFFF

// Mutation is one replicated cell write or row delete. Batch-level metadata
// (origin, sequence, HLC, schema epoch/hash) lives in MutationBatch.
type Mutation struct {
	TableID  uint32
	RowID    ids.RowID
	ColumnID uint32
	Value    Value
	Flags    MutationFlags
}

// IsTombstone reports whether the mutation deletes the row.
func (m *Mutation) IsTombstone() bool { return m.Flags&FlagTombstone != 0 }

// MutationBatch is one committed transaction's replication unit.
type MutationBatch struct {
	ProtocolVersion uint16

	TxID       ids.TxID
	OriginNode ids.NodeID
	Sequence   uint64
	HLC        uint64
	// SchemaEpoch/SchemaHash carry the writer's schema provenance. Equal
	// epochs need not mean equal content across concurrent branches, so
	// receivers match both against the current manifest or a persisted
	// compatible ancestor before applying.
	SchemaEpoch uint64
	SchemaHash  [32]byte

	Mutations []Mutation
}

// Version returns the batch's conflict version.
func (b *MutationBatch) Version() crdt.Version {
	return crdt.Version{HLC: b.HLC, NodeID: b.OriginNode}
}

// Limits guards decoding. Values larger than MaxValueBytes are rejected.
type Limits struct {
	MaxValueBytes       int
	MaxMutations        int
	MaxTransactionBytes int64
}

// DefaultLimits caps values at 16 MiB, 100k mutations per batch, and 64 MiB total transaction size.
func DefaultLimits() Limits {
	return Limits{
		MaxValueBytes:       16 << 20,
		MaxMutations:        100_000,
		MaxTransactionBytes: 64 << 20,
	}
}

// BatchHeaderSize is the constant header size for a MutationBatch encoding (94 bytes).
const BatchHeaderSize = 2 + 16 + 16 + 8 + 8 + 8 + 32 + 4

// MutationHeaderSize is the per-mutation fixed header size (28 bytes).
const MutationHeaderSize = 4 + 16 + 4 + 4

// EncodedMutationsSize returns the total encoded byte size of a slice of mutations,
// including the batch header.
func EncodedMutationsSize(mutations []Mutation) int {
	sz := BatchHeaderSize
	for i := range mutations {
		sz += MutationHeaderSize + mutations[i].Value.EncodedSize()
	}
	return sz
}

// EncodedBatchSize returns the total encoded byte size of b.
func EncodedBatchSize(b *MutationBatch) int {
	return EncodedMutationsSize(b.Mutations)
}

// EncodeBatch appends the binary encoding of b to dst.
func EncodeBatch(dst []byte, b *MutationBatch) []byte {
	dst = binary.BigEndian.AppendUint16(dst, b.ProtocolVersion)
	dst = append(dst, b.TxID[:]...)
	dst = append(dst, b.OriginNode[:]...)
	dst = binary.BigEndian.AppendUint64(dst, b.Sequence)
	dst = binary.BigEndian.AppendUint64(dst, b.HLC)
	dst = binary.BigEndian.AppendUint64(dst, b.SchemaEpoch)
	dst = append(dst, b.SchemaHash[:]...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(b.Mutations)))
	for i := range b.Mutations {
		m := &b.Mutations[i]
		dst = binary.BigEndian.AppendUint32(dst, m.TableID)
		dst = append(dst, m.RowID[:]...)
		dst = binary.BigEndian.AppendUint32(dst, m.ColumnID)
		dst = binary.BigEndian.AppendUint32(dst, uint32(m.Flags))
		dst = AppendValue(dst, m.Value)
	}
	return dst
}

// DecodeBatch decodes one batch from the front of src.
func DecodeBatch(src []byte, lim Limits) (*MutationBatch, []byte, error) {
	const hdrLen = 2 + 16 + 16 + 8 + 8 + 8 + 32 + 4
	if len(src) < hdrLen {
		return nil, nil, fmt.Errorf("codec: truncated batch header")
	}
	b := &MutationBatch{}
	b.ProtocolVersion = binary.BigEndian.Uint16(src[0:2])
	copy(b.TxID[:], src[2:18])
	copy(b.OriginNode[:], src[18:34])
	b.Sequence = binary.BigEndian.Uint64(src[34:42])
	b.HLC = binary.BigEndian.Uint64(src[42:50])
	b.SchemaEpoch = binary.BigEndian.Uint64(src[50:58])
	copy(b.SchemaHash[:], src[58:90])
	n := binary.BigEndian.Uint32(src[90:94])
	if n > uint32(lim.MaxMutations) {
		return nil, nil, fmt.Errorf("codec: batch of %d mutations exceeds limit %d", n, lim.MaxMutations)
	}
	rest := src[hdrLen:]
	b.Mutations = make([]Mutation, 0, min(n, 1024))
	for i := uint32(0); i < n; i++ {
		const mhdr = 4 + 16 + 4 + 4
		if len(rest) < mhdr {
			return nil, nil, fmt.Errorf("codec: truncated mutation %d", i)
		}
		var m Mutation
		m.TableID = binary.BigEndian.Uint32(rest[0:4])
		copy(m.RowID[:], rest[4:20])
		m.ColumnID = binary.BigEndian.Uint32(rest[20:24])
		m.Flags = MutationFlags(binary.BigEndian.Uint32(rest[24:28]))
		rest = rest[mhdr:]
		v, r, err := ConsumeValue(rest, lim.MaxValueBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("codec: mutation %d: %w", i, err)
		}
		m.Value = v
		rest = r
		if m.IsTombstone() && m.ColumnID != ColumnTombstone {
			return nil, nil, fmt.Errorf("codec: mutation %d: tombstone with bad column id", i)
		}
		b.Mutations = append(b.Mutations, m)
	}
	consumed := len(src) - len(rest)
	if lim.MaxTransactionBytes > 0 && int64(consumed) > lim.MaxTransactionBytes {
		return nil, nil, fmt.Errorf("codec: batch of %d bytes exceeds MaxTransactionBytes %d", consumed, lim.MaxTransactionBytes)
	}
	return b, rest, nil
}

// CellState is the stored winning value of one cell.
type CellState struct {
	Version crdt.Version
	Value   Value
}

// EncodeCellState appends version + value.
func EncodeCellState(dst []byte, s CellState) []byte {
	dst = binary.BigEndian.AppendUint64(dst, s.Version.HLC)
	dst = append(dst, s.Version.NodeID[:]...)
	return AppendValue(dst, s.Value)
}

// DecodeCellState decodes version + value.
func DecodeCellState(src []byte, lim Limits) (CellState, error) {
	if len(src) < 24 {
		return CellState{}, fmt.Errorf("codec: truncated cell state")
	}
	var s CellState
	s.Version.HLC = binary.BigEndian.Uint64(src[0:8])
	copy(s.Version.NodeID[:], src[8:24])
	v, rest, err := ConsumeValue(src[24:], lim.MaxValueBytes)
	if err != nil {
		return CellState{}, err
	}
	if len(rest) != 0 {
		return CellState{}, fmt.Errorf("codec: %d trailing bytes in cell state", len(rest))
	}
	s.Value = v
	return s, nil
}

// EncodeTombstone encodes a tombstone version (8-byte HLC + 16-byte node).
func EncodeTombstone(dst []byte, v crdt.Version) []byte {
	dst = binary.BigEndian.AppendUint64(dst, v.HLC)
	return append(dst, v.NodeID[:]...)
}

// DecodeTombstone decodes a tombstone version.
func DecodeTombstone(src []byte) (crdt.Version, error) {
	if len(src) != 24 {
		return crdt.Version{}, fmt.Errorf("codec: bad tombstone length %d", len(src))
	}
	var v crdt.Version
	v.HLC = binary.BigEndian.Uint64(src[0:8])
	copy(v.NodeID[:], src[8:24])
	return v, nil
}

// SnapshotManifest describes a logical current-state snapshot.
type SnapshotManifest struct {
	FormatVersion   uint16
	SnapshotID      ids.TxID
	DBID            ids.DBID
	SchemaEpoch     uint64
	SchemaHash      [32]byte
	CreatedHLC      uint64
	StateGeneration uint64
	ChunkCount      uint64
	EncodedBytes    uint64
	ContentHash     [32]byte
	Watermarks      []OriginWatermark
}

// OriginWatermark is the highest contiguous sequence held for one origin.
type OriginWatermark struct {
	Origin   ids.NodeID
	Sequence uint64
}

// EncodeManifest appends the manifest encoding.
func EncodeManifest(dst []byte, m *SnapshotManifest) []byte {
	dst = binary.BigEndian.AppendUint16(dst, m.FormatVersion)
	dst = append(dst, m.SnapshotID[:]...)
	dst = append(dst, m.DBID[:]...)
	dst = binary.BigEndian.AppendUint64(dst, m.SchemaEpoch)
	dst = append(dst, m.SchemaHash[:]...)
	dst = binary.BigEndian.AppendUint64(dst, m.CreatedHLC)
	dst = binary.BigEndian.AppendUint64(dst, m.StateGeneration)
	dst = binary.BigEndian.AppendUint64(dst, m.ChunkCount)
	dst = binary.BigEndian.AppendUint64(dst, m.EncodedBytes)
	dst = append(dst, m.ContentHash[:]...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(m.Watermarks)))
	for _, w := range m.Watermarks {
		dst = append(dst, w.Origin[:]...)
		dst = binary.BigEndian.AppendUint64(dst, w.Sequence)
	}
	return dst
}

// DecodeManifest decodes one manifest.
func DecodeManifest(src []byte) (*SnapshotManifest, []byte, error) {
	const hdr = 2 + 16 + 16 + 8 + 32 + 8 + 8 + 8 + 8 + 32 + 4
	if len(src) < hdr {
		return nil, nil, fmt.Errorf("codec: truncated snapshot manifest")
	}
	m := &SnapshotManifest{}
	m.FormatVersion = binary.BigEndian.Uint16(src[0:2])
	copy(m.SnapshotID[:], src[2:18])
	copy(m.DBID[:], src[18:34])
	m.SchemaEpoch = binary.BigEndian.Uint64(src[34:42])
	copy(m.SchemaHash[:], src[42:74])
	m.CreatedHLC = binary.BigEndian.Uint64(src[74:82])
	m.StateGeneration = binary.BigEndian.Uint64(src[82:90])
	m.ChunkCount = binary.BigEndian.Uint64(src[90:98])
	m.EncodedBytes = binary.BigEndian.Uint64(src[98:106])
	copy(m.ContentHash[:], src[106:138])
	n := binary.BigEndian.Uint32(src[138:142])
	if n > 4096 {
		return nil, nil, fmt.Errorf("codec: absurd watermark count %d", n)
	}
	rest := src[hdr:]
	if len(rest) < int(n)*24 {
		return nil, nil, fmt.Errorf("codec: truncated snapshot watermarks")
	}
	for i := uint32(0); i < n; i++ {
		var w OriginWatermark
		copy(w.Origin[:], rest[:16])
		w.Sequence = binary.BigEndian.Uint64(rest[16:24])
		rest = rest[24:]
		m.Watermarks = append(m.Watermarks, w)
	}
	return m, rest, nil
}

// SnapshotCell is one versioned cell or tombstone in a snapshot chunk.
type SnapshotCell struct {
	TableID  uint32
	RowID    ids.RowID
	ColumnID uint32 // ColumnTombstone for tombstones
	Version  crdt.Version
	Value    Value // ignored for tombstones
}

// EncodeSnapshotCells appends cells.
func EncodeSnapshotCells(dst []byte, cells []SnapshotCell) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(cells)))
	for i := range cells {
		c := &cells[i]
		dst = binary.BigEndian.AppendUint32(dst, c.TableID)
		dst = append(dst, c.RowID[:]...)
		dst = binary.BigEndian.AppendUint32(dst, c.ColumnID)
		dst = binary.BigEndian.AppendUint64(dst, c.Version.HLC)
		dst = append(dst, c.Version.NodeID[:]...)
		dst = AppendValue(dst, c.Value)
	}
	return dst
}

// DecodeSnapshotCells decodes cells with bounds checking.
func DecodeSnapshotCells(src []byte, lim Limits, maxCells int) ([]SnapshotCell, []byte, error) {
	if len(src) < 4 {
		return nil, nil, fmt.Errorf("codec: truncated snapshot chunk")
	}
	n := binary.BigEndian.Uint32(src[:4])
	if n > uint32(maxCells) {
		return nil, nil, fmt.Errorf("codec: snapshot chunk of %d cells exceeds limit %d", n, maxCells)
	}
	rest := src[4:]
	cells := make([]SnapshotCell, 0, min(n, 1024))
	for i := uint32(0); i < n; i++ {
		const hdr = 4 + 16 + 4 + 8 + 16
		if len(rest) < hdr {
			return nil, nil, fmt.Errorf("codec: truncated snapshot cell %d", i)
		}
		var c SnapshotCell
		c.TableID = binary.BigEndian.Uint32(rest[0:4])
		copy(c.RowID[:], rest[4:20])
		c.ColumnID = binary.BigEndian.Uint32(rest[20:24])
		c.Version.HLC = binary.BigEndian.Uint64(rest[24:32])
		copy(c.Version.NodeID[:], rest[32:48])
		v, r, err := ConsumeValue(rest[48:], lim.MaxValueBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("codec: snapshot cell %d: %w", i, err)
		}
		c.Value = v
		rest = r
		cells = append(cells, c)
	}
	return cells, rest, nil
}

func min(a, b uint32) int {
	if a < b {
		return int(a)
	}
	return int(b)
}
