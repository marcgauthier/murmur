// Package replication implements masterless multi-writer replication.
//
// One ordered bidirectional QUIC stream per session carries framed messages:
// handshake, mutation batches, durable acknowledgements, gap requests, and
// logical snapshots. Per-origin log order is preserved on the wire; batches
// keep their original origin/sequence identity for multi-hop forwarding.
package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

// Protocol version 6 identifies the managed RIME record runtime and refuses
// peers from the removed SQL cell protocol.
const (
	ProtocolVersion    uint16 = 6
	MinProtocolVersion uint16 = 6
)

// Capability bits.
const (
	CapOriginSignatures uint64 = 1 << 4
	CapMergePolicies    uint64 = 1 << 5
	// Bit 0 was CapZstd and is retired: zstd was removed, and the bit
	// is never reused so an old zstd peer cannot misread deflate
	// frames as zstd.
	CapPlumtree          uint64 = 1 << 1
	CapProgressPages     uint64 = 1 << 2
	CapTransactionChunks uint64 = 1 << 3
	// CapCompression advertises frame compression through the
	// compression.Codec registry (codec id carried in the frame flags).
	CapCompression uint64 = 1 << 6
)

// CapRequiredMask marks a handshake's capabilities as required: when the
// peer sets it, every other bit it advertises must be understood or the
// session is refused. Without the mask, unknown bits are ignored so old
// binaries keep interoperating with newer optional features.
//
// Bits 1..31 are reserved for optional target-subsystem capabilities
// (SWIM membership envelopes, range/chunk sync, Plumtree, snapshot
// generations, restore identity markers); each is defined alongside its
// feature and added to KnownCaps there. Bits 32..62 are spare.
const CapRequiredMask uint64 = 1 << 63

// KnownCaps is every capability bit this binary understands.
const KnownCaps uint64 = CapMergePolicies | CapOriginSignatures | CapCompression | CapPlumtree | CapProgressPages | CapTransactionChunks

// NegotiateCapabilities intersects peer-advertised capabilities with local
// support. Unknown bits are ignored unless the peer marks its set required
// (CapRequiredMask), in which case any unknown bit refuses the session:
// an old peer must never silently accept a transfer it cannot interpret.
func NegotiateCapabilities(peerCaps uint64) (uint64, error) {
	if peerCaps&CapRequiredMask != 0 {
		if unknown := peerCaps &^ KnownCaps &^ CapRequiredMask; unknown != 0 {
			return 0, fmt.Errorf("replication: peer requires unknown capabilities %#x", unknown)
		}
	}
	return peerCaps & KnownCaps, nil
}

// NegotiateDisseminationCapabilities rejects a configured-mode mismatch.
// Plumtree uses a required capability bit so mixed-mode clusters fail during
// the authenticated handshake instead of silently falling back per peer.
func NegotiateDisseminationCapabilities(peerCaps uint64, localPlumtree bool) (uint64, error) {
	usable, err := NegotiateCapabilities(peerCaps)
	if err != nil {
		return 0, err
	}
	if (peerCaps&CapPlumtree != 0) != localPlumtree {
		return 0, fmt.Errorf("replication: dissemination mode mismatch")
	}
	return usable, nil
}

// Frame layout: magic u16 | version u16 | type u16 | flags u16 | len u32 | payload.
const (
	frameMagic  uint16 = 0x5244 // "RD"
	frameHdrLen        = 12
	// MaxFrameBytes bounds one frame payload (no unbounded network allocs).
	MaxFrameBytes = 8 << 20
)

// Frame flags.
//
// Bit 0 was FlagZstd and is retired (never reused). Bit 1 marks a
// compressed payload and bits 8-15 carry the compression.Codec wire id.
const (
	FlagCompressed uint16 = 1 << 1

	flagCodecShift = 8
)

// CompressedFlags returns the frame flags for a payload compressed with
// codec id.
func CompressedFlags(id uint8) uint16 { return FlagCompressed | uint16(id)<<flagCodecShift }

// FlagCodecID extracts the compression codec id from frame flags.
func FlagCodecID(flags uint16) uint8 { return uint8(flags >> flagCodecShift) }

// Message types.
const (
	MsgHello            uint16 = 1
	MsgWelcome          uint16 = 2
	MsgBatches          uint16 = 3
	MsgAck              uint16 = 4
	MsgNeed             uint16 = 5
	MsgSnapshotRequest  uint16 = 6
	MsgSnapshotManifest uint16 = 7
	MsgSnapshotChunk    uint16 = 8
	MsgSnapshotDone     uint16 = 9
	MsgPing             uint16 = 10
	MsgPong             uint16 = 11
	MsgError            uint16 = 12
	// Schema synchronization (architecture/schema.md section 50).
	MsgSchemaRequest            uint16 = 13
	MsgSchemaManifest           uint16 = 14
	MsgSchemaAck                uint16 = 15
	MsgPlumtreeData             uint16 = 16
	MsgPlumtreeIHave            uint16 = 17
	MsgPlumtreePrune            uint16 = 18
	MsgPlumtreeGraft            uint16 = 19
	MsgProgressRequest          uint16 = 20
	MsgProgressPage             uint16 = 21
	MsgTransactionChunk         uint16 = 22
	MsgChunkNeed                uint16 = 23
	MsgChunkAvailabilityRequest uint16 = 24
	MsgChunkAvailabilityPage    uint16 = 25
)

const ChunkAvailabilityPageEntries = 32

type ChunkAvailability struct {
	TxID       ids.TxID
	Origin     ids.NodeID
	Sequence   uint64
	ChunkCount uint32
	Received   []bool
}
type ChunkAvailabilityPage struct {
	Items []ChunkAvailability
	More  bool
}

func EncodeChunkAvailabilityRequest(dst []byte, after ids.TxID) []byte {
	return append(dst, after[:]...)
}
func DecodeChunkAvailabilityRequest(src []byte) (ids.TxID, error) {
	var tx ids.TxID
	if len(src) != len(tx) {
		return tx, fmt.Errorf("replication: bad chunk availability cursor")
	}
	copy(tx[:], src)
	return tx, nil
}
func EncodeChunkAvailabilityPage(dst []byte, page ChunkAvailabilityPage) []byte {
	if page.More {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(page.Items)))
	for _, item := range page.Items {
		dst = append(dst, item.TxID[:]...)
		dst = append(dst, item.Origin[:]...)
		dst = binary.BigEndian.AppendUint64(dst, item.Sequence)
		dst = binary.BigEndian.AppendUint32(dst, item.ChunkCount)
		bitmap := make([]byte, (len(item.Received)+7)/8)
		for i, yes := range item.Received {
			if yes {
				bitmap[i/8] |= 1 << uint(i%8)
			}
		}
		dst = append(dst, bitmap...)
	}
	return dst
}
func DecodeChunkAvailabilityPage(src []byte) (ChunkAvailabilityPage, error) {
	if len(src) < 3 || src[0] > 1 {
		return ChunkAvailabilityPage{}, fmt.Errorf("replication: malformed chunk availability page")
	}
	n := int(binary.BigEndian.Uint16(src[1:3]))
	if n > ChunkAvailabilityPageEntries {
		return ChunkAvailabilityPage{}, fmt.Errorf("replication: too many chunk availability records")
	}
	page := ChunkAvailabilityPage{More: src[0] == 1}
	off := 3
	for i := 0; i < n; i++ {
		if len(src)-off < 44 {
			return ChunkAvailabilityPage{}, fmt.Errorf("replication: truncated chunk availability record")
		}
		var item ChunkAvailability
		copy(item.TxID[:], src[off:off+16])
		off += 16
		copy(item.Origin[:], src[off:off+16])
		off += 16
		item.Sequence = binary.BigEndian.Uint64(src[off : off+8])
		off += 8
		item.ChunkCount = binary.BigEndian.Uint32(src[off : off+4])
		off += 4
		if item.TxID.IsZero() || item.Origin.IsZero() || item.Sequence == 0 || item.ChunkCount == 0 || item.ChunkCount > 65536 || (i > 0 && bytes.Compare(page.Items[i-1].TxID[:], item.TxID[:]) >= 0) {
			return ChunkAvailabilityPage{}, fmt.Errorf("replication: invalid chunk availability identity")
		}
		sz := (int(item.ChunkCount) + 7) / 8
		if len(src)-off < sz {
			return ChunkAvailabilityPage{}, fmt.Errorf("replication: truncated chunk bitmap")
		}
		item.Received = make([]bool, item.ChunkCount)
		found := false
		for j := range item.Received {
			item.Received[j] = src[off+j/8]&(1<<uint(j%8)) != 0
			found = found || item.Received[j]
		}
		if item.ChunkCount%8 != 0 && src[off+sz-1]>>uint(item.ChunkCount%8) != 0 {
			return ChunkAvailabilityPage{}, fmt.Errorf("replication: nonzero unused chunk bitmap bits")
		}
		if !found {
			return ChunkAvailabilityPage{}, fmt.Errorf("replication: empty staged chunk bitmap")
		}
		off += sz
		page.Items = append(page.Items, item)
	}
	if off != len(src) {
		return ChunkAvailabilityPage{}, fmt.Errorf("replication: trailing chunk availability bytes")
	}
	return page, nil
}

type ChunkNeed struct {
	Origin   ids.NodeID
	Sequence uint64
	TxID     ids.TxID
	Missing  []uint32
}

func EncodeChunkNeed(dst []byte, n ChunkNeed) []byte {
	dst = append(dst, n.Origin[:]...)
	dst = binary.BigEndian.AppendUint64(dst, n.Sequence)
	dst = append(dst, n.TxID[:]...)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(n.Missing)))
	for _, i := range n.Missing {
		dst = binary.BigEndian.AppendUint32(dst, i)
	}
	return dst
}

func DecodeChunkNeed(src []byte) (ChunkNeed, error) {
	var n ChunkNeed
	if len(src) < 44 {
		return n, fmt.Errorf("replication: truncated chunk need")
	}
	copy(n.Origin[:], src[:16])
	n.Sequence = binary.BigEndian.Uint64(src[16:24])
	copy(n.TxID[:], src[24:40])
	count := binary.BigEndian.Uint32(src[40:44])
	if count == 0 || count > 65536 || len(src) != 44+int(count)*4 || n.Origin.IsZero() || n.Sequence == 0 || n.TxID.IsZero() {
		return n, fmt.Errorf("replication: malformed chunk need")
	}
	n.Missing = make([]uint32, count)
	seen := map[uint32]bool{}
	for i := range n.Missing {
		n.Missing[i] = binary.BigEndian.Uint32(src[44+i*4 : 48+i*4])
		if seen[n.Missing[i]] {
			return ChunkNeed{}, fmt.Errorf("replication: duplicate missing chunk")
		}
		seen[n.Missing[i]] = true
	}
	return n, nil
}

const ProgressPageEntries = 128

// ProgressItem advertises contiguous durable application progress and the
// retained contiguous origin-log suffix. Observed is reserved for durable
// staging; until staging is enabled it equals Applied.
type ProgressItem struct {
	Origin          ids.NodeID
	Applied         uint64
	Observed        uint64
	RetainedFrom    uint64
	RetainedThrough uint64
}

type ProgressPage struct {
	Items []ProgressItem
	More  bool
}

func EncodeProgressRequest(dst []byte, after ids.NodeID) []byte {
	return append(dst, after[:]...)
}

func DecodeProgressRequest(src []byte) (ids.NodeID, error) {
	var after ids.NodeID
	if len(src) != len(after) {
		return after, fmt.Errorf("replication: bad progress request length %d", len(src))
	}
	copy(after[:], src)
	return after, nil
}

func EncodeProgressPage(dst []byte, page ProgressPage) []byte {
	if page.More {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(page.Items)))
	for _, item := range page.Items {
		dst = append(dst, item.Origin[:]...)
		dst = binary.BigEndian.AppendUint64(dst, item.Applied)
		dst = binary.BigEndian.AppendUint64(dst, item.Observed)
		dst = binary.BigEndian.AppendUint64(dst, item.RetainedFrom)
		dst = binary.BigEndian.AppendUint64(dst, item.RetainedThrough)
	}
	return dst
}

func DecodeProgressPage(src []byte) (ProgressPage, error) {
	if len(src) < 3 || src[0] > 1 {
		return ProgressPage{}, fmt.Errorf("replication: malformed progress page header")
	}
	n := int(binary.BigEndian.Uint16(src[1:3]))
	if n > ProgressPageEntries || len(src) != 3+n*48 {
		return ProgressPage{}, fmt.Errorf("replication: malformed progress page size %d", len(src))
	}
	page := ProgressPage{More: src[0] == 1, Items: make([]ProgressItem, 0, n)}
	for i := 0; i < n; i++ {
		rest := src[3+i*48 : 3+(i+1)*48]
		var item ProgressItem
		copy(item.Origin[:], rest[:16])
		item.Applied = binary.BigEndian.Uint64(rest[16:24])
		item.Observed = binary.BigEndian.Uint64(rest[24:32])
		item.RetainedFrom = binary.BigEndian.Uint64(rest[32:40])
		item.RetainedThrough = binary.BigEndian.Uint64(rest[40:48])
		if item.Origin.IsZero() || (i > 0 && page.Items[i-1].Origin.Compare(item.Origin) >= 0) || item.Observed < item.Applied {
			return ProgressPage{}, fmt.Errorf("replication: invalid progress item %d", i)
		}
		if item.RetainedThrough == 0 {
			if item.RetainedFrom != item.Applied+1 {
				return ProgressPage{}, fmt.Errorf("replication: invalid empty history bounds at item %d", i)
			}
		} else if item.RetainedFrom == 0 || item.RetainedFrom > item.RetainedThrough || item.RetainedThrough > item.Applied {
			return ProgressPage{}, fmt.Errorf("replication: invalid retained history bounds at item %d", i)
		}
		page.Items = append(page.Items, item)
	}
	return page, nil
}

// PlumtreeHint identifies one durable transaction for IHAVE/GRAFT and
// protects against a peer advertising different bytes for the same identity.
type PlumtreeHint struct {
	ID     [32]byte
	Origin ids.NodeID
	Seq    uint64
}

func EncodePlumtreeHint(dst []byte, h PlumtreeHint) []byte {
	dst = append(dst, h.ID[:]...)
	dst = append(dst, h.Origin[:]...)
	return binary.BigEndian.AppendUint64(dst, h.Seq)
}

func DecodePlumtreeHint(src []byte) (PlumtreeHint, error) {
	if len(src) != 56 {
		return PlumtreeHint{}, fmt.Errorf("replication: plumtree hint length %d", len(src))
	}
	var h PlumtreeHint
	copy(h.ID[:], src[:32])
	copy(h.Origin[:], src[32:48])
	h.Seq = binary.BigEndian.Uint64(src[48:56])
	if h.Origin.IsZero() || h.Seq == 0 {
		return PlumtreeHint{}, fmt.Errorf("replication: invalid plumtree identity")
	}
	return h, nil
}

// EncodePlumtreeData prefixes an encoded one-batch payload with its identity.
func EncodePlumtreeData(dst []byte, id [32]byte, batch []byte) []byte {
	dst = append(dst, id[:]...)
	return append(dst, batch...)
}

func DecodePlumtreeData(src []byte) ([32]byte, []byte, error) {
	var id [32]byte
	if len(src) <= len(id) {
		return id, nil, fmt.Errorf("replication: truncated plumtree data")
	}
	copy(id[:], src[:32])
	return id, src[32:], nil
}

// Error codes for MsgError.
const (
	ErrSnapshotRequired uint16 = 1
	ErrSchemaMismatch   uint16 = 2
	ErrProtocolMismatch uint16 = 3
	ErrPeerNotAllowed   uint16 = 4
	ErrBadMessage       uint16 = 5
	ErrPeerExcludedCode uint16 = 6
	ErrRangeUnavailable uint16 = 7
	ErrOverloadedCode   uint16 = 8
	// ErrSnapshotBusy tells a snapshot requester the source already has
	// an outbound transfer in flight for it. The requester backs off and
	// the stall watchdog re-requests; older peers log it as a plain
	// peer error and converge through the same watchdog.
	ErrSnapshotBusy uint16 = 9
)

// Hello is the replication handshake.
type Hello struct {
	ProtocolVersion     uint16
	MinProtocolVersion  uint16
	NodeID              ids.NodeID
	DBID                ids.DBID
	SchemaEpoch         uint64
	SchemaHash          [32]byte
	SchemaAuthorNode    ids.NodeID
	SchemaTimeCreated   uint64
	Capabilities        uint64
	MaxTransactionBytes uint64
	Have                []codec.OriginWatermark
}

// EncodeHello appends the Hello encoding.
func EncodeHello(dst []byte, h *Hello) []byte {
	dst = binary.BigEndian.AppendUint16(dst, h.ProtocolVersion)
	dst = binary.BigEndian.AppendUint16(dst, h.MinProtocolVersion)
	dst = append(dst, h.NodeID[:]...)
	dst = append(dst, h.DBID[:]...)
	dst = binary.BigEndian.AppendUint64(dst, h.SchemaEpoch)
	dst = append(dst, h.SchemaHash[:]...)
	dst = append(dst, h.SchemaAuthorNode[:]...)
	dst = binary.BigEndian.AppendUint64(dst, h.SchemaTimeCreated)
	dst = binary.BigEndian.AppendUint64(dst, h.Capabilities)
	dst = binary.BigEndian.AppendUint64(dst, h.MaxTransactionBytes)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(h.Have)))
	for _, w := range h.Have {
		dst = append(dst, w.Origin[:]...)
		dst = binary.BigEndian.AppendUint64(dst, w.Sequence)
	}
	return dst
}

// DecodeHello decodes a Hello/Welcome payload.
func DecodeHello(src []byte) (*Hello, error) {
	const hdr = 2 + 2 + 16 + 16 + 8 + 32 + 16 + 8 + 8 + 8 + 4
	if len(src) < hdr {
		return nil, fmt.Errorf("replication: truncated hello")
	}
	h := &Hello{}
	h.ProtocolVersion = binary.BigEndian.Uint16(src[0:2])
	h.MinProtocolVersion = binary.BigEndian.Uint16(src[2:4])
	copy(h.NodeID[:], src[4:20])
	copy(h.DBID[:], src[20:36])
	h.SchemaEpoch = binary.BigEndian.Uint64(src[36:44])
	copy(h.SchemaHash[:], src[44:76])
	copy(h.SchemaAuthorNode[:], src[76:92])
	h.SchemaTimeCreated = binary.BigEndian.Uint64(src[92:100])
	h.Capabilities = binary.BigEndian.Uint64(src[100:108])
	h.MaxTransactionBytes = binary.BigEndian.Uint64(src[108:116])
	n := binary.BigEndian.Uint32(src[116:120])
	if n > 4096 {
		return nil, fmt.Errorf("replication: absurd watermark count %d", n)
	}
	rest := src[hdr:]
	if len(rest) < int(n)*24 {
		return nil, fmt.Errorf("replication: truncated hello watermarks")
	}
	for i := uint32(0); i < n; i++ {
		var w codec.OriginWatermark
		copy(w.Origin[:], rest[:16])
		w.Sequence = binary.BigEndian.Uint64(rest[16:24])
		rest = rest[24:]
		h.Have = append(h.Have, w)
	}
	return h, nil
}

// EncodeWatermarks encodes a watermark list (Ack payload).
func EncodeWatermarks(dst []byte, wms []codec.OriginWatermark) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(wms)))
	for _, w := range wms {
		dst = append(dst, w.Origin[:]...)
		dst = binary.BigEndian.AppendUint64(dst, w.Sequence)
	}
	return dst
}

// DecodeWatermarks decodes a watermark list.
func DecodeWatermarks(src []byte) ([]codec.OriginWatermark, error) {
	if len(src) < 4 {
		return nil, fmt.Errorf("replication: truncated watermarks")
	}
	n := binary.BigEndian.Uint32(src[:4])
	if n > 4096 {
		return nil, fmt.Errorf("replication: absurd watermark count %d", n)
	}
	rest := src[4:]
	if len(rest) < int(n)*24 {
		return nil, fmt.Errorf("replication: truncated watermarks")
	}
	out := make([]codec.OriginWatermark, 0, n)
	for i := uint32(0); i < n; i++ {
		var w codec.OriginWatermark
		copy(w.Origin[:], rest[:16])
		w.Sequence = binary.BigEndian.Uint64(rest[16:24])
		rest = rest[24:]
		out = append(out, w)
	}
	return out, nil
}

// EncodeBatches encodes batches (count + concatenated self-delimiting batches).
func EncodeBatches(dst []byte, batches []*codec.MutationBatch) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(batches)))
	for _, b := range batches {
		dst = codec.EncodeBatch(dst, b)
	}
	return dst
}

// DecodeBatches decodes batches.
func DecodeBatches(src []byte, lim codec.Limits) ([]*codec.MutationBatch, error) {
	if len(src) < 4 {
		return nil, fmt.Errorf("replication: truncated batches")
	}
	n := binary.BigEndian.Uint32(src[:4])
	if n > uint32(lim.MaxMutations) {
		return nil, fmt.Errorf("replication: absurd batch count %d", n)
	}
	rest := src[4:]
	out := make([]*codec.MutationBatch, 0, min(n, 64))
	for i := uint32(0); i < n; i++ {
		b, r, err := codec.DecodeBatch(rest, lim)
		if err != nil {
			return nil, fmt.Errorf("replication: batch %d: %w", i, err)
		}
		out = append(out, b)
		rest = r
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("replication: %d trailing bytes in batches", len(rest))
	}
	return out, nil
}

// Need requests retransmission of one origin from a sequence.
type Need struct {
	Origin  ids.NodeID
	FromSeq uint64
}

// EncodeNeed encodes a Need payload.
func EncodeNeed(dst []byte, n Need) []byte {
	dst = append(dst, n.Origin[:]...)
	return binary.BigEndian.AppendUint64(dst, n.FromSeq)
}

// DecodeNeed decodes a Need payload.
func DecodeNeed(src []byte) (Need, error) {
	if len(src) != 24 {
		return Need{}, fmt.Errorf("replication: bad need length %d", len(src))
	}
	var n Need
	copy(n.Origin[:], src[:16])
	n.FromSeq = binary.BigEndian.Uint64(src[16:24])
	return n, nil
}

// EncodeError encodes an Error payload.
func EncodeError(dst []byte, code uint16, msg string) []byte {
	if len(msg) > 1024 {
		msg = msg[:1024]
	}
	dst = binary.BigEndian.AppendUint16(dst, code)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(msg)))
	return append(dst, msg...)
}

// DecodeError decodes an Error payload.
func DecodeError(src []byte) (uint16, string, error) {
	if len(src) < 4 {
		return 0, "", fmt.Errorf("replication: truncated error")
	}
	code := binary.BigEndian.Uint16(src[:2])
	n := binary.BigEndian.Uint16(src[2:4])
	if len(src[4:]) < int(n) {
		return 0, "", fmt.Errorf("replication: truncated error message")
	}
	return code, string(src[4 : 4+n]), nil
}

// SnapshotChunk is one MsgSnapshotChunk payload (before optional compression).
type SnapshotChunk struct {
	Index uint64
	Last  bool
	Cells []codec.SnapshotCell
}

// EncodeSnapshotChunk encodes a chunk.
func EncodeSnapshotChunk(dst []byte, c *SnapshotChunk) []byte {
	dst = binary.BigEndian.AppendUint64(dst, c.Index)
	if c.Last {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	return codec.EncodeSnapshotCells(dst, c.Cells)
}

// DecodeSnapshotChunk decodes a chunk.
func DecodeSnapshotChunk(src []byte, lim codec.Limits, maxCells int) (*SnapshotChunk, error) {
	if len(src) < 9 {
		return nil, fmt.Errorf("replication: truncated snapshot chunk")
	}
	c := &SnapshotChunk{Index: binary.BigEndian.Uint64(src[:8]), Last: src[8] == 1}
	cells, rest, err := codec.DecodeSnapshotCells(src[9:], lim, maxCells)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("replication: %d trailing bytes in snapshot chunk", len(rest))
	}
	c.Cells = cells
	return c, nil
}

// Frame is one decoded wire frame.
type Frame struct {
	Type    uint16
	Flags   uint16
	Payload []byte
}

// WriteFrame writes one frame to w.
func WriteFrame(w io.Writer, typ uint16, flags uint16, payload []byte) error {
	if len(payload) > MaxFrameBytes {
		return fmt.Errorf("replication: frame of %d bytes exceeds %d", len(payload), MaxFrameBytes)
	}
	var hdr [frameHdrLen]byte
	binary.BigEndian.PutUint16(hdr[0:2], frameMagic)
	binary.BigEndian.PutUint16(hdr[2:4], ProtocolVersion)
	binary.BigEndian.PutUint16(hdr[4:6], typ)
	binary.BigEndian.PutUint16(hdr[6:8], flags)
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// ErrMembershipStream is returned when a stream begins with the SWIM memberlist header.
type ErrMembershipStream struct {
	Prefix []byte
}

func (e *ErrMembershipStream) Error() string {
	return "replication: received memberlist stream"
}

// ReadFrame reads one frame with bounded allocation.
func ReadFrame(r io.Reader) (*Frame, error) {
	var hdr [frameHdrLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(hdr[0:4]) == 0x53504544 { // transport.StreamMagic "SPED"
		return nil, &ErrMembershipStream{Prefix: hdr[:]}
	}
	if binary.BigEndian.Uint16(hdr[0:2]) != frameMagic {
		return nil, fmt.Errorf("replication: bad frame magic")
	}
	ver := binary.BigEndian.Uint16(hdr[2:4])
	if ver < MinProtocolVersion || ver > ProtocolVersion {
		return nil, fmt.Errorf("replication: protocol version %d unsupported", ver)
	}
	n := binary.BigEndian.Uint32(hdr[8:12])
	if n > MaxFrameBytes {
		return nil, fmt.Errorf("replication: frame length %d exceeds %d", n, MaxFrameBytes)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return &Frame{
		Type:    binary.BigEndian.Uint16(hdr[4:6]),
		Flags:   binary.BigEndian.Uint16(hdr[6:8]),
		Payload: payload,
	}, nil
}

func min(a, b uint32) int {
	if a < b {
		return int(a)
	}
	return int(b)
}

// Schema-sync message bounds (no unbounded network allocs).
const (
	maxSchemaRequestIDs = 256
	maxSchemaRevisions  = 64
	// maxSchemaPayload caps one MsgSchemaManifest payload well under the
	// frame limit; ancestry beyond the budget is fetched incrementally by
	// revision ID.
	maxSchemaPayload = 1 << 20
)

// SchemaRequest asks a peer for schema revisions: its current tip and/or
// specific revision IDs.
type SchemaRequest struct {
	WantCurrent bool
	WantIDs     [][32]byte
}

// EncodeSchemaRequest appends the encoding of r to dst.
func EncodeSchemaRequest(dst []byte, r *SchemaRequest) []byte {
	var flags byte
	if r.WantCurrent {
		flags |= 1
	}
	dst = append(dst, flags)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(r.WantIDs)))
	for _, id := range r.WantIDs {
		dst = append(dst, id[:]...)
	}
	return dst
}

// DecodeSchemaRequest decodes one MsgSchemaRequest payload.
func DecodeSchemaRequest(src []byte) (*SchemaRequest, error) {
	if len(src) < 5 {
		return nil, fmt.Errorf("replication: truncated schema request")
	}
	r := &SchemaRequest{WantCurrent: src[0]&1 != 0}
	n := binary.BigEndian.Uint32(src[1:5])
	if n > maxSchemaRequestIDs {
		return nil, fmt.Errorf("replication: absurd schema request count %d", n)
	}
	if len(src) != 5+int(n)*32 {
		return nil, fmt.Errorf("replication: malformed schema request")
	}
	for i := uint32(0); i < n; i++ {
		var id [32]byte
		copy(id[:], src[5+i*32:5+(i+1)*32])
		r.WantIDs = append(r.WantIDs, id)
	}
	return r, nil
}

// SchemaManifestMsg carries schema revisions. Revisions[0] is always the
// sender's current tip; the rest is ancestry backfill.
type SchemaManifestMsg struct {
	Revisions []*schema.Manifest
}

// EncodeSchemaManifest appends the encoding of m to dst.
func EncodeSchemaManifest(dst []byte, m *SchemaManifestMsg) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(m.Revisions)))
	for _, r := range m.Revisions {
		enc := schema.EncodeManifest(r)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(enc)))
		dst = append(dst, enc...)
	}
	return dst
}

// DecodeSchemaManifest decodes one MsgSchemaManifest payload, authenticating
// every revision (hash mismatch fails the whole message).
func DecodeSchemaManifest(src []byte) (*SchemaManifestMsg, error) {
	if len(src) > maxSchemaPayload {
		return nil, fmt.Errorf("replication: schema manifest of %d bytes exceeds limit", len(src))
	}
	if len(src) < 4 {
		return nil, fmt.Errorf("replication: truncated schema manifest")
	}
	n := binary.BigEndian.Uint32(src[0:4])
	if n == 0 || n > maxSchemaRevisions {
		return nil, fmt.Errorf("replication: absurd schema revision count %d", n)
	}
	rest := src[4:]
	out := &SchemaManifestMsg{}
	for i := uint32(0); i < n; i++ {
		if len(rest) < 4 {
			return nil, fmt.Errorf("replication: truncated schema revision %d", i)
		}
		ln := binary.BigEndian.Uint32(rest[0:4])
		rest = rest[4:]
		if uint64(ln) > uint64(len(rest)) {
			return nil, fmt.Errorf("replication: truncated schema revision %d", i)
		}
		rev, err := schema.DecodeManifest(rest[:ln])
		if err != nil {
			return nil, fmt.Errorf("replication: schema revision %d: %w", i, err)
		}
		out.Revisions = append(out.Revisions, rev)
		rest = rest[ln:]
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("replication: schema manifest has %d trailing bytes", len(rest))
	}
	return out, nil
}

// SchemaAck acknowledges the sender's current schema identity after a
// successful adopt or a no-op match.
type SchemaAck struct {
	Version uint64
	Hash    [32]byte
}

// EncodeSchemaAck appends the encoding of a to dst.
func EncodeSchemaAck(dst []byte, a *SchemaAck) []byte {
	dst = binary.BigEndian.AppendUint64(dst, a.Version)
	dst = append(dst, a.Hash[:]...)
	return dst
}

// DecodeSchemaAck decodes one MsgSchemaAck payload.
func DecodeSchemaAck(src []byte) (*SchemaAck, error) {
	if len(src) != 40 {
		return nil, fmt.Errorf("replication: malformed schema ack")
	}
	a := &SchemaAck{Version: binary.BigEndian.Uint64(src[0:8])}
	copy(a.Hash[:], src[8:40])
	return a, nil
}
