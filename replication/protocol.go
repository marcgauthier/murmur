// Package replication implements masterless multi-writer replication.
//
// One ordered bidirectional QUIC stream per session carries framed messages:
// handshake, mutation batches, durable acknowledgements, gap requests, and
// logical snapshots. Per-origin log order is preserved on the wire; batches
// keep their original origin/sequence identity for multi-hop forwarding.
package replication

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
)

// Protocol versions.
const (
	ProtocolVersion    uint16 = 1
	MinProtocolVersion uint16 = 1
)

// Capability bits.
const (
	CapZstd uint64 = 1 << 0
)

// Frame layout: magic u16 | version u16 | type u16 | flags u16 | len u32 | payload.
const (
	frameMagic  uint16 = 0x5244 // "RD"
	frameHdrLen        = 12
	// MaxFrameBytes bounds one frame payload (no unbounded network allocs).
	MaxFrameBytes = 8 << 20
)

// Frame flags.
const (
	FlagZstd uint16 = 1 << 0
)

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
)

// Error codes for MsgError.
const (
	ErrSnapshotRequired uint16 = 1
	ErrSchemaMismatch   uint16 = 2
	ErrProtocolMismatch uint16 = 3
	ErrPeerNotAllowed   uint16 = 4
	ErrBadMessage       uint16 = 5
)

// Hello is the replication handshake.
type Hello struct {
	ProtocolVersion    uint16
	MinProtocolVersion uint16
	NodeID             ids.NodeID
	DBID               ids.DBID
	SchemaEpoch        uint64
	SchemaHash         [32]byte
	Capabilities       uint64
	Have               []codec.OriginWatermark
}

// EncodeHello appends the Hello encoding.
func EncodeHello(dst []byte, h *Hello) []byte {
	dst = binary.BigEndian.AppendUint16(dst, h.ProtocolVersion)
	dst = binary.BigEndian.AppendUint16(dst, h.MinProtocolVersion)
	dst = append(dst, h.NodeID[:]...)
	dst = append(dst, h.DBID[:]...)
	dst = binary.BigEndian.AppendUint64(dst, h.SchemaEpoch)
	dst = append(dst, h.SchemaHash[:]...)
	dst = binary.BigEndian.AppendUint64(dst, h.Capabilities)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(h.Have)))
	for _, w := range h.Have {
		dst = append(dst, w.Origin[:]...)
		dst = binary.BigEndian.AppendUint64(dst, w.Sequence)
	}
	return dst
}

// DecodeHello decodes a Hello/Welcome payload.
func DecodeHello(src []byte) (*Hello, error) {
	const hdr = 2 + 2 + 16 + 16 + 8 + 32 + 8 + 4
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
	h.Capabilities = binary.BigEndian.Uint64(src[76:84])
	n := binary.BigEndian.Uint32(src[84:88])
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

// SnapshotChunk is one MsgSnapshotChunk payload (before optional zstd).
type SnapshotChunk struct {
	Last  bool
	Cells []codec.SnapshotCell
}

// EncodeSnapshotChunk encodes a chunk.
func EncodeSnapshotChunk(dst []byte, c *SnapshotChunk) []byte {
	if c.Last {
		dst = append(dst, 1)
	} else {
		dst = append(dst, 0)
	}
	return codec.EncodeSnapshotCells(dst, c.Cells)
}

// DecodeSnapshotChunk decodes a chunk.
func DecodeSnapshotChunk(src []byte, lim codec.Limits, maxCells int) (*SnapshotChunk, error) {
	if len(src) < 1 {
		return nil, fmt.Errorf("replication: truncated snapshot chunk")
	}
	c := &SnapshotChunk{Last: src[0] == 1}
	cells, rest, err := codec.DecodeSnapshotCells(src[1:], lim, maxCells)
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

// ReadFrame reads one frame with bounded allocation.
func ReadFrame(r io.Reader) (*Frame, error) {
	var hdr [frameHdrLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
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
