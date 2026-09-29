// Package filefetch transfers content-addressed file objects between nodes
// over dedicated QUIC streams.
//
// The fetch endpoint is separate from the replication session stream so bulk
// object bytes never head-of-line-block membership or control traffic, but it
// shares the cluster mTLS credentials and DBID policy: only cluster members
// presenting an authorized certificate for the same database can fetch.
//
// Trust is anchored in replicated file metadata, never in the serving peer:
// the receiver stages container bytes to a temporary file and publishes them
// only after the object store verifies the expected digest and length.
// Corrupt or truncated sources fail closed and the fetcher tries the next
// source.
package filefetch

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/marcgauthier/spedsql/ids"
	"github.com/marcgauthier/spedsql/objectstore"
)

// Protocol version 1: request/response object fetch with byte offsets.
const ProtocolVersion uint16 = 1

// Frame layout: magic u16 | version u16 | type u16 | reserved u16 | len u32 | payload.
const (
	frameMagic  uint16 = 0x4646 // "FF"
	frameHdrLen        = 12
	// MaxFramePayload bounds one frame payload (no unbounded network allocs).
	MaxFramePayload = 128 << 10
	// ChunkBytes is the object payload per MsgChunk frame.
	ChunkBytes = 64 << 10
)

// Message types.
const (
	MsgRequest uint16 = 1
	MsgHeader  uint16 = 2
	MsgChunk   uint16 = 3
	MsgDone    uint16 = 4
	MsgError   uint16 = 5
)

// Header status codes.
const (
	StatusOK        uint16 = 0
	StatusNotFound  uint16 = 1
	StatusBadOffset uint16 = 2
	StatusRefused   uint16 = 3
)

// Error codes for MsgError payloads.
const (
	ErrCodeRefused   uint16 = 1
	ErrCodeNotFound  uint16 = 2
	ErrCodeBadOffset uint16 = 3
	ErrCodeInternal  uint16 = 4
)

// Request asks for the container bytes of digest starting at offset.
type Request struct {
	DBID   ids.DBID
	Digest objectstore.Digest
	Offset uint64
}

// requestLen is dbid(16) + digest(32) + offset(8).
const requestLen = 56

// EncodeRequest encodes a request payload.
func EncodeRequest(r *Request) []byte {
	out := make([]byte, requestLen)
	copy(out[0:16], r.DBID[:])
	copy(out[16:48], r.Digest[:])
	binary.BigEndian.PutUint64(out[48:56], r.Offset)
	return out
}

// DecodeRequest decodes a request payload.
func DecodeRequest(b []byte) (*Request, error) {
	if len(b) != requestLen {
		return nil, fmt.Errorf("filefetch: request is %d bytes, want %d", len(b), requestLen)
	}
	r := &Request{Offset: binary.BigEndian.Uint64(b[48:56])}
	copy(r.DBID[:], b[0:16])
	copy(r.Digest[:], b[16:48])
	return r, nil
}

// Header answers a request: status plus the total container length.
type Header struct {
	Status       uint16
	ContainerLen uint64
}

// headerLen is status(2) + reserved(2) + containerLen(8).
const headerLen = 12

// EncodeHeader encodes a header payload.
func EncodeHeader(h *Header) []byte {
	out := make([]byte, headerLen)
	binary.BigEndian.PutUint16(out[0:2], h.Status)
	binary.BigEndian.PutUint64(out[4:12], h.ContainerLen)
	return out
}

// DecodeHeader decodes a header payload.
func DecodeHeader(b []byte) (*Header, error) {
	if len(b) != headerLen {
		return nil, fmt.Errorf("filefetch: header is %d bytes, want %d", len(b), headerLen)
	}
	return &Header{
		Status:       binary.BigEndian.Uint16(b[0:2]),
		ContainerLen: binary.BigEndian.Uint64(b[4:12]),
	}, nil
}

// ErrorDetail is one MsgError payload: code u16 + UTF-8 message.
type ErrorDetail struct {
	Code    uint16
	Message string
}

// EncodeError encodes an error payload.
func EncodeError(code uint16, msg string) []byte {
	if len(msg) > MaxFramePayload-2 {
		msg = msg[:MaxFramePayload-2]
	}
	out := make([]byte, 2+len(msg))
	binary.BigEndian.PutUint16(out[0:2], code)
	copy(out[2:], msg)
	return out
}

// DecodeError decodes an error payload.
func DecodeError(b []byte) (*ErrorDetail, error) {
	if len(b) < 2 {
		return nil, fmt.Errorf("filefetch: error payload too short")
	}
	return &ErrorDetail{Code: binary.BigEndian.Uint16(b[0:2]), Message: string(b[2:])}, nil
}

// WriteFrame writes one framed message.
func WriteFrame(w io.Writer, typ uint16, payload []byte) error {
	if len(payload) > MaxFramePayload {
		return fmt.Errorf("filefetch: frame of %d bytes exceeds %d", len(payload), MaxFramePayload)
	}
	var hdr [frameHdrLen]byte
	binary.BigEndian.PutUint16(hdr[0:2], frameMagic)
	binary.BigEndian.PutUint16(hdr[2:4], ProtocolVersion)
	binary.BigEndian.PutUint16(hdr[4:6], typ)
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// Frame is one decoded message.
type Frame struct {
	Type    uint16
	Payload []byte
}

// ReadFrame reads one framed message.
func ReadFrame(r io.Reader) (*Frame, error) {
	var hdr [frameHdrLen]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint16(hdr[0:2]) != frameMagic {
		return nil, fmt.Errorf("filefetch: bad frame magic")
	}
	if binary.BigEndian.Uint16(hdr[2:4]) != ProtocolVersion {
		return nil, fmt.Errorf("filefetch: unsupported protocol version %d", binary.BigEndian.Uint16(hdr[2:4]))
	}
	n := binary.BigEndian.Uint32(hdr[8:12])
	if n > MaxFramePayload {
		return nil, fmt.Errorf("filefetch: frame of %d bytes exceeds %d", n, MaxFramePayload)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return &Frame{Type: binary.BigEndian.Uint16(hdr[4:6]), Payload: payload}, nil
}
