package codec

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/marcgauthier/spedsql/ids"
)

const (
	TransactionChunkVersion    uint16 = 1
	TransactionChunkSize              = 64 << 10
	transactionChunkMagic             = "TXCH"
	transactionChunkHeaderSize        = 138
	maxTransactionChunkCount          = 65_536
)

// TransactionChunk is one canonical fragment of an encoded MutationBatch.
// All chunks in a transaction repeat the same identity and whole-batch digest.
type TransactionChunk struct {
	Version     uint16
	Origin      ids.NodeID
	Sequence    uint64
	TxID        ids.TxID
	SchemaEpoch uint64
	SchemaHash  [32]byte
	TotalLength uint64
	Digest      [sha256.Size]byte
	Index       uint32
	Count       uint32
	Data        []byte
}

// EncodeTransactionChunks serializes a batch once, then returns canonical
// 64 KiB frames. A frame is independently bounded; the digest authenticates
// the complete canonical transaction bytes after assembly.
func EncodeTransactionChunks(batch *MutationBatch, maxTransactionBytes int64) ([][]byte, error) {
	frames := make([][]byte, 0)
	err := VisitTransactionChunks(batch, maxTransactionBytes, func(_ uint32, frame []byte) error {
		frames = append(frames, append([]byte(nil), frame...))
		return nil
	})
	return frames, err
}

// VisitTransactionChunks encodes and visits one frame at a time. It retains
// the canonical transaction bytes for digesting but does not accumulate a
// second transaction-sized slice of encoded frames. The callback must consume
// or copy frame before returning.
func VisitTransactionChunks(batch *MutationBatch, maxTransactionBytes int64, visit func(index uint32, frame []byte) error) error {
	if batch == nil || batch.OriginNode.IsZero() || batch.Sequence == 0 || batch.TxID.IsZero() || len(batch.Mutations) == 0 {
		return fmt.Errorf("codec: invalid transaction identity or empty batch")
	}
	if visit == nil {
		return fmt.Errorf("codec: nil transaction chunk visitor")
	}
	raw := EncodeBatch(nil, batch)
	if maxTransactionBytes <= 0 {
		maxTransactionBytes = 64 << 20
	}
	if int64(len(raw)) > maxTransactionBytes {
		return fmt.Errorf("codec: transaction has %d bytes, limit is %d", len(raw), maxTransactionBytes)
	}
	count := (len(raw) + TransactionChunkSize - 1) / TransactionChunkSize
	if count < 1 || count > maxTransactionChunkCount {
		return fmt.Errorf("codec: transaction requires %d chunks, maximum is %d", count, maxTransactionChunkCount)
	}
	digest := sha256.Sum256(raw)
	frameBuf := make([]byte, 0, transactionChunkHeaderSize+TransactionChunkSize)
	for index, start := 0, 0; start < len(raw); index, start = index+1, start+TransactionChunkSize {
		end := start + TransactionChunkSize
		if end > len(raw) {
			end = len(raw)
		}
		chunk := &TransactionChunk{
			Version: TransactionChunkVersion, Origin: batch.OriginNode,
			Sequence: batch.Sequence, TxID: batch.TxID,
			SchemaEpoch: batch.SchemaEpoch, SchemaHash: batch.SchemaHash,
			TotalLength: uint64(len(raw)), Digest: digest,
			Index: uint32(index), Count: uint32(count), Data: raw[start:end],
		}
		frame, err := EncodeTransactionChunk(frameBuf[:0], chunk, maxTransactionBytes)
		if err != nil {
			return err
		}
		if err := visit(uint32(index), frame); err != nil {
			return err
		}
		frameBuf = frame
	}
	return nil
}

// EncodeTransactionChunk encodes one frame after checking its canonical
// index, count, payload length, identity, and total transaction budget.
func EncodeTransactionChunk(dst []byte, chunk *TransactionChunk, maxTransactionBytes int64) ([]byte, error) {
	if err := validateTransactionChunk(chunk, maxTransactionBytes); err != nil {
		return nil, err
	}
	dst = append(dst, transactionChunkMagic...)
	dst = binary.BigEndian.AppendUint16(dst, chunk.Version)
	dst = append(dst, chunk.Origin[:]...)
	dst = binary.BigEndian.AppendUint64(dst, chunk.Sequence)
	dst = append(dst, chunk.TxID[:]...)
	dst = binary.BigEndian.AppendUint64(dst, chunk.SchemaEpoch)
	dst = append(dst, chunk.SchemaHash[:]...)
	dst = binary.BigEndian.AppendUint64(dst, chunk.TotalLength)
	dst = append(dst, chunk.Digest[:]...)
	dst = binary.BigEndian.AppendUint32(dst, chunk.Index)
	dst = binary.BigEndian.AppendUint32(dst, chunk.Count)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(chunk.Data)))
	dst = append(dst, chunk.Data...)
	return dst, nil
}

// DecodeTransactionChunk validates a frame before copying its bounded payload.
func DecodeTransactionChunk(src []byte, maxTransactionBytes int64) (*TransactionChunk, error) {
	if len(src) < transactionChunkHeaderSize {
		return nil, fmt.Errorf("codec: truncated transaction chunk header")
	}
	if string(src[:4]) != transactionChunkMagic {
		return nil, fmt.Errorf("codec: invalid transaction chunk magic")
	}
	c := &TransactionChunk{}
	off := 4
	c.Version = binary.BigEndian.Uint16(src[off : off+2])
	off += 2
	copy(c.Origin[:], src[off:off+16])
	off += 16
	c.Sequence = binary.BigEndian.Uint64(src[off : off+8])
	off += 8
	copy(c.TxID[:], src[off:off+16])
	off += 16
	c.SchemaEpoch = binary.BigEndian.Uint64(src[off : off+8])
	off += 8
	copy(c.SchemaHash[:], src[off:off+32])
	off += 32
	c.TotalLength = binary.BigEndian.Uint64(src[off : off+8])
	off += 8
	copy(c.Digest[:], src[off:off+sha256.Size])
	off += sha256.Size
	c.Index = binary.BigEndian.Uint32(src[off : off+4])
	off += 4
	c.Count = binary.BigEndian.Uint32(src[off : off+4])
	off += 4
	payloadLen := binary.BigEndian.Uint32(src[off : off+4])
	off += 4
	if uint64(payloadLen) != uint64(len(src)-off) {
		return nil, fmt.Errorf("codec: transaction chunk payload length mismatch")
	}
	c.Data = src[off:]
	if err := validateTransactionChunk(c, maxTransactionBytes); err != nil {
		return nil, err
	}
	c.Data = append([]byte(nil), c.Data...)
	return c, nil
}

// AssembleTransactionChunks requires one copy of every chunk, validates
// common metadata and the whole-batch digest, then decodes one complete batch.
// It never returns a partially decoded transaction.
func AssembleTransactionChunks(chunks []*TransactionChunk, limits Limits) (*MutationBatch, error) {
	if len(chunks) == 0 || chunks[0] == nil {
		return nil, fmt.Errorf("codec: no transaction chunks")
	}
	first := chunks[0]
	maxBytes := limits.MaxTransactionBytes
	if maxBytes <= 0 {
		maxBytes = 64 << 20
	}
	if err := validateTransactionChunk(first, maxBytes); err != nil {
		return nil, err
	}
	if uint32(len(chunks)) != first.Count || first.TotalLength > uint64(maxBytes) || first.TotalLength > uint64(math.MaxInt) {
		return nil, fmt.Errorf("codec: incomplete or over-budget transaction chunks")
	}
	ordered := make([]*TransactionChunk, first.Count)
	for _, chunk := range chunks {
		if err := validateTransactionChunk(chunk, maxBytes); err != nil {
			return nil, err
		}
		if !sameTransactionChunk(first, chunk) {
			return nil, fmt.Errorf("codec: transaction chunk metadata mismatch")
		}
		if ordered[chunk.Index] != nil {
			return nil, fmt.Errorf("codec: duplicate transaction chunk %d", chunk.Index)
		}
		ordered[chunk.Index] = chunk
	}
	raw := make([]byte, 0, int(first.TotalLength))
	for i, chunk := range ordered {
		if chunk == nil {
			return nil, fmt.Errorf("codec: missing transaction chunk %d", i)
		}
		raw = append(raw, chunk.Data...)
	}
	if uint64(len(raw)) != first.TotalLength || sha256.Sum256(raw) != first.Digest {
		return nil, fmt.Errorf("codec: transaction digest or total length mismatch")
	}
	batch, rest, err := DecodeBatch(raw, limits)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 || batch.OriginNode != first.Origin || batch.Sequence != first.Sequence || batch.TxID != first.TxID || batch.SchemaEpoch != first.SchemaEpoch || batch.SchemaHash != first.SchemaHash {
		return nil, fmt.Errorf("codec: assembled batch identity mismatch")
	}
	return batch, nil
}

func validateTransactionChunk(c *TransactionChunk, maxTransactionBytes int64) error {
	if c == nil || c.Version != TransactionChunkVersion || c.Origin.IsZero() || c.Sequence == 0 || c.TxID.IsZero() {
		return fmt.Errorf("codec: invalid transaction chunk identity or version")
	}
	if maxTransactionBytes <= 0 {
		maxTransactionBytes = 64 << 20
	}
	if c.TotalLength == 0 || c.TotalLength > uint64(maxTransactionBytes) || c.TotalLength > uint64(math.MaxInt) {
		return fmt.Errorf("codec: transaction length %d exceeds limit %d", c.TotalLength, maxTransactionBytes)
	}
	wantCount := (c.TotalLength + TransactionChunkSize - 1) / TransactionChunkSize
	if c.Count == 0 || c.Count > maxTransactionChunkCount || uint64(c.Count) != wantCount || c.Index >= c.Count {
		return fmt.Errorf("codec: invalid transaction chunk index/count")
	}
	wantLen := uint64(TransactionChunkSize)
	if c.Index == c.Count-1 {
		wantLen = c.TotalLength - uint64(c.Index)*TransactionChunkSize
	}
	if uint64(len(c.Data)) != wantLen {
		return fmt.Errorf("codec: non-canonical transaction chunk length %d, want %d", len(c.Data), wantLen)
	}
	return nil
}

func sameTransactionChunk(a, b *TransactionChunk) bool {
	return a.Version == b.Version && a.Origin == b.Origin && a.Sequence == b.Sequence && a.TxID == b.TxID &&
		a.SchemaEpoch == b.SchemaEpoch && a.SchemaHash == b.SchemaHash && a.TotalLength == b.TotalLength &&
		a.Digest == b.Digest && a.Count == b.Count
}
