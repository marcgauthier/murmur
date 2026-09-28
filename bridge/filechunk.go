package bridge

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// File object transfer across the bridge.
//
// File metadata crosses as ordinary bundle records (see capture and import);
// object bytes cross as recipient-sealed chunk artifacts. Each chunk carries
// plaintext bytes re-encrypted under the bridge recipient key (never the
// Low at-rest key): High validates the envelope, streams the plaintext into
// its own object store, and verifies the replicated digest before the file
// becomes available. Neither domain shares its storage wrapping key.
//
// Chunks are idempotent by (digest, index): re-delivery converges, and a
// conflicting payload for one index quarantines loudly. Lineage matches the
// metadata path: the sealed stream must authorize the signer exactly as
// bundle manifests do.

// FileChunk is one opened object chunk.
type FileChunk struct {
	Stream   string
	Digest   [32]byte
	Index    uint32
	Count    uint32
	TotalLen uint64
	Bytes    []byte
	Signer   [keyIDSize]byte
}

var fileChunkMagic = [4]byte{'S', 'P', 'F', '1'}

const fileChunkVersion uint16 = 1

// encodeFileChunk frames one chunk: magic(4) + version(2) + streamLen(2) +
// stream + digest(32) + index(4) + count(4) + totalLen(8) + bytes.
func encodeFileChunk(stream string, digest [32]byte, index, count uint32, totalLen uint64, chunk []byte) []byte {
	out := make([]byte, 0, 4+2+2+len(stream)+32+4+4+8+len(chunk))
	out = append(out, fileChunkMagic[:]...)
	var tmp [8]byte
	binary.BigEndian.PutUint16(tmp[:2], fileChunkVersion)
	out = append(out, tmp[:2]...)
	binary.BigEndian.PutUint16(tmp[:2], uint16(len(stream)))
	out = append(out, tmp[:2]...)
	out = append(out, stream...)
	out = append(out, digest[:]...)
	binary.BigEndian.PutUint32(tmp[:4], index)
	out = append(out, tmp[:4]...)
	binary.BigEndian.PutUint32(tmp[:4], count)
	out = append(out, tmp[:4]...)
	binary.BigEndian.PutUint64(tmp[:], totalLen)
	out = append(out, tmp[:]...)
	out = append(out, chunk...)
	return out
}

// decodeFileChunk parses and validates one chunk framing.
func decodeFileChunk(b []byte, limits Limits) (*FileChunk, error) {
	if len(b) < 4+2+2+32+4+4+8 {
		return nil, fmt.Errorf("bridge: file chunk of %d bytes is truncated", len(b))
	}
	if string(b[:4]) != string(fileChunkMagic[:]) {
		return nil, fmt.Errorf("bridge: bad file chunk magic")
	}
	if binary.BigEndian.Uint16(b[4:6]) != fileChunkVersion {
		return nil, fmt.Errorf("bridge: unsupported file chunk version %d", binary.BigEndian.Uint16(b[4:6]))
	}
	streamLen := int(binary.BigEndian.Uint16(b[6:8]))
	off := 8
	if streamLen == 0 || streamLen > maxNameLen || len(b) < off+streamLen+32+4+4+8 {
		return nil, fmt.Errorf("bridge: file chunk stream length %d invalid", streamLen)
	}
	stream := string(b[off : off+streamLen])
	off += streamLen
	var digest [32]byte
	copy(digest[:], b[off:off+32])
	off += 32
	index := binary.BigEndian.Uint32(b[off : off+4])
	off += 4
	count := binary.BigEndian.Uint32(b[off : off+4])
	off += 4
	totalLen := binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	payload := b[off:]
	if count == 0 || index >= count {
		return nil, fmt.Errorf("bridge: file chunk %d of %d invalid", index, count)
	}
	if uint64(len(payload)) > uint64(limits.MaxPayloadBytes) {
		return nil, fmt.Errorf("bridge: file chunk of %d bytes exceeds payload limit", len(payload))
	}
	if totalLen > uint64(limits.MaxPayloadBytes)*uint64(count) {
		return nil, fmt.Errorf("bridge: file chunk total %d exceeds payload bounds", totalLen)
	}
	return &FileChunk{Stream: stream, Digest: digest, Index: index, Count: count, TotalLen: totalLen, Bytes: payload}, nil
}

// SealFileChunk seals one plaintext object chunk for the recipient. The
// framing (stream, digest, position, total) travels inside the envelope, so
// tampering fails authentication; the artifact filename repeats the routing
// hint outside it.
func SealFileChunk(signer *SignerKey, recipient [keyIDSize]byte, stream string, digest [32]byte, index, count uint32, totalLen uint64, chunk []byte, limits Limits) ([]byte, error) {
	if signer == nil {
		return nil, fmt.Errorf("bridge: file chunk seal requires a signing key")
	}
	if stream == "" || len(stream) > maxNameLen {
		return nil, fmt.Errorf("bridge: invalid file chunk stream")
	}
	if count == 0 || index >= count {
		return nil, fmt.Errorf("bridge: file chunk %d of %d invalid", index, count)
	}
	// Ciphertext-then-plaintext chunk bytes do not benefit from bundle
	// compression; the envelope seals the framing directly.
	return sealEnvelope(signer, recipient, encodeFileChunk(stream, digest, index, count, totalLen, chunk), limits)
}

// OpenFileChunk verifies and opens one sealed chunk, enforcing the same
// signer/stream authorization as bundles.
func OpenFileChunk(data []byte, trust *TrustStore, limits Limits) (*FileChunk, error) {
	framing, signerID, err := openEnvelope(data, trust, limits)
	if err != nil {
		return nil, err
	}
	chunk, err := decodeFileChunk(framing, limits)
	if err != nil {
		return nil, err
	}
	if !trust.authorized(chunk.Stream, signerID) {
		return nil, fmt.Errorf("bridge: signer is not authorized for stream %q", chunk.Stream)
	}
	chunk.Signer = signerID
	return chunk, nil
}

// FileChunkArtifactName names a chunk sidecar: the filename repeats routing
// (also sealed inside) so receivers and operators can sort without keys.
func FileChunkArtifactName(stream string, digest [32]byte, index, count uint32) string {
	s := sanitizeStream(stream)
	if len(s) > 64 {
		s = s[:64]
	}
	return fmt.Sprintf("fobj-%s-%s-%06d-of-%06d.fobj", s, hex.EncodeToString(digest[:]), index, count)
}

// ParseFileChunkArtifactName extracts routing from a chunk sidecar name.
// Parsing anchors right: the stream segment may itself contain dashes.
func ParseFileChunkArtifactName(name string) (stream string, digest [32]byte, index, count uint32, err error) {
	fail := func() (string, [32]byte, uint32, uint32, error) {
		return "", digest, 0, 0, fmt.Errorf("bridge: malformed file chunk name %q", name)
	}
	if !strings.HasSuffix(name, ".fobj") || !strings.HasPrefix(name, "fobj-") {
		return "", digest, 0, 0, fmt.Errorf("bridge: %q is not a file chunk artifact", name)
	}
	rest := strings.TrimSuffix(strings.TrimPrefix(name, "fobj-"), ".fobj")
	// Trailing "-of-<count>" (the digest and index segments cannot contain
	// it, so the last occurrence is the count separator).
	ofIdx := strings.LastIndex(rest, "-of-")
	if ofIdx < 0 {
		return fail()
	}
	cnt, cerr := strconv.ParseUint(rest[ofIdx+4:], 10, 32)
	if cerr != nil || cnt == 0 {
		return fail()
	}
	rest = rest[:ofIdx]
	// Trailing "-<index>".
	dash := strings.LastIndexByte(rest, '-')
	if dash < 0 {
		return fail()
	}
	idx, ierr := strconv.ParseUint(rest[dash+1:], 10, 32)
	if ierr != nil || uint64(idx) >= uint64(cnt) {
		return fail()
	}
	rest = rest[:dash]
	// Trailing "-<digesthex>".
	dash = strings.LastIndexByte(rest, '-')
	if dash < 0 {
		return fail()
	}
	raw, derr := hex.DecodeString(rest[dash+1:])
	if derr != nil || len(raw) != 32 {
		return fail()
	}
	copy(digest[:], raw)
	stream = rest[:dash]
	if stream == "" {
		return fail()
	}
	return stream, digest, uint32(idx), uint32(cnt), nil
}
