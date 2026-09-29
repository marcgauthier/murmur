package crypto

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

// Encrypted container format (one container per Pebble content file).
//
//	offsets 0..512:   two 256-byte header slots (dual-commit; higher valid
//	                  slotSeq wins, surviving torn header writes)
//	offset 512..:     data records and checkpoint records (append-only)
//
// Header slot (256 bytes):
//
//	magic[8]="NOMADENC" | version u16 | slotSeq u64 | alg u16 | keyID[16]
//	| epochSeq u64 | nonceLen u8 | nonce[32] | sealed[sealedHeaderLen]
//	| reserved (zero-padded to 256)
//
// Sealed header payload (112 bytes):
//
//	dbID[16] | fileID[16] | epochID[32] | checkpointSeq u64
//	| checkpointOffset u64 | recordSeqHi u64 | logicalSize u64
//	| createdAt u64 | keyGen u64
//
// Data record (one sealed extent of a chunk; each spill seals only new bytes,
// so append-heavy files like WALs have ~1x write amplification):
//
//	type u8=1 | chunkIndex u64 | recordSeq u64 | epochSeq u64 | epochID[32]
//	| chunkOffset u32 | plainLen u32 | nonceLen u8 | nonce | sealedLen u32
//	| sealed
//
// A chunk's plaintext is the seq-ordered layering of its extents over zeros.
// Extents may overlap; the newest recordSeq wins per byte.
//
// Checkpoint record:
//
//	type u8=2 | checkpointSeq u64 | epochSeq u64 | epochID[32] | logicalSize u64
//	| recordSeqHi u64 | committedEnd u64 | nonceLen u8 | nonce | sealedLen u32
//	| sealed
//
// Checkpoint payload: entryCount u32 + entries of
//
//	chunkIndex u64 | newestSeq u64 | chunkLen u32 | baseSeq u64
//
// committedEnd bounds the committed record region (bytes past it are an
// uncommitted torn tail and are ignored). Extent lists are rebuilt at open by
// scanning record envelopes in [512, committedEnd); baseSeq prunes extents
// superseded by a merged positional record.
//
// Key hierarchy (all HKDF-SHA256 with distinct purpose labels):
//
//	dataKey (registry, per generation)
//	  -> fileKey = HKDF(dataKey, salt=fileID, "nomadsql/file-key/v1"|...)
//	  -> epochKey = HKDF(fileKey, salt=epochID, "nomadsql/epoch-key/v1"|...)
//
// Record nonces are monotonically increasing counters encoded big-endian at
// the adapter's nonce size, starting at 0 per epoch; epochs cap at 2^32
// records, then a fresh epoch starts. (Key, nonce) pairs never repeat:
// epoch keys are unique per writable reopen. Headers use random nonces
// under a fixed header key (HKDF(dataKey, salt=keyID,
// "nomadsql/file-header/v1"|...)) since readers derive it before learning
// the file id.
//
// AAD binds dbID, fileID, algorithm, key id, epoch, sequence counters,
// chunk index, offsets, and lengths, so cross-file splices, replays, and
// reordered records all fail authentication.
const (
	fileMagic       = "NOMADENC"
	fileVersion     = 1
	headerSlotLen   = 256
	headerSlotCount = 2
	headerTotalLen  = headerSlotLen * headerSlotCount

	sealedHeaderPayloadLen = 112
	sealedHeaderLen        = sealedHeaderPayloadLen + 16 // AES/GCM-style 16-byte tag

	recordTypeData       = 1
	recordTypeCheckpoint = 2

	// ChunkSize is the plaintext chunk granularity (64 KiB).
	ChunkSize = 64 << 10

	maxNonceLen = 32
)

// FileIDLen is the length of random file ids.
const FileIDLen = 16

// EpochIDLen is the length of random writer-epoch ids.
const EpochIDLen = 32

var (
	fileMagicBytes = []byte(fileMagic)
	chunkAADPrefix = []byte("chunk-record")
	ckptAADPrefix  = []byte("checkpoint")
)

// fileHeader is a verified header slot.
type fileHeader struct {
	slotSeq          uint64
	alg              AlgorithmID
	keyID            [KeyIDLen]byte
	epochSeq         uint64
	dbID             [16]byte
	fileID           [FileIDLen]byte
	epochID          [EpochIDLen]byte
	checkpointSeq    uint64
	checkpointOffset uint64
	recordSeqHi      uint64
	logicalSize      uint64
	createdAt        uint64
	keyGen           uint64
}

// encodeHeaderSlot renders a 256-byte header slot. Headers use the fixed
// header-subkey domain so readers can derive it before learning the epoch.
func encodeHeaderSlot(h *fileHeader, dataKey []byte) ([]byte, error) {
	sub, err := DeriveSubkey(dataKey, h.alg, h.keyID[:], fileHeaderInfo(h.alg, h.keyID[:], h.dbID[:]))
	if err != nil {
		return nil, err
	}
	defer Zero(sub)
	aead, err := h.alg.NewAEAD(sub)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	var payload [sealedHeaderPayloadLen]byte
	copy(payload[0:16], h.dbID[:])
	copy(payload[16:32], h.fileID[:])
	copy(payload[32:64], h.epochID[:])
	binary.LittleEndian.PutUint64(payload[64:72], h.checkpointSeq)
	binary.LittleEndian.PutUint64(payload[72:80], h.checkpointOffset)
	binary.LittleEndian.PutUint64(payload[80:88], h.recordSeqHi)
	binary.LittleEndian.PutUint64(payload[88:96], h.logicalSize)
	binary.LittleEndian.PutUint64(payload[96:104], h.createdAt)
	binary.LittleEndian.PutUint64(payload[104:112], h.keyGen)
	slot := make([]byte, headerSlotLen)
	copy(slot[0:8], fileMagicBytes)
	binary.LittleEndian.PutUint16(slot[8:10], fileVersion)
	binary.LittleEndian.PutUint64(slot[10:18], h.slotSeq)
	binary.LittleEndian.PutUint16(slot[18:20], uint16(h.alg))
	copy(slot[20:36], h.keyID[:])
	binary.LittleEndian.PutUint64(slot[36:44], h.epochSeq)
	slot[44] = byte(len(nonce))
	copy(slot[45:77], nonce)
	// AAD covers envelope bytes 0..77; sealed blob at 77..77+sealedHeaderLen.
	aad := slot[:77]
	sealed := aead.Seal(nil, nonce, payload[:], aad)
	if len(sealed) != sealedHeaderLen {
		return nil, fmt.Errorf("crypto: unexpected header seal length %d", len(sealed))
	}
	copy(slot[77:77+sealedHeaderLen], sealed)
	return slot, nil
}

// decodeHeaderSlot verifies one slot. It returns ErrAuth/ErrCorrupt on any
// tamper or structural problem.
func decodeHeaderSlot(slot []byte, dbID [16]byte, getKey func([KeyIDLen]byte) ([]byte, AlgorithmID, error)) (*fileHeader, error) {
	if len(slot) != headerSlotLen {
		return nil, fmt.Errorf("%w: header slot length %d", ErrCorrupt, len(slot))
	}
	if !bytes.Equal(slot[0:8], fileMagicBytes) {
		return nil, fmt.Errorf("%w: bad file magic", ErrCorrupt)
	}
	if binary.LittleEndian.Uint16(slot[8:10]) != fileVersion {
		return nil, fmt.Errorf("%w: file version %d", ErrCorrupt, binary.LittleEndian.Uint16(slot[8:10]))
	}
	alg := AlgorithmID(binary.LittleEndian.Uint16(slot[18:20]))
	if _, err := alg.KeySize(); err != nil {
		return nil, fmt.Errorf("%w: file algorithm: %v", ErrCorrupt, err)
	}
	var keyID [KeyIDLen]byte
	copy(keyID[:], slot[20:36])
	nonceLen := int(slot[44])
	if nonceLen <= 0 || nonceLen > maxNonceLen {
		return nil, fmt.Errorf("%w: header nonce length %d", ErrCorrupt, nonceLen)
	}
	nonce := append([]byte(nil), slot[45:45+nonceLen]...)
	// Reserved bytes must be zero (tamper in the padding is corruption).
	for _, b := range slot[77+sealedHeaderLen:] {
		if b != 0 {
			return nil, fmt.Errorf("%w: header reserved bytes", ErrCorrupt)
		}
	}
	dataKey, keyAlg, err := getKey(keyID)
	if err != nil {
		return nil, err
	}
	defer Zero(dataKey)
	if keyAlg != alg {
		return nil, fmt.Errorf("%w: header/registry algorithm mismatch", ErrAuth)
	}
	// Headers use the fixed header-subkey domain: readers derive it before
	// learning the sealed epoch id.
	sub, err := DeriveSubkey(dataKey, alg, keyID[:], fileHeaderInfo(alg, keyID[:], dbID[:]))
	if err != nil {
		return nil, err
	}
	defer Zero(sub)
	aead, err := alg.NewAEAD(sub)
	if err != nil {
		return nil, err
	}
	if aead.NonceSize() != nonceLen {
		return nil, fmt.Errorf("%w: header nonce length %d for %s", ErrCorrupt, nonceLen, alg)
	}
	payload, err := aead.Open(nil, nonce, slot[77:77+sealedHeaderLen], slot[:77])
	if err != nil {
		return nil, fmt.Errorf("%w: header seal: %v", ErrAuth, err)
	}
	h := &fileHeader{
		slotSeq:          binary.LittleEndian.Uint64(slot[10:18]),
		alg:              alg,
		keyID:            keyID,
		epochSeq:         binary.LittleEndian.Uint64(slot[36:44]),
		checkpointSeq:    binary.LittleEndian.Uint64(payload[64:72]),
		checkpointOffset: binary.LittleEndian.Uint64(payload[72:80]),
		recordSeqHi:      binary.LittleEndian.Uint64(payload[80:88]),
		logicalSize:      binary.LittleEndian.Uint64(payload[88:96]),
		createdAt:        binary.LittleEndian.Uint64(payload[96:104]),
		keyGen:           binary.LittleEndian.Uint64(payload[104:112]),
	}
	copy(h.dbID[:], payload[0:16])
	copy(h.fileID[:], payload[16:32])
	copy(h.epochID[:], payload[32:64])
	if h.dbID != dbID {
		return nil, fmt.Errorf("%w: file belongs to another database", ErrAuth)
	}
	if h.checkpointOffset != 0 && h.checkpointOffset < headerTotalLen {
		return nil, fmt.Errorf("%w: checkpoint offset %d", ErrCorrupt, h.checkpointOffset)
	}
	if h.logicalSize > 1<<60 {
		return nil, fmt.Errorf("%w: logical size absurd", ErrCorrupt)
	}
	return h, nil
}

// putCounter encodes a record counter big-endian into a nonce buffer.
func putCounter(nonce []byte, counter uint32) {
	for i := len(nonce) - 1; i >= 0 && counter > 0; i-- {
		// Fill from the least significant byte; leading bytes stay zero.
		shift := uint(len(nonce)-1-i) * 8
		if shift >= 32 {
			break
		}
		nonce[i] = byte(counter >> shift)
	}
}

// fileKeyInfo builds the HKDF info binding for per-file keys.
func fileKeyInfo(alg AlgorithmID, keyID, fileID, dbID []byte) []byte {
	var info bytes.Buffer
	info.Write([]byte("nomadsql/file-key/v1"))
	var tmp [2]byte
	binary.LittleEndian.PutUint16(tmp[:], uint16(alg))
	info.Write(tmp[:])
	info.Write(keyID)
	info.Write(fileID)
	info.Write(dbID)
	return info.Bytes()
}

// epochKeyInfo builds the HKDF info binding for writer-epoch keys.
func epochKeyInfo(alg AlgorithmID, keyID []byte, epochSeq uint64, epochID, fileID []byte) []byte {
	var info bytes.Buffer
	info.Write([]byte("nomadsql/epoch-key/v1"))
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[:2], uint16(alg))
	info.Write(tmp[:2])
	info.Write(keyID)
	binary.LittleEndian.PutUint64(tmp[:], epochSeq)
	info.Write(tmp[:])
	info.Write(epochID)
	info.Write(fileID)
	return info.Bytes()
}

// deriveFileKey derives a file's 32-byte key from its generation data key.
func deriveFileKey(dataKey []byte, alg AlgorithmID, keyID, fileID, dbID []byte) ([]byte, error) {
	if len(dataKey) != 32 {
		return nil, fmt.Errorf("crypto: data key must be 32 bytes, got %d", len(dataKey))
	}
	out := make([]byte, 32)
	r := hkdf.New(sha256.New, dataKey, fileID, fileKeyInfo(alg, keyID, fileID, dbID))
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, fmt.Errorf("crypto: hkdf: %w", err)
	}
	return out, nil
}

// deriveEpochKey derives one epoch's algorithm-sized key from the file key.
func deriveEpochKey(fileKey []byte, alg AlgorithmID, keyID []byte, epochSeq uint64, epochID, fileID []byte) ([]byte, error) {
	if len(fileKey) != 32 {
		return nil, fmt.Errorf("crypto: file key must be 32 bytes, got %d", len(fileKey))
	}
	out := make([]byte, alg.mustKeySize())
	r := hkdf.New(sha256.New, fileKey, epochID, epochKeyInfo(alg, keyID, epochSeq, epochID, fileID))
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, fmt.Errorf("crypto: hkdf: %w", err)
	}
	return out, nil
}

// fileHeaderInfo builds the HKDF info binding for header subkeys. Headers
// use their own domain (not the epoch domain) so readers can derive the
// header subkey before learning the epoch id.
func fileHeaderInfo(alg AlgorithmID, keyID, dbID []byte) []byte {
	var info bytes.Buffer
	info.Write([]byte("nomadsql/file-header/v1"))
	var tmp [2]byte
	binary.LittleEndian.PutUint16(tmp[:], uint16(alg))
	info.Write(tmp[:])
	info.Write(keyID)
	info.Write(dbID)
	return info.Bytes()
}

// sealRecord seals plaintext for one record envelope, returning the full
// record bytes (envelope + sealedLen + sealed). The nonce is the record
// counter encoded big-endian at the adapter's nonce size.
func sealRecord(recType byte, fixed []byte, plaintext, aad []byte, aead interface {
	NonceSize() int
	Seal(dst, nonce, plaintext, additionalData []byte) []byte
}, counter uint32,
) ([]byte, error) {
	nonce := make([]byte, aead.NonceSize())
	putCounter(nonce, counter)
	sealed := aead.Seal(nil, nonce, plaintext, aad)
	var rec bytes.Buffer
	rec.WriteByte(recType)
	rec.Write(fixed)
	rec.WriteByte(byte(len(nonce)))
	rec.Write(nonce)
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(sealed)))
	rec.Write(tmp[:])
	rec.Write(sealed)
	return rec.Bytes(), nil
}

// dataFixedLen is the fixed envelope length of a data record.
const dataFixedLen = 8 + 8 + 8 + EpochIDLen + 4 + 4

// chunkEnvelope renders the fixed part of a data record.
func chunkEnvelope(chunkIndex, recordSeq, epochSeq uint64, epochID []byte, chunkOffset, plainLen uint32) []byte {
	env := make([]byte, dataFixedLen)
	binary.LittleEndian.PutUint64(env[0:8], chunkIndex)
	binary.LittleEndian.PutUint64(env[8:16], recordSeq)
	binary.LittleEndian.PutUint64(env[16:24], epochSeq)
	copy(env[24:24+EpochIDLen], epochID)
	binary.LittleEndian.PutUint32(env[24+EpochIDLen:], chunkOffset)
	binary.LittleEndian.PutUint32(env[28+EpochIDLen:], plainLen)
	return env
}

// chunkAAD binds a data record to its file, epoch, position, and length.
func chunkAAD(dbID, fileID []byte, alg AlgorithmID, keyID []byte, env []byte) []byte {
	aad := make([]byte, 0, len(chunkAADPrefix)+16+16+2+16+len(env))
	aad = append(aad, chunkAADPrefix...)
	aad = append(aad, dbID...)
	aad = append(aad, fileID...)
	var tmp [2]byte
	binary.LittleEndian.PutUint16(tmp[:], uint16(alg))
	aad = append(aad, tmp[:]...)
	aad = append(aad, keyID...)
	aad = append(aad, env...)
	return aad
}

// ckptFixedLen is the fixed envelope length of a checkpoint record.
const ckptFixedLen = 8 + 8 + EpochIDLen + 8 + 8 + 8

// checkpointEnvelope renders the fixed part of a checkpoint record.
func checkpointEnvelope(checkpointSeq, epochSeq uint64, epochID []byte, logicalSize, recordSeqHi, committedEnd uint64) []byte {
	env := make([]byte, ckptFixedLen)
	binary.LittleEndian.PutUint64(env[0:8], checkpointSeq)
	binary.LittleEndian.PutUint64(env[8:16], epochSeq)
	copy(env[16:16+EpochIDLen], epochID)
	binary.LittleEndian.PutUint64(env[16+EpochIDLen:], logicalSize)
	binary.LittleEndian.PutUint64(env[24+EpochIDLen:], recordSeqHi)
	binary.LittleEndian.PutUint64(env[32+EpochIDLen:], committedEnd)
	return env
}

// checkpointAAD binds a checkpoint record to its file, epoch, and counters.
func checkpointAAD(dbID, fileID []byte, alg AlgorithmID, keyID []byte, env []byte) []byte {
	aad := make([]byte, 0, len(ckptAADPrefix)+16+16+2+16+len(env))
	aad = append(aad, ckptAADPrefix...)
	aad = append(aad, dbID...)
	aad = append(aad, fileID...)
	var tmp [2]byte
	binary.LittleEndian.PutUint16(tmp[:], uint16(alg))
	aad = append(aad, tmp[:]...)
	aad = append(aad, keyID...)
	aad = append(aad, env...)
	return aad
}

// chunkCommit is one checkpoint entry: the newest sealed extent of a chunk,
// its logical length, and the merge base that prunes older extents.
type chunkCommit struct {
	chunkIndex uint64
	newestSeq  uint64
	chunkLen   uint32
	baseSeq    uint64
}

const chunkCommitLen = 8 + 8 + 4 + 8

// encodeCheckpointPayload renders the sealed checkpoint body.
func encodeCheckpointPayload(entries []chunkCommit) []byte {
	out := make([]byte, 4+len(entries)*chunkCommitLen)
	binary.LittleEndian.PutUint32(out[0:4], uint32(len(entries)))
	off := 4
	for _, e := range entries {
		binary.LittleEndian.PutUint64(out[off:off+8], e.chunkIndex)
		binary.LittleEndian.PutUint64(out[off+8:off+16], e.newestSeq)
		binary.LittleEndian.PutUint32(out[off+16:off+20], e.chunkLen)
		binary.LittleEndian.PutUint64(out[off+20:off+28], e.baseSeq)
		off += chunkCommitLen
	}
	return out
}

// decodeCheckpointPayload parses a verified checkpoint body.
func decodeCheckpointPayload(body []byte) ([]chunkCommit, error) {
	if len(body) < 4 {
		return nil, fmt.Errorf("%w: checkpoint body truncated", ErrCorrupt)
	}
	count := binary.LittleEndian.Uint32(body[0:4])
	if uint64(len(body)) != 4+uint64(count)*chunkCommitLen {
		return nil, fmt.Errorf("%w: checkpoint body length", ErrCorrupt)
	}
	entries := make([]chunkCommit, count)
	off := 4
	for i := range entries {
		entries[i] = chunkCommit{
			chunkIndex: binary.LittleEndian.Uint64(body[off : off+8]),
			newestSeq:  binary.LittleEndian.Uint64(body[off+8 : off+16]),
			chunkLen:   binary.LittleEndian.Uint32(body[off+16 : off+20]),
			baseSeq:    binary.LittleEndian.Uint64(body[off+20 : off+28]),
		}
		if entries[i].chunkLen == 0 || entries[i].chunkLen > ChunkSize {
			return nil, fmt.Errorf("%w: checkpoint chunk length", ErrCorrupt)
		}
		if entries[i].newestSeq == 0 || entries[i].baseSeq > entries[i].newestSeq {
			return nil, fmt.Errorf("%w: checkpoint seqs", ErrCorrupt)
		}
		off += chunkCommitLen
	}
	return entries, nil
}

// extent is one scanned data record.
type extent struct {
	recordSeq   uint64
	fileOffset  uint64
	chunkOffset uint32
	plainLen    uint32
	epochSeq    uint64
	epochID     [EpochIDLen]byte
	recLen      int64 // total on-disk record length
}

// recordSizes bounds record parsing.
const maxSealedRecordLen = ChunkSize + 64
