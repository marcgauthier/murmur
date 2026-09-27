package crypto

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// Tuning for the encrypted container.
const (
	// maxResidentBufs bounds buffered plaintext chunks per open file (64
	// x 64 KiB = 4 MiB). Overflow seals pending ranges and evicts buffers;
	// evicted chunks reload from extents on demand.
	maxResidentBufs = 64
	// maxExtentsPerChunk merges a chunk's extents into one positional
	// record once the in-memory list exceeds this, bounding read layering.
	maxExtentsPerChunk = 256
	// compactMinGarbage triggers a rewrite once stale bytes exceed it.
	compactMinGarbage = 4 << 20
)

// chunkState is one chunk's in-memory state: sealed extents plus an
// optional resident plaintext buffer with an unsealed [lo,hi) range.
type chunkState struct {
	newestSeq uint64
	chunkLen  uint32
	baseSeq   uint64
	extents   []extent // recordSeq ascending
	buf       []byte   // len ChunkSize when resident
	lo, hi    uint32
	clean     bool // no unsealed bytes in buf
	resident  bool
}

// epochAEAD caches one epoch's derived AEAD.
type epochAEAD struct {
	id   [EpochIDLen]byte
	aead epochAEADInner
}

type epochAEADInner interface {
	NonceSize() int
	Overhead() int
	Seal(dst, nonce, plaintext, additionalData []byte) []byte
	Open(dst, nonce, ciphertext, additionalData []byte) ([]byte, error)
}

// encryptedFile implements vfs.File over an encrypted container.
type encryptedFile struct {
	fs       *EncryptedFS
	f        vfs.File
	name     string
	category vfs.DiskWriteCategory
	readOnly bool

	dataKey []byte // 32 bytes; nil until first commit for fresh files
	fileKey []byte // HKDF-derived per-file key (32 bytes)
	hdr     fileHeader
	hasHdr  bool

	// Live write state.
	epochSeq      uint64
	epochID       [EpochIDLen]byte
	recCounter    uint32 // record counter within the epoch (nonces)
	recordSeqHi   uint64
	checkpointSeq uint64
	logicalSize   uint64
	appendOffset  int64
	committedEnd  uint64
	chunks        map[uint64]*chunkState
	committed     bool
	liveSealed    int64
	prevCkptLen   int64
	fileID        [FileIDLen]byte
	keyID         [KeyIDLen]byte
	hasKey        bool
	alg           AlgorithmID

	pos    int64
	closed bool

	mu        sync.Mutex
	lastChunk uint64
	lastPlain []byte
	lastOK    bool
	epochs    map[uint64]*epochAEAD

	stats *FSStats // shared counters (atomics inside)
}

var _ vfs.File = (*encryptedFile)(nil)

// openEncrypted opens an existing container; writeMode bumps the writer epoch.
func openEncrypted(fs *EncryptedFS, f vfs.File, name string, cat vfs.DiskWriteCategory, size int64, writeMode bool) (*encryptedFile, error) {
	ef := &encryptedFile{
		fs: fs, f: f, name: name, category: cat,
		readOnly: !writeMode,
		chunks:   make(map[uint64]*chunkState),
		epochs:   make(map[uint64]*epochAEAD),
		stats:    &fs.stats,
	}
	if size == 0 {
		// Lenient-empty: a zero-byte file never held a commit (any Sync
		// writes headers first), so it reads as empty. This matches stock
		// Pebble, which replays empty WALs as valid, and keeps crash
		// recovery working when a crash lands between Create and the first
		// write. Counted for tamper monitoring.
		fs.stats.ShortFiles.Add(1)
		if writeMode {
			if err := ef.startFresh(); err != nil {
				f.Close()
				return nil, err
			}
			return ef, nil
		}
		ef.readOnly = true
		ef.committed = true
		return ef, nil
	}
	if size < headerTotalLen {
		f.Close()
		return nil, fmt.Errorf("%w: %s: short file (%d bytes)", ErrCorrupt, name, size)
	}
	hdr, err := readHeader(f, fs.dbID, func(id [KeyIDLen]byte) ([]byte, AlgorithmID, error) {
		return fs.reg.GetKey(bgCtx(), id)
	})
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	dataKey, alg, err := fs.reg.GetKey(bgCtx(), hdr.keyID)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	ef.dataKey = dataKey
	ef.alg = alg
	fk, err := deriveFileKey(dataKey, alg, hdr.keyID[:], hdr.fileID[:], hdr.dbID[:])
	if err != nil {
		f.Close()
		Zero(dataKey)
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	ef.fileKey = fk
	ef.hdr = *hdr
	ef.hasHdr = true
	ef.fileID = hdr.fileID
	ef.keyID = hdr.keyID
	ef.hasKey = true
	ef.logicalSize = hdr.logicalSize
	ef.recordSeqHi = hdr.recordSeqHi
	ef.checkpointSeq = hdr.checkpointSeq
	ef.appendOffset = size
	if hdr.checkpointOffset != 0 {
		commits, ckptLen, committedEnd, err := ef.readCheckpoint(hdr)
		if err != nil {
			f.Close()
			Zero(dataKey)
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		ef.prevCkptLen = ckptLen
		ef.committedEnd = committedEnd
		if err := ef.scanExtents(commits, committedEnd); err != nil {
			f.Close()
			Zero(dataKey)
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		ef.liveSealed = ckptLen
		for _, cs := range ef.chunks {
			for _, e := range cs.extents {
				ef.liveSealed += e.recLen
			}
		}
	}
	if writeMode {
		// New writer epoch per write-open; the bump commits at first Sync.
		ef.epochSeq = hdr.epochSeq + 1
		if _, err := rand.Read(ef.epochID[:]); err != nil {
			f.Close()
			return nil, err
		}
	} else {
		ef.epochSeq = hdr.epochSeq
		ef.epochID = hdr.epochID
		ef.committed = true
	}
	return ef, nil
}

// startFresh initializes write state for a new (or zero-byte) file. New
// files use the active generation key and algorithm.
func (ef *encryptedFile) startFresh() error {
	if _, err := rand.Read(ef.fileID[:]); err != nil {
		return err
	}
	// Rotate the active key first when it expired (best effort: a rotation
	// failure must not wedge file creation; the current key stays valid).
	if check := ef.fs.rotationCheck; check != nil {
		if err := check(); err != nil {
			ef.fs.log.Warn("expiry rotation failed", "err", err.Error())
		}
	}
	active, err := ef.fs.reg.ActiveKey(bgCtx())
	if err != nil {
		return err
	}
	defer Zero(active.Key)
	ef.alg = active.Alg
	ef.keyID = active.ID
	ef.dataKey = bytesClone(active.Key)
	ef.hasKey = true
	fk, err := deriveFileKey(active.Key, active.Alg, active.ID[:], ef.fileID[:], ef.fs.dbID[:])
	if err != nil {
		Zero(ef.dataKey)
		ef.dataKey = nil
		ef.hasKey = false
		return err
	}
	ef.fileKey = fk
	ef.epochSeq = 1
	if _, err := rand.Read(ef.epochID[:]); err != nil {
		return err
	}
	ef.appendOffset = headerTotalLen
	ef.committed = false
	return nil
}

func bytesClone(b []byte) []byte { return append([]byte(nil), b...) }

// readHeader verifies both slots and returns the newest valid one. Pristine
// zero slots (never written) are skipped; any non-zero invalid slot fails
// closed. Crash windows only produce zero or complete slots because header
// writes are single-sector, so a partial slot means tamper or catastrophic
// storage failure. This makes every header byte tamper-evident.
func readHeader(f vfs.File, dbID [16]byte, getKey func([KeyIDLen]byte) ([]byte, AlgorithmID, error)) (*fileHeader, error) {
	slots := make([]byte, headerTotalLen)
	if _, err := io.ReadFull(&sectionReader{f: f}, slots); err != nil {
		return nil, fmt.Errorf("%w: reading header: %v", ErrCorrupt, err)
	}
	slotA := slots[0:headerSlotLen]
	slotB := slots[headerSlotLen:headerTotalLen]
	zeroA, zeroB := isZeroSlot(slotA), isZeroSlot(slotB)
	if zeroA && zeroB {
		return nil, fmt.Errorf("%w: no valid header", ErrCorrupt)
	}
	var hA, hB *fileHeader
	if !zeroA {
		var err error
		if hA, err = decodeHeaderSlot(slotA, dbID, getKey); err != nil {
			return nil, err
		}
	}
	if !zeroB {
		var err error
		if hB, err = decodeHeaderSlot(slotB, dbID, getKey); err != nil {
			return nil, err
		}
	}
	switch {
	case hA != nil && hB != nil:
		if hB.slotSeq > hA.slotSeq {
			return hB, nil
		}
		return hA, nil
	case hA != nil:
		return hA, nil
	default:
		return hB, nil
	}
}

func isZeroSlot(slot []byte) bool {
	for _, b := range slot {
		if b != 0 {
			return false
		}
	}
	return true
}

// sectionReader reads sequentially from a vfs.File at an offset.
type sectionReader struct {
	f   vfs.File
	off int64
}

func (s *sectionReader) Read(p []byte) (int, error) {
	n, err := s.f.ReadAt(p, s.off)
	s.off += int64(n)
	return n, err
}

// rawRecord is one parsed (not verified) record envelope.
type rawRecord struct {
	typ       byte
	fixed     []byte // fixed envelope bytes (for AAD rebuild)
	nonce     []byte
	sealedOff int64
	sealedLen uint32
	totalLen  int64
}

// readRawRecord parses the envelope of the record at off.
func readRawRecord(f vfs.File, off int64) (*rawRecord, error) {
	head := make([]byte, 1+ckptFixedLen+1+maxNonceLen+4)
	n, err := f.ReadAt(head, off)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("%w: record read: %v", ErrCorrupt, err)
	}
	head = head[:n]
	if len(head) < 2 {
		return nil, fmt.Errorf("%w: record truncated", ErrCorrupt)
	}
	var fixedLen int
	var maxSealed uint32
	switch head[0] {
	case recordTypeData:
		fixedLen, maxSealed = dataFixedLen, maxSealedRecordLen
	case recordTypeCheckpoint:
		fixedLen, maxSealed = ckptFixedLen, 1<<30
	default:
		return nil, fmt.Errorf("%w: record type %d", ErrCorrupt, head[0])
	}
	if len(head) < 1+fixedLen+1 {
		return nil, fmt.Errorf("%w: record envelope truncated", ErrCorrupt)
	}
	fixed := append([]byte(nil), head[1:1+fixedLen]...)
	nonceLen := int(head[1+fixedLen])
	if nonceLen <= 0 || nonceLen > maxNonceLen || len(head) < 1+fixedLen+1+nonceLen+4 {
		return nil, fmt.Errorf("%w: record envelope", ErrCorrupt)
	}
	nonce := append([]byte(nil), head[1+fixedLen+1:1+fixedLen+1+nonceLen]...)
	sealedLen := binary.LittleEndian.Uint32(head[1+fixedLen+1+nonceLen:])
	if sealedLen > maxSealed {
		return nil, fmt.Errorf("%w: record size absurd", ErrCorrupt)
	}
	prefixLen := int64(1 + fixedLen + 1 + nonceLen + 4)
	return &rawRecord{
		typ: head[0], fixed: fixed, nonce: nonce,
		sealedOff: off + prefixLen, sealedLen: sealedLen,
		totalLen: prefixLen + int64(sealedLen),
	}, nil
}

func readSealed(f vfs.File, r *rawRecord) ([]byte, error) {
	sealed := make([]byte, r.sealedLen)
	if _, err := io.ReadFull(&sectionReader{f: f, off: r.sealedOff}, sealed); err != nil {
		return nil, fmt.Errorf("%w: record body: %v", ErrCorrupt, err)
	}
	return sealed, nil
}

// readCheckpoint verifies and decodes the committed checkpoint record,
// returning the chunk commits, the checkpoint record length, and the
// committed end offset.
func (ef *encryptedFile) readCheckpoint(hdr *fileHeader) ([]chunkCommit, int64, uint64, error) {
	r, err := readRawRecord(ef.f, int64(hdr.checkpointOffset))
	if err != nil {
		return nil, 0, 0, err
	}
	if r.typ != recordTypeCheckpoint {
		return nil, 0, 0, fmt.Errorf("%w: checkpoint type %d", ErrCorrupt, r.typ)
	}
	env := r.fixed
	ckptSeq := binary.LittleEndian.Uint64(env[0:8])
	epochSeq := binary.LittleEndian.Uint64(env[8:16])
	var epochID [EpochIDLen]byte
	copy(epochID[:], env[16:16+EpochIDLen])
	logicalSize := binary.LittleEndian.Uint64(env[16+EpochIDLen:])
	recordSeqHi := binary.LittleEndian.Uint64(env[24+EpochIDLen:])
	committedEnd := binary.LittleEndian.Uint64(env[32+EpochIDLen:])
	if ckptSeq != hdr.checkpointSeq || epochSeq != hdr.epochSeq ||
		epochID != hdr.epochID || logicalSize != hdr.logicalSize ||
		recordSeqHi != hdr.recordSeqHi {
		return nil, 0, 0, fmt.Errorf("%w: checkpoint does not match committed header", ErrAuth)
	}
	if committedEnd != hdr.checkpointOffset+uint64(r.totalLen) {
		return nil, 0, 0, fmt.Errorf("%w: committed end mismatch", ErrAuth)
	}
	if committedEnd < headerTotalLen {
		return nil, 0, 0, fmt.Errorf("%w: committed end", ErrCorrupt)
	}
	sealed, err := readSealed(ef.f, r)
	if err != nil {
		return nil, 0, 0, err
	}
	aead, err := ef.epochCipher(epochSeq, epochID)
	if err != nil {
		return nil, 0, 0, err
	}
	aad := checkpointAAD(hdr.dbID[:], hdr.fileID[:], hdr.alg, hdr.keyID[:], env)
	body, err := aead.Open(nil, r.nonce, sealed, aad)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("%w: checkpoint seal: %v", ErrAuth, err)
	}
	entries, err := decodeCheckpointPayload(body)
	if err != nil {
		return nil, 0, 0, err
	}
	seen := make(map[uint64]bool, len(entries))
	for _, e := range entries {
		if e.newestSeq > hdr.recordSeqHi {
			return nil, 0, 0, fmt.Errorf("%w: checkpoint recordSeq", ErrCorrupt)
		}
		if seen[e.chunkIndex] {
			return nil, 0, 0, fmt.Errorf("%w: duplicate checkpoint chunk", ErrCorrupt)
		}
		seen[e.chunkIndex] = true
	}
	return entries, r.totalLen, committedEnd, nil
}

// scanExtents rebuilds extent lists by parsing envelopes in
// [headerTotalLen, committedEnd). Bytes past committedEnd are an uncommitted
// torn tail and are ignored.
func (ef *encryptedFile) scanExtents(commits []chunkCommit, committedEnd uint64) error {
	byChunk := make(map[uint64]*chunkState, len(commits))
	for _, c := range commits {
		byChunk[c.chunkIndex] = &chunkState{
			newestSeq: c.newestSeq, chunkLen: c.chunkLen, baseSeq: c.baseSeq,
			clean: true,
		}
	}
	var lastDataSeq, lastCkptSeq uint64
	for off := uint64(headerTotalLen); off < committedEnd; {
		r, err := readRawRecord(ef.f, int64(off))
		if err != nil {
			return err
		}
		if off+uint64(r.totalLen) > committedEnd {
			return fmt.Errorf("%w: record crosses committed end", ErrCorrupt)
		}
		switch r.typ {
		case recordTypeData:
			env := r.fixed
			chunk := binary.LittleEndian.Uint64(env[0:8])
			seq := binary.LittleEndian.Uint64(env[8:16])
			epochSeq := binary.LittleEndian.Uint64(env[16:24])
			var epochID [EpochIDLen]byte
			copy(epochID[:], env[24:24+EpochIDLen])
			chunkOff := binary.LittleEndian.Uint32(env[24+EpochIDLen:])
			plainLen := binary.LittleEndian.Uint32(env[28+EpochIDLen:])
			if seq <= lastDataSeq {
				return fmt.Errorf("%w: record seq order", ErrAuth)
			}
			lastDataSeq = seq
			if seq > ef.recordSeqHi {
				return fmt.Errorf("%w: record seq %d beyond committed %d", ErrCorrupt, seq, ef.recordSeqHi)
			}
			if plainLen == 0 || plainLen > ChunkSize || uint64(chunkOff)+uint64(plainLen) > ChunkSize {
				return fmt.Errorf("%w: extent bounds", ErrCorrupt)
			}
			cs, ok := byChunk[chunk]
			if !ok {
				return fmt.Errorf("%w: extent for unknown chunk", ErrCorrupt)
			}
			cs.extents = append(cs.extents, extent{
				recordSeq: seq, fileOffset: off,
				chunkOffset: chunkOff, plainLen: plainLen,
				epochSeq: epochSeq, epochID: epochID,
				recLen: r.totalLen,
			})
		case recordTypeCheckpoint:
			seq := binary.LittleEndian.Uint64(r.fixed[0:8])
			if seq <= lastCkptSeq {
				return fmt.Errorf("%w: checkpoint seq order", ErrAuth)
			}
			lastCkptSeq = seq
		}
		off += uint64(r.totalLen)
	}
	// Prune superseded extents and cross-check the commits.
	for c, cs := range byChunk {
		kept := cs.extents[:0]
		var newest uint64
		for _, e := range cs.extents {
			if e.recordSeq < cs.baseSeq {
				continue
			}
			kept = append(kept, e)
			newest = e.recordSeq
		}
		if len(kept) == 0 {
			return fmt.Errorf("%w: chunk %d has no live extents", ErrAuth, c)
		}
		if newest != cs.newestSeq {
			return fmt.Errorf("%w: chunk %d newest seq %d, committed %d", ErrAuth, c, newest, cs.newestSeq)
		}
		cs.extents = kept
		var maxEnd uint32
		for _, e := range kept {
			if end := e.chunkOffset + e.plainLen; end > maxEnd {
				maxEnd = end
			}
		}
		if maxEnd != cs.chunkLen {
			return fmt.Errorf("%w: chunk %d length", ErrAuth, c)
		}
	}
	ef.chunks = byChunk
	return nil
}

// epochCipher derives (and caches) one epoch's AEAD.
func (ef *encryptedFile) epochCipher(epochSeq uint64, epochID [EpochIDLen]byte) (epochAEADInner, error) {
	if e, ok := ef.epochs[epochSeq]; ok {
		if e.id != epochID {
			return nil, fmt.Errorf("%w: epoch id mismatch", ErrAuth)
		}
		return e.aead, nil
	}
	sub, err := deriveEpochKey(ef.fileKey, ef.alg, ef.keyID[:], epochSeq, epochID[:], ef.fileID[:])
	if err != nil {
		return nil, err
	}
	defer Zero(sub)
	a, err := ef.alg.NewAEAD(sub)
	if err != nil {
		return nil, err
	}
	ef.epochs[epochSeq] = &epochAEAD{id: epochID, aead: a}
	return a, nil
}

// allocCounterLocked allocates the next record counter, rotating to a fresh
// epoch past 2^32 records. Callers hold ef.mu.
func (ef *encryptedFile) allocCounterLocked() (uint32, error) {
	c := ef.recCounter
	ef.recCounter++
	if ef.recCounter == 0 {
		if err := ef.rotateEpochLocked(); err != nil {
			return 0, err
		}
	}
	return c, nil
}

// rotateEpochLocked starts a fresh random writer epoch.
func (ef *encryptedFile) rotateEpochLocked() error {
	if ef.epochSeq == ^uint64(0) {
		return fmt.Errorf("crypto: epoch counter exhausted")
	}
	ef.epochSeq++
	ef.recCounter = 0
	if _, err := rand.Read(ef.epochID[:]); err != nil {
		return fmt.Errorf("crypto: rand: %w", err)
	}
	return nil
}

// ensureKey is a defensive fallback: write handles take the active key at
// creation and never switch mid-write, so this only fires for mishandled
// state (treated as a fresh active-key adoption, without rotation).
func (ef *encryptedFile) ensureKey() error {
	if ef.hasKey {
		return nil
	}
	active, err := ef.fs.reg.ActiveKey(bgCtx())
	if err != nil {
		return err
	}
	defer Zero(active.Key)
	ef.alg = active.Alg
	ef.keyID = active.ID
	ef.dataKey = bytesClone(active.Key)
	fk, err := deriveFileKey(active.Key, active.Alg, active.ID[:], ef.fileID[:], ef.fs.dbID[:])
	if err != nil {
		Zero(ef.dataKey)
		ef.dataKey = nil
		return err
	}
	ef.fileKey = fk
	ef.hasKey = true
	return nil
}

// decryptExtent verifies and decrypts one extent.
func (ef *encryptedFile) decryptExtent(chunk uint64, e extent) ([]byte, error) {
	r, err := readRawRecord(ef.f, int64(e.fileOffset))
	if err != nil {
		return nil, err
	}
	if r.typ != recordTypeData {
		return nil, fmt.Errorf("%w: extent type", ErrCorrupt)
	}
	env := r.fixed
	if binary.LittleEndian.Uint64(env[0:8]) != chunk ||
		binary.LittleEndian.Uint64(env[8:16]) != e.recordSeq ||
		binary.LittleEndian.Uint64(env[16:24]) != e.epochSeq ||
		binary.LittleEndian.Uint32(env[24+EpochIDLen:]) != e.chunkOffset ||
		binary.LittleEndian.Uint32(env[28+EpochIDLen:]) != e.plainLen {
		return nil, fmt.Errorf("%w: extent does not match index", ErrAuth)
	}
	var epochID [EpochIDLen]byte
	copy(epochID[:], env[24:24+EpochIDLen])
	if epochID != e.epochID {
		return nil, fmt.Errorf("%w: extent epoch", ErrAuth)
	}
	sealed, err := readSealed(ef.f, r)
	if err != nil {
		return nil, err
	}
	aead, err := ef.epochCipher(e.epochSeq, e.epochID)
	if err != nil {
		return nil, err
	}
	aad := chunkAAD(ef.fs.dbID[:], ef.fileID[:], ef.alg, ef.keyID[:], env)
	plain, err := aead.Open(nil, r.nonce, sealed, aad)
	if err != nil {
		return nil, fmt.Errorf("%w: extent seal: %v", ErrAuth, err)
	}
	if uint32(len(plain)) != e.plainLen {
		return nil, fmt.Errorf("%w: extent length", ErrCorrupt)
	}
	ef.stats.BytesDecrypted.Add(uint64(len(plain)))
	return plain, nil
}

// ReadAt implements vfs.File.
func (ef *encryptedFile) ReadAt(p []byte, off int64) (int, error) {
	ef.mu.Lock()
	defer ef.mu.Unlock()
	if ef.closed {
		return 0, fmt.Errorf("crypto: read on closed file %s", ef.name)
	}
	if off < 0 {
		return 0, fmt.Errorf("crypto: negative read offset")
	}
	if uint64(off) >= ef.logicalSize || len(p) == 0 {
		return 0, io.EOF
	}
	end := uint64(off) + uint64(len(p))
	eof := false
	if end > ef.logicalSize {
		end = ef.logicalSize
		eof = true
	}
	n := 0
	for cur := uint64(off); cur < end; {
		c := cur / ChunkSize
		coff := cur % ChunkSize
		want := end - cur
		if avail := ChunkSize - coff; want > avail {
			want = avail
		}
		cs := ef.chunks[c]
		var src []byte
		switch {
		case cs != nil && cs.resident:
			src = cs.buf[coff : coff+want]
		case cs != nil:
			plain, err := ef.cachedAssemble(c, cs)
			if err != nil {
				return n, err
			}
			src = plain[coff : coff+want]
		default:
			src = zeroChunk[coff : coff+want]
		}
		copy(p[n:n+int(want)], src)
		n += int(want)
		cur += want
	}
	if eof {
		return n, io.EOF
	}
	return n, nil
}

var zeroChunk = make([]byte, ChunkSize)

// cachedAssemble serves non-resident chunk reads through a single-entry cache.
func (ef *encryptedFile) cachedAssemble(c uint64, cs *chunkState) ([]byte, error) {
	if ef.lastOK && ef.lastChunk == c {
		return ef.lastPlain, nil
	}
	plain, err := ef.assemble(c, cs)
	if err != nil {
		return nil, err
	}
	ef.lastChunk, ef.lastPlain, ef.lastOK = c, plain, true
	return plain, nil
}

// assemble layers a chunk's extents over zeros.
func (ef *encryptedFile) assemble(c uint64, cs *chunkState) ([]byte, error) {
	out := make([]byte, ChunkSize)
	for _, e := range cs.extents {
		plain, err := ef.decryptExtent(c, e)
		if err != nil {
			Zero(out)
			return nil, err
		}
		copy(out[e.chunkOffset:], plain)
		Zero(plain)
	}
	return out, nil
}

// Read implements vfs.File.
func (ef *encryptedFile) Read(p []byte) (int, error) {
	n, err := ef.ReadAt(p, ef.getPos())
	if n > 0 {
		ef.mu.Lock()
		ef.pos += int64(n)
		ef.mu.Unlock()
		return n, nil
	}
	return 0, err
}

func (ef *encryptedFile) getPos() int64 {
	ef.mu.Lock()
	defer ef.mu.Unlock()
	return ef.pos
}

// WriteAt implements vfs.File.
func (ef *encryptedFile) WriteAt(p []byte, off int64) (int, error) {
	ef.mu.Lock()
	defer ef.mu.Unlock()
	if ef.closed {
		return 0, fmt.Errorf("crypto: write on closed file %s", ef.name)
	}
	if ef.readOnly {
		return 0, fmt.Errorf("crypto: write on read-only file %s", ef.name)
	}
	if off < 0 {
		return 0, fmt.Errorf("crypto: negative write offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(off)+uint64(len(p)) > 1<<60 {
		return 0, fmt.Errorf("crypto: write too large")
	}
	n := 0
	for cur := uint64(off); cur < uint64(off)+uint64(len(p)); {
		c := cur / ChunkSize
		coff := cur % ChunkSize
		cs := ef.chunks[c]
		if cs == nil {
			cs = &chunkState{clean: true}
			ef.chunks[c] = cs
		}
		if !cs.resident {
			buf, err := ef.assemble(c, cs)
			if err != nil {
				return n, err
			}
			cs.buf, cs.resident = buf, true
		}
		want := uint64(len(p)) - uint64(n)
		if avail := ChunkSize - coff; want > avail {
			want = avail
		}
		copy(cs.buf[coff:], p[n:n+int(want)])
		if cs.clean {
			cs.lo, cs.hi, cs.clean = uint32(coff), uint32(coff+want), false
		} else {
			if uint32(coff) < cs.lo {
				cs.lo = uint32(coff)
			}
			if uint32(coff+want) > cs.hi {
				cs.hi = uint32(coff + want)
			}
		}
		if end := uint32(coff + want); end > cs.chunkLen {
			cs.chunkLen = end
		}
		n += int(want)
		cur += want
		if ef.lastOK && ef.lastChunk == c {
			ef.lastOK = false
		}
	}
	if end := uint64(off) + uint64(len(p)); end > ef.logicalSize {
		ef.logicalSize = end
	}
	ef.committed = false
	if ef.residentCountLocked() > maxResidentBufs {
		if err := ef.spillLocked(); err != nil {
			return n, err
		}
	}
	return n, nil
}

// Write implements vfs.File.
func (ef *encryptedFile) Write(p []byte) (int, error) {
	ef.mu.Lock()
	pos := ef.pos
	ef.mu.Unlock()
	n, err := ef.WriteAt(p, pos)
	if n > 0 {
		ef.mu.Lock()
		ef.pos += int64(n)
		ef.mu.Unlock()
	}
	return n, err
}

func (ef *encryptedFile) residentCountLocked() int {
	n := 0
	for _, cs := range ef.chunks {
		if cs.resident {
			n++
		}
	}
	return n
}

// spillLocked seals every unsealed range (deltas only) and evicts resident
// buffers beyond the cap. No checkpoint or header is written.
func (ef *encryptedFile) spillLocked() error {
	if err := ef.ensureKey(); err != nil {
		return err
	}
	chunks := make([]uint64, 0, len(ef.chunks))
	for c, cs := range ef.chunks {
		if cs.resident && !cs.clean {
			chunks = append(chunks, c)
		}
	}
	sort.Slice(chunks, func(i, j int) bool { return chunks[i] < chunks[j] })
	for _, c := range chunks {
		cs := ef.chunks[c]
		if err := ef.sealRangeLocked(c, cs); err != nil {
			return err
		}
		if len(cs.extents) > maxExtentsPerChunk {
			if err := ef.mergeChunkLocked(c, cs); err != nil {
				return err
			}
		}
	}
	// Evict beyond the cap (any order; hot chunks reload on demand).
	for _, cs := range ef.chunks {
		if ef.residentCountLocked() <= maxResidentBufs {
			break
		}
		if cs.resident && cs.clean {
			Zero(cs.buf)
			cs.buf, cs.resident = nil, false
		}
	}
	return nil
}

// sealRangeLocked seals one chunk's unsealed [lo,hi) range as an extent.
func (ef *encryptedFile) sealRangeLocked(c uint64, cs *chunkState) error {
	if cs.clean || !cs.resident {
		return nil
	}
	ctr, err := ef.allocCounterLocked()
	if err != nil {
		return err
	}
	aead, err := ef.epochCipher(ef.epochSeq, ef.epochID)
	if err != nil {
		return err
	}
	ef.recordSeqHi++
	env := chunkEnvelope(c, ef.recordSeqHi, ef.epochSeq, ef.epochID[:], cs.lo, cs.hi-cs.lo)
	aad := chunkAAD(ef.fs.dbID[:], ef.fileID[:], ef.alg, ef.keyID[:], env)
	rec, err := sealRecord(recordTypeData, env, cs.buf[cs.lo:cs.hi], aad, aead, ctr)
	if err != nil {
		return err
	}
	if _, err := ef.writeFullAt(rec, ef.appendOffset); err != nil {
		return err
	}
	cs.extents = append(cs.extents, extent{
		recordSeq: ef.recordSeqHi, fileOffset: uint64(ef.appendOffset),
		chunkOffset: cs.lo, plainLen: cs.hi - cs.lo,
		epochSeq: ef.epochSeq, epochID: ef.epochID,
		recLen: int64(len(rec)),
	})
	cs.newestSeq = ef.recordSeqHi
	ef.liveSealed += int64(len(rec))
	ef.appendOffset += int64(len(rec))
	ef.stats.BytesEncrypted.Add(uint64(cs.hi - cs.lo))
	cs.clean = true
	return nil
}

// mergeChunkLocked collapses a chunk's extents into one positional record.
func (ef *encryptedFile) mergeChunkLocked(c uint64, cs *chunkState) error {
	plain, err := ef.assemble(c, cs)
	if err != nil {
		return err
	}
	ctr, err := ef.allocCounterLocked()
	if err != nil {
		Zero(plain)
		return err
	}
	aead, err := ef.epochCipher(ef.epochSeq, ef.epochID)
	if err != nil {
		Zero(plain)
		return err
	}
	ef.recordSeqHi++
	env := chunkEnvelope(c, ef.recordSeqHi, ef.epochSeq, ef.epochID[:], 0, cs.chunkLen)
	aad := chunkAAD(ef.fs.dbID[:], ef.fileID[:], ef.alg, ef.keyID[:], env)
	rec, err := sealRecord(recordTypeData, env, plain[:cs.chunkLen], aad, aead, ctr)
	Zero(plain)
	if err != nil {
		return err
	}
	if _, err := ef.writeFullAt(rec, ef.appendOffset); err != nil {
		return err
	}
	for _, e := range cs.extents {
		ef.liveSealed -= e.recLen
	}
	cs.extents = []extent{{
		recordSeq: ef.recordSeqHi, fileOffset: uint64(ef.appendOffset),
		chunkOffset: 0, plainLen: cs.chunkLen,
		epochSeq: ef.epochSeq, epochID: ef.epochID,
		recLen: int64(len(rec)),
	}}
	cs.newestSeq = ef.recordSeqHi
	cs.baseSeq = ef.recordSeqHi
	ef.liveSealed += int64(len(rec))
	ef.appendOffset += int64(len(rec))
	ef.stats.BytesEncrypted.Add(uint64(cs.chunkLen))
	return nil
}

func (ef *encryptedFile) writeFullAt(b []byte, off int64) (int, error) {
	n := 0
	for n < len(b) {
		m, err := ef.f.WriteAt(b[n:], off+int64(n))
		n += m
		if err != nil {
			return n, err
		}
		if m == 0 {
			return n, io.ErrShortWrite
		}
	}
	return n, nil
}

// commitLocked seals dirty ranges, appends a checkpoint, commits the header,
// and optionally fsyncs.
func (ef *encryptedFile) commitLocked(doSync bool) error {
	if err := ef.spillLocked(); err != nil {
		return err
	}
	if ef.committed && ef.hasHdr {
		if doSync {
			return ef.f.Sync()
		}
		return nil
	}
	if err := ef.ensureKey(); err != nil {
		return err
	}
	entries := make([]chunkCommit, 0, len(ef.chunks))
	for c, cs := range ef.chunks {
		if len(cs.extents) == 0 {
			return fmt.Errorf("crypto: chunk %d has no sealed extents", c)
		}
		entries = append(entries, chunkCommit{
			chunkIndex: c, newestSeq: cs.newestSeq,
			chunkLen: cs.chunkLen, baseSeq: cs.baseSeq,
		})
	}
	ctr, err := ef.allocCounterLocked()
	if err != nil {
		return err
	}
	aead, err := ef.epochCipher(ef.epochSeq, ef.epochID)
	if err != nil {
		return err
	}
	if aead.Overhead() != 16 {
		return fmt.Errorf("crypto: unexpected tag length %d", aead.Overhead())
	}
	ef.checkpointSeq++
	payload := encodeCheckpointPayload(entries)
	// committedEnd is exactly the post-checkpoint append offset; the record
	// length is fully determined before sealing.
	recLen := int64(1+ckptFixedLen+1+aead.NonceSize()+4) + int64(len(payload)) + 16
	env := checkpointEnvelope(ef.checkpointSeq, ef.epochSeq, ef.epochID[:],
		ef.logicalSize, ef.recordSeqHi, uint64(ef.appendOffset)+uint64(recLen))
	aad := checkpointAAD(ef.fs.dbID[:], ef.fileID[:], ef.alg, ef.keyID[:], env)
	rec, err := sealRecord(recordTypeCheckpoint, env, payload, aad, aead, ctr)
	if err != nil {
		return err
	}
	if int64(len(rec)) != recLen {
		return fmt.Errorf("crypto: checkpoint length drift")
	}
	ckptOff := uint64(ef.appendOffset)
	if _, err := ef.writeFullAt(rec, ef.appendOffset); err != nil {
		return err
	}
	ef.appendOffset += int64(len(rec))
	ef.liveSealed += int64(len(rec)) - ef.prevCkptLen
	ef.prevCkptLen = int64(len(rec))
	slotSeq := ef.hdr.slotSeq + 1
	if !ef.hasHdr {
		slotSeq = 1
	}
	nh := fileHeader{
		slotSeq: slotSeq, alg: ef.alg, keyID: ef.keyID,
		epochSeq: ef.epochSeq, dbID: ef.fs.dbID, fileID: ef.fileID,
		epochID: ef.epochID, checkpointSeq: ef.checkpointSeq,
		checkpointOffset: ckptOff, recordSeqHi: ef.recordSeqHi,
		logicalSize: ef.logicalSize, createdAt: uint64(nowUnix()),
		keyGen: ef.fs.reg.Generation(),
	}
	if ef.hasHdr {
		nh.createdAt = ef.hdr.createdAt
	}
	slot, err := encodeHeaderSlot(&nh, ef.dataKey)
	if err != nil {
		return err
	}
	if _, err := ef.writeFullAt(slot, int64((slotSeq%2)*headerSlotLen)); err != nil {
		return err
	}
	if doSync {
		if err := ef.f.Sync(); err != nil {
			return err
		}
	}
	ef.hdr = nh
	ef.hasHdr = true
	ef.committed = true
	return nil
}

// Sync implements vfs.File.
func (ef *encryptedFile) Sync() error {
	ef.mu.Lock()
	defer ef.mu.Unlock()
	if ef.closed {
		return fmt.Errorf("crypto: sync on closed file %s", ef.name)
	}
	if ef.readOnly {
		return nil
	}
	if err := ef.commitLocked(true); err != nil {
		return err
	}
	ef.maybeCompactLocked()
	return nil
}

// maybeCompactLocked rewrites the file when stale bytes pile up. Compaction
// failures are logged and counted, never fatal: the commit is already
// durable when compaction runs.
func (ef *encryptedFile) maybeCompactLocked() {
	garbage := (ef.appendOffset - headerTotalLen) - ef.liveSealed
	if garbage < compactMinGarbage {
		return
	}
	if ef.appendOffset > 0 && garbage*10 < ef.appendOffset {
		return
	}
	before := ef.appendOffset
	if err := ef.rewriteLocked(); err != nil {
		ef.fs.stats.CompactErrors.Add(1)
		ef.fs.log.Warn("encrypted file compaction failed", "file", ef.name, "err", err.Error())
		return
	}
	ef.fs.stats.Compactions.Add(1)
	if reclaimed := before - ef.appendOffset; reclaimed > 0 {
		ef.fs.stats.BytesReclaimed.Add(uint64(reclaimed))
	}
}

// rewriteLocked rebuilds the container with one extent per chunk, dropping
// stale records. The original is untouched until the verified replacement
// renames over it.
func (ef *encryptedFile) rewriteLocked() error {
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	tmpName := ef.name + ".compact-" + hex.EncodeToString(nonce[:])
	tf, err := ef.fs.base.Create(tmpName, ef.category)
	if err != nil {
		return err
	}
	var newEpoch [EpochIDLen]byte
	if _, err := rand.Read(newEpoch[:]); err != nil {
		tf.Close()
		return err
	}
	newSeq := ef.epochSeq + 1
	sub, err := deriveEpochKey(ef.fileKey, ef.alg, ef.keyID[:], newSeq, newEpoch[:], ef.fileID[:])
	if err != nil {
		tf.Close()
		return err
	}
	defer Zero(sub)
	aead, err := ef.alg.NewAEAD(sub)
	if err != nil {
		tf.Close()
		return err
	}
	chunks := make([]uint64, 0, len(ef.chunks))
	for c := range ef.chunks {
		chunks = append(chunks, c)
	}
	sort.Slice(chunks, func(i, j int) bool { return chunks[i] < chunks[j] })
	off := int64(headerTotalLen)
	newChunks := make(map[uint64]*chunkState, len(ef.chunks))
	var wctr uint32 // rewrite-image record counter (one record per chunk)
	fail := func(err error) error {
		tf.Close()
		_ = ef.fs.base.Remove(tmpName)
		return err
	}
	for _, c := range chunks {
		cs := ef.chunks[c]
		var plain []byte
		if cs.resident {
			plain = append([]byte(nil), cs.buf[:cs.chunkLen]...)
		} else {
			p, err := ef.assemble(c, cs)
			if err != nil {
				return fail(err)
			}
			plain = append([]byte(nil), p[:cs.chunkLen]...)
			Zero(p)
		}
		ef.recordSeqHi++
		env := chunkEnvelope(c, ef.recordSeqHi, newSeq, newEpoch[:], 0, cs.chunkLen)
		aad := chunkAAD(ef.fs.dbID[:], ef.fileID[:], ef.alg, ef.keyID[:], env)
		rec, err := sealRecord(recordTypeData, env, plain, aad, aead, wctr)
		wctr++
		Zero(plain)
		if err != nil {
			return fail(err)
		}
		if _, err := writeFullAt(tf, rec, off); err != nil {
			return fail(err)
		}
		newChunks[c] = &chunkState{
			newestSeq: ef.recordSeqHi, chunkLen: cs.chunkLen, baseSeq: ef.recordSeqHi,
			extents: []extent{{
				recordSeq: ef.recordSeqHi, fileOffset: uint64(off),
				chunkOffset: 0, plainLen: cs.chunkLen,
				epochSeq: newSeq, epochID: newEpoch, recLen: int64(len(rec)),
			}},
			clean: true,
		}
		off += int64(len(rec))
	}
	entries := make([]chunkCommit, 0, len(newChunks))
	for c, cs := range newChunks {
		entries = append(entries, chunkCommit{
			chunkIndex: c, newestSeq: cs.newestSeq,
			chunkLen: cs.chunkLen, baseSeq: cs.baseSeq,
		})
	}
	ef.checkpointSeq++
	payload := encodeCheckpointPayload(entries)
	recLen := int64(1+ckptFixedLen+1+aead.NonceSize()+4) + int64(len(payload)) + int64(aead.Overhead())
	env := checkpointEnvelope(ef.checkpointSeq, newSeq, newEpoch[:],
		ef.logicalSize, ef.recordSeqHi, uint64(off)+uint64(recLen))
	aad := checkpointAAD(ef.fs.dbID[:], ef.fileID[:], ef.alg, ef.keyID[:], env)
	crec, err := sealRecord(recordTypeCheckpoint, env, payload, aad, aead, wctr)
	if err != nil {
		return fail(err)
	}
	if int64(len(crec)) != recLen {
		return fail(fmt.Errorf("crypto: checkpoint length drift"))
	}
	ckptOff := uint64(off)
	if _, err := writeFullAt(tf, crec, off); err != nil {
		return fail(err)
	}
	off += int64(len(crec))
	nh := ef.hdr
	nh.slotSeq++
	nh.epochSeq = newSeq
	nh.epochID = newEpoch
	nh.checkpointSeq = ef.checkpointSeq
	nh.checkpointOffset = ckptOff
	nh.recordSeqHi = ef.recordSeqHi
	slot, err := encodeHeaderSlot(&nh, ef.dataKey)
	if err != nil {
		return fail(err)
	}
	if _, err := writeFullAt(tf, slot, int64((nh.slotSeq%2)*headerSlotLen)); err != nil {
		return fail(err)
	}
	if err := tf.Sync(); err != nil {
		return fail(err)
	}
	if err := tf.Close(); err != nil {
		_ = ef.fs.base.Remove(tmpName)
		return err
	}
	if err := ef.fs.verifyImage(tmpName); err != nil {
		_ = ef.fs.base.Remove(tmpName)
		return fmt.Errorf("compact verify: %w", err)
	}
	if err := ef.fs.base.Rename(tmpName, ef.name); err != nil {
		_ = ef.fs.base.Remove(tmpName)
		return err
	}
	if err := ef.fs.syncParentDir(ef.name); err != nil {
		return err
	}
	nf, err := ef.fs.base.OpenReadWrite(ef.name, ef.category)
	if err != nil {
		return err
	}
	_ = ef.f.Close()
	ef.f = nf
	ef.hdr = nh
	ef.epochSeq = newSeq
	ef.epochID = newEpoch
	ef.recCounter = wctr // continue the adopted epoch's counter: no reuse
	ef.chunks = newChunks
	ef.appendOffset = off
	ef.prevCkptLen = int64(len(crec))
	ef.liveSealed = off - headerTotalLen
	ef.lastOK = false
	return nil
}

func writeFullAt(f vfs.File, b []byte, off int64) (int, error) {
	n := 0
	for n < len(b) {
		m, err := f.WriteAt(b[n:], off+int64(n))
		n += m
		if err != nil {
			return n, err
		}
		if m == 0 {
			return n, io.ErrShortWrite
		}
	}
	return n, nil
}

// Close implements vfs.File. A write handle with uncommitted data commits
// without syncing (clean-close readability), then releases everything.
func (ef *encryptedFile) Close() error {
	// Unregister first so a blocked close can't wedge the FS.
	if !ef.readOnly {
		ef.fs.unmarkWriter(ef.name)
	}
	ef.fs.untrackOpen(ef)
	ef.mu.Lock()
	defer ef.mu.Unlock()
	if ef.closed {
		return nil
	}
	ef.closed = true
	var firstErr error
	if !ef.readOnly && (!ef.committed || ef.hasUnsealedLocked()) {
		if err := ef.commitLocked(false); err != nil {
			firstErr = err
		}
	}
	for _, cs := range ef.chunks {
		Zero(cs.buf)
		cs.buf = nil
	}
	Zero(ef.dataKey)
	ef.dataKey = nil
	Zero(ef.fileKey)
	ef.fileKey = nil
	Zero(ef.lastPlain)
	ef.lastPlain = nil
	if err := ef.f.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (ef *encryptedFile) hasUnsealedLocked() bool {
	for _, cs := range ef.chunks {
		if cs.resident && !cs.clean {
			return true
		}
	}
	return false
}

// Stat implements vfs.File, reporting the logical size.
func (ef *encryptedFile) Stat() (vfs.FileInfo, error) {
	ef.mu.Lock()
	size := ef.logicalSize
	ef.mu.Unlock()
	fi, err := ef.f.Stat()
	if err != nil {
		return nil, err
	}
	return &sizedFileInfo{FileInfo: fi, size: int64(size)}, nil
}

// SyncTo implements vfs.File. Queues the physical prefix; no guarantee.
func (ef *encryptedFile) SyncTo(length int64) (bool, error) {
	ef.mu.Lock()
	off := ef.appendOffset
	ef.mu.Unlock()
	if length < off {
		off = length
	}
	if off < 0 {
		off = 0
	}
	return ef.f.SyncTo(off)
}

// SyncData implements vfs.File (same as Sync: no separate metadata).
func (ef *encryptedFile) SyncData() error { return ef.Sync() }

// Preallocate implements vfs.File, adjusting for container overhead.
func (ef *encryptedFile) Preallocate(offset, length int64) error {
	overhead := (length/ChunkSize+2)*(maxNonceLen+64) + headerTotalLen
	return ef.f.Preallocate(offset, length+overhead)
}

// Prefetch implements vfs.File. Logical/physical offsets differ, so no-op.
func (ef *encryptedFile) Prefetch(_, _ int64) error { return nil }

// Fd implements vfs.File.
func (ef *encryptedFile) Fd() uintptr { return vfs.InvalidFd }

// keyIDOf returns the committed key id (valid after first commit).
func (ef *encryptedFile) keyIDOf() ([KeyIDLen]byte, bool) {
	ef.mu.Lock()
	defer ef.mu.Unlock()
	return ef.keyID, ef.hasKey
}

// scrubLocked verifies every record seal in the committed region. Callers
// hold ef.mu. Used by verifyImage for full tamper sweeps.
func (ef *encryptedFile) scrubLocked(committedEnd uint64) error {
	for off := uint64(headerTotalLen); off < committedEnd; {
		r, err := readRawRecord(ef.f, int64(off))
		if err != nil {
			return err
		}
		if off+uint64(r.totalLen) > committedEnd {
			return fmt.Errorf("%w: record crosses committed end", ErrCorrupt)
		}
		sealed, err := readSealed(ef.f, r)
		if err != nil {
			return err
		}
		switch r.typ {
		case recordTypeData:
			env := r.fixed
			epochSeq := binary.LittleEndian.Uint64(env[16:24])
			var epochID [EpochIDLen]byte
			copy(epochID[:], env[24:24+EpochIDLen])
			aead, err := ef.epochCipher(epochSeq, epochID)
			if err != nil {
				return err
			}
			aad := chunkAAD(ef.fs.dbID[:], ef.fileID[:], ef.alg, ef.keyID[:], env)
			if _, err := aead.Open(nil, r.nonce, sealed, aad); err != nil {
				return fmt.Errorf("%w: extent seal at %d: %v", ErrAuth, off, err)
			}
		case recordTypeCheckpoint:
			env := r.fixed
			epochSeq := binary.LittleEndian.Uint64(env[8:16])
			var epochID [EpochIDLen]byte
			copy(epochID[:], env[16:16+EpochIDLen])
			aead, err := ef.epochCipher(epochSeq, epochID)
			if err != nil {
				return err
			}
			aad := checkpointAAD(ef.fs.dbID[:], ef.fileID[:], ef.alg, ef.keyID[:], env)
			if _, err := aead.Open(nil, r.nonce, sealed, aad); err != nil {
				return fmt.Errorf("%w: checkpoint seal at %d: %v", ErrAuth, off, err)
			}
		}
		off += uint64(r.totalLen)
	}
	return nil
}
