package spool

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/marcgauthier/murmur/compression"
)

// LoadFunc receives current record versions during a load scan.
// Each key is delivered exactly once with its current version;
// older occurrences are skipped and blocks with no current versions
// produce no callback. Tombstones are delivered for keys whose
// current version is a deletion, so a reload over dirty application
// memory still converges. Record slices reference the decoded block
// buffer: copy to retain. A non-nil return aborts the load with that
// error.
type LoadFunc func([]Record) error

// LoadOptions configures a standalone offline load.
type LoadOptions struct {
	// Path is the store directory.
	Path string
	// MasterKey is the 32-byte store key. Exactly one of MasterKey
	// and Passphrase must be set for encrypted stores.
	MasterKey []byte
	// Passphrase unlocks passphrase-protected stores.
	Passphrase string
	// WrappingKeyID, when set, must match the envelope's
	// authenticated wrapping id.
	WrappingKeyID string
	// ContextID, when 16 bytes, must match the store's persisted
	// database context. Empty accepts the persisted context.
	ContextID []byte
	// Codecs registers custom compression codecs (ids 128-255) needed
	// to read blocks written under them.
	Codecs []compression.Codec
}

// Load streams every key's current version from an offline store
// into fn. It takes no lock: use it on quiesced stores (or accept a
// possibly partial trailing view); online loading is OpenAndLoad.
func Load(path string, masterKey []byte, fn LoadFunc) error {
	return LoadWithOptions(LoadOptions{Path: path, MasterKey: masterKey}, fn)
}

// LoadWithOptions is Load with passphrase, wrapping-id and context
// support.
func LoadWithOptions(o LoadOptions, fn LoadFunc) error {
	if o.Path == "" {
		return fmt.Errorf("spool: Path is required")
	}
	if fn == nil {
		return fmt.Errorf("spool: LoadFunc is required")
	}
	if len(o.ContextID) != 0 && len(o.ContextID) != 16 {
		return fmt.Errorf("spool: ContextID must be 16 bytes, got %d", len(o.ContextID))
	}
	if len(o.WrappingKeyID) > maxWrapIDLen {
		return fmt.Errorf("spool: WrappingKeyID of %d bytes exceeds %d", len(o.WrappingKeyID), maxWrapIDLen)
	}
	l, err := openLoader(o)
	if err != nil {
		return err
	}
	_, _, _, _, err = l.scanAll(l.man.members, fn, nil, nil)
	return err
}

// loader holds the read-side pieces shared by Open's rebuild and
// standalone loads.
type loader struct {
	dir  string
	man  *manifest
	ring *keyring // nil when unencrypted
	comp *compressor
	ctx  [16]byte // database context binding block AAD
}

// openLoader reads the manifest and key container without locking,
// epoch changes or side effects.
func openLoader(o LoadOptions) (*loader, error) {
	raw, err := os.ReadFile(filepath.Join(o.Path, manifestFileName))
	if err != nil {
		return nil, fmt.Errorf("spool: read manifest: %w", err)
	}
	m, err := parseManifest(raw)
	if err != nil {
		return nil, err
	}
	if len(o.ContextID) == 16 && string(o.ContextID) != string(m.context[:]) {
		return nil, fmt.Errorf("spool: supplied database context does not match store: %w", ErrContextMismatch)
	}
	l := &loader{dir: o.Path, man: m, ctx: m.context}
	if m.encrypted {
		if len(o.MasterKey) == 0 && o.Passphrase == "" {
			return nil, fmt.Errorf("spool: store is encrypted: %w", ErrWrongKey)
		}
		ring, err := readKeys(o.Path, o.MasterKey, o.Passphrase, m.storeID, m.context)
		if err != nil {
			return nil, err
		}
		if o.WrappingKeyID != "" && o.WrappingKeyID != ring.wrapID {
			return nil, fmt.Errorf("spool: wrapping-key id %q != envelope %q: %w",
				o.WrappingKeyID, ring.wrapID, ErrWrongKey)
		}
		l.ring = ring
		l.ctx = ring.context
	} else if o.WrappingKeyID != "" {
		return nil, fmt.Errorf("spool: WrappingKeyID requires encryption")
	}
	// Decompression always supports every registered codec
	// regardless of the writer's setting.
	comp, err := newCompressor(CompressionNone, o.Codecs)
	if err != nil {
		return nil, err
	}
	l.comp = comp
	return l, nil
}

// framedBlock is one framed block with its sealed payload.
type framedBlock struct {
	hdr    *blockHeader
	sealed []byte // payload without header
	off    int64
}

// assembledGroup is one file-complete commit group awaiting
// validation: data blocks in file order plus its completion.
type assembledGroup struct {
	id         uint64
	data       []*framedBlock
	completion *framedBlock
	firstOff   int64
}

// assembleFile walks one segment's framing and assembles commit
// groups. Groups are contiguous on disk (the committer appends each
// group atomically), so assembly is strict: a data block opening a
// new group while another is open, or a completion closing the wrong
// group, is corruption. A trailing group without its completion is
// an incomplete tail and is discarded as a whole. It returns the
// complete groups in file order, the valid end, and the highest
// block sequence among complete frames.
func assembleFile(f *os.File, size int64, id uint64) (groups []*assembledGroup, validEnd int64, maxBlock uint64, discarded uint64, err error) {
	var offs []int64
	var off int64
	for off < size {
		next, rerr := peekBlockNext(f, off)
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, off, 0, 0, fmt.Errorf("spool: segment %d offset %d: %w", id, off, rerr)
		}
		offs = append(offs, off)
		off = next
	}
	frames := make([]*framedBlock, len(offs))
	for i, boff := range offs {
		hdr, frame, _, rerr := readBlockFrame(f, boff, absMaxBlockBytes)
		if rerr != nil {
			return nil, boff, 0, 0, fmt.Errorf("spool: segment %d offset %d: %w", id, boff, rerr)
		}
		frames[i] = &framedBlock{hdr: hdr, sealed: frame[blockHeaderLen:], off: boff}
	}
	var open *assembledGroup
	closeOpen := func() {
		if open != nil {
			for _, db := range open.data {
				if db.hdr.blockSeq > maxBlock {
					maxBlock = db.hdr.blockSeq
				}
			}
			if open.completion != nil && open.completion.hdr.blockSeq > maxBlock {
				maxBlock = open.completion.hdr.blockSeq
			}
			groups = append(groups, open)
			open = nil
		}
	}
	for _, b := range frames {
		if b.hdr.completion {
			if open == nil {
				return nil, b.off, 0, 0, fmt.Errorf("spool: segment %d offset %d: orphan completion: %w", id, b.off, ErrCorrupt)
			}
			open.completion = b
			closeOpen()
			continue
		}
		// Data blocks carry their group id in authenticated
		// plaintext, unreadable before decryption. Assembly is
		// positional: data blocks accumulate on the open group
		// and the completion binds them; plaintext framing is
		// verified after decryption.
		if open == nil {
			open = &assembledGroup{firstOff: b.off}
		}
		open.data = append(open.data, b)
	}
	validEnd = off
	if open != nil {
		// Incomplete trailing group: discard whole, including
		// individually complete data blocks.
		validEnd = open.firstOff
		discarded = 1
	}
	return groups, validEnd, maxBlock, discarded, nil
}

// validatedRecords holds one decrypted data block with its records.
type validatedRecords struct {
	recs []Record
	locs []Location
}

// validateGroup authenticates and decrypts a complete group's data
// and completion, verifying ordering, counts, sizes and the content
// digest. Any failure fails closed: complete groups are never
// truncation.
func (l *loader) validateGroup(fileID uint64, g *assembledGroup) ([]validatedRecords, completion, error) {
	var out []validatedRecords
	chdr := g.completion.hdr
	ccomp, err := l.openPayload(chdr, g.completion.sealed)
	if err != nil {
		return nil, completion{}, fmt.Errorf("spool: segment %d group completion: %w", fileID, err)
	}
	c, err := decodeCompletion(ccomp)
	if err != nil {
		return nil, completion{}, fmt.Errorf("spool: segment %d group completion: %w", fileID, err)
	}
	if uint64(len(g.data)) != uint64(c.blockCount) {
		return nil, completion{}, fmt.Errorf("spool: segment %d group %d has %d data blocks, completion says %d: %w",
			fileID, c.groupID, len(g.data), c.blockCount, ErrCorrupt)
	}
	seqs := make([]uint64, len(g.data))
	sealed := make([][]byte, len(g.data))
	for i, b := range g.data {
		seqs[i] = b.hdr.blockSeq
		sealed[i] = b.sealed
		if seqs[i] != c.blockSeqs[i] {
			return nil, completion{}, fmt.Errorf("spool: segment %d group %d block %d identity mismatch: %w",
				fileID, c.groupID, i, ErrCorrupt)
		}
	}
	if digest := groupDigest(c.groupID, c.blockCount, seqs, sealed); digest != c.digest {
		return nil, completion{}, fmt.Errorf("spool: segment %d group %d digest mismatch: %w", fileID, c.groupID, ErrCorrupt)
	}
	var totalRecords uint32
	var totalPlain uint64
	for i, b := range g.data {
		plain, err := l.openPayload(b.hdr, b.sealed)
		if err != nil {
			return nil, completion{}, fmt.Errorf("spool: segment %d offset %d: %w", fileID, b.off, err)
		}
		gid, bidx, bcnt, recs, err := decodeBody(plain, b.hdr.recordCount)
		if err != nil {
			return nil, completion{}, fmt.Errorf("spool: segment %d offset %d: %w", fileID, b.off, err)
		}
		if gid != c.groupID || bidx != uint32(i) || bcnt != c.blockCount {
			return nil, completion{}, fmt.Errorf("spool: segment %d group %d block %d framing mismatch: %w",
				fileID, c.groupID, i, ErrCorrupt)
		}
		vr := validatedRecords{recs: recs, locs: make([]Location, len(recs))}
		for j := range recs {
			r := &recs[j]
			size := uint32(len(r.Key) + len(r.Value) + 17)
			vr.locs[j] = Location{
				FileID:      fileID,
				BlockID:     b.hdr.blockSeq,
				BlockOffset: b.off,
				RecordIndex: uint32(j),
				Sequence:    r.Sequence,
				Size:        size,
			}
		}
		totalRecords += uint32(len(recs))
		totalPlain += uint64(len(plain))
		out = append(out, vr)
	}
	if totalRecords != c.totalRecords || totalPlain != c.totalPlain {
		return nil, completion{}, fmt.Errorf("spool: segment %d group %d declares %d records/%d bytes, found %d/%d: %w",
			fileID, c.groupID, c.totalRecords, c.totalPlain, totalRecords, totalPlain, ErrCorrupt)
	}
	return out, c, nil
}

// visitValidatedFile assembles, validates and decrypts one file's
// complete groups, calling visit per record in file order with its
// on-disk location. Record values borrow decrypted block buffers
// and are valid only during the visit call. It returns the highest
// block and group ids, the valid end, and discarded tail groups.
func (l *loader) visitValidatedFile(id uint64, visit func(r Record, loc Location) error) (maxBlock, maxGroup uint64, validEnd int64, discarded uint64, err error) {
	path := filepath.Join(l.dir, segmentsDirName, segmentFileName(id))
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("spool: open segment %d: %w", id, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("spool: stat segment %d: %w", id, err)
	}
	groups, validEnd, _, discarded, err := assembleFile(f, st.Size(), id)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	for _, g := range groups {
		blocks, c, verr := l.validateGroup(id, g)
		if verr != nil {
			// Structurally complete groups fail closed,
			// including at the tail: a torn tail can only
			// yield structurally incomplete frames (the
			// EOF path above discards those), so bytes
			// that frame completely but fail
			// authentication, digests, or framing are
			// genuine corruption, never truncation.
			return 0, 0, 0, 0, verr
		}
		if c.groupID > maxGroup {
			maxGroup = c.groupID
		}
		for _, b := range g.data {
			if b.hdr.blockSeq > maxBlock {
				maxBlock = b.hdr.blockSeq
			}
		}
		if g.completion.hdr.blockSeq > maxBlock {
			maxBlock = g.completion.hdr.blockSeq
		}
		for _, b := range blocks {
			for j := range b.recs {
				if err := visit(b.recs[j], b.locs[j]); err != nil {
					return 0, 0, 0, 0, err
				}
			}
		}
	}
	return maxBlock, maxGroup, validEnd, discarded, nil
}

// winnerLoc is one key's winning version for second-pass delivery.
type winnerLoc struct {
	seq  uint64
	tomb bool
	loc  Location
}

// scanAll streams every member segment in two passes. Pass one
// validates complete groups and resolves each key's winner by
// highest logical sequence (physical order is not sufficient once
// compaction relocates versions). Pass two re-reads winner blocks
// newest-file-first and delivers each winner exactly once. Only
// key/location metadata is kept between passes. It returns the
// highest block and group ids, per-file valid ends, and discarded
// tail groups.
func (l *loader) scanAll(members []uint64, fn LoadFunc, idx *index, fs *fileStats) (maxBlock, maxGroup uint64, validEnds map[uint64]int64, discarded uint64, err error) {
	ends := make(map[uint64]int64, len(members))
	var standalone map[string]winnerLoc
	if idx == nil {
		standalone = make(map[string]winnerLoc)
	}
	for _, id := range members {
		mb, mg, end, disc, verr := l.visitValidatedFile(id, func(r Record, loc Location) error {
			if idx != nil {
				size := uint32(len(r.Key) + len(r.Value) + 17)
				idx.commitRecord(fs, string(r.Key), r.Sequence, r.Deleted, loc, size)
				return nil
			}
			k := string(r.Key)
			if w, ok := standalone[k]; !ok || r.Sequence > w.seq {
				standalone[k] = winnerLoc{seq: r.Sequence, tomb: r.Deleted, loc: loc}
			}
			return nil
		})
		ends[id] = end
		discarded += disc
		if mb > maxBlock {
			maxBlock = mb
		}
		if mg > maxGroup {
			maxGroup = mg
		}
		if verr != nil {
			return maxBlock, maxGroup, ends, discarded, verr
		}
	}
	if fn == nil {
		return maxBlock, maxGroup, ends, discarded, nil
	}
	winners := standalone
	if idx != nil {
		winners = idx.snapshotWinners()
	}
	// Pass two: newest files first, delivering winner locations only.
	for i := len(members) - 1; i >= 0; i-- {
		id := members[i]
		var pending []Record
		flush := func() error {
			if len(pending) == 0 {
				return nil
			}
			out := append([]Record(nil), pending...)
			pending = pending[:0]
			return fn(out)
		}
		var lastOff int64 = -1
		_, _, _, _, verr := l.visitValidatedFile(id, func(r Record, loc Location) error {
			w, ok := winners[string(r.Key)]
			if !ok || w.seq != r.Sequence || w.loc != loc {
				return nil
			}
			if loc.BlockOffset != lastOff {
				if err := flush(); err != nil {
					return err
				}
				lastOff = loc.BlockOffset
			}
			// Copy: the block buffer is released after visit.
			nr := Record{Key: append([]byte(nil), r.Key...), Sequence: r.Sequence, Deleted: r.Deleted}
			if !r.Deleted {
				nr.Value = append([]byte(nil), r.Value...)
			}
			pending = append(pending, nr)
			return nil
		})
		if verr != nil {
			return maxBlock, maxGroup, ends, discarded, verr
		}
		if err := flush(); err != nil {
			return maxBlock, maxGroup, ends, discarded, err
		}
	}
	return maxBlock, maxGroup, ends, discarded, nil
}

// OpenAndLoad opens the store and streams every key's current
// version into fn after the index rebuild.
func OpenAndLoad(opts Options, fn LoadFunc) (*Store, error) {
	if fn == nil {
		return nil, fmt.Errorf("spool: LoadFunc is required")
	}
	return openStore(opts, fn)
}

// rebuild scans member segments into the index and statistics,
// optionally streaming winners to fn. It returns the highest block
// and group ids, per-file valid ends, and discarded tail groups.
func (s *Store) rebuild(fn LoadFunc, ids []uint64) (uint64, uint64, map[uint64]int64, uint64, error) {
	l := &loader{dir: s.dir, man: s.man, ring: s.ring, comp: s.comp, ctx: s.ctx}
	maxBlock, maxGroup, ends, discarded, err := l.scanAll(ids, fn, s.idx, s.fstats)
	if err != nil {
		return 0, 0, nil, 0, err
	}
	// Count truncated tails: files whose valid end falls short.
	segDir := filepath.Join(s.dir, segmentsDirName)
	for id, end := range ends {
		st, serr := os.Stat(filepath.Join(segDir, segmentFileName(id)))
		if serr != nil {
			continue
		}
		if end < st.Size() {
			s.truncatedTails.Add(1)
		}
	}
	return maxBlock, maxGroup, ends, discarded, nil
}

// truncateActive cuts the active segment to its scanned valid end,
// discarding a torn tail before it can be buried by new appends.
func (s *Store) truncateActive(id uint64, validEnd int64) error {
	path := filepath.Join(s.dir, segmentsDirName, segmentFileName(id))
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("spool: stat active segment: %w", err)
	}
	if validEnd >= st.Size() {
		return nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("spool: open active segment: %w", err)
	}
	defer f.Close()
	if err := f.Truncate(validEnd); err != nil {
		return fmt.Errorf("spool: truncate torn tail: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("spool: sync truncated segment: %w", err)
	}
	return nil
}

// openPayload authenticates, decrypts and decompresses one sealed
// block body.
func (l *loader) openPayload(hdr *blockHeader, sealed []byte) ([]byte, error) {
	crypt, key, err := l.cryptFor(hdr)
	if err != nil {
		return nil, err
	}
	// The seal bound the header with a zero sealed-length field plus
	// the store id and database context; the loader must open with
	// the same associated data.
	hdr59 := make([]byte, 59)
	copy(hdr59, hdr.raw[:59])
	hdr59[30], hdr59[31], hdr59[32], hdr59[33] = 0, 0, 0, 0
	comp, err := crypt.open(key, hdr.nonce, sealed, blockAAD(hdr59, l.man.storeID[:], l.ctx[:]))
	if err != nil {
		return nil, err
	}
	return l.comp.decompress(hdr.compression, comp, hdr.uncompressedLen)
}

// cryptFor resolves a block's algorithm and key.
func (l *loader) cryptFor(hdr *blockHeader) (blockCrypt, [32]byte, error) {
	switch hdr.algo {
	case algoNone:
		if l.ring != nil {
			return blockCrypt{}, [32]byte{}, fmt.Errorf("spool: plain block in encrypted store: %w", ErrCorrupt)
		}
		if hdr.keyID != 0 {
			return blockCrypt{}, [32]byte{}, fmt.Errorf("spool: plain block with key id: %w", ErrCorrupt)
		}
		return blockCrypt{algo: algoNone}, [32]byte{}, nil
	case algoAESGCM:
		if l.ring == nil {
			return blockCrypt{}, [32]byte{}, fmt.Errorf("spool: encrypted block in plain store: %w", ErrCorrupt)
		}
		key, ok := l.ring.byID(hdr.keyID)
		if !ok {
			return blockCrypt{}, [32]byte{}, fmt.Errorf("spool: key id %d: %w", hdr.keyID, ErrUnknownKeyID)
		}
		if len(hdr.nonce) != 12 {
			return blockCrypt{}, [32]byte{}, fmt.Errorf("spool: bad AES-GCM nonce size %d: %w", len(hdr.nonce), ErrCorrupt)
		}
		return blockCrypt{algo: hdr.algo}, key, nil
	default:
		return blockCrypt{}, [32]byte{}, fmt.Errorf("spool: unknown block algorithm %d: %w", hdr.algo, ErrCorrupt)
	}
}
