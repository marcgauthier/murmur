// Package objectstore stores immutable file payloads as authenticated encrypted
// chunk streams, independently of SQL and replicated metadata.
package objectstore

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	Version   = 1
	ChunkSize = 64 << 10
	headerLen = 17
	footerLen = 40
)

var (
	ErrInvalidObject = errors.New("objectstore: invalid encrypted object")
	ErrClosed        = errors.New("objectstore: store is closed")
)

var magic = [4]byte{'S', 'P', 'F', 'O'}

// Digest identifies the plaintext content, not its randomized ciphertext.
type Digest [sha256.Size]byte

func (d Digest) String() string { return hex.EncodeToString(d[:]) }

// Info describes a verified immutable object.
type Info struct {
	Digest Digest
	Length int64
}

// Store writes only authenticated ciphertext under root/objects. The
// application controls the independent 256-bit object-storage key.
type Store struct {
	root string
	// keys holds one 256-bit key per object-key generation. Generation 1
	// is implicit (legacy "<digest>.spfo" names); rotation adds newer
	// generations ("<digest>.g<N>.spfo"). The ring holds the current key
	// plus, during and after an interrupted rotation, older keys needed
	// to read not-yet-rewritten objects.
	keys    map[uint32][32]byte
	current uint32
	mu      sync.RWMutex
	dead    bool
	pins    map[Digest]int
}

// New opens (or creates) a local object store with the current-generation
// key. Existing directories are not recursively chmod'ed; the dedicated
// objects directory is created private. When a previous rotation was
// interrupted (objects span generations), New fails loudly: reopen with
// OpenGenerations and the keys of every generation present.
func New(root string, key []byte) (*Store, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("objectstore: key must be 32 bytes")
	}
	if root == "" {
		return nil, fmt.Errorf("objectstore: root is required")
	}
	current, gens, err := openLayout(root)
	if err != nil {
		return nil, err
	}
	for gen := range gens {
		if gen != current {
			return nil, fmt.Errorf("objectstore: objects span key generations (have %d, want only %d): rotation was interrupted; reopen with every generation key", gen, current)
		}
	}
	var kb [32]byte
	copy(kb[:], key)
	return &Store{root: root, keys: map[uint32][32]byte{current: kb}, current: current, pins: make(map[Digest]int)}, nil
}

// Close wipes every configured key and prevents further operations.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.dead {
		for gen, key := range s.keys {
			wipe(key[:])
			delete(s.keys, gen)
		}
		s.dead = true
	}
	return nil
}

// Put streams plaintext into an encrypted temporary file, then atomically
// publishes it under its plaintext digest. No plaintext staging file is made.
func (s *Store) Put(ctx context.Context, src io.Reader) (Info, error) {
	key, err := s.keyCopy()
	if err != nil {
		return Info{}, err
	}
	defer wipe(key[:])
	aead, err := makeAEAD(key[:])
	if err != nil {
		return Info{}, err
	}
	f, err := os.CreateTemp(filepath.Join(s.root, "objects"), ".stage-*")
	if err != nil {
		return Info{}, err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err := f.Chmod(0600); err != nil {
		_ = f.Close()
		return Info{}, err
	}
	var header [headerLen]byte
	copy(header[:4], magic[:])
	header[4] = Version
	binary.BigEndian.PutUint32(header[5:9], ChunkSize)
	if _, err := rand.Read(header[9:]); err != nil {
		_ = f.Close()
		return Info{}, err
	}
	if _, err := f.Write(header[:]); err != nil {
		_ = f.Close()
		return Info{}, err
	}
	var prefix [8]byte
	copy(prefix[:], header[9:])
	chunk := make([]byte, ChunkSize)
	h := sha256.New()
	var total int64
	var index uint64
	for {
		if err := ctx.Err(); err != nil {
			_ = f.Close()
			return Info{}, err
		}
		n, readErr := io.ReadFull(src, chunk)
		if n > 0 {
			if err := writeRecord(f, aead, header[:], prefix, index, chunk[:n]); err != nil {
				_ = f.Close()
				return Info{}, err
			}
			_, _ = h.Write(chunk[:n])
			total += int64(n)
			index++
			if index > uint64(^uint32(0)) {
				_ = f.Close()
				return Info{}, fmt.Errorf("objectstore: too many chunks")
			}
		}
		if readErr == nil {
			continue
		}
		if !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			_ = f.Close()
			return Info{}, readErr
		}
		break
	}
	var digest Digest
	copy(digest[:], h.Sum(nil))
	var footer [footerLen]byte
	binary.BigEndian.PutUint64(footer[:8], uint64(total))
	copy(footer[8:], digest[:])
	sealedFooter := aead.Seal(nil, nonce(prefix, index), footer[:], recordAAD(header[:], index, 0))
	var end [4]byte
	if _, err := f.Write(end[:]); err != nil {
		_ = f.Close()
		return Info{}, err
	}
	if _, err := f.Write(sealedFooter); err != nil {
		_ = f.Close()
		return Info{}, err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return Info{}, err
	}
	if err := f.Close(); err != nil {
		return Info{}, err
	}
	dest := s.objectPath(digest)
	if err := os.Link(tmp, dest); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return Info{}, err
		}
		// Content-addressed collisions are idempotent only if the existing
		// immutable object authenticates to the same length and digest.
		info, verifyErr := s.Read(ctx, digest, io.Discard)
		if verifyErr != nil {
			return Info{}, fmt.Errorf("objectstore: existing object failed verification: %w", verifyErr)
		}
		if info.Length != total {
			return Info{}, fmt.Errorf("objectstore: existing object length mismatch")
		}
		return info, nil
	}
	if err := syncDir(filepath.Dir(dest)); err != nil {
		return Info{}, err
	}
	return Info{Digest: digest, Length: total}, nil
}

// Read streams and authenticates an object to dst. Chunk authentication is
// checked before each chunk is written; the final authenticated trailer checks
// total length and the whole-object digest. Callers must check the returned
// error before treating the entire destination as complete.
func (s *Store) Read(ctx context.Context, digest Digest, dst io.Writer) (Info, error) {
	pin, err := s.Pin(digest)
	if err != nil {
		return Info{}, err
	}
	defer pin.Close()
	path, key, _, err := s.resolve(digest)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Info{}, &os.PathError{Op: "open", Path: s.objectPath(digest), Err: os.ErrNotExist}
		}
		return Info{}, err
	}
	defer wipe(key[:])
	aead, err := makeAEAD(key[:])
	if err != nil {
		return Info{}, err
	}
	f, err := os.Open(path)
	if err != nil {
		return Info{}, err
	}
	defer f.Close()
	return readStream(ctx, aead, f, digest, dst)
}

// readStream authenticates a container stream and writes its plaintext to
// dst. The container digest must equal want; the authenticated trailer must
// agree on total length and whole-object digest.
func readStream(ctx context.Context, aead cipher.AEAD, r io.Reader, want Digest, dst io.Writer) (Info, error) {
	var header [headerLen]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Info{}, fmt.Errorf("%w: header: %v", ErrInvalidObject, err)
	}
	if string(header[:4]) != string(magic[:]) || header[4] != Version || binary.BigEndian.Uint32(header[5:9]) != ChunkSize {
		return Info{}, ErrInvalidObject
	}
	var prefix [8]byte
	copy(prefix[:], header[9:])
	h := sha256.New()
	var total int64
	var index uint64
	for {
		if err := ctx.Err(); err != nil {
			return Info{}, err
		}
		var sizeBuf [4]byte
		if _, err := io.ReadFull(r, sizeBuf[:]); err != nil {
			return Info{}, fmt.Errorf("%w: missing final trailer: %v", ErrInvalidObject, err)
		}
		size := binary.BigEndian.Uint32(sizeBuf[:])
		if size == 0 {
			sealed := make([]byte, footerLen+aead.Overhead())
			if _, err := io.ReadFull(r, sealed); err != nil {
				return Info{}, fmt.Errorf("%w: truncated final trailer: %v", ErrInvalidObject, err)
			}
			footer, err := aead.Open(nil, nonce(prefix, index), sealed, recordAAD(header[:], index, 0))
			if err != nil || len(footer) != footerLen {
				return Info{}, fmt.Errorf("%w: final authentication failed", ErrInvalidObject)
			}
			var extra [1]byte
			if n, err := r.Read(extra[:]); n != 0 || !errors.Is(err, io.EOF) {
				return Info{}, fmt.Errorf("%w: trailing bytes", ErrInvalidObject)
			}
			var actual Digest
			copy(actual[:], h.Sum(nil))
			declaredLength := binary.BigEndian.Uint64(footer[:8])
			var declared Digest
			copy(declared[:], footer[8:])
			if declaredLength != uint64(total) || declared != actual || declared != want {
				return Info{}, fmt.Errorf("%w: length or digest mismatch", ErrInvalidObject)
			}
			return Info{Digest: actual, Length: total}, nil
		}
		if size > ChunkSize {
			return Info{}, fmt.Errorf("%w: chunk length %d exceeds limit", ErrInvalidObject, size)
		}
		sealed := make([]byte, int(size)+aead.Overhead())
		if _, err := io.ReadFull(r, sealed); err != nil {
			return Info{}, fmt.Errorf("%w: truncated chunk: %v", ErrInvalidObject, err)
		}
		plain, err := aead.Open(nil, nonce(prefix, index), sealed, recordAAD(header[:], index, size))
		if err != nil || len(plain) != int(size) {
			return Info{}, fmt.Errorf("%w: chunk authentication failed", ErrInvalidObject)
		}
		if _, err := h.Write(plain); err != nil {
			return Info{}, err
		}
		if err := writeAll(dst, plain); err != nil {
			return Info{}, err
		}
		total += int64(len(plain))
		index++
		if index > uint64(^uint32(0)) {
			return Info{}, fmt.Errorf("%w: too many chunks", ErrInvalidObject)
		}
	}
}

// InstallVerified verifies a staged container file against the expected
// plaintext digest and length, then atomically publishes it under its digest.
// Trust is anchored in the caller's expected values (replicated metadata),
// never in the staged bytes: any mismatch fails closed and the staged file
// is left for the caller to discard. Publication is idempotent: when the
// object already exists and verifies, the staged file is removed and the
// existing info is returned.
func (s *Store) InstallVerified(ctx context.Context, stagedPath string, want Digest, wantLength int64) (Info, error) {
	key, err := s.keyCopy()
	if err != nil {
		return Info{}, err
	}
	defer wipe(key[:])
	aead, err := makeAEAD(key[:])
	if err != nil {
		return Info{}, err
	}
	f, err := os.Open(stagedPath)
	if err != nil {
		return Info{}, err
	}
	info, verr := readStream(ctx, aead, f, want, io.Discard)
	_ = f.Close()
	if verr != nil {
		return Info{}, verr
	}
	if info.Length != wantLength {
		return Info{}, fmt.Errorf("%w: staged length %d, want %d", ErrInvalidObject, info.Length, wantLength)
	}
	dest := s.objectPath(want)
	if err := os.Link(stagedPath, dest); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return Info{}, err
		}
		// Duplicate transfer: the object is already published. Verify the
		// winner before accepting it.
		existing, verifyErr := s.Read(ctx, want, io.Discard)
		if verifyErr != nil {
			return Info{}, fmt.Errorf("objectstore: existing object failed verification: %w", verifyErr)
		}
		if existing.Length != wantLength {
			return Info{}, fmt.Errorf("objectstore: existing object length mismatch")
		}
		_ = os.Remove(stagedPath)
		return existing, nil
	}
	if err := syncDir(filepath.Dir(dest)); err != nil {
		return Info{}, err
	}
	_ = os.Remove(stagedPath)
	return info, nil
}

// Has reports whether a digest-named object is present. It does not certify
// object integrity; use Read when content is consumed.
func (s *Store) Has(digest Digest) bool {
	if _, _, _, err := s.resolve(digest); err != nil {
		return false
	}
	return true
}

// Pin keeps an object's directory entry available for a reader or
// external export. Close the returned pin when that use ends.
func (s *Store) Pin(digest Digest) (*Pin, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Collection uses the same lock: existence and retention must be atomic.
	if _, _, _, err := s.resolveLocked(digest); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &os.PathError{Op: "stat", Path: s.objectPathFor(digest, s.current), Err: os.ErrNotExist}
		}
		return nil, err
	}
	s.pins[digest]++
	return &Pin{store: s, digest: digest}, nil
}

// ServedObject is a pinned container open for serving byte ranges to fetch
// peers. The receiver verifies content against replicated metadata; the
// server sends stored bytes verbatim.
type ServedObject struct {
	f    *os.File
	size int64
	pin  *Pin
}

// Serve opens a container for range serving, holding a retention pin so
// collection cannot remove it mid-transfer.
func (s *Store) Serve(digest Digest) (*ServedObject, error) {
	if s.isClosed() {
		return nil, ErrClosed
	}
	pin, err := s.Pin(digest)
	if err != nil {
		return nil, err
	}
	openResolved := func() (*os.File, error) {
		path, _, _, err := s.resolve(digest)
		if err != nil {
			return nil, err
		}
		return os.Open(path)
	}
	f, err := openResolved()
	if err != nil && os.IsNotExist(err) {
		// A rotation may have replaced the resolved generation between
		// the pin and the open; resolve once more.
		f, err = openResolved()
	}
	if err != nil {
		_ = pin.Close()
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		_ = pin.Close()
		return nil, err
	}
	return &ServedObject{f: f, size: fi.Size(), pin: pin}, nil
}

// Size is the total container length.
func (o *ServedObject) Size() int64 { return o.size }

// ReadAt reads container bytes at an offset.
func (o *ServedObject) ReadAt(p []byte, off int64) (int, error) {
	return o.f.ReadAt(p, off)
}

// Close releases the file and the retention pin.
func (o *ServedObject) Close() error {
	if o == nil {
		return nil
	}
	ferr := o.f.Close()
	_ = o.pin.Close()
	return ferr
}

// Pin represents a retention pin. Close is idempotent.
type Pin struct {
	store  *Store
	digest Digest
	once   sync.Once
}

func (p *Pin) Close() error {
	if p == nil || p.store == nil {
		return nil
	}
	p.once.Do(func() {
		p.store.mu.Lock()
		defer p.store.mu.Unlock()
		if p.store.pins[p.digest] <= 1 {
			delete(p.store.pins, p.digest)
		} else {
			p.store.pins[p.digest]--
		}
	})
	return nil
}

// List verifies every object and returns its digest and plaintext length.
// Corrupt objects fail the inventory scan closed.
func (s *Store) List(ctx context.Context) ([]Info, error) {
	if s.isClosed() {
		return nil, ErrClosed
	}
	entries, err := os.ReadDir(filepath.Join(s.root, "objects"))
	if err != nil {
		return nil, err
	}
	// Deduplicate across generations (rotation residue): Read resolves
	// the newest generation with a known key.
	seen := make(map[Digest]bool)
	infos := make([]Info, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		digest, _, ok := parseObjectName(entry.Name())
		if !ok {
			if strings.HasSuffix(entry.Name(), ".spfo") {
				return nil, fmt.Errorf("%w: malformed object filename", ErrInvalidObject)
			}
			continue
		}
		if seen[digest] {
			continue
		}
		seen[digest] = true
		info, err := s.Read(ctx, digest, io.Discard)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// Collect removes unreferenced candidates that have aged at least
// grace and are not pinned by readers or exports. The caller supplies the
// authoritative reference inventory; this package does not own SQL metadata.
func (s *Store) Collect(referenced []Digest, grace time.Duration) ([]Digest, error) {
	if grace < 0 {
		return nil, fmt.Errorf("objectstore: negative collection grace")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return nil, ErrClosed
	}
	keep := make(map[Digest]struct{}, len(referenced))
	for _, digest := range referenced {
		keep[digest] = struct{}{}
	}
	dir := filepath.Join(s.root, "objects")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	cutoff := time.Now().Add(-grace)
	var removed []Digest
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		digest, _, ok := parseObjectName(entry.Name())
		if !ok {
			continue
		}
		if _, ok := keep[digest]; ok || s.pins[digest] > 0 {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return removed, err
		}
		if grace > 0 && info.ModTime().After(cutoff) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return removed, err
		}
		removed = append(removed, digest)
	}
	if len(removed) > 0 {
		if err := syncDir(dir); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

func (s *Store) keyCopy() ([32]byte, error) {
	return s.keyCopyFor(s.CurrentGeneration())
}

func (s *Store) isClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dead
}

func (s *Store) objectPath(d Digest) string {
	return s.objectPathFor(d, s.CurrentGeneration())
}

func makeAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func writeRecord(w io.Writer, aead cipher.AEAD, header []byte, prefix [8]byte, index uint64, plain []byte) error {
	if len(plain) == 0 || len(plain) > ChunkSize || index > uint64(^uint32(0)) {
		return ErrInvalidObject
	}
	sealed := aead.Seal(nil, nonce(prefix, index), plain, recordAAD(header, index, uint32(len(plain))))
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(plain)))
	if _, err := w.Write(size[:]); err != nil {
		return err
	}
	_, err := w.Write(sealed)
	return err
}

func nonce(prefix [8]byte, index uint64) []byte {
	var n [12]byte
	copy(n[:8], prefix[:])
	binary.BigEndian.PutUint32(n[8:], uint32(index))
	return n[:]
}

func recordAAD(header []byte, index uint64, size uint32) []byte {
	var aad [headerLen + 12]byte
	copy(aad[:headerLen], header)
	binary.BigEndian.PutUint64(aad[headerLen:headerLen+8], index)
	binary.BigEndian.PutUint32(aad[headerLen+8:], size)
	return aad[:]
}

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func writeAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if n > 0 {
			b = b[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
