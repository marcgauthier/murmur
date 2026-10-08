package objectstore

import (
	"bytes"
	"context"
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
	"sort"
	"strconv"
	"strings"
)

// Object-key generations and rotation.
//
// Each object file's name carries its key generation: "<digest>.spfo" is
// generation 1 (implicit, the original format) and "<digest>.g<N>.spfo" is
// generation N. The current generation persists in the "generation" file
// (a plaintext decimal; absent means 1). Rotation rewrites every object
// under a new key, publishes the new generation, then deletes superseded
// files; a crash anywhere in between reopens with both keys and completes
// automatically. Keys never touch disk: the operator supplies the current
// key at open (plus older keys only to recover an interrupted rotation).

// generationFile is the persisted current generation, relative to root.
const generationFile = "generation"

// openLayout prepares the objects directory and reports the current key
// generation plus every generation with files on disk.
func openLayout(root string) (uint32, map[uint32]bool, error) {
	if root == "" {
		return 0, nil, fmt.Errorf("objectstore: root is required")
	}
	objects := filepath.Join(root, "objects")
	if err := os.MkdirAll(objects, 0700); err != nil {
		return 0, nil, fmt.Errorf("objectstore: create root: %w", err)
	}
	st, err := os.Lstat(objects)
	if err != nil {
		return 0, nil, err
	}
	if !st.IsDir() {
		return 0, nil, fmt.Errorf("objectstore: object path is not a directory")
	}
	if err := os.Chmod(objects, 0700); err != nil {
		return 0, nil, fmt.Errorf("objectstore: protect object directory: %w", err)
	}
	current, err := readGeneration(root)
	if err != nil {
		return 0, nil, err
	}
	gens, err := presentGenerations(objects)
	if err != nil {
		return 0, nil, err
	}
	return current, gens, nil
}

// readGeneration returns the persisted current generation (1 when absent).
func readGeneration(root string) (uint32, error) {
	raw, err := os.ReadFile(filepath.Join(root, generationFile))
	if err != nil {
		if os.IsNotExist(err) {
			return 1, nil
		}
		return 0, err
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 32)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("objectstore: malformed generation file")
	}
	return uint32(n), nil
}

// writeGeneration durably publishes the current generation.
func writeGeneration(root string, gen uint32) error {
	tmp, err := os.CreateTemp(root, "tmp_generation_")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.WriteString(fmt.Sprintf("%d\n", gen)); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(root, generationFile)); err != nil {
		return err
	}
	return syncDir(root)
}

// parseObjectName splits "<digest>.spfo" (generation 1) and
// "<digest>.g<N>.spfo". Anything else is not an object file.
func parseObjectName(name string) (Digest, uint32, bool) {
	var digest Digest
	if !strings.HasSuffix(name, ".spfo") {
		return digest, 0, false
	}
	base := strings.TrimSuffix(name, ".spfo")
	gen := uint32(1)
	if i := strings.LastIndexByte(base, '.'); i >= 0 {
		if !strings.HasPrefix(base[i:], ".g") {
			return digest, 0, false
		}
		n, err := strconv.ParseUint(base[i+2:], 10, 32)
		if err != nil || n < 2 {
			return digest, 0, false
		}
		gen = uint32(n)
		base = base[:i]
	}
	if len(base) != hex.EncodedLen(32) {
		return digest, 0, false
	}
	raw, err := hex.DecodeString(base)
	if err != nil || len(raw) != 32 {
		return digest, 0, false
	}
	copy(digest[:], raw)
	return digest, gen, true
}

// objectName renders the filename for a digest generation.
func objectName(d Digest, gen uint32) string {
	if gen <= 1 {
		return d.String() + ".spfo"
	}
	return d.String() + ".g" + strconv.FormatUint(uint64(gen), 10) + ".spfo"
}

// presentGenerations lists generations with files on disk.
func presentGenerations(objects string) (map[uint32]bool, error) {
	entries, err := os.ReadDir(objects)
	if err != nil {
		return nil, err
	}
	out := make(map[uint32]bool)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, gen, ok := parseObjectName(e.Name()); ok {
			out[gen] = true
		}
	}
	return out, nil
}

// objectPathFor names one generation's file.
func (s *Store) objectPathFor(d Digest, gen uint32) string {
	return filepath.Join(s.root, "objects", objectName(d, gen))
}

// keyCopyFor copies one generation's key.
func (s *Store) keyCopyFor(gen uint32) ([32]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.dead {
		return [32]byte{}, ErrClosed
	}
	key, ok := s.keys[gen]
	if !ok {
		return [32]byte{}, fmt.Errorf("objectstore: no key for generation %d", gen)
	}
	return key, nil
}

// resolve locates a digest, preferring the current generation and falling
// back to older generations with known keys. It returns the path, key, and
// generation of the winner.
func (s *Store) resolve(digest Digest) (string, [32]byte, uint32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.resolveLocked(digest)
}

// resolveLocked requires s.mu to be held for reading or writing.
func (s *Store) resolveLocked(digest Digest) (string, [32]byte, uint32, error) {
	if s.dead {
		return "", [32]byte{}, 0, ErrClosed
	}
	gens := make([]uint32, 0, len(s.keys))
	for gen := range s.keys {
		gens = append(gens, gen)
	}
	sort.Slice(gens, func(i, j int) bool { return gens[i] > gens[j] })
	for _, gen := range gens {
		path := filepath.Join(s.root, "objects", objectName(digest, gen))
		if _, err := os.Stat(path); err == nil {
			return path, s.keys[gen], gen, nil
		} else if !os.IsNotExist(err) {
			return "", [32]byte{}, 0, err
		}
	}
	return "", [32]byte{}, 0, os.ErrNotExist
}

// OpenGenerations opens a store with several generation keys (recovery after
// an interrupted rotation, or a store whose current key plus older keys are
// all supplied). It completes any interrupted rotation: objects present in
// both an older and a newer generation drop the older file; objects stuck on
// an older generation rewrite forward. Afterwards only the current
// generation's key is retained.
func OpenGenerations(root string, keys map[uint32][]byte) (*Store, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("objectstore: at least one key is required")
	}
	ring := make(map[uint32][32]byte, len(keys))
	for gen, key := range keys {
		if gen < 1 {
			return nil, fmt.Errorf("objectstore: invalid generation %d", gen)
		}
		if len(key) != 32 {
			return nil, fmt.Errorf("objectstore: key must be 32 bytes")
		}
		var kb [32]byte
		copy(kb[:], key)
		ring[gen] = kb
	}
	current, gens, err := openLayout(root)
	if err != nil {
		return nil, err
	}
	// An interrupted rotation leaves files above the persisted generation
	// (the in-memory target never reached the generation file): adopt the
	// newest generation present so recovery completes forward.
	for gen := range gens {
		if gen > current {
			current = gen
		}
	}
	if _, ok := ring[current]; !ok {
		return nil, fmt.Errorf("objectstore: current generation %d has no key", current)
	}
	for gen := range gens {
		if _, ok := ring[gen]; !ok {
			return nil, fmt.Errorf("objectstore: generation %d present on disk has no key", gen)
		}
	}
	s := &Store{root: root, keys: ring, current: current, pins: make(map[Digest]int)}
	if err := s.finalizeRotation(context.Background(), nil); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// OpenWithPrevious opens a store with the current-generation key plus the
// previous generation's key for recovering an interrupted rotation. When no
// older generation is present the previous key is accepted and ignored, so
// operators can leave it configured until every node has rotated.
func OpenWithPrevious(root string, currentKey, prevKey []byte) (*Store, error) {
	if len(currentKey) != 32 || len(prevKey) != 32 {
		return nil, fmt.Errorf("objectstore: keys must be 32 bytes")
	}
	current, gens, err := openLayout(root)
	if err != nil {
		return nil, err
	}
	for gen := range gens {
		if gen > current {
			current = gen
		}
	}
	ring := map[uint32][]byte{current: currentKey}
	if current > 1 {
		ring[current-1] = prevKey
	}
	return OpenGenerations(root, ring)
}

// CurrentGeneration reports the store's current key generation.
func (s *Store) CurrentGeneration() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}

// RotateKey re-encrypts every object under newKey, publishes the next
// generation, and drops superseded files and the old key. Objects created
// concurrently land on the new generation and are never missed. Progress
// reports completed/total objects and may be nil. A crash mid-rotation
// reopens via OpenGenerations (old plus new key) and completes there.
func (s *Store) RotateKey(ctx context.Context, newKey []byte, progress func(done, total int)) error {
	if len(newKey) != 32 {
		return fmt.Errorf("objectstore: key must be 32 bytes")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.dead {
		s.mu.Unlock()
		return ErrClosed
	}
	next := s.current + 1
	if next < 2 {
		s.mu.Unlock()
		return fmt.Errorf("objectstore: generation overflow")
	}
	var nb [32]byte
	copy(nb[:], newKey)
	s.keys[next] = nb
	s.current = next
	s.mu.Unlock()

	if err := s.finalizeRotation(ctx, progress); err != nil {
		return err
	}
	// Drop superseded keys from memory now that only the new generation
	// has files on disk.
	s.mu.Lock()
	defer s.mu.Unlock()
	for gen, key := range s.keys {
		if gen == s.current {
			continue
		}
		wipe(key[:])
		delete(s.keys, gen)
	}
	return nil
}

// finalizeRotation rewrites every below-current object forward, publishes
// the generation file, and deletes superseded files. It is idempotent:
// completed objects (newer sibling present) skip the rewrite, vanished
// objects skip silently (collection raced), and re-running converges.
func (s *Store) finalizeRotation(ctx context.Context, progress func(done, total int)) error {
	s.mu.RLock()
	current := s.current
	s.mu.RUnlock()

	entries, err := os.ReadDir(filepath.Join(s.root, "objects"))
	if err != nil {
		return err
	}
	type job struct {
		digest Digest
		from   uint32
	}
	var jobs []job
	seen := make(map[Digest]uint32) // digest -> newest gen present
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		digest, gen, ok := parseObjectName(e.Name())
		if !ok || gen >= current {
			continue
		}
		if prev, dup := seen[digest]; dup && prev >= gen {
			continue
		}
		seen[digest] = gen
	}
	for digest, gen := range seen {
		jobs = append(jobs, job{digest: digest, from: gen})
	}
	sort.Slice(jobs, func(i, j int) bool { return bytes.Compare(jobs[i].digest[:], jobs[j].digest[:]) < 0 })
	for i, jb := range jobs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.rewriteForward(ctx, jb.digest, jb.from, current); err != nil {
			return err
		}
		if progress != nil {
			progress(i+1, len(jobs))
		}
	}
	if err := writeGeneration(s.root, current); err != nil {
		return err
	}
	// Delete superseded files only where the newer sibling verified present.
	for _, jb := range jobs {
		if _, err := os.Stat(s.objectPathFor(jb.digest, current)); err != nil {
			continue
		}
		_ = os.Remove(s.objectPathFor(jb.digest, jb.from))
	}
	return syncDir(filepath.Join(s.root, "objects"))
}

// rekeyStream decrypts a container record-by-record with oldAEAD and
// re-seals it with newAEAD, refreshing the container nonce prefix. The
// plaintext digest must equal want; any authentication failure fails closed
// without writing a partial target (the caller discards the temp file).
func rekeyStream(ctx context.Context, oldAEAD, newAEAD cipher.AEAD, src io.Reader, dst io.Writer, want Digest) error {
	var oldHeader [headerLen]byte
	if _, err := io.ReadFull(src, oldHeader[:]); err != nil {
		return fmt.Errorf("%w: header: %v", ErrInvalidObject, err)
	}
	if string(oldHeader[:4]) != string(magic[:]) || oldHeader[4] != Version || binary.BigEndian.Uint32(oldHeader[5:9]) != ChunkSize {
		return ErrInvalidObject
	}
	var oldPrefix [8]byte
	copy(oldPrefix[:], oldHeader[9:])
	var newHeader [headerLen]byte
	copy(newHeader[:4], magic[:])
	newHeader[4] = Version
	binary.BigEndian.PutUint32(newHeader[5:9], ChunkSize)
	if _, err := rand.Read(newHeader[9:]); err != nil {
		return err
	}
	if _, err := dst.Write(newHeader[:]); err != nil {
		return err
	}
	var newPrefix [8]byte
	copy(newPrefix[:], newHeader[9:])
	hasher := sha256.New()
	var total int64
	var index uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var sizeBuf [4]byte
		if _, err := io.ReadFull(src, sizeBuf[:]); err != nil {
			return fmt.Errorf("%w: missing final trailer: %v", ErrInvalidObject, err)
		}
		size := binary.BigEndian.Uint32(sizeBuf[:])
		if size == 0 {
			sealed := make([]byte, footerLen+oldAEAD.Overhead())
			if _, err := io.ReadFull(src, sealed); err != nil {
				return fmt.Errorf("%w: truncated final trailer: %v", ErrInvalidObject, err)
			}
			footer, err := oldAEAD.Open(nil, nonce(oldPrefix, index), sealed, recordAAD(oldHeader[:], index, 0))
			if err != nil || len(footer) != footerLen {
				return fmt.Errorf("%w: final authentication failed", ErrInvalidObject)
			}
			var extra [1]byte
			if n, err := src.Read(extra[:]); n != 0 || !errors.Is(err, io.EOF) {
				return fmt.Errorf("%w: trailing bytes", ErrInvalidObject)
			}
			var actual Digest
			copy(actual[:], hasher.Sum(nil))
			declaredLength := binary.BigEndian.Uint64(footer[:8])
			var declared Digest
			copy(declared[:], footer[8:])
			if declaredLength != uint64(total) || declared != actual || declared != want {
				return fmt.Errorf("%w: length or digest mismatch", ErrInvalidObject)
			}
			sealedFooter := newAEAD.Seal(nil, nonce(newPrefix, index), footer, recordAAD(newHeader[:], index, 0))
			var end [4]byte
			if _, err := dst.Write(end[:]); err != nil {
				return err
			}
			if _, err := dst.Write(sealedFooter); err != nil {
				return err
			}
			return nil
		}
		if size > ChunkSize {
			return fmt.Errorf("%w: chunk length %d exceeds limit", ErrInvalidObject, size)
		}
		sealed := make([]byte, int(size)+oldAEAD.Overhead())
		if _, err := io.ReadFull(src, sealed); err != nil {
			return fmt.Errorf("%w: truncated chunk: %v", ErrInvalidObject, err)
		}
		plain, err := oldAEAD.Open(nil, nonce(oldPrefix, index), sealed, recordAAD(oldHeader[:], index, size))
		if err != nil || len(plain) != int(size) {
			return fmt.Errorf("%w: chunk authentication failed", ErrInvalidObject)
		}
		if _, err := hasher.Write(plain); err != nil {
			return err
		}
		if err := writeRecord(dst, newAEAD, newHeader[:], newPrefix, index, plain); err != nil {
			return err
		}
		total += int64(len(plain))
		index++
		if index > uint64(^uint32(0)) {
			return fmt.Errorf("%w: too many chunks", ErrInvalidObject)
		}
	}
}

// rewriteForward re-encrypts one object's oldest generation forward to the
// target generation. A present target file skips the work (a previous run
// completed it); callers delete the superseded source afterwards.
func (s *Store) rewriteForward(ctx context.Context, digest Digest, from, to uint32) error {
	if _, err := os.Stat(s.objectPathFor(digest, to)); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	// Pin by digest so collection cannot remove the source mid-rewrite.
	pin, err := s.Pin(digest)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // collection raced; nothing to rewrite
		}
		return err
	}
	defer pin.Close()
	oldKey, err := s.keyCopyFor(from)
	if err != nil {
		return err
	}
	defer wipe(oldKey[:])
	newKey, err := s.keyCopyFor(to)
	if err != nil {
		return err
	}
	defer wipe(newKey[:])
	oldAEAD, err := makeAEAD(oldKey[:])
	if err != nil {
		return err
	}
	newAEAD, err := makeAEAD(newKey[:])
	if err != nil {
		return err
	}
	src, err := os.Open(s.objectPathFor(digest, from))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer src.Close()
	tmp, err := os.CreateTemp(filepath.Join(s.root, "objects"), "tmp_rot_")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	// Stream decrypt-with-old / encrypt-with-new without materializing
	// plaintext: decode each record, then re-seal it into the new file.
	// The container nonce prefix refreshes (record nonces must never
	// repeat under a new key with attacker-visible plaintext).
	if err := rekeyStream(ctx, oldAEAD, newAEAD, src, tmp, digest); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	// Verify the rewritten file under the new key before publishing.
	verify, err := os.Open(tmpName)
	if err != nil {
		return err
	}
	_, verr := readStream(ctx, newAEAD, verify, digest, io.Discard)
	_ = verify.Close()
	if verr != nil {
		return verr
	}
	if err := os.Chmod(tmpName, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmpName, s.objectPathFor(digest, to)); err != nil {
		return err
	}
	return syncDir(filepath.Join(s.root, "objects"))
}
