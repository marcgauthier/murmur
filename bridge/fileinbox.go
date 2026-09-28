package bridge

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Inbox file-object staging.
//
// Sealed chunk artifacts stage as files under the inbox's fobj directory
// with progress journaled in state.json. A complete chunk set becomes ready
// for import; the importer installs it once the matching file metadata has
// imported (either arrival order works). Chunks are idempotent by
// (digest, index); a conflicting payload for one index quarantines the whole
// object loudly.

// fileStaged tracks one inbound object's chunks.
type fileStaged struct {
	stream   string
	count    uint32
	totalLen uint64
	// got maps chunk index to the base64 plaintext digest.
	got  map[uint32]string
	size int64
}

// fileStagedJSON persists one inbound object.
type fileStagedJSON struct {
	Stream   string            `json:"stream"`
	Count    uint32            `json:"count"`
	TotalLen uint64            `json:"total_len"`
	Got      map[string]string `json:"got,omitempty"`
	Size     int64             `json:"size"`
}

// fileQuarantineJSON persists one quarantined object.
type fileQuarantineJSON struct {
	Stream string `json:"stream"`
	Reason string `json:"reason"`
}

// ReadyFile is one complete staged object awaiting metadata + install.
type ReadyFile struct {
	Digest   [32]byte
	Stream   string
	TotalLen uint64
}

// FileProgress reports inbound object staging for status and tests.
type FileProgress struct {
	Pending []string
	Ready   []string
	// Quarantined maps digest hex to reason.
	Quarantined map[string]string
}

// receiveFileChunk validates, verifies, and stages one chunk artifact.
func (in *Inbox) receiveFileChunk(a Artifact) error {
	if err := ValidateArtifact(a, in.limits); err != nil {
		return err
	}
	nameStream, nameDigest, nameIndex, nameCount, err := ParseFileChunkArtifactName(a.Name)
	if err != nil {
		return err
	}
	chunk, err := OpenFileChunk(a.Data, in.trust, in.limits)
	if err != nil {
		return err
	}
	// The filename is a routing hint; the sealed framing is authoritative.
	// A mismatch means a mislabeled or transplanted artifact: refuse it.
	if chunk.Stream != nameStream || chunk.Digest != nameDigest || chunk.Index != nameIndex || chunk.Count != nameCount {
		return fmt.Errorf("bridge: file chunk filename does not match sealed routing")
	}
	// Identity compares plaintext: re-sealed re-deliveries differ in sealed
	// bytes (random content key) while carrying identical content.
	plainSum := sha256.Sum256(chunk.Bytes)
	plainB64 := base64.StdEncoding.EncodeToString(plainSum[:])
	key := hex.EncodeToString(chunk.Digest[:])

	in.mu.Lock()
	defer in.mu.Unlock()
	if _, ok := in.fileQuar[key]; ok {
		return fmt.Errorf("bridge: file object %s is quarantined", key)
	}
	st := in.fileLocked(key)
	if st.count == 0 {
		st.stream = chunk.Stream
		st.count = chunk.Count
		st.totalLen = chunk.TotalLen
		st.got = make(map[uint32]string)
	}
	if st.stream != chunk.Stream || st.count != chunk.Count || st.totalLen != chunk.TotalLen {
		if st.count > 0 && uint32(len(st.got)) == st.count {
			return nil // a complete set is already staged; ignore differently-chunked redelivery
		}
		return in.quarantineFileLocked(key, st, a, fmt.Sprintf("conflicting chunk identity for object %s", key))
	}
	if want, ok := st.got[chunk.Index]; ok {
		if want == plainB64 {
			return nil // identical re-delivery
		}
		return in.quarantineFileLocked(key, st, a, fmt.Sprintf("conflicting content for object %s chunk %d", key, chunk.Index))
	}
	if err := admit("inbox", int64(len(a.Data)), in.used, in.limits.MaxStagingBytes, in.entriesLocked(), in.limits.MaxStagingEntries); err != nil {
		return err
	}
	file := fmt.Sprintf("fobj-%s-%06d.fobj", key, chunk.Index)
	if err := writeFileSync(in.fobjDir, file, a.Data); err != nil {
		return err
	}
	st.got[chunk.Index] = plainB64
	st.size += int64(len(a.Data))
	in.used += int64(len(a.Data))
	return in.saveLocked()
}

// fileLocked returns the staging state for a digest, creating it.
func (in *Inbox) fileLocked(key string) *fileStaged {
	st, ok := in.files[key]
	if !ok {
		st = &fileStaged{got: make(map[uint32]string)}
		in.files[key] = st
	}
	return st
}

// ReadyFiles lists complete staged objects not yet installed.
func (in *Inbox) ReadyFiles() []ReadyFile {
	in.mu.Lock()
	defer in.mu.Unlock()
	var out []ReadyFile
	for key, st := range in.files {
		if st.count == 0 || uint32(len(st.got)) != st.count {
			continue
		}
		if _, ok := in.fileQuar[key]; ok {
			continue
		}
		raw, err := hex.DecodeString(key)
		if err != nil || len(raw) != 32 {
			continue
		}
		var digest [32]byte
		copy(digest[:], raw)
		out = append(out, ReadyFile{Digest: digest, Stream: st.stream, TotalLen: st.totalLen})
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Digest[:], out[j].Digest[:]) < 0
	})
	return out
}

// openReadyFile reads, re-verifies, and orders one complete object's chunks.
func (in *Inbox) openReadyFile(digest [32]byte) ([]*FileChunk, error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	key := hex.EncodeToString(digest[:])
	st, ok := in.files[key]
	if !ok || st.count == 0 || uint32(len(st.got)) != st.count {
		return nil, fmt.Errorf("bridge: file object %s is not staged complete", key)
	}
	out := make([]*FileChunk, 0, st.count)
	for index := uint32(0); index < st.count; index++ {
		raw, err := os.ReadFile(filepath.Join(in.fobjDir, fmt.Sprintf("fobj-%s-%06d.fobj", key, index)))
		if err != nil {
			return nil, fmt.Errorf("bridge: staged file chunk unreadable: %w", err)
		}
		chunk, err := OpenFileChunk(raw, in.trust, in.limits)
		if err != nil {
			return nil, err
		}
		if chunk.Digest != digest || chunk.Index != index || chunk.Count != st.count || chunk.TotalLen != st.totalLen {
			return nil, fmt.Errorf("bridge: staged file chunk %d identity drift", index)
		}
		out = append(out, chunk)
	}
	return out, nil
}

// removeFileLocked drops one staged object (files plus journal) and
// subtracts its staged bytes. The caller holds in.mu.
func (in *Inbox) removeFileLocked(key string) {
	st, ok := in.files[key]
	if !ok {
		return
	}
	for index := range st.got {
		_ = os.Remove(filepath.Join(in.fobjDir, fmt.Sprintf("fobj-%s-%06d.fobj", key, index)))
	}
	in.used -= st.size
	delete(in.files, key)
}

// FileApplied discards a successfully installed object's staging.
func (in *Inbox) FileApplied(digest [32]byte) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	in.removeFileLocked(hex.EncodeToString(digest[:]))
	return in.saveLocked()
}

// quarantineFileLocked moves a staged object to quarantine with a reason.
// The caller holds in.mu.
func (in *Inbox) quarantineFileLocked(key string, st *fileStaged, _ Artifact, reason string) error {
	for index := range st.got {
		src := filepath.Join(in.fobjDir, fmt.Sprintf("fobj-%s-%06d.fobj", key, index))
		dst := filepath.Join(in.quarDir, fmt.Sprintf("fobj-%s-%06d.fobj", key, index))
		if _, err := os.Stat(src); err == nil {
			_ = os.Rename(src, dst)
		}
	}
	in.fileQuar[key] = &fileQuarantineJSON{Stream: st.stream, Reason: reason}
	delete(in.files, key)
	// Bytes move with the files; used stays constant.
	if err := in.saveLocked(); err != nil {
		return err
	}
	return fmt.Errorf("bridge: quarantined file object %s: %s", key, reason)
}

// QuarantineFile quarantines a staged object (for import-time failures).
func (in *Inbox) QuarantineFile(digest [32]byte, reason string) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	key := hex.EncodeToString(digest[:])
	st, ok := in.files[key]
	if !ok {
		return fmt.Errorf("bridge: file object %s is not staged", key)
	}
	return in.quarantineFileLocked(key, st, Artifact{}, reason)
}

// RetryQuarantinedFile returns a quarantined object to staging for another
// import attempt.
func (in *Inbox) RetryQuarantinedFile(digest [32]byte) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	key := hex.EncodeToString(digest[:])
	rec, ok := in.fileQuar[key]
	if !ok {
		return fmt.Errorf("bridge: file object %s is not quarantined", key)
	}
	// Quarantined chunk files move back; the got-set rebuilds from disk.
	st := &fileStaged{stream: rec.Stream, got: make(map[uint32]string)}
	entries, err := os.ReadDir(in.quarDir)
	if err != nil {
		return err
	}
	prefix := fmt.Sprintf("fobj-%s-", key)
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || len(name) != len(prefix)+6+len(".fobj") || name[:len(prefix)] != prefix {
			continue
		}
		if err := os.Rename(filepath.Join(in.quarDir, name), filepath.Join(in.fobjDir, name)); err != nil {
			return err
		}
	}
	entries, err = os.ReadDir(in.fobjDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || len(name) != len(prefix)+6+len(".fobj") || name[:len(prefix)] != prefix {
			continue
		}
		var index uint32
		if _, err := fmt.Sscanf(name[len(prefix):len(prefix)+6], "%d", &index); err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(in.fobjDir, name))
		if err != nil {
			continue
		}
		chunk, err := OpenFileChunk(raw, in.trust, in.limits)
		if err != nil || chunk.Digest != digest {
			continue
		}
		if st.count == 0 {
			st.count, st.totalLen = chunk.Count, chunk.TotalLen
		}
		if chunk.Count != st.count || chunk.TotalLen != st.totalLen {
			continue
		}
		sum := sha256.Sum256(chunk.Bytes)
		st.got[index] = base64.StdEncoding.EncodeToString(sum[:])
		fi, err := e.Info()
		if err == nil {
			st.size += fi.Size()
		}
	}
	if len(st.got) == 0 {
		return fmt.Errorf("bridge: quarantined file object %s has no chunks to retry", key)
	}
	in.files[key] = st
	delete(in.fileQuar, key)
	return in.saveLocked()
}

// FileProgress reports inbound object staging state.
func (in *Inbox) FileProgress() FileProgress {
	in.mu.Lock()
	defer in.mu.Unlock()
	out := FileProgress{Quarantined: make(map[string]string)}
	for key, st := range in.files {
		if st.count > 0 && uint32(len(st.got)) == st.count {
			out.Ready = append(out.Ready, key)
		} else {
			out.Pending = append(out.Pending, key)
		}
	}
	for key, rec := range in.fileQuar {
		out.Quarantined[key] = rec.Reason
	}
	sort.Strings(out.Pending)
	sort.Strings(out.Ready)
	return out
}
