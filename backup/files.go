package backup

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// File-object coverage for backups.
//
// Object-inclusive backups hard-link files/objects plus the key-generation
// marker into staging. Object files are immutable with atomic renames, so
// hard links are a consistent snapshot — except across a concurrent key
// rotation, which rewrites generations mid-walk. The snapshot retries until
// every staged object sits on the staged generation marker (bounded
// attempts, then a loud error instead of a mixed archive). Fetch staging
// and writer temp files never enter the archive: partial bytes are
// resumable, never restorable state.

// filesSnapshotAttempts bounds snapshot retries across a live rotation.
const filesSnapshotAttempts = 3

// snapshotFiles stages files/objects plus the generation marker, returning
// the staged object count and bytes. Staging wipes and retries while a
// concurrent rotation mixes generations.
func snapshotFiles(filesDir, stagingFiles string, log Logger) (int, int64, error) {
	var lastErr error
	for attempt := 0; attempt < filesSnapshotAttempts; attempt++ {
		if attempt > 0 {
			log.Debug("backup: files snapshot raced rotation, retrying", "attempt", attempt+1)
		}
		if err := os.RemoveAll(stagingFiles); err != nil {
			return 0, 0, fmt.Errorf("backup: clear files staging: %w", err)
		}
		count, bytes, err := linkFilesTree(filesDir, stagingFiles)
		if err != nil {
			return 0, 0, err
		}
		gen, genOK := readStagedGeneration(stagingFiles)
		if uniform, uerr := stagedUniform(stagingFiles, gen, genOK); uerr != nil {
			return 0, 0, uerr
		} else if !uniform {
			lastErr = fmt.Errorf("backup: files snapshot raced an object-key rotation")
			continue
		}
		return count, bytes, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("backup: files snapshot failed")
	}
	return 0, 0, lastErr
}

// linkFilesTree hard-links (falling back to copy) files/objects plus the
// generation marker into staging, skipping fetch staging and temp files.
func linkFilesTree(filesDir, stagingFiles string) (int, int64, error) {
	objects := filepath.Join(filesDir, "objects")
	entries, err := os.ReadDir(objects)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil // files enabled but nothing stored yet
		}
		return 0, 0, fmt.Errorf("backup: read files objects: %w", err)
	}
	stagingObjects := filepath.Join(stagingFiles, "objects")
	if err := os.MkdirAll(stagingObjects, 0o700); err != nil {
		return 0, 0, fmt.Errorf("backup: mkdir files staging: %w", err)
	}
	var count int
	var total int64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "tmp_") {
			continue
		}
		if _, _, ok := splitObjectName(name); !ok {
			continue // staging residue or foreign file; never restorable state
		}
		src := filepath.Join(objects, name)
		dst := filepath.Join(stagingObjects, name)
		if err := os.Link(src, dst); err != nil {
			if os.IsNotExist(err) {
				continue // collection/rotation raced; the file is gone either way
			}
			// Cross-mount staging or privilege limits: copy instead.
			if copyErr := copyRawFile(src, dst); copyErr != nil {
				if os.IsNotExist(copyErr) {
					continue
				}
				return 0, 0, fmt.Errorf("backup: stage object %s: %w", name, copyErr)
			}
		}
		fi, err := os.Stat(dst)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return 0, 0, fmt.Errorf("backup: stat staged object %s: %w", name, err)
		}
		count++
		total += fi.Size()
	}
	// The generation marker copies (never links: rotation rewrites it).
	if _, err := os.Stat(filepath.Join(filesDir, "generation")); err == nil {
		if err := copyRawFile(filepath.Join(filesDir, "generation"), filepath.Join(stagingFiles, "generation")); err != nil {
			if !os.IsNotExist(err) {
				return 0, 0, fmt.Errorf("backup: stage generation marker: %w", err)
			}
		}
	}
	return count, total, nil
}

// readStagedGeneration parses the staged marker (absent means generation 1).
func readStagedGeneration(stagingFiles string) (uint32, bool) {
	raw, err := os.ReadFile(filepath.Join(stagingFiles, "generation"))
	if err != nil {
		return 1, true
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 32)
	if err != nil || n < 1 {
		return 0, false
	}
	return uint32(n), true
}

// stagedUniform reports whether every staged object sits on the marker
// generation (a concurrent rotation mixes them).
func stagedUniform(stagingFiles string, gen uint32, genOK bool) (bool, error) {
	if !genOK {
		return false, fmt.Errorf("backup: staged generation marker is malformed")
	}
	entries, err := os.ReadDir(filepath.Join(stagingFiles, "objects"))
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, fmt.Errorf("backup: verify files staging: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, fileGen, ok := splitObjectName(e.Name()); ok && fileGen != gen {
			return false, nil
		}
	}
	return true, nil
}

// splitObjectName parses "<digest>.spfo" (generation 1) and
// "<digest>.g<N>.spfo". It mirrors the objectstore layout without importing
// it (backup stays dependency-light); unknown names are not objects.
func splitObjectName(name string) (string, uint32, bool) {
	if !strings.HasSuffix(name, ".spfo") {
		return "", 0, false
	}
	base := strings.TrimSuffix(name, ".spfo")
	gen := uint32(1)
	if i := strings.LastIndexByte(base, '.'); i >= 0 {
		if !strings.HasPrefix(base[i:], ".g") {
			return "", 0, false
		}
		n, err := strconv.ParseUint(base[i+2:], 10, 32)
		if err != nil || n < 2 {
			return "", 0, false
		}
		gen = uint32(n)
		base = base[:i]
	}
	if len(base) != 64 {
		return "", 0, false
	}
	for i := 0; i < len(base); i++ {
		c := base[i]
		if c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' {
			continue
		}
		return "", 0, false
	}
	return base, gen, true
}
