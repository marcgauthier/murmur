package crypto

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// ManagerOptions configures the encryption manager.
type ManagerOptions struct {
	FS *EncryptedFS
	// Roots are content directories scanned for rotation (db dir, wal dir).
	Roots []string
	// MinRotationInterval rate-limits RotateDataKey. Zero disables.
	MinRotationInterval time.Duration
	// MaxKeyLifetime expires data keys older than this. Zero disables expiry.
	MaxKeyLifetime time.Duration
	// ExpiryInterval is the worker tick. Zero means 5 minutes.
	ExpiryInterval time.Duration
	// MaxFilesPerTick bounds re-encryption work per worker tick.
	MaxFilesPerTick int
	// Now injects time (tests).
	Now func() time.Time
}

// Encryption phases for status.
const (
	PhaseIdle       = "idle"
	PhaseRotating   = "rotating"
	PhaseRewriting  = "rewriting"
	PhaseRecovering = "recovering"
)

// EncryptionStatus describes at-rest encryption state.
type EncryptionStatus struct {
	Algorithm         string
	StorageKeyID      string
	Generation        uint64
	ActiveDataKeyID   string
	KeysActive        int
	KeysExpired       int
	RegistryBytes     int64
	DataKeyExpiry     time.Time
	LastRotation      time.Time
	LastRewrite       time.Time
	LastRewriteFiles  int
	RewriteInProgress bool
	FilesDone         uint64
	FilesTotal        uint64
	BytesDone         uint64
	BytesTotal        uint64
	FilesChecked      uint64
	FilesRewrote      uint64
	FilesSkipped      uint64
	WorkerErrors      uint64
	FS                FSCounters
}

// Manager drives data-key rotation, file re-encryption, key retirement, and
// the expiry worker. It is safe for concurrent use.
type Manager struct {
	fs    *EncryptedFS
	reg   *Registry
	roots []string

	minInterval time.Duration
	maxLifetime time.Duration
	tick        time.Duration
	maxPerTick  int
	now         func() time.Time

	mu            sync.Mutex
	lastRotation  time.Time
	lastRewrite   time.Time
	lastRewriteN  int
	rewriteActive bool
	filesDone     uint64
	filesTotal    uint64
	bytesDone     uint64
	bytesTotal    uint64

	// Age anchor: wall age at open plus monotonic elapsed afterwards, so
	// clock rollback cannot extend expiry during a process lifetime.
	anchorWall time.Time
	anchorMono time.Time
	anchorAge  time.Duration

	filesChecked atomic.Uint64
	filesRewrote atomic.Uint64
	filesSkipped atomic.Uint64
	workerErrors atomic.Uint64
}

// NewManager builds the manager.
func NewManager(opt ManagerOptions) (*Manager, error) {
	if opt.FS == nil {
		return nil, fmt.Errorf("crypto: manager requires an FS")
	}
	tick := opt.ExpiryInterval
	if tick <= 0 {
		tick = 5 * time.Minute
	}
	maxPerTick := opt.MaxFilesPerTick
	if maxPerTick <= 0 {
		maxPerTick = 16
	}
	now := opt.Now
	if now == nil {
		now = time.Now
	}
	m := &Manager{
		fs: opt.FS, reg: opt.FS.Registry(), roots: opt.Roots,
		minInterval: opt.MinRotationInterval, maxLifetime: opt.MaxKeyLifetime,
		tick: tick, maxPerTick: maxPerTick, now: now,
	}
	m.resetAgeAnchor()
	return m, nil
}

// resetAgeAnchor anchors generation age at the wall age now, plus monotonic
// elapsed afterwards.
func (m *Manager) resetAgeAnchor() {
	wall := m.now()
	var created time.Time
	if meta, err := m.reg.ActiveKeyMeta(); err == nil {
		created = meta.CreatedAt
	}
	age := wall.Sub(created)
	if age < 0 {
		age = 0
	}
	m.anchorWall = wall
	m.anchorMono = time.Now()
	m.anchorAge = age
}

// activeAge is the active generation's age: the max of monotonic elapsed
// (rollback-proof during the process lifetime) and wall elapsed (covers
// wall jumps forward; early rotation is always safe).
func (m *Manager) activeAge() time.Duration {
	mono := m.anchorAge + time.Since(m.anchorMono)
	wall := m.anchorAge + m.now().Sub(m.anchorWall)
	if wall > mono {
		return wall
	}
	return mono
}

// MaybeRotateOnExpiry starts a new generation when the active key exceeded
// MaxKeyLifetime. It is the FS pre-create hook.
func (m *Manager) MaybeRotateOnExpiry() error {
	if m.maxLifetime <= 0 {
		return nil
	}
	m.mu.Lock()
	over := m.activeAge() > m.maxLifetime
	m.mu.Unlock()
	if !over {
		return nil
	}
	_, _, err := m.reg.NewGeneration(bgCtx())
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.lastRotation = m.now()
	m.resetAgeAnchor()
	m.mu.Unlock()
	return nil
}

// RotateDataKey starts a new data-key generation. Files written afterwards
// use the fresh key; existing files keep theirs until re-encrypted (lazily
// by the expiry worker past MaxKeyLifetime, or eagerly by
// RewriteEncryptedFiles).
func (m *Manager) RotateDataKey(ctx context.Context) (uint64, error) {
	m.mu.Lock()
	if m.minInterval > 0 && !m.lastRotation.IsZero() && m.now().Sub(m.lastRotation) < m.minInterval {
		wait := m.minInterval - m.now().Sub(m.lastRotation)
		m.mu.Unlock()
		return 0, fmt.Errorf("%w: next rotation in %s", ErrRotationTooSoon, wait.Round(time.Second))
	}
	m.mu.Unlock()
	gen, _, err := m.reg.NewGeneration(ctx)
	if err != nil {
		return 0, err
	}
	m.mu.Lock()
	m.lastRotation = m.now()
	m.resetAgeAnchor()
	m.mu.Unlock()
	return gen, nil
}

// RewriteEncryptedFiles re-encrypts every content file with the active key
// and the current default algorithm. Callers must quiesce Pebble first (no
// open write handles); any busy file aborts the rewrite. A resumable
// per-file journal makes the rewrite crash-safe; keys retire only after the
// full reference inventory is rebuilt.
func (m *Manager) RewriteEncryptedFiles(ctx context.Context) (rewrote, skipped int, err error) {
	m.mu.Lock()
	if m.rewriteActive {
		m.mu.Unlock()
		return 0, 0, fmt.Errorf("crypto: rewrite already in progress")
	}
	m.rewriteActive = true
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.rewriteActive = false
		m.lastRewrite = m.now()
		m.lastRewriteN = rewrote
		m.mu.Unlock()
	}()
	// Rotate first when the active key already expired, so the rewrite
	// lands on a fresh key.
	if m.maxLifetime > 0 && m.activeAge() > m.maxLifetime {
		if _, err := m.RotateDataKey(ctx); err != nil {
			return 0, 0, err
		}
	}
	files, err := m.scan(m.roots)
	if err != nil {
		return 0, 0, err
	}
	j := &rewriteJournal{Version: 1, StartedAt: m.now().UTC()}
	for _, path := range files {
		j.Files = append(j.Files, rewriteFileEntry{Path: path})
	}
	if err := m.writeJournal(j); err != nil {
		return 0, 0, err
	}
	var totalBytes uint64
	for _, path := range files {
		if fi, err := m.fs.base.Stat(path); err == nil {
			totalBytes += uint64(fi.Size())
		}
	}
	m.mu.Lock()
	m.filesTotal = uint64(len(files))
	m.filesDone = 0
	m.bytesTotal = totalBytes
	m.bytesDone = 0
	m.mu.Unlock()
	var errs []error
	for i := range j.Files {
		if ctx.Err() != nil {
			return rewrote, skipped, ctx.Err()
		}
		e := &j.Files[i]
		done, skip, rerr := m.RewriteFile(e.Path)
		if rerr != nil {
			errs = append(errs, fmt.Errorf("%s: %w", e.Path, rerr))
			continue
		}
		if skip {
			skipped++
			e.State = "skipped"
		} else {
			if done {
				rewrote++
			}
			e.State = "done"
			m.mu.Lock()
			m.filesDone++
			if fi, err := m.fs.base.Stat(e.Path); err == nil {
				m.bytesDone += uint64(fi.Size())
			}
			m.mu.Unlock()
		}
		// Journal every file so a crash resumes precisely.
		if werr := m.writeJournal(j); werr != nil {
			return rewrote, skipped, werr
		}
	}
	if skipped > 0 {
		errs = append(errs, fmt.Errorf("crypto: %d files skipped (open write handles); quiesce Pebble first", skipped))
	}
	// Retire keys only after rebuilding the full reference inventory.
	if rerr := m.RetireUnreferencedKeys(ctx); rerr != nil {
		errs = append(errs, rerr)
	}
	m.removeJournal()
	return rewrote, skipped, errors.Join(errs...)
}

// RewriteFile re-encrypts one file. skip=true means a write handle is open
// (or the file is empty).
func (m *Manager) RewriteFile(path string) (done, skip bool, err error) {
	if m.fs.isWriteOpen(path) {
		m.fs.stats.RewriteSkipped.Add(1)
		return false, true, nil
	}
	if fi, err := m.fs.base.Stat(path); err != nil || fi.Size() == 0 {
		if err != nil {
			return false, false, err
		}
		return false, true, nil // zero-byte: nothing to do
	}
	plain, err := m.readLogical(path)
	if err != nil {
		return false, false, err
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return false, false, err
	}
	tmp := path + ".rewrite-" + hex.EncodeToString(nonce[:])
	w, err := m.fs.Create(tmp, "")
	if err != nil {
		return false, false, err
	}
	if _, err := w.Write(plain); err != nil {
		w.Close()
		_ = m.fs.base.Remove(tmp)
		return false, false, err
	}
	if err := w.Sync(); err != nil {
		w.Close()
		_ = m.fs.base.Remove(tmp)
		return false, false, err
	}
	if err := w.Close(); err != nil {
		_ = m.fs.base.Remove(tmp)
		return false, false, err
	}
	Zero(plain)
	if err := m.fs.verifyImage(tmp); err != nil {
		_ = m.fs.base.Remove(tmp)
		return false, false, fmt.Errorf("rewrite verify: %w", err)
	}
	if err := m.fs.base.Rename(tmp, path); err != nil {
		_ = m.fs.base.Remove(tmp)
		return false, false, err
	}
	if err := m.fs.syncParentDir(path); err != nil {
		return false, false, err
	}
	// The new image is installed. The old key is retired later, only after
	// rebuilding the full reference inventory (checkpoints may pin it).
	m.fs.stats.Rewrites.Add(1)
	return true, false, nil
}

func (m *Manager) readLogical(path string) ([]byte, error) {
	f, err := m.fs.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out, err := io.ReadAll(fileReader{f: f})
	if err != nil {
		return nil, err
	}
	m.fs.stats.RewriteBytes.Add(uint64(len(out)))
	return out, nil
}

// fileReader adapts vfs.File.Read to io.Reader.
type fileReader struct{ f vfs.File }

func (r fileReader) Read(p []byte) (int, error) { return r.f.Read(p) }

// scan lists candidate content files across roots (non-recursive).
func (m *Manager) scan(roots []string) ([]string, error) {
	var out []string
	for _, root := range roots {
		names, err := m.fs.base.List(root)
		if err != nil {
			continue // a missing dir is fine
		}
		for _, name := range names {
			if isRegistryArtifact(name) || strings.Contains(name, ".compact-") ||
				strings.Contains(name, ".rewrite-") || strings.Contains(name, ".tmp-") ||
				name == "LOCK" || name == rewriteJournalName {
				continue
			}
			full := m.fs.base.PathJoin(root, name)
			fi, err := m.fs.base.Stat(full)
			if err != nil || fi.IsDir() {
				continue
			}
			if fi.Size() == 0 {
				continue
			}
			out = append(out, full)
		}
	}
	return out, nil
}

// KeyRefs counts references to one key.
type KeyRefs struct {
	LiveFiles   uint64
	OpenHandles uint64
	Checkpoints uint64
	Backups     uint64
}

// BuildInventory scans live roots, pinned checkpoint/backup dirs, and open
// handles to reconstruct per-key references.
func (m *Manager) BuildInventory() (map[string]*KeyRefs, error) {
	inv := make(map[string]*KeyRefs)
	ref := func(id string) *KeyRefs {
		r, ok := inv[id]
		if !ok {
			r = &KeyRefs{}
			inv[id] = r
		}
		return r
	}
	live, err := m.scan(m.roots)
	if err != nil {
		return nil, err
	}
	for _, path := range live {
		id, known, err := m.fs.peekKeyID(path)
		if err != nil || !known {
			continue
		}
		ref(string(id[:])).LiveFiles++
	}
	for _, p := range m.reg.Pins() {
		files, err := m.scan([]string{p.Path})
		if err != nil {
			continue
		}
		for _, path := range files {
			id, known, err := m.fs.peekKeyID(path)
			if err != nil || !known {
				continue
			}
			r := ref(string(id[:]))
			if p.Kind == "checkpoint" {
				r.Checkpoints++
			} else {
				r.Backups++
			}
		}
	}
	for id, n := range m.fs.OpenHandleCounts() {
		ref(id).OpenHandles += n
	}
	return inv, nil
}

// RetireUnreferencedKeys rebuilds the reference inventory and removes keys
// with no live files, open handles, checkpoints, or backups. The active
// generation key is never retired.
func (m *Manager) RetireUnreferencedKeys(ctx context.Context) error {
	inv, err := m.BuildInventory()
	if err != nil {
		return err
	}
	activeID, err := m.reg.ActiveKeyID()
	if err != nil {
		return err
	}
	var errs []error
	for _, k := range m.reg.Keys() {
		if k.ID == activeID {
			continue
		}
		if r := inv[string(k.ID[:])]; r != nil &&
			(r.LiveFiles+r.OpenHandles+r.Checkpoints+r.Backups) > 0 {
			continue
		}
		if err := m.reg.RemoveKey(ctx, k.ID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// RetireKeyTargeted removes one key when a scan finds no references to it
// (used by expiry ticks without a full inventory rebuild).
func (m *Manager) RetireKeyTargeted(ctx context.Context, id [KeyIDLen]byte) error {
	if counts := m.fs.OpenHandleCounts(); counts[string(id[:])] > 0 {
		return nil
	}
	roots := append(append([]string{}, m.roots...), pinPaths(m.reg.Pins())...)
	files, err := m.scan(roots)
	if err != nil {
		return err
	}
	for _, path := range files {
		if kid, known, err := m.fs.peekKeyID(path); err == nil && known && kid == id {
			return nil // still referenced
		}
	}
	return m.reg.RemoveKey(ctx, id)
}

func pinPaths(pins []Pin) []string {
	out := make([]string, 0, len(pins))
	for _, p := range pins {
		out = append(out, p.Path)
	}
	return out
}

// RunExpiryTick expires old keys and re-encrypts files still using expired
// keys, bounded by MaxFilesPerTick. Only touched (expired-key) files are
// rewritten. Ticks stand down while an explicit rewrite runs.
func (m *Manager) RunExpiryTick(ctx context.Context) (checked, rewrote, skipped int, err error) {
	m.mu.Lock()
	active := m.rewriteActive
	m.mu.Unlock()
	if active {
		return 0, 0, 0, nil
	}
	if m.maxLifetime > 0 {
		if _, err := m.reg.ExpireBefore(ctx, m.now().Add(-m.maxLifetime)); err != nil {
			m.workerErrors.Add(1)
			return 0, 0, 0, err
		}
	}
	expired := make(map[string]bool)
	for _, k := range m.reg.Keys() {
		if k.State == KeyExpired {
			expired[string(k.ID[:])] = true
		}
	}
	if len(expired) == 0 {
		return 0, 0, 0, nil
	}
	files, err := m.scan(m.roots)
	if err != nil {
		return 0, 0, 0, err
	}
	var errs []error
	var retired []string
	for _, path := range files {
		if rewrote >= m.maxPerTick {
			break
		}
		if ctx.Err() != nil {
			return checked, rewrote, skipped, ctx.Err()
		}
		keyID, known, err := m.fs.peekKeyID(path)
		if err != nil || !known {
			continue
		}
		if !expired[string(keyID[:])] {
			continue
		}
		checked++
		done, skip, rerr := m.RewriteFile(path)
		if rerr != nil {
			errs = append(errs, rerr)
			m.workerErrors.Add(1)
			continue
		}
		if skip {
			skipped++
			continue
		}
		if done {
			rewrote++
			retired = append(retired, string(keyID[:]))
		}
	}
	// Retire the replaced keys when nothing references them anymore.
	seen := make(map[string]bool)
	for _, ks := range retired {
		if seen[ks] {
			continue
		}
		seen[ks] = true
		var id [KeyIDLen]byte
		copy(id[:], ks)
		if err := m.RetireKeyTargeted(ctx, id); err != nil {
			errs = append(errs, err)
		}
	}
	m.filesChecked.Add(uint64(checked))
	m.filesRewrote.Add(uint64(rewrote))
	m.filesSkipped.Add(uint64(skipped))
	return checked, rewrote, skipped, errors.Join(errs...)
}

// RunExpiryWorker runs expiry ticks until ctx ends.
func (m *Manager) RunExpiryWorker(ctx context.Context) {
	t := time.NewTicker(m.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _, _, _ = m.RunExpiryTick(ctx)
		}
	}
}

// CleanupStaging removes crashed compaction/rewrite temp files.
func (m *Manager) CleanupStaging() {
	for _, root := range m.roots {
		names, err := m.fs.base.List(root)
		if err != nil {
			continue
		}
		for _, name := range names {
			if strings.Contains(name, ".compact-") || strings.Contains(name, ".rewrite-") {
				_ = m.fs.base.Remove(m.fs.base.PathJoin(root, name))
			}
		}
	}
}

// Status snapshots encryption state.
func (m *Manager) Status() EncryptionStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	activeID, _ := m.reg.ActiveKeyID()
	st := EncryptionStatus{
		Algorithm:         m.reg.DefaultAlgorithm().String(),
		StorageKeyID:      m.reg.StorageKeyID(),
		Generation:        m.reg.Generation(),
		ActiveDataKeyID:   hex.EncodeToString(activeID[:]),
		RegistryBytes:     m.reg.DiskBytes(),
		LastRotation:      m.lastRotation,
		LastRewrite:       m.lastRewrite,
		LastRewriteFiles:  m.lastRewriteN,
		RewriteInProgress: m.rewriteActive,
		FilesDone:         m.filesDone,
		FilesTotal:        m.filesTotal,
		BytesDone:         m.bytesDone,
		BytesTotal:        m.bytesTotal,
		FilesChecked:      m.filesChecked.Load(),
		FilesRewrote:      m.filesRewrote.Load(),
		FilesSkipped:      m.filesSkipped.Load(),
		WorkerErrors:      m.workerErrors.Load(),
		FS:                m.fs.Stats(),
	}
	var oldest time.Time
	for _, k := range m.reg.Keys() {
		switch k.State {
		case KeyActive:
			st.KeysActive++
			if oldest.IsZero() || k.CreatedAt.Before(oldest) {
				oldest = k.CreatedAt
			}
		case KeyExpired:
			st.KeysExpired++
		}
	}
	if !oldest.IsZero() && m.maxLifetime > 0 {
		st.DataKeyExpiry = oldest.Add(m.maxLifetime)
	}
	return st
}

// rewriteJournalName is the resumable rewrite journal in the first root.
const rewriteJournalName = "REWRITE.journal"

type rewriteFileEntry struct {
	Path  string `json:"path"`
	State string `json:"state"` // pending, done, skipped
}

type rewriteJournal struct {
	Version   int                `json:"version"`
	StartedAt time.Time          `json:"startedAt"`
	Files     []rewriteFileEntry `json:"files"`
}

func (m *Manager) journalPath() string {
	root := ""
	if len(m.roots) > 0 {
		root = m.roots[0]
	}
	return filepath.Join(root, rewriteJournalName)
}

func (m *Manager) writeJournal(j *rewriteJournal) error {
	raw, err := json.Marshal(j)
	if err != nil {
		return err
	}
	// The journal is plaintext metadata (paths only, no keys) written
	// through the base FS to avoid encryption recursion.
	return atomicWriteFile(m.journalPath(), raw)
}

func (m *Manager) removeJournal() {
	_ = m.fs.base.Remove(m.journalPath())
}

// HasJournal reports whether a rewrite journal awaits recovery.
func (m *Manager) HasJournal() bool {
	_, err := m.fs.base.Stat(m.journalPath())
	return err == nil
}

// ResumeRewrite recovers an interrupted rewrite: it verifies or discards
// incomplete temp work, completes valid staged images, reprocesses pending
// files, then retires unreferenced keys. Callers must keep Pebble closed
// throughout (as during the rewrite itself).
func (m *Manager) ResumeRewrite(ctx context.Context) error {
	raw, err := os.ReadFile(m.journalPath())
	if err != nil {
		return fmt.Errorf("crypto: read rewrite journal: %w", err)
	}
	var j rewriteJournal
	if err := json.Unmarshal(raw, &j); err != nil || j.Version != 1 {
		return fmt.Errorf("crypto: invalid rewrite journal: %w", err)
	}
	for i := range j.Files {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		e := &j.Files[i]
		switch e.State {
		case "done":
			// Completed before the crash: the image must verify, else
			// something tampered post-rename and we fail closed.
			if err := m.fs.verifyImage(e.Path); err != nil {
				return fmt.Errorf("crypto: rewritten file %s invalid: %w", e.Path, err)
			}
		default:
			// Pending/skipped: a crash may have left orphan staging temps
			// (possibly valid but unjournaled); discard them and reprocess
			// the file from its committed image (rewrite preserves content,
			// so redoing a completed rename is safe).
			m.sweepTemps(e.Path)
			done, skip, rerr := m.RewriteFile(e.Path)
			if rerr != nil {
				return fmt.Errorf("%s: %w", e.Path, rerr)
			}
			if skip {
				e.State = "skipped"
			} else if done {
				e.State = "done"
			} else {
				e.State = "done"
			}
			if werr := m.writeJournal(&j); werr != nil {
				return werr
			}
		}
	}
	if err := m.RetireUnreferencedKeys(ctx); err != nil {
		return err
	}
	m.removeJournal()
	return nil
}

// sweepTemps removes orphan staging files for one path.
func (m *Manager) sweepTemps(path string) {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	names, err := m.fs.base.List(dir)
	if err != nil {
		return
	}
	for _, name := range names {
		if strings.HasPrefix(name, base+".rewrite-") || strings.HasPrefix(name, base+".compact-") {
			_ = m.fs.base.Remove(m.fs.base.PathJoin(dir, name))
		}
	}
}
