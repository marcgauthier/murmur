package bridge

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// inboxDigestWindow bounds retained per-sequence digests for conflict
// detection on replayed bundles.
const inboxDigestWindow = 1024

// QuarantineRecord explains one held bundle.
type QuarantineRecord struct {
	BundleID string
	First    uint64
	Last     uint64
	Reason   string
}

// inboxMissingEnumCap bounds Missing enumeration: a staged bundle far
// ahead of Applied would otherwise make gap listing unbounded.
const inboxMissingEnumCap = 4096

// StreamProgress reports per-stream inbox state. List fields may be
// truncated (see ProgressBounded); the Total fields always report exact
// counts.
type StreamProgress struct {
	Stream          string
	Observed        uint64
	Applied         uint64
	Staged          []uint64
	StagedTotal     int
	Missing         []uint64 // first gaps in (Applied..Observed], capped
	MissingTotal    uint64   // exact gap count
	Quarantine      []QuarantineRecord
	QuarantineTotal int
	Holds           []SchemaHold
	HoldsTotal      int
}

type stagedBundle struct {
	file     string
	bundleID string
	first    uint64
	last     uint64
	digest   string
	size     int64 // staged byte size, for capacity accounting
}

type streamState struct {
	Observed uint64
	Applied  uint64
	Staged   map[uint64]*stagedBundle // by first seq
	Quar     map[uint64]*QuarantineRecord
	Holds    map[uint64]*SchemaHold
	Digests  map[uint64]string // recent (stream,seq)->payload digest
}

type inboxPersist struct {
	Streams  map[string]*streamStateJSON    `json:"streams"`
	Files    map[string]*fileStagedJSON     `json:"files,omitempty"`
	FileQuar map[string]*fileQuarantineJSON `json:"file_quarantine,omitempty"`
}

type streamStateJSON struct {
	Observed uint64                       `json:"observed"`
	Applied  uint64                       `json:"applied"`
	Staged   map[string]*stagedBundleJSON `json:"staged,omitempty"`
	Quar     map[string]*QuarantineRecord `json:"quarantine,omitempty"`
	Holds    map[string]*SchemaHold       `json:"holds,omitempty"`
	Digests  map[string]string            `json:"digests,omitempty"`
}

type stagedBundleJSON struct {
	File     string `json:"file"`
	BundleID string `json:"bundle"`
	First    uint64 `json:"first"`
	Last     uint64 `json:"last"`
	Digest   string `json:"digest"`
}

// Inbox is the durable High-side receive journal: staged bundles, contiguous
// per-stream progress with explicit gaps, quarantine, and restart recovery.
// Staging/download completion is never treated as applied: only a committed
// import advances Applied. Safe for concurrent use.
type Inbox struct {
	mu      sync.Mutex
	dir     string
	staged  string
	quarDir string
	fobjDir string
	trust   *TrustStore
	limits  Limits
	streams map[string]*streamState
	files   map[string]*fileStaged
	// fileQuar quarantines whole objects by digest hex.
	fileQuar map[string]*fileQuarantineJSON
	used     int64 // staged+quarantine bytes, recomputed from disk on open
}

// OpenInbox opens (creating) the journal at dir. Staged bundles and progress
// survive restarts; outstanding imports resume where they stopped.
func OpenInbox(dir string, trust *TrustStore, limits Limits) (*Inbox, error) {
	if trust == nil {
		return nil, fmt.Errorf("bridge: inbox requires a trust store")
	}
	staged := filepath.Join(dir, "staged")
	quarDir := filepath.Join(dir, "quarantine")
	fobjDir := filepath.Join(dir, "fobj")
	if err := os.MkdirAll(staged, 0o755); err != nil {
		return nil, fmt.Errorf("bridge: inbox dir: %w", err)
	}
	if err := os.MkdirAll(quarDir, 0o755); err != nil {
		return nil, fmt.Errorf("bridge: inbox quarantine dir: %w", err)
	}
	if err := os.MkdirAll(fobjDir, 0o755); err != nil {
		return nil, fmt.Errorf("bridge: inbox file dir: %w", err)
	}
	in := &Inbox{dir: dir, staged: staged, quarDir: quarDir, fobjDir: fobjDir, trust: trust,
		limits: limits.withDefaults(), streams: make(map[string]*streamState),
		files: make(map[string]*fileStaged), fileQuar: make(map[string]*fileQuarantineJSON)}
	if err := in.load(); err != nil {
		return nil, err
	}
	// Restart-safe accounting: recompute usage from the journals.
	stagedBytes, err := journalBytes(in.staged)
	if err != nil {
		return nil, fmt.Errorf("bridge: inbox capacity scan: %w", err)
	}
	quarBytes, err := journalBytes(in.quarDir)
	if err != nil {
		return nil, fmt.Errorf("bridge: inbox capacity scan: %w", err)
	}
	fobjBytes, err := journalBytes(in.fobjDir)
	if err != nil {
		return nil, fmt.Errorf("bridge: inbox capacity scan: %w", err)
	}
	in.used = stagedBytes + quarBytes + fobjBytes
	return in, nil
}

// Usage reports durable journal usage against its bounds.
func (in *Inbox) Usage() CapacityUsage {
	in.mu.Lock()
	defer in.mu.Unlock()
	return CapacityUsage{BytesUsed: in.used, BytesMax: in.limits.MaxStagingBytes,
		EntriesUsed: in.entriesLocked(), EntriesMax: in.limits.MaxStagingEntries}
}

func (in *Inbox) entriesLocked() int {
	n := 0
	for _, st := range in.streams {
		n += len(st.Staged) + len(st.Quar)
	}
	for _, st := range in.files {
		n += len(st.got)
	}
	n += len(in.fileQuar)
	return n
}

func (in *Inbox) streamLocked(name string) *streamState {
	st, ok := in.streams[name]
	if !ok {
		st = &streamState{Staged: make(map[uint64]*stagedBundle), Quar: make(map[uint64]*QuarantineRecord), Holds: make(map[uint64]*SchemaHold), Digests: make(map[uint64]string)}
		in.streams[name] = st
	}
	return st
}

func (in *Inbox) load() error {
	raw, err := os.ReadFile(filepath.Join(in.dir, "state.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("bridge: inbox state: %w", err)
	}
	if len(raw) > 4<<20 {
		return fmt.Errorf("bridge: inbox state of %d bytes is absurd", len(raw))
	}
	var persisted inboxPersist
	if err := json.Unmarshal(raw, &persisted); err != nil {
		return fmt.Errorf("bridge: inbox state corrupt: %w", err)
	}
	for name, ss := range persisted.Streams {
		st := in.streamLocked(name)
		st.Observed = ss.Observed
		st.Applied = ss.Applied
		for k, v := range ss.Staged {
			var first uint64
			if _, err := fmt.Sscanf(k, "%d", &first); err != nil {
				return fmt.Errorf("bridge: inbox staged key %q", k)
			}
			st.Staged[first] = &stagedBundle{file: v.File, bundleID: v.BundleID, first: v.First, last: v.Last, digest: v.Digest}
		}
		for k, v := range ss.Quar {
			var first uint64
			if _, err := fmt.Sscanf(k, "%d", &first); err != nil {
				return fmt.Errorf("bridge: inbox quarantine key %q", k)
			}
			cp := *v
			st.Quar[first] = &cp
		}
		for k, v := range ss.Holds {
			var first uint64
			if _, err := fmt.Sscanf(k, "%d", &first); err != nil {
				return fmt.Errorf("bridge: inbox hold key %q", k)
			}
			if v == nil {
				continue
			}
			cp := *v
			cp.First = first
			st.Holds[first] = &cp
		}
		for k, v := range ss.Digests {
			var seq uint64
			if _, err := fmt.Sscanf(k, "%d", &seq); err != nil {
				return fmt.Errorf("bridge: inbox digest key %q", k)
			}
			st.Digests[seq] = v
		}
	}
	// Drop staged entries whose files are gone (crash between state write
	// and file sync cannot happen — files land first — but operator
	// cleanup might); the bundle will be re-received.
	for _, st := range in.streams {
		for first, sb := range st.Staged {
			fi, err := os.Stat(filepath.Join(in.staged, sb.file))
			if err != nil {
				delete(st.Staged, first)
				continue
			}
			sb.size = fi.Size()
		}
		for first := range st.Holds {
			if _, ok := st.Staged[first]; !ok {
				delete(st.Holds, first)
			}
		}
	}
	for key, fs := range persisted.Files {
		if fs == nil {
			continue
		}
		st := &fileStaged{stream: fs.Stream, count: fs.Count, totalLen: fs.TotalLen, got: make(map[uint32]string)}
		for idxStr, digest := range fs.Got {
			var index uint32
			if _, err := fmt.Sscanf(idxStr, "%d", &index); err != nil {
				continue
			}
			fi, err := os.Stat(filepath.Join(in.fobjDir, fmt.Sprintf("fobj-%s-%06d.fobj", key, index)))
			if err != nil {
				continue // chunk file gone; sender re-delivers
			}
			st.got[index] = digest
			st.size += fi.Size()
		}
		if len(st.got) > 0 {
			in.files[key] = st
		}
	}
	for key, rec := range persisted.FileQuar {
		if rec == nil {
			continue
		}
		cp := *rec
		in.fileQuar[key] = &cp
	}
	return in.saveLocked()
}

func (in *Inbox) saveLocked() error {
	persisted := inboxPersist{Streams: make(map[string]*streamStateJSON, len(in.streams))}
	for name, st := range in.streams {
		ss := &streamStateJSON{Observed: st.Observed, Applied: st.Applied,
			Staged: make(map[string]*stagedBundleJSON), Quar: make(map[string]*QuarantineRecord),
			Holds:   make(map[string]*SchemaHold),
			Digests: make(map[string]string)}
		for first, sb := range st.Staged {
			ss.Staged[fmt.Sprint(first)] = &stagedBundleJSON{File: sb.file, BundleID: sb.bundleID, First: sb.first, Last: sb.last, Digest: sb.digest}
		}
		for first, q := range st.Quar {
			cp := *q
			ss.Quar[fmt.Sprint(first)] = &cp
		}
		for first, h := range st.Holds {
			cp := *h
			ss.Holds[fmt.Sprint(first)] = &cp
		}
		for seq, d := range st.Digests {
			ss.Digests[fmt.Sprint(seq)] = d
		}
		persisted.Streams[name] = ss
	}
	if len(in.files) > 0 {
		persisted.Files = make(map[string]*fileStagedJSON, len(in.files))
		for key, st := range in.files {
			got := make(map[string]string, len(st.got))
			for index, digest := range st.got {
				got[fmt.Sprint(index)] = digest
			}
			persisted.Files[key] = &fileStagedJSON{Stream: st.stream, Count: st.count, TotalLen: st.totalLen, Got: got, Size: st.size}
		}
	}
	if len(in.fileQuar) > 0 {
		persisted.FileQuar = make(map[string]*fileQuarantineJSON, len(in.fileQuar))
		for key, rec := range in.fileQuar {
			cp := *rec
			persisted.FileQuar[key] = &cp
		}
	}
	raw, err := json.Marshal(persisted)
	if err != nil {
		return err
	}
	return writeFileSync(in.dir, "state.json", append(raw, '\n'))
}

// Receive validates, opens, and stages one artifact. Identical replays are
// idempotent; conflicting content under a known identity is quarantined.
// Staging never advances Applied.
func (in *Inbox) Receive(a Artifact) error {
	if err := ValidateArtifact(a, in.limits); err != nil {
		return err
	}
	if strings.HasSuffix(a.Name, ".fobj") {
		return in.receiveFileChunk(a)
	}
	bundle, err := OpenBundle(a.Data, in.trust, in.limits)
	if err != nil {
		return err
	}
	m := bundle.Manifest
	digest := sha256.Sum256(a.Data)
	digestB64 := base64.StdEncoding.EncodeToString(digest[:])

	in.mu.Lock()
	defer in.mu.Unlock()
	st := in.streamLocked(m.Stream)
	// Duplicate or conflicting delivery?
	if m.SeqLast <= st.Applied {
		if want, ok := st.Digests[m.SeqFirst]; ok && want != digestB64 {
			return in.quarantineLocked(st, m, a, "conflicting content for applied sequence")
		}
		return nil // idempotent replay
	}
	if sb, ok := st.Staged[m.SeqFirst]; ok {
		if sb.digest == digestB64 && sb.last == m.SeqLast {
			return nil // identical re-delivery
		}
		return in.quarantineLocked(st, m, a, "conflicting content for staged sequence")
	}
	if _, ok := st.Quar[m.SeqFirst]; ok {
		return fmt.Errorf("bridge: stream %q seq %d is quarantined", m.Stream, m.SeqFirst)
	}
	// Explicit backpressure: refuse before staging anything.
	if err := admit("inbox", int64(len(a.Data)), in.used, in.limits.MaxStagingBytes, in.entriesLocked(), in.limits.MaxStagingEntries); err != nil {
		return err
	}
	// Stage the bytes before recording them.
	file := fmt.Sprintf("%s-%020d-%020d.spb", sanitizeStream(m.Stream), m.SeqFirst, m.SeqLast)
	if err := writeFileSync(in.staged, file, a.Data); err != nil {
		return err
	}
	st.Staged[m.SeqFirst] = &stagedBundle{file: file, bundleID: m.BundleID.String(), first: m.SeqFirst, last: m.SeqLast, digest: digestB64, size: int64(len(a.Data))}
	in.used += int64(len(a.Data))
	if m.SeqLast > st.Observed {
		st.Observed = m.SeqLast
	}
	return in.saveLocked()
}

// NextImport returns the next contiguous importable bundle (Applied+1 when
// staged, unquarantined, and unheld), or nil when none is ready (gap,
// hold, or empty). Held streams are skipped, never driven.
func (in *Inbox) NextImport() (bundle *Bundle, file string, err error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	names := make([]string, 0, len(in.streams))
	for name := range in.streams {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		st := in.streams[name]
		want := st.Applied + 1
		if in.isHeldLocked(st, want) {
			continue
		}
		sb, ok := st.Staged[want]
		if !ok {
			// Gap, empty, or import-failure hold (whose bytes moved to
			// quarantine): nothing importable. Conflict records never
			// remove staging, so a quarantined-but-staged original
			// still imports; the conflict itself is never driven.
			continue
		}
		bundle, err := in.openStagedLocked(sb.file)
		if err != nil {
			return nil, "", err
		}
		return bundle, sb.file, nil
	}
	return nil, "", nil
}

// openStagedLocked reads and re-validates one staged file.
func (in *Inbox) openStagedLocked(file string) (*Bundle, error) {
	raw, err := os.ReadFile(filepath.Join(in.staged, file))
	if err != nil {
		return nil, fmt.Errorf("bridge: staged bundle unreadable: %w", err)
	}
	return OpenBundle(raw, in.trust, in.limits)
}

// MarkApplied advances contiguous progress after a committed import and
// reclaims the bundle file (keeping recent history per retention).
func (in *Inbox) MarkApplied(stream string, first, last uint64) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	st, ok := in.streams[stream]
	if !ok {
		return fmt.Errorf("bridge: unknown stream %q", stream)
	}
	sb, ok := st.Staged[first]
	if !ok || sb.last != last {
		return fmt.Errorf("bridge: stream %q [%d..%d] is not staged", stream, first, last)
	}
	if first != st.Applied+1 {
		return fmt.Errorf("bridge: stream %q [%d..%d] is not contiguous at applied %d", stream, first, last, st.Applied)
	}
	delete(st.Staged, first)
	delete(st.Quar, first)
	delete(st.Holds, first)
	in.used -= sb.size
	st.Applied = last
	for seq := first; seq <= last; seq++ {
		st.Digests[seq] = sb.digest
	}
	// Prune the digest window and reclaim old bundle files.
	for seq := range st.Digests {
		if seq+inboxDigestWindow <= st.Applied {
			delete(st.Digests, seq)
		}
	}
	if err := in.saveLocked(); err != nil {
		return err
	}
	_ = os.Remove(filepath.Join(in.staged, sb.file))
	return nil
}

// Streams returns the names of all tracked streams in sorted order.
func (in *Inbox) Streams() []string {
	in.mu.Lock()
	defer in.mu.Unlock()
	names := make([]string, 0, len(in.streams))
	for name := range in.streams {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// SyncAuthoritativeProgress aligns the inbox's applied sequence with authoritative storage.
func (in *Inbox) SyncAuthoritativeProgress(stream string, applied uint64) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	st, ok := in.streams[stream]
	if !ok {
		st = in.streamLocked(stream)
	}
	if applied <= st.Applied {
		return nil
	}
	st.Applied = applied
	if st.Observed < applied {
		st.Observed = applied
	}
	for first, sb := range st.Staged {
		if sb.last <= applied {
			delete(st.Staged, first)
			delete(st.Quar, first)
			delete(st.Holds, first)
			in.used -= sb.size
			_ = os.Remove(filepath.Join(in.staged, sb.file))
		}
	}
	return in.saveLocked()
}

// Quarantine holds one staged bundle with a reason, preserving its bytes.
// Later sequences stay staged behind the gap.
func (in *Inbox) Quarantine(stream string, first uint64, reason string) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	st, ok := in.streams[stream]
	if !ok {
		return fmt.Errorf("bridge: unknown stream %q", stream)
	}
	sb, ok := st.Staged[first]
	if !ok {
		return fmt.Errorf("bridge: stream %q seq %d is not staged", stream, first)
	}
	st.Quar[first] = &QuarantineRecord{BundleID: sb.bundleID, First: sb.first, Last: sb.last, Reason: reason}
	dst := filepath.Join(in.quarDir, sb.file)
	_ = os.Rename(filepath.Join(in.staged, sb.file), dst)
	delete(st.Staged, first)
	return in.saveLocked()
}

// quarantineLocked records terminal conflicting content: the bytes are
// preserved for forensics under a .conflict name that RetryQuarantined
// never re-drives (operator resolution only).
// ErrQuarantined indicates a delivery held for conflicting content. The
// bytes are preserved for forensics; operator resolution is required.
var ErrQuarantined = errors.New("bridge: conflicting content quarantined")

func (in *Inbox) quarantineLocked(st *streamState, m Manifest, a Artifact, reason string) error {
	// Forensic preservation counts against capacity like any other
	// journaled bytes: when full the delivery reports backpressure and
	// can be retried, instead of growing the journal without bound.
	if err := admit("inbox", int64(len(a.Data)), in.used, in.limits.MaxStagingBytes, in.entriesLocked(), in.limits.MaxStagingEntries); err != nil {
		return err
	}
	st.Quar[m.SeqFirst] = &QuarantineRecord{BundleID: m.BundleID.String(), First: m.SeqFirst, Last: m.SeqLast, Reason: reason}
	if m.SeqLast > st.Observed {
		st.Observed = m.SeqLast
	}
	file := fmt.Sprintf("%s-%020d-%020d.conflict", sanitizeStream(m.Stream), m.SeqFirst, m.SeqLast)
	if err := writeFileSync(in.quarDir, file, a.Data); err != nil {
		return err
	}
	in.used += int64(len(a.Data))
	if err := in.saveLocked(); err != nil {
		return err
	}
	return fmt.Errorf("%w: %s", ErrQuarantined, reason)
}

// RetryQuarantined re-drives one quarantined bundle's bytes back into
// staging (e.g. after a High migration resolved a schema hold). The caller
// re-runs import; a repeated failure re-quarantines.
func (in *Inbox) RetryQuarantined(stream string, first uint64) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	st, ok := in.streams[stream]
	if !ok {
		return fmt.Errorf("bridge: unknown stream %q", stream)
	}
	q, ok := st.Quar[first]
	if !ok {
		return fmt.Errorf("bridge: stream %q seq %d is not quarantined", stream, first)
	}
	// Locate the preserved bytes by bundle identity. Conflict holds are
	// terminal (operator resolution only) and never re-driven.
	preserved, err := in.scanQuarantineLocked()
	if err != nil {
		return err
	}
	file := preserved[q.BundleID]
	if file == "" {
		return fmt.Errorf("bridge: quarantined bundle %q is not retryable (conflicting content requires operator resolution)", q.BundleID)
	}
	if err := os.Rename(filepath.Join(in.quarDir, file), filepath.Join(in.staged, file)); err != nil {
		return fmt.Errorf("bridge: restore quarantined bundle: %w", err)
	}
	raw, err := os.ReadFile(filepath.Join(in.staged, file))
	if err != nil {
		return fmt.Errorf("bridge: staged bundle unreadable: %w", err)
	}
	digest := sha256.Sum256(raw)
	delete(st.Quar, first)
	st.Staged[first] = &stagedBundle{file: file, bundleID: q.BundleID, first: q.First, last: q.Last,
		digest: base64.StdEncoding.EncodeToString(digest[:]), size: int64(len(raw))}
	return in.saveLocked()
}

// scanQuarantineLocked maps bundle identities to their preserved,
// re-driveable files. Conflict forensics (`.conflict`) and unreadable
// files are skipped: they are never replayed.
func (in *Inbox) scanQuarantineLocked() (map[string]string, error) {
	entries, err := os.ReadDir(in.quarDir)
	if err != nil {
		return nil, fmt.Errorf("bridge: quarantine scan: %w", err)
	}
	found := make(map[string]string)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".conflict") {
			continue
		}
		// Quarantined files keep their staged names.
		raw, err := os.ReadFile(filepath.Join(in.quarDir, e.Name()))
		if err != nil {
			continue
		}
		bundle, err := OpenBundle(raw, in.trust, in.limits)
		if err != nil {
			continue
		}
		id := bundle.Manifest.BundleID.String()
		if _, ok := found[id]; !ok {
			found[id] = e.Name()
		}
	}
	return found, nil
}

// ReplayItem describes one quarantined bundle and whether its bytes are
// available for replay.
type ReplayItem struct {
	Stream    string
	First     uint64
	Last      uint64
	BundleID  string
	Reason    string
	Retryable bool // false needs operator resolution (conflicting content)
}

// ReplayStatus lists quarantined bundles oldest-first per stream with an
// exact total. It carries identities and reasons only, never payloads.
type ReplayStatus struct {
	Items []ReplayItem
	Total int
}

// ReplayStatus reports which quarantined bundles can be replayed. max
// caps the listed items per inbox (non-positive selects a default).
func (in *Inbox) ReplayStatus(max int) (ReplayStatus, error) {
	if max <= 0 {
		max = 64
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	preserved, err := in.scanQuarantineLocked()
	if err != nil {
		return ReplayStatus{}, err
	}
	names := make([]string, 0, len(in.streams))
	for name := range in.streams {
		names = append(names, name)
	}
	sort.Strings(names)
	var out ReplayStatus
	for _, name := range names {
		st := in.streams[name]
		firsts := make([]uint64, 0, len(st.Quar))
		for first := range st.Quar {
			firsts = append(firsts, first)
		}
		sort.Slice(firsts, func(i, j int) bool { return firsts[i] < firsts[j] })
		for _, first := range firsts {
			q := st.Quar[first]
			out.Total++
			if len(out.Items) >= max {
				continue
			}
			_, retryable := preserved[q.BundleID]
			out.Items = append(out.Items, ReplayItem{Stream: name, First: q.First,
				Last: q.Last, BundleID: q.BundleID, Reason: q.Reason, Retryable: retryable})
		}
	}
	return out, nil
}

// RetryResult reports one attempted quarantine replay.
type RetryResult struct {
	Stream  string
	First   uint64
	Retried bool
	Err     string // empty when retried
}

// RetryAllQuarantined replays every retryable quarantined bundle in stream
// order, continuing past individual failures. Terminal conflicts report
// an error per item and are never re-driven.
func (in *Inbox) RetryAllQuarantined() []RetryResult {
	in.mu.Lock()
	type key struct {
		stream string
		first  uint64
	}
	var keys []key
	for name, st := range in.streams {
		for first := range st.Quar {
			keys = append(keys, key{name, first})
		}
	}
	in.mu.Unlock()
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].stream != keys[j].stream {
			return keys[i].stream < keys[j].stream
		}
		return keys[i].first < keys[j].first
	})
	out := make([]RetryResult, 0, len(keys))
	for _, k := range keys {
		r := RetryResult{Stream: k.stream, First: k.first}
		if err := in.RetryQuarantined(k.stream, k.first); err != nil {
			r.Err = err.Error()
		} else {
			r.Retried = true
		}
		out = append(out, r)
	}
	return out
}

// BacklogAge reports how long the oldest staged-but-unapplied bundle has
// waited, from staging-file modification times (files are never modified
// in place). It reports false when nothing is staged.
func (in *Inbox) BacklogAge(now time.Time) (time.Duration, bool) {
	in.mu.Lock()
	defer in.mu.Unlock()
	var oldest time.Time
	found := false
	for _, st := range in.streams {
		for _, sb := range st.Staged {
			fi, err := os.Stat(filepath.Join(in.staged, sb.file))
			if err != nil {
				continue
			}
			if !found || fi.ModTime().Before(oldest) {
				oldest, found = fi.ModTime(), true
			}
		}
	}
	if !found {
		return 0, false
	}
	age := now.Sub(oldest)
	if age < 0 {
		age = 0
	}
	return age, true
}

// Progress snapshots every stream's contiguous progress, gaps, and holds.
// Gap enumeration is capped (see inboxMissingEnumCap) with an exact total;
// other lists are complete.
func (in *Inbox) Progress() []StreamProgress {
	return in.progress(-1)
}

// ProgressBounded snapshots per-stream progress with each list capped at
// max entries per stream (non-positive selects a default). Totals always
// report exact counts.
func (in *Inbox) ProgressBounded(max int) []StreamProgress {
	if max <= 0 {
		max = 64
	}
	return in.progress(max)
}

// progress snapshots per-stream progress; max <= 0 leaves the staged,
// quarantine, and hold lists complete while gap enumeration stays capped.
func (in *Inbox) progress(max int) []StreamProgress {
	in.mu.Lock()
	defer in.mu.Unlock()
	names := make([]string, 0, len(in.streams))
	for name := range in.streams {
		names = append(names, name)
	}
	sort.Strings(names)
	var out []StreamProgress
	for _, name := range names {
		st := in.streams[name]
		sp := StreamProgress{Stream: name, Observed: st.Observed, Applied: st.Applied}
		for seq := range st.Staged {
			sp.Staged = append(sp.Staged, seq)
		}
		sort.Slice(sp.Staged, func(i, j int) bool { return sp.Staged[i] < sp.Staged[j] })
		sp.StagedTotal = len(sp.Staged)
		if max > 0 && len(sp.Staged) > max {
			sp.Staged = sp.Staged[:max]
		}
		missingCap := inboxMissingEnumCap
		if max > 0 && max < missingCap {
			missingCap = max
		}
		sp.Missing, sp.MissingTotal = missingBounded(st, missingCap)
		for _, q := range st.Quar {
			cp := *q
			sp.Quarantine = append(sp.Quarantine, cp)
		}
		sort.Slice(sp.Quarantine, func(i, j int) bool { return sp.Quarantine[i].First < sp.Quarantine[j].First })
		sp.QuarantineTotal = len(sp.Quarantine)
		if max > 0 && len(sp.Quarantine) > max {
			sp.Quarantine = sp.Quarantine[:max]
		}
		for _, h := range st.Holds {
			cp := *h
			sp.Holds = append(sp.Holds, cp)
		}
		sort.Slice(sp.Holds, func(i, j int) bool { return sp.Holds[i].First < sp.Holds[j].First })
		sp.HoldsTotal = len(sp.Holds)
		if max > 0 && len(sp.Holds) > max {
			sp.Holds = sp.Holds[:max]
		}
		out = append(out, sp)
	}
	return out
}

// missingBounded enumerates the first cap gaps in (Applied..Observed] and
// reports the exact gap count, without iterating the range: staged spans
// merge into intervals for skip-ahead enumeration and an arithmetic total.
func missingBounded(st *streamState, cap int) ([]uint64, uint64) {
	type span struct{ first, last uint64 }
	spans := make([]span, 0, len(st.Staged))
	for first, sb := range st.Staged {
		if sb.last < first {
			continue
		}
		spans = append(spans, span{first, sb.last})
	}
	sort.Slice(spans, func(i, j int) bool {
		if spans[i].first != spans[j].first {
			return spans[i].first < spans[j].first
		}
		return spans[i].last < spans[j].last
	})
	merged := spans[:0]
	for _, s := range spans {
		if len(merged) > 0 && s.first <= merged[len(merged)-1].last+1 {
			if s.last > merged[len(merged)-1].last {
				merged[len(merged)-1].last = s.last
			}
			continue
		}
		merged = append(merged, s)
	}
	lo := st.Applied + 1
	var covered, quar uint64
	for _, s := range merged {
		if s.last < lo || s.first > st.Observed {
			continue
		}
		first, last := s.first, s.last
		if first < lo {
			first = lo
		}
		if last > st.Observed {
			last = st.Observed
		}
		covered += last - first + 1
	}
	for seq := range st.Quar {
		if seq < lo || seq > st.Observed {
			continue
		}
		// Quarantined-but-staged originals are already covered above;
		// only exclusively-quarantined sequences shrink the gap.
		coveredByStaged := false
		for _, s := range merged {
			if seq >= s.first && seq <= s.last {
				coveredByStaged = true
				break
			}
		}
		if !coveredByStaged {
			quar++
		}
	}
	var total uint64
	if st.Observed >= lo {
		total = st.Observed - lo + 1 - covered - quar
	}
	var out []uint64
	cursor := lo
	si := 0
	for len(out) < cap && cursor <= st.Observed {
		for si < len(merged) && merged[si].last < cursor {
			si++
		}
		if si < len(merged) && merged[si].first <= cursor {
			cursor = merged[si].last + 1
			if cursor == 0 { // uint64 wraparound guard
				break
			}
			continue
		}
		if _, bad := st.Quar[cursor]; bad {
			cursor++
			if cursor == 0 {
				break
			}
			continue
		}
		out = append(out, cursor)
		cursor++
		if cursor == 0 {
			break
		}
	}
	return out, total
}

func sanitizeStream(s string) string {
	// Stream identities already satisfy the name policy; this is defense
	// in depth for filesystem safety.
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			out = append(out, c)
		} else {
			out = append(out, '_')
		}
	}
	if len(out) == 0 {
		return "stream"
	}
	return string(out)
}
