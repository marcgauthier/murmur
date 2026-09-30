package bridge

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcgauthier/murmur/ids"
)

// EventState tracks publication of one outbox event.
type EventState int

const (
	// EventPending awaits (re)publication.
	EventPending EventState = iota
	// EventPublished reached High durably.
	EventPublished
	// EventFailed exhausted retries (or failed validation) and needs
	// operator attention; it stays queued ahead of later events.
	EventFailed
)

// Event is one durable transaction-linked export record. The batch carries
// its source identity (TxID/origin/origin-sequence), so forwarding and
// replay cannot duplicate exports; Seq is the exporter's contiguous
// numbering consumed by bundle manifests.
type Event struct {
	Seq         uint64
	Batch       Batch
	Origin      ids.NodeID
	OriginSeq   uint64
	SchemaEpoch uint64
	SchemaHash  [32]byte
	EnqueuedAt  time.Time
	Attempts    int
	State       EventState
	LastError   string
	size        int64 // journaled byte size, for capacity accounting
}

type eventFile struct {
	Seq         uint64 `json:"seq"`
	BatchB64    string `json:"batch"`
	Origin      string `json:"origin"`
	OriginSeq   uint64 `json:"origin_seq"`
	SchemaEpoch uint64 `json:"schema_epoch"`
	SchemaHash  string `json:"schema_hash"`
	EnqueuedMs  int64  `json:"enqueued_ms"`
	Attempts    int    `json:"attempts"`
	State       int    `json:"state"`
	LastError   string `json:"last_error,omitempty"`
}

type outboxState struct {
	NextSeq     uint64            `json:"next_seq"`
	Resume      map[string]uint64 `json:"resume"`
	PendingFile []pendingFileJSON `json:"pending_files,omitempty"`
}

// pendingFileJSON is one object whose metadata published while its bytes
// were unavailable for chunking. The next drain retries it. Size is
// journaled because chunk counts derive from it and metadata is gone.
type pendingFileJSON struct {
	Digest string `json:"digest"`
	Stream string `json:"stream"`
	Size   int64  `json:"size"`
}

// PendingFile is one object awaiting chunk publication.
type PendingFile struct {
	Digest [32]byte
	Stream string
	Size   int64
}

// Outbox is a durable export journal: captured batches append crash-safely,
// publication advances per-event state, and retention reclaims published
// history. It is safe for concurrent use; typically one capturer appends
// while one publisher drains.
type Outbox struct {
	mu     sync.Mutex
	dir    string
	events string
	limits Limits
	next   uint64
	resume map[string]uint64
	bySeq  map[uint64]*Event
	// pendingFiles tracks objects whose metadata published while bytes
	// were missing, keyed by digest hex. Retried on every drain.
	pendingFiles map[string]pendingFileJSON
	used         int64 // journaled bytes, recomputed from disk on open
	keys         []OutboxKey
}

// OpenOutbox opens (creating) the journal at dir and replays it. Pending
// and failed events survive restarts for redelivery. Events rest as
// plaintext JSON; use OpenOutboxEncrypted for authenticated encryption.
func OpenOutbox(dir string, limits Limits) (*Outbox, error) {
	return openOutbox(dir, limits, nil)
}

// OpenOutboxEncrypted opens the journal with authenticated encryption at
// rest. At least one key is required; the last key seals new writes while
// the whole ring stays readable, so rotation adds the successor before
// retiring the predecessor. Keys never touch the journal: the caller
// re-supplies them on every open. Plaintext event files are rejected
// loudly (drain the journal unencrypted before migrating), as are files
// sealed under unknown keys or with any tampering.
func OpenOutboxEncrypted(dir string, limits Limits, keys ...OutboxKey) (*Outbox, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("bridge: encrypted outbox requires at least one key")
	}
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if err := validateOutboxKey(k); err != nil {
			return nil, err
		}
		if seen[k.ID] {
			return nil, fmt.Errorf("bridge: duplicate outbox key %q", k.ID)
		}
		seen[k.ID] = true
	}
	return openOutbox(dir, limits, keys)
}

func openOutbox(dir string, limits Limits, keys []OutboxKey) (*Outbox, error) {
	events := filepath.Join(dir, "events")
	if err := os.MkdirAll(events, 0o755); err != nil {
		return nil, fmt.Errorf("bridge: outbox dir: %w", err)
	}
	o := &Outbox{dir: dir, events: events, limits: limits.withDefaults(), next: 1,
		resume: make(map[string]uint64), bySeq: make(map[uint64]*Event),
		pendingFiles: make(map[string]pendingFileJSON),
		keys:         append([]OutboxKey(nil), keys...)}
	if err := o.loadState(); err != nil {
		return nil, err
	}
	if err := o.replay(); err != nil {
		return nil, err
	}
	// Restart-safe accounting: recompute usage from the journal itself.
	// No separate counter can drift or corrupt.
	used, err := journalBytes(o.events)
	if err != nil {
		return nil, fmt.Errorf("bridge: outbox capacity scan: %w", err)
	}
	o.used = used
	return o, nil
}

// Usage reports durable journal usage against its bounds.
func (o *Outbox) Usage() CapacityUsage {
	o.mu.Lock()
	defer o.mu.Unlock()
	return CapacityUsage{BytesUsed: o.used, BytesMax: o.limits.MaxStagingBytes,
		EntriesUsed: len(o.bySeq) + len(o.pendingFiles), EntriesMax: o.limits.MaxStagingEntries}
}

func (o *Outbox) loadState() error {
	raw, err := os.ReadFile(filepath.Join(o.dir, "state.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("bridge: outbox state: %w", err)
	}
	if len(raw) > 1<<20 {
		return fmt.Errorf("bridge: outbox state of %d bytes is absurd", len(raw))
	}
	var st outboxState
	if err := json.Unmarshal(raw, &st); err != nil {
		return fmt.Errorf("bridge: outbox state corrupt: %w", err)
	}
	if st.NextSeq > 1 {
		o.next = st.NextSeq
	}
	for k, v := range st.Resume {
		o.resume[k] = v
	}
	for _, pf := range st.PendingFile {
		o.pendingFiles[pf.Digest] = pf
	}
	return nil
}

func (o *Outbox) saveStateLocked() error {
	st := outboxState{NextSeq: o.next, Resume: o.resume}
	for _, pf := range o.pendingFiles {
		st.PendingFile = append(st.PendingFile, pf)
	}
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return writeFileSync(o.dir, "state.json", append(raw, '\n'))
}

// AddPendingFile records an object whose bytes were unavailable at publish
// time. It is retried on later drains; duplicates collapse by digest.
func (o *Outbox) AddPendingFile(digest [32]byte, stream string, size int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := hex.EncodeToString(digest[:])
	if _, ok := o.pendingFiles[key]; ok {
		return nil
	}
	if err := admit("outbox", 128, o.used, o.limits.MaxStagingBytes, len(o.bySeq)+len(o.pendingFiles), o.limits.MaxStagingEntries); err != nil {
		return err
	}
	o.pendingFiles[key] = pendingFileJSON{Digest: key, Stream: stream, Size: size}
	return o.saveStateLocked()
}

// PendingFiles lists objects awaiting chunk publication, by digest order.
func (o *Outbox) PendingFiles() []PendingFile {
	o.mu.Lock()
	defer o.mu.Unlock()
	keys := make([]string, 0, len(o.pendingFiles))
	for k := range o.pendingFiles {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []PendingFile
	for _, k := range keys {
		raw, err := hex.DecodeString(k)
		if err != nil || len(raw) != 32 {
			continue
		}
		var digest [32]byte
		copy(digest[:], raw)
		out = append(out, PendingFile{Digest: digest, Stream: o.pendingFiles[k].Stream, Size: o.pendingFiles[k].Size})
	}
	return out
}

// RemovePendingFile drops a published object from the retry journal.
func (o *Outbox) RemovePendingFile(digest [32]byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := hex.EncodeToString(digest[:])
	if _, ok := o.pendingFiles[key]; !ok {
		return nil
	}
	delete(o.pendingFiles, key)
	return o.saveStateLocked()
}

func (o *Outbox) replay() error {
	entries, err := os.ReadDir(o.events)
	if err != nil {
		return fmt.Errorf("bridge: outbox scan: %w", err)
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".json") || strings.HasSuffix(name, ".tmp.json") {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSuffix(name, ".json"), 10, 64)
		if err != nil {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(o.events, name))
		if err != nil {
			return fmt.Errorf("bridge: outbox read %q: %w", name, err)
		}
		if len(raw) > o.limits.MaxPayloadBytes*2+4096 {
			return fmt.Errorf("bridge: outbox event %q of %d bytes exceeds bound", name, len(raw))
		}
		if isOutboxEnvelope(raw) {
			if len(o.keys) == 0 {
				return fmt.Errorf("bridge: outbox event %q is encrypted but no keys were supplied", name)
			}
			plaintext, err := openOutboxEventSeq(raw, seq, o.keys)
			if err != nil {
				return fmt.Errorf("bridge: outbox event %q: %w", name, err)
			}
			raw = plaintext
		} else if len(o.keys) > 0 {
			return fmt.Errorf("bridge: outbox event %q is plaintext but encryption is configured (drain the journal unencrypted before migrating)", name)
		}
		var ef eventFile
		if err := json.Unmarshal(raw, &ef); err != nil {
			return fmt.Errorf("bridge: outbox event %q corrupt: %w", name, err)
		}
		if ef.Seq != seq {
			return fmt.Errorf("bridge: outbox event %q carries seq %d", name, ef.Seq)
		}
		ev, err := decodeEvent(ef, o.limits)
		if err != nil {
			return fmt.Errorf("bridge: outbox event %q: %w", name, err)
		}
		ev.size = int64(len(raw))
		o.bySeq[seq] = ev
		if seq >= o.next {
			o.next = seq + 1
		}
	}
	return nil
}

func decodeEvent(ef eventFile, limits Limits) (*Event, error) {
	batchRaw, err := base64.StdEncoding.DecodeString(ef.BatchB64)
	if err != nil {
		return nil, fmt.Errorf("invalid batch encoding: %w", err)
	}
	batches, err := decodeBatches(batchRaw, limits)
	if err != nil {
		return nil, err
	}
	if len(batches) != 1 {
		return nil, fmt.Errorf("event carries %d batches", len(batches))
	}
	origin, err := ids.ParseNodeID(ef.Origin)
	if err != nil {
		return nil, fmt.Errorf("invalid origin: %w", err)
	}
	hashRaw, err := base64.StdEncoding.DecodeString(ef.SchemaHash)
	if err != nil || len(hashRaw) != 32 {
		return nil, fmt.Errorf("invalid schema hash")
	}
	var hash [32]byte
	copy(hash[:], hashRaw)
	if ef.State < int(EventPending) || ef.State > int(EventFailed) {
		return nil, fmt.Errorf("invalid state %d", ef.State)
	}
	return &Event{
		Seq:         ef.Seq,
		Batch:       batches[0],
		Origin:      origin,
		OriginSeq:   ef.OriginSeq,
		SchemaEpoch: ef.SchemaEpoch,
		SchemaHash:  hash,
		EnqueuedAt:  time.UnixMilli(ef.EnqueuedMs).UTC(),
		Attempts:    ef.Attempts,
		State:       EventState(ef.State),
		LastError:   ef.LastError,
	}, nil
}

func encodeEvent(ev *Event) ([]byte, error) {
	section, err := encodeBatches([]Batch{ev.Batch})
	if err != nil {
		return nil, err
	}
	ef := eventFile{
		Seq:         ev.Seq,
		BatchB64:    base64.StdEncoding.EncodeToString(section),
		Origin:      ev.Origin.String(),
		OriginSeq:   ev.OriginSeq,
		SchemaEpoch: ev.SchemaEpoch,
		SchemaHash:  base64.StdEncoding.EncodeToString(ev.SchemaHash[:]),
		EnqueuedMs:  ev.EnqueuedAt.UnixMilli(),
		Attempts:    ev.Attempts,
		State:       int(ev.State),
		LastError:   ev.LastError,
	}
	raw, err := json.Marshal(ef)
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

// encodeEventLocked marshals one event for the journal, sealing it under
// the current write key when encryption is configured. Callers hold o.mu;
// every write uses a fresh random nonce.
func (o *Outbox) encodeEventLocked(ev *Event) ([]byte, error) {
	raw, err := encodeEvent(ev)
	if err != nil {
		return nil, err
	}
	if len(o.keys) == 0 {
		return raw, nil
	}
	return sealOutboxEvent(raw, ev.Seq, o.keys[len(o.keys)-1])
}

// RotateKey adds a key to the ring; it becomes the write key for new and
// rewritten files while previously sealed files stay readable. An ID
// already present is replaced and promoted.
func (o *Outbox) RotateKey(key OutboxKey) error {
	if err := validateOutboxKey(key); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	ring := o.keys[:0]
	for _, k := range o.keys {
		if k.ID != key.ID {
			ring = append(ring, k)
		}
	}
	o.keys = append(ring, key)
	return nil
}

// Rekey rewrites every journaled event file under the ring key id. A crash
// mid-rekey leaves a mixed journal that still opens while the ring holds
// both keys; drop the predecessor only after Rekey succeeds.
func (o *Outbox) Rekey(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	var target *OutboxKey
	for i := range o.keys {
		if o.keys[i].ID == id {
			target = &o.keys[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("bridge: outbox key %q is not available", id)
	}
	for _, seq := range o.sortedSeqsLocked() {
		ev := o.bySeq[seq]
		plain, err := encodeEvent(ev)
		if err != nil {
			return err
		}
		raw, err := sealOutboxEvent(plain, seq, *target)
		if err != nil {
			return err
		}
		if err := writeFileSync(o.events, eventName(seq), raw); err != nil {
			return err
		}
		o.used += int64(len(raw)) - ev.size
		ev.size = int64(len(raw))
	}
	return nil
}

// DropKey removes a key from the ring. Files sealed under it become
// unreadable, so rekey (or drain) first. The last key cannot be dropped:
// that would silently flip the journal back to plaintext.
func (o *Outbox) DropKey(id string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	ring := o.keys[:0]
	found := false
	for _, k := range o.keys {
		if k.ID == id {
			found = true
			continue
		}
		ring = append(ring, k)
	}
	if !found {
		return fmt.Errorf("bridge: outbox key %q is not available", id)
	}
	if len(ring) == 0 {
		return fmt.Errorf("bridge: cannot drop the last outbox key")
	}
	o.keys = ring
	return nil
}

// KeyIDs lists the key identities currently on the ring, oldest first.
// IDs are public routing metadata; key bytes never leave the caller.
func (o *Outbox) KeyIDs() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]string, 0, len(o.keys))
	for _, k := range o.keys {
		out = append(out, k.ID)
	}
	return out
}

// Append durably enqueues one captured batch and advances the origin resume
// point atomically with it: the event lands before the resume moves, so a
// crash replays at most once (downstream dedup keys on TxID/export seq).
func (o *Outbox) Append(batch Batch, origin ids.NodeID, originSeq uint64, schemaEpoch uint64, schemaHash [32]byte) (uint64, error) {
	if err := batch.Validate(o.limits); err != nil {
		return 0, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	seq := o.next
	ev := &Event{
		Seq: seq, Batch: batch, Origin: origin, OriginSeq: originSeq,
		SchemaEpoch: schemaEpoch, SchemaHash: schemaHash,
		EnqueuedAt: time.Now().UTC(),
	}
	raw, err := o.encodeEventLocked(ev)
	if err != nil {
		return 0, err
	}
	// Explicit backpressure: refuse before writing anything, so a full
	// journal never silently drops queued or incoming work.
	if err := admit("outbox", int64(len(raw)), o.used, o.limits.MaxStagingBytes, len(o.bySeq), o.limits.MaxStagingEntries); err != nil {
		return 0, err
	}
	if err := writeFileSync(o.events, eventName(seq), raw); err != nil {
		return 0, err
	}
	o.next++
	o.resume[origin.String()] = originSeq
	if err := o.saveStateLocked(); err != nil {
		return 0, err
	}
	ev.size = int64(len(raw))
	o.bySeq[seq] = ev
	o.used += ev.size
	return seq, nil
}

// Resume returns the last captured origin sequence (zero when none).
func (o *Outbox) Resume(origin ids.NodeID) uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.resume[origin.String()]
}

// AdvanceResume records scanned source sequences that contain no exportable
// mutations. Captured events must already be durable before this is called.
func (o *Outbox) AdvanceResume(origin ids.NodeID, originSeq uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	key := origin.String()
	if originSeq <= o.resume[key] {
		return nil
	}
	old := o.resume[key]
	o.resume[key] = originSeq
	if err := o.saveStateLocked(); err != nil {
		o.resume[key] = old
		return err
	}
	return nil
}

// Pending returns unpublished events (pending and failed) in seq order,
// capped at limit (zero means all).
func (o *Outbox) Pending(limit int) []*Event {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []*Event
	for _, seq := range o.sortedSeqsLocked() {
		ev := o.bySeq[seq]
		if ev.State == EventPublished {
			continue
		}
		cp := *ev
		out = append(out, &cp)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// MarkPublished records durable publication of seq.
func (o *Outbox) MarkPublished(seq uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	ev, ok := o.bySeq[seq]
	if !ok {
		return fmt.Errorf("bridge: outbox has no event %d", seq)
	}
	ev.State = EventPublished
	ev.LastError = ""
	raw, err := o.encodeEventLocked(ev)
	if err != nil {
		return err
	}
	if err := writeFileSync(o.events, eventName(seq), raw); err != nil {
		return err
	}
	// State rewrites are never refused: the event was admitted already.
	o.used += int64(len(raw)) - ev.size
	ev.size = int64(len(raw))
	return nil
}

// MarkFailed records a failed publication attempt (attempts accumulate for
// backoff/retention decisions).
func (o *Outbox) MarkFailed(seq uint64, reason error) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	ev, ok := o.bySeq[seq]
	if !ok {
		return fmt.Errorf("bridge: outbox has no event %d", seq)
	}
	ev.State = EventFailed
	ev.Attempts++
	if reason != nil {
		ev.LastError = reason.Error()
		if len(ev.LastError) > 512 {
			ev.LastError = ev.LastError[:512]
		}
	}
	raw, err := o.encodeEventLocked(ev)
	if err != nil {
		return err
	}
	if err := writeFileSync(o.events, eventName(seq), raw); err != nil {
		return err
	}
	o.used += int64(len(raw)) - ev.size
	ev.size = int64(len(raw))
	return nil
}

// GC deletes published event files, retaining the newest keepPublished
// (pending/failed events are always retained). It returns removals.
func (o *Outbox) GC(keepPublished int) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var published []uint64
	for _, seq := range o.sortedSeqsLocked() {
		if o.bySeq[seq].State == EventPublished {
			published = append(published, seq)
		}
	}
	drop := len(published) - keepPublished
	if drop <= 0 {
		return 0, nil
	}
	removed := 0
	for _, seq := range published[:drop] {
		if err := os.Remove(filepath.Join(o.events, eventName(seq))); err != nil && !os.IsNotExist(err) {
			return removed, fmt.Errorf("bridge: outbox gc: %w", err)
		}
		if ev := o.bySeq[seq]; ev != nil {
			o.used -= ev.size
		}
		delete(o.bySeq, seq)
		removed++
	}
	return removed, nil
}

// Len returns the journaled event count (all states).
func (o *Outbox) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.bySeq)
}

func (o *Outbox) sortedSeqsLocked() []uint64 {
	out := make([]uint64, 0, len(o.bySeq))
	for seq := range o.bySeq {
		out = append(out, seq)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func eventName(seq uint64) string { return fmt.Sprintf("%020d.json", seq) }

// writeFileSync writes name under dir atomically (tmp + rename) with file
// and directory sync.
func writeFileSync(dir, name string, raw []byte) error {
	tmp, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return fmt.Errorf("bridge: stage %s: %w", name, err)
	}
	tmpName := tmp.Name()
	done := false
	defer func() {
		if !done {
			os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return fmt.Errorf("bridge: write %s: %w", name, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("bridge: sync %s: %w", name, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("bridge: close %s: %w", name, err)
	}
	if err := os.Rename(tmpName, filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("bridge: publish %s: %w", name, err)
	}
	done = true
	if dfd, err := os.Open(dir); err == nil {
		_ = dfd.Sync()
		_ = dfd.Close()
	}
	return nil
}
