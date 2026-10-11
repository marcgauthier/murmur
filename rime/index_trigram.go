package rime

import (
	"bytes"
	"cmp"
	"fmt"
	"reflect"
	"strings"

	"github.com/marcgauthier/murmur/rime/trigram"
)

// TrigramHit is one ranked trigram search result. Key carries the row's
// primary key, Score carries the match quality in [0,1], and Edits carries
// total Levenshtein edits in Fuzzy mode (0 otherwise).
type TrigramHit = trigram.Hit

// TrigramMode selects the SearchTrigram matching contract.
type TrigramMode = trigram.Mode

// Trigram search modes.
const (
	TrigramContains = trigram.Contains
	TrigramSimilar  = trigram.Similar
	TrigramFuzzy    = trigram.Fuzzy
)

// TrigramOptions tunes SearchTrigram; see trigram.Options.
type TrigramOptions = trigram.Options

// IndexKind selects the index built by CreateIndex.
type IndexKind int

const (
	// IndexTrigram builds an in-memory trigram similarity index over a
	// string field. The index is local derived state: it is maintained on
	// every commit and rebuilt from rows, never persisted or replicated.
	IndexTrigram IndexKind = iota + 1
)

// CreateIndex builds kind over table's field from the current rows and
// maintains it on later commits. Re-creating an existing index rebuilds it
// idempotently. The build serializes with commits, so large tables stall
// writers while scanning; concurrent readers proceed.
func (db *DB) CreateIndex(table, field string, kind IndexKind) error {
	if kind != IndexTrigram {
		return fmt.Errorf("%w: unsupported index kind %d", ErrBadSchema, kind)
	}
	if db.closed.Load() {
		return ErrDBClosed
	}
	t, ok := db.tableByName(table)
	if !ok {
		return fmt.Errorf("%w: table %q is not registered", ErrBadSchema, table)
	}
	return t.createIndex(field, kind)
}

// createIndex backs innerTable. It holds commitMu across the scan and the
// installation: commit preparation and publication both require commitMu,
// so no commit can prepare index effects without the new index and publish
// after it is installed. The retained read snapshot keeps GC from
// reclaiming scanned rows. No plan-cache invalidation is needed: the
// planner never consults trigram indexes, and every trigram reader takes
// the index lock.
func (t *Table[T]) createIndex(field string, kind IndexKind) error {
	fm, ok := t.sch.byName[field]
	if !ok {
		return fmt.Errorf("%w: table %s has no field %q", ErrBadSchema, t.name, field)
	}
	if kind != IndexTrigram {
		return fmt.Errorf("%w: unsupported index kind %d", ErrBadSchema, kind)
	}
	if t.sch.fields[fm].typ.Kind() != reflect.String {
		return fmt.Errorf("%w: trigram index on %s requires a string field, got %s",
			ErrBadSchema, field, t.sch.fields[fm].typ)
	}
	t.db.commitMu.Lock()
	defer t.db.commitMu.Unlock()
	snap := t.db.ReadTx()
	defer snap.Close()
	rows, err := t.Where().In(snap).Find()
	if err != nil {
		return err
	}
	built := newTrigramIdx()
	for _, rec := range rows {
		built.add(t.keyStr[field](rec), t.pkGet(rec))
	}
	t.idx.mu.Lock()
	defer t.idx.mu.Unlock()
	t.idx.trigram[field] = built
	for _, f := range t.idx.trigramFields {
		if f == field {
			return nil
		}
	}
	t.idx.trigramFields = append(t.idx.trigramFields, field)
	return nil
}

// trigramStore is the postings backend for one trigram index. Postings map
// packed trigram keys to the primary keys whose field value produced them.
// The slice implementation below keeps memory proportional to postings
// entries; a future ordinal-bitmap backend can replace it without touching
// maintenance or search code.
type trigramStore interface {
	// add records key under every trigram. trigrams must already be
	// deduplicated per value so each key appears at most once per
	// posting.
	add(trigrams []uint32, key any)
	// remove drops key's single entry under every trigram, deleting
	// emptied postings. Missing entries are ignored.
	remove(trigrams []uint32, key any)
	// prepare reserves map entries and slice capacity ahead of
	// installation so addPrepared does not allocate. It is logically
	// invisible to lookups; prune removes entries that stayed empty.
	prepare(trigrams []uint32, reserve int)
	// addPrepared installs key under previously prepared trigrams.
	addPrepared(trigrams []uint32, key any)
	// prune deletes empty postings created by an aborted preparation.
	prune(trigrams []uint32)
	// appendKeys appends the keys posted under t to out.
	appendKeys(t uint32, out []any) []any
	// terms reports the distinct trigram count.
	terms() int
	// refs reports the total key-reference count.
	refs() int64
}

// slicePostings stores postings as key slices: roughly one interface word
// per entry plus map and slice overhead, far cheaper than a map per
// trigram. Removal finds the entry linearly and swap-removes it; see the
// deletion-throughput benchmark for hot-posting behavior.
type slicePostings struct {
	post map[uint32][]any
	n    int64
}

func newSlicePostings() *slicePostings {
	return &slicePostings{post: map[uint32][]any{}}
}

func (s *slicePostings) add(trigrams []uint32, key any) {
	for _, t := range trigrams {
		s.post[t] = append(s.post[t], key)
		s.n++
	}
}

func (s *slicePostings) remove(trigrams []uint32, key any) {
	for _, t := range trigrams {
		keys, ok := s.post[t]
		if !ok {
			continue
		}
		for i, k := range keys {
			if k == key {
				last := len(keys) - 1
				keys[i] = keys[last]
				keys[last] = nil
				keys = keys[:last]
				s.n--
				break
			}
		}
		if len(keys) == 0 {
			delete(s.post, t)
		} else {
			s.post[t] = keys
		}
	}
}

func (s *slicePostings) prepare(trigrams []uint32, reserve int) {
	if reserve < 1 {
		reserve = 1
	}
	for _, t := range trigrams {
		keys, ok := s.post[t]
		if !ok {
			s.post[t] = make([]any, 0, reserve)
			continue
		}
		if missing := len(keys) + reserve - cap(keys); missing > 0 {
			grown := make([]any, len(keys), cap(keys)+missing)
			copy(grown, keys)
			s.post[t] = grown
		}
	}
}

func (s *slicePostings) addPrepared(trigrams []uint32, key any) {
	for _, t := range trigrams {
		keys := s.post[t]
		if len(keys) == cap(keys) {
			panic("rime: missing prepared trigram capacity")
		}
		s.post[t] = append(keys, key)
		s.n++
	}
}

func (s *slicePostings) prune(trigrams []uint32) {
	for _, t := range trigrams {
		if keys, ok := s.post[t]; ok && len(keys) == 0 {
			delete(s.post, t)
		}
	}
}

func (s *slicePostings) appendKeys(t uint32, out []any) []any {
	return append(out, s.post[t]...)
}

func (s *slicePostings) terms() int { return len(s.post) }

func (s *slicePostings) refs() int64 { return s.n }

// trigramIdx is one field's trigram similarity index: normalized text is
// decomposed into the union of full-field unpadded trigrams (which power
// substring containment) and per-word padded trigrams (which power word
// similarity), and every trigram posts the owning primary key.
type trigramIdx struct {
	store trigramStore
}

func newTrigramIdx() *trigramIdx {
	return &trigramIdx{store: newSlicePostings()}
}

// extractTrigrams normalizes value and returns its deduplicated index
// trigrams: the union both sources share. Order is irrelevant to postings
// add and remove, so the unsorted set form skips the sort.
func extractTrigrams(value string) []uint32 {
	norm, words := trigram.Prepare(value)
	return trigram.FieldTrigramSet(norm, words)
}

func (t *trigramIdx) add(value string, key any) {
	t.store.add(extractTrigrams(value), key)
}

func (t *trigramIdx) remove(value string, key any) {
	t.store.remove(extractTrigrams(value), key)
}

// trigramSearchRetries bounds snapshot/index generation retries before
// SearchTrigram falls back to a full filtered scan.
const trigramSearchRetries = 4

// SearchTrigram searches field's trigram index for query and returns ranked
// hits, best first. The field must carry a trigram index (via the
// `rime:"trigram"` tag or CreateIndex). Modes:
//
// Contains matches values containing the query as a substring (score 1.0,
// key-ordered). Queries under 3 characters use a filtered scan.
//
// Similar ranks values by word-level trigram Jaccard similarity: every
// query word must reach Options.Threshold and the score is the mean of the
// per-word bests.
//
// Fuzzy matches values whose words fall within Options.MaxEdits of every
// query word, ranked by total edits ascending, then mean similarity
// descending.
//
// An empty query matches nothing. Candidates always verify against pinned
// snapshot rows, so concurrent commits can delay the search but never leak
// mixed-generation results.
func (t *Table[T]) SearchTrigram(field, query string, mode trigram.Mode, opts trigram.Options) ([]TrigramHit, error) {
	if _, ok := t.sch.byName[field]; !ok {
		return nil, fmt.Errorf("%w: table %s has no field %q", ErrBadSchema, t.name, field)
	}
	t.idx.mu.RLock()
	index, ok := t.idx.trigram[field]
	t.idx.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: table %s field %q has no trigram index", ErrBadSchema, t.name, field)
	}
	switch mode {
	case trigram.Contains, trigram.Similar, trigram.Fuzzy:
	default:
		return nil, fmt.Errorf("%w: invalid trigram search mode %d", ErrBadSchema, mode)
	}
	opts = opts.WithDefaults()
	norm, qWords := trigram.Prepare(query)
	if norm == "" {
		return nil, nil
	}
	qs := trigram.PrepareQueryWords(qWords)
	if t.trigramNeedsScan(mode, opts, norm, qs) {
		return t.trigramScan(field, mode, opts, norm, qs)
	}
	snap, keys, pinned := t.pinTrigramCandidates(func() []any {
		return t.trigramCandidates(index, mode, opts, norm, qs)
	})
	if !pinned {
		return t.trigramScan(field, mode, opts, norm, qs)
	}
	defer snap.Close()
	return t.scoreTrigramCandidates(field, snap, mode, opts, norm, qs, keys), nil
}

// trigramNeedsScan reports whether the query cannot use index candidates
// and must score every row: short Contains queries, non-positive Similar
// thresholds (zero similarity passes, so every row qualifies), and Fuzzy
// queries where the q-gram lemma proves overlap filtering unsafe.
func (t *Table[T]) trigramNeedsScan(mode trigram.Mode, opts trigram.Options, norm string, qs []trigram.QueryWord) bool {
	switch mode {
	case trigram.Contains:
		return len(norm) < trigram.TrigramLen
	case trigram.Similar:
		return len(qs) == 0 || opts.Threshold <= 0
	case trigram.Fuzzy:
		if len(qs) == 0 {
			return true
		}
		for _, qw := range qs {
			if trigram.MinOverlap(len(qw.Tri), opts.MaxEdits) == 0 {
				return true
			}
		}
		return false
	default:
		return true
	}
}

// pinTrigramCandidates opens a retained snapshot and collects index
// candidates while the snapshot is provably latest: viewMu readers block
// publication, so the mutable latest-state index cannot advance between
// the generation check and the candidate grab. Locks release before the
// caller verifies and ranks, which may be expensive. It reports false
// after bounded retries so the caller can scan instead.
func (t *Table[T]) pinTrigramCandidates(gather func() []any) (snap *Tx, keys []any, pinned bool) {
	for range trigramSearchRetries {
		snap = t.db.ReadTx()
		t.db.viewMu.RLock()
		if t.db.latest() != snap.snap {
			t.db.viewMu.RUnlock()
			snap.Close()
			continue
		}
		t.idx.mu.RLock()
		keys = gather()
		t.idx.mu.RUnlock()
		t.db.viewMu.RUnlock()
		return snap, keys, true
	}
	return nil, nil, false
}

// trigramCandidates gathers candidate keys from the index. The caller must
// hold the index lock on a generation matching its pinned snapshot.
// Candidates are a superset of the true matches; scoring verifies.
func (t *Table[T]) trigramCandidates(index *trigramIdx, mode trigram.Mode, opts trigram.Options, norm string, qs []trigram.QueryWord) []any {
	switch mode {
	case trigram.Contains:
		return trigramIntersectCounts(index, []countedQuery{{trigrams: trigram.QueryTrigrams(norm), min: -1}})
	case trigram.Similar:
		queries := make([]countedQuery, len(qs))
		for i, qw := range qs {
			queries[i] = countedQuery{trigrams: qw.Tri, min: trigram.MinSimilarOverlap(len(qw.Tri), opts.Threshold)}
		}
		return trigramIntersectCounts(index, queries)
	case trigram.Fuzzy:
		queries := make([]countedQuery, len(qs))
		for i, qw := range qs {
			queries[i] = countedQuery{trigrams: qw.Tri, min: trigram.MinOverlap(len(qw.Tri), opts.MaxEdits)}
		}
		return trigramIntersectCounts(index, queries)
	default:
		return nil
	}
}

// countedQuery is one word's trigram filter: keys sharing at least min of
// its trigrams qualify. A negative min requires every trigram.
type countedQuery struct {
	trigrams []uint32
	min      int
}

// trigramIntersectCounts returns keys passing every word filter: AND
// semantics across query words.
func trigramIntersectCounts(index *trigramIdx, queries []countedQuery) []any {
	var out []any
	for qi, q := range queries {
		need := q.min
		if need < 0 {
			need = len(q.trigrams)
		}
		counts := map[any]int{}
		var keys []any
		for _, tg := range q.trigrams {
			keys = index.store.appendKeys(tg, keys[:0])
			for _, k := range keys {
				counts[k]++
			}
		}
		pass := make([]any, 0, len(counts))
		for k, n := range counts {
			if n >= need {
				pass = append(pass, k)
			}
		}
		if qi == 0 {
			out = pass
			continue
		}
		keep := make(map[any]struct{}, len(pass))
		for _, k := range pass {
			keep[k] = struct{}{}
		}
		filtered := out[:0]
		for _, k := range out {
			if _, ok := keep[k]; ok {
				filtered = append(filtered, k)
			}
		}
		out = filtered
		if len(out) == 0 {
			return nil
		}
	}
	return out
}

// scoreTrigramCandidates verifies and scores candidate keys against pinned
// snapshot rows, returning ranked hits with the limit applied. Keys that
// no longer resolve are skipped.
func (t *Table[T]) scoreTrigramCandidates(field string, snap *Tx, mode trigram.Mode, opts trigram.Options, norm string, qs []trigram.QueryWord, keys []any) []TrigramHit {
	var hits []TrigramHit
	bound := t.In(snap)
	for _, key := range keys {
		rec, found, err := bound.Lookup(key)
		if err != nil || !found {
			continue
		}
		if hit, ok := t.scoreTrigramValue(field, rec, key, mode, opts, norm, qs); ok {
			hits = append(hits, hit)
		}
	}
	return finishTrigramHits(hits, mode, opts.Limit)
}

// trigramScan scores every row at a fresh snapshot. It backs queries the
// index cannot filter (short Contains, zero-threshold Similar, unfilterable
// Fuzzy) and generation-mismatch retries.
func (t *Table[T]) trigramScan(field string, mode trigram.Mode, opts trigram.Options, norm string, qs []trigram.QueryWord) ([]TrigramHit, error) {
	snap := t.db.ReadTx()
	defer snap.Close()
	rows, err := t.Where().In(snap).Find()
	if err != nil {
		return nil, err
	}
	var hits []TrigramHit
	for _, rec := range rows {
		if hit, ok := t.scoreTrigramValue(field, rec, t.pkGet(rec), mode, opts, norm, qs); ok {
			hits = append(hits, hit)
		}
	}
	return finishTrigramHits(hits, mode, opts.Limit), nil
}

// scoreTrigramValue scores one record's field value, reporting whether it
// matches.
func (t *Table[T]) scoreTrigramValue(field string, rec *T, key any, mode trigram.Mode, opts trigram.Options, norm string, qs []trigram.QueryWord) (TrigramHit, bool) {
	dnorm, dwords := trigram.Prepare(t.keyStr[field](rec))
	switch mode {
	case trigram.Contains:
		if !strings.Contains(dnorm, norm) {
			return TrigramHit{}, false
		}
		return TrigramHit{Key: key, Score: 1}, true
	case trigram.Similar:
		score, ok := trigram.ScoreSimilarSets(qs, dwords, opts.Threshold)
		if !ok {
			return TrigramHit{}, false
		}
		return TrigramHit{Key: key, Score: score}, true
	case trigram.Fuzzy:
		edits, sim, ok := trigram.ScoreFuzzySets(qs, dwords, opts.MaxEdits)
		if !ok {
			return TrigramHit{}, false
		}
		return TrigramHit{Key: key, Score: sim, Edits: edits}, true
	default:
		return TrigramHit{}, false
	}
}

// finishTrigramHits ranks hits and applies the limit.
func finishTrigramHits(hits []TrigramHit, mode trigram.Mode, limit int) []TrigramHit {
	trigram.SortHits(hits, mode, func(a, b any) bool { return compareTrigramKeys(a, b) < 0 })
	if limit > 0 && len(hits) > limit {
		hits = hits[:limit]
	}
	return hits
}

// trigram key classes order exotic keys deterministically when primary key
// types differ, which cannot happen within one table but must still rank.
const (
	trigramClassInt = iota
	trigramClassUint
	trigramClassFloat
	trigramClassString
	trigramClassBytes16
	trigramClassOther
)

// compareTrigramKeys totally orders primary keys for deterministic ranking
// ties. Supported key kinds compare natively (named types included);
// anything else falls back to formatted comparison.
func compareTrigramKeys(a, b any) int {
	ca, cb := classifyTrigramKey(a), classifyTrigramKey(b)
	if ca.class != cb.class {
		return cmp.Compare(ca.class, cb.class)
	}
	switch ca.class {
	case trigramClassInt:
		return cmp.Compare(ca.i, cb.i)
	case trigramClassUint:
		return cmp.Compare(ca.u, cb.u)
	case trigramClassFloat:
		if ca.f == cb.f {
			return 0
		}
		if ca.f < cb.f {
			return -1
		}
		return 1
	case trigramClassBytes16:
		return bytes.Compare(ca.b[:], cb.b[:])
	default:
		return strings.Compare(ca.s, cb.s)
	}
}

type trigramKeyClass struct {
	class int
	i     int64
	u     uint64
	f     float64
	s     string
	b     [16]byte
}

func classifyTrigramKey(v any) trigramKeyClass {
	switch k := v.(type) {
	case string:
		return trigramKeyClass{class: trigramClassString, s: k}
	case UUID:
		return trigramKeyClass{class: trigramClassBytes16, b: [16]byte(k)}
	case [16]byte:
		return trigramKeyClass{class: trigramClassBytes16, b: k}
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return trigramKeyClass{class: trigramClassOther}
	}
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return trigramKeyClass{class: trigramClassInt, i: rv.Int()}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return trigramKeyClass{class: trigramClassUint, u: rv.Uint()}
	case reflect.Float32, reflect.Float64:
		return trigramKeyClass{class: trigramClassFloat, f: rv.Float()}
	case reflect.String:
		return trigramKeyClass{class: trigramClassString, s: rv.String()}
	case reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 && rv.Len() == 16 {
			var out trigramKeyClass
			out.class = trigramClassBytes16
			for i := 0; i < 16; i++ {
				out.b[i] = byte(rv.Index(i).Uint())
			}
			return out
		}
	}
	return trigramKeyClass{class: trigramClassOther, s: fmt.Sprintf("%T\x00%v", v, v)}
}
