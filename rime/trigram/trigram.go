// Package trigram provides pure, standard-library-only helpers for RIME's
// in-memory trigram similarity index: normalization, trigram extraction,
// Jaccard similarity, bounded Levenshtein distance, and ranked-hit sorting.
//
// All matching is byte-oriented over ASCII-folded text, consistent with RIME's
// prefix index and LIKE semantics: there is no Unicode collation.
package trigram

import (
	"slices"
	"strings"
)

// TrigramLen is the fixed n-gram width used throughout this package.
const TrigramLen = 3

// Mode selects the SearchTrigram matching contract.
type Mode int

const (
	// Contains matches values containing the query as a substring.
	// Candidates come from unpadded trigram containment and every hit is
	// verified, so there are no false positives or negatives.
	Contains Mode = iota + 1
	// Similar ranks values by word-level trigram Jaccard similarity.
	// Every query word must meet Threshold; the score is the mean of the
	// per-word best similarities.
	Similar
	// Fuzzy matches values whose words fall within MaxEdits of every query
	// word, ranked by total edits ascending, then mean similarity
	// descending. Candidates come from trigram overlap with a
	// q-gram-lemma-derived minimum; when the lemma proves filtering
	// unsafe the caller scans instead, so no in-budget match is missed.
	Fuzzy
)

// Defaults for Options.
const (
	// DefaultThreshold is the minimum per-word Jaccard similarity for
	// Similar mode.
	DefaultThreshold = 0.3
	// DefaultMaxEdits is the maximum Levenshtein edits per word for Fuzzy
	// mode.
	DefaultMaxEdits = 2
)

// Options tunes SearchTrigram. Zero values select defaults: Threshold 0.3,
// MaxEdits 2, unlimited hits.
type Options struct {
	// Threshold is the minimum per-word Jaccard similarity in Similar
	// mode. Every query word must meet it. Values below 0 behave as 0;
	// values above 1 match nothing.
	Threshold float64
	// MaxEdits is the maximum Levenshtein edits per word in Fuzzy mode.
	// Negative values behave as 0.
	MaxEdits int
	// Limit caps the returned hits after ranking. Values <= 0 mean all.
	Limit int
}

// WithDefaults returns o with zero values replaced by defaults.
func (o Options) WithDefaults() Options {
	if o.Threshold == 0 {
		o.Threshold = DefaultThreshold
	}
	if o.MaxEdits == 0 {
		o.MaxEdits = DefaultMaxEdits
	}
	return o
}

// Hit is one ranked search result. Key carries the caller's row identity.
type Hit struct {
	Key   any
	Score float64
	// Edits holds total Levenshtein edits in Fuzzy mode and is 0 in the
	// other modes.
	Edits int
}

// SortHits orders hits per mode: Contains by key, Similar by score
// descending, Fuzzy by edits ascending then score descending. Ties always
// break by key through less, which must order keys deterministically and
// must not be nil.
func SortHits(hits []Hit, mode Mode, less func(a, b any) bool) {
	switch mode {
	case Fuzzy:
		slices.SortStableFunc(hits, func(a, b Hit) int {
			if a.Edits != b.Edits {
				return cmpInt(a.Edits, b.Edits)
			}
			if a.Score != b.Score {
				return cmpFloatDesc(a.Score, b.Score)
			}
			return cmpBool(less(a.Key, b.Key), less(b.Key, a.Key))
		})
	case Similar:
		slices.SortStableFunc(hits, func(a, b Hit) int {
			if a.Score != b.Score {
				return cmpFloatDesc(a.Score, b.Score)
			}
			return cmpBool(less(a.Key, b.Key), less(b.Key, a.Key))
		})
	default:
		slices.SortStableFunc(hits, func(a, b Hit) int {
			return cmpBool(less(a.Key, b.Key), less(b.Key, a.Key))
		})
	}
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpFloatDesc(a, b float64) int {
	switch {
	case a > b:
		return -1
	case a < b:
		return 1
	default:
		return 0
	}
}

// cmpBool orders strict-weak less results: aBefore sorts first, bBefore
// sorts last, neither (equal keys) compares equal.
func cmpBool(aBefore, bBefore bool) int {
	switch {
	case aBefore && !bBefore:
		return -1
	case bBefore && !aBefore:
		return 1
	default:
		return 0
	}
}

// Normalize folds s for trigram matching: ASCII uppercase maps to
// lowercase, every run of ASCII whitespace collapses to one space, and
// surrounding whitespace is trimmed. Indexing, candidate generation, and
// verification must all compare normalized forms.
func Normalize(s string) string {
	if strings.IndexByte(s, ' ') < 0 && strings.IndexAny(s, "ABCDEFGHIJKLMNOPQRSTUVWXYZ\t\n\r\f\v") < 0 {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	pendingSpace := false
	started := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			if started {
				pendingSpace = true
			}
		case c >= 'A' && c <= 'Z':
			if pendingSpace {
				b.WriteByte(' ')
				pendingSpace = false
			}
			b.WriteByte(c + ('a' - 'A'))
			started = true
		default:
			if pendingSpace {
				b.WriteByte(' ')
				pendingSpace = false
			}
			b.WriteByte(c)
			started = true
		}
	}
	return b.String()
}

// SplitWords splits normalized text into words. It accepts unnormalized
// input but callers should normalize first so word boundaries agree with
// the indexed form.
func SplitWords(norm string) []string {
	return strings.Fields(norm)
}

// Prepare normalizes s and returns the normalized form with its words.
func Prepare(s string) (norm string, words []string) {
	norm = Normalize(s)
	return norm, SplitWords(norm)
}

// Pack encodes three bytes as one uint32 trigram key.
func Pack(a, b, c byte) uint32 {
	return uint32(a)<<16 | uint32(b)<<8 | uint32(c)
}

// ExtractUnpadded calls add for every consecutive 3-byte window of s,
// including windows spanning spaces. Inputs shorter than 3 bytes yield
// nothing.
func ExtractUnpadded(s string, add func(uint32)) {
	for i := 0; i+TrigramLen <= len(s); i++ {
		add(Pack(s[i], s[i+1], s[i+2]))
	}
}

// ExtractPadded calls add for every 3-byte window of word padded with two
// spaces on each side, so short words still yield trigrams. The word must
// already be normalized; empty words yield nothing.
func ExtractPadded(word string, add func(uint32)) {
	if word == "" {
		return
	}
	padded := "  " + word + "  "
	for i := 0; i+TrigramLen <= len(padded); i++ {
		add(Pack(padded[i], padded[i+1], padded[i+2]))
	}
}

// AppendUnpadded appends every consecutive 3-byte window of s to buf.
func AppendUnpadded(s string, buf []uint32) []uint32 {
	for i := 0; i+TrigramLen <= len(s); i++ {
		buf = append(buf, Pack(s[i], s[i+1], s[i+2]))
	}
	return buf
}

// AppendWordTrigrams appends word's padded trigrams to buf. The word must
// already be normalized; empty words append nothing. Output may contain
// duplicates and is unsorted; run it through SortUnique for set semantics.
func AppendWordTrigrams(word string, buf []uint32) []uint32 {
	if word == "" {
		return buf
	}
	padded := "  " + word + "  "
	for i := 0; i+TrigramLen <= len(padded); i++ {
		buf = append(buf, Pack(padded[i], padded[i+1], padded[i+2]))
	}
	return buf
}

// SortUnique sorts tri in place and drops duplicates, returning the set
// form. The backing array past the result must not be retained.
func SortUnique(tri []uint32) []uint32 {
	slices.Sort(tri)
	return slices.Compact(tri)
}

// QueryTrigrams returns the sorted unique unpadded trigrams of normalized
// query text for Contains candidate generation.
func QueryTrigrams(norm string) []uint32 {
	if len(norm) < TrigramLen {
		return nil
	}
	return SortUnique(AppendUnpadded(norm, make([]uint32, 0, len(norm))))
}

// WordTrigrams returns the sorted unique padded trigrams of one normalized
// word for Similar and Fuzzy matching.
func WordTrigrams(word string) []uint32 {
	if word == "" {
		return nil
	}
	return SortUnique(AppendWordTrigrams(word, make([]uint32, 0, len(word)+4)))
}

// FieldTrigrams returns the sorted unique union of the full-field unpadded
// trigrams and every word's padded trigrams for one normalized field
// value. Both sources share one postings map: the union can only add
// candidates, while verification and scoring stay authoritative.
func FieldTrigrams(norm string, words []string) []uint32 {
	if norm == "" && len(words) == 0 {
		return nil
	}
	buf := make([]uint32, 0, len(norm)+8*len(words))
	buf = AppendUnpadded(norm, buf)
	for _, w := range words {
		buf = AppendWordTrigrams(w, buf)
	}
	return SortUnique(buf)
}

// FieldTrigramSet returns the same union as FieldTrigrams as a deduplicated
// but unsorted slice. Index maintenance uses it instead of FieldTrigrams:
// postings add and remove are set operations, so paying for a sort on
// every write is pure overhead.
func FieldTrigramSet(norm string, words []string) []uint32 {
	if norm == "" && len(words) == 0 {
		return nil
	}
	set := make(map[uint32]struct{}, min(len(norm)/2+len(words)*4, 4096))
	ExtractUnpadded(norm, func(t uint32) { set[t] = struct{}{} })
	for _, w := range words {
		ExtractPadded(w, func(t uint32) { set[t] = struct{}{} })
	}
	out := make([]uint32, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	return out
}

// Jaccard returns the trigram-set similarity of two sorted unique trigram
// slices: |A&B| / |A|B|. Identical sets score 1 and disjoint sets score 0.
// Two empty sets score 1.
func Jaccard(a, b []uint32) float64 {
	if len(a) == 0 && len(b) == 0 {
		return 1
	}
	inter := 0
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			inter++
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	union := len(a) + len(b) - inter
	if union == 0 {
		return 1
	}
	return float64(inter) / float64(union)
}

// MinOverlap returns the minimum shared-trigram count that keeps Fuzzy
// candidate filtering complete: one edit destroys at most TrigramLen
// trigrams, so a word within maxEdits of a query word with nQuery
// trigrams shares at least nQuery-3*maxEdits of them. A result of 0 means
// filtering is unsafe and the caller must scan instead.
func MinOverlap(nQuery, maxEdits int) int {
	min := nQuery - TrigramLen*maxEdits
	if min < 0 {
		return 0
	}
	return min
}

// MinSimilarOverlap returns the minimum shared-trigram count that keeps
// Similar candidate filtering complete at threshold: Jaccard(Q,D) >= t
// implies the intersection holds at least ceil(t*n/(1+t)) of the query
// word's n trigrams, so words sharing fewer can never reach the
// threshold. Thresholds <= 0 return 0, meaning every row qualifies and
// the caller must scan instead.
func MinSimilarOverlap(nQuery int, threshold float64) int {
	if threshold <= 0 || nQuery <= 0 {
		return 0
	}
	min := threshold * float64(nQuery) / (1 + threshold)
	m := int(min)
	if float64(m) < min {
		m++
	}
	if m < 1 {
		m = 1
	}
	return m
}

// QueryWord is one normalized query word with its prepared trigram set.
// Prepare it once per search and reuse it across candidates.
type QueryWord struct {
	Word string
	Tri  []uint32
}

// PrepareQueryWords prepares every query word's trigram set once.
func PrepareQueryWords(words []string) []QueryWord {
	out := make([]QueryWord, len(words))
	for i, w := range words {
		out[i] = QueryWord{Word: w, Tri: WordTrigrams(w)}
	}
	return out
}

// Distance returns the byte-oriented Levenshtein edit distance of a and b
// when it does not exceed maxEdits, reporting false otherwise. When ok is
// false the distance is 0 and carries no meaning. The two-row dynamic
// program exits early once every way forward exceeds the budget. Negative
// budgets behave as 0. Short words run on stack buffers without
// allocating.
func Distance(a, b string, maxEdits int) (int, bool) {
	if maxEdits < 0 {
		maxEdits = 0
	}
	if abs(len(a)-len(b)) > maxEdits {
		return 0, false
	}
	if len(a) == 0 || len(b) == 0 {
		d := len(a) + len(b)
		return d, d <= maxEdits
	}
	if len(b)+1 <= distanceStackLen {
		var prevArr, currArr [distanceStackLen]int
		return distanceRows(a, b, maxEdits, prevArr[:len(b)+1], currArr[:len(b)+1])
	}
	return distanceRows(a, b, maxEdits, make([]int, len(b)+1), make([]int, len(b)+1))
}

// distanceStackLen bounds the stack-buffered Levenshtein fast path: words
// of 63 bytes or fewer never allocate.
const distanceStackLen = 64

func distanceRows(a, b string, maxEdits int, prev, curr []int) (int, bool) {
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		rowMin := curr[0]
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			d := min3(prev[j]+1, curr[j-1]+1, prev[j-1]+cost)
			curr[j] = d
			if d < rowMin {
				rowMin = d
			}
		}
		if rowMin > maxEdits {
			return 0, false
		}
		prev, curr = curr, prev
	}
	if d := prev[len(b)]; d <= maxEdits {
		return d, true
	}
	return 0, false
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

func min3(a, b, c int) int {
	if a > b {
		a = b
	}
	if a > c {
		a = c
	}
	return a
}

// ScoreSimilar scores normalized document words against normalized query
// words: every query word's best word-level Jaccard must reach threshold,
// and the returned score is the mean of those bests. It reports false when
// any query word falls short or either side has no words.
func ScoreSimilar(qWords, dWords []string, threshold float64) (float64, bool) {
	return ScoreSimilarSets(PrepareQueryWords(qWords), dWords, threshold)
}

// ScoreSimilarSets is ScoreSimilar over prepared query words. Callers
// scoring many candidates prepare once and reuse the sets; document word
// sets build into one reused buffer without maps.
func ScoreSimilarSets(qs []QueryWord, dWords []string, threshold float64) (float64, bool) {
	if len(qs) == 0 || len(dWords) == 0 {
		return 0, false
	}
	var buf []uint32
	sum := 0.0
	for _, qw := range qs {
		best := 0.0
		for _, dw := range dWords {
			dt := SortUnique(AppendWordTrigrams(dw, buf[:0]))
			if s := Jaccard(qw.Tri, dt); s > best {
				best = s
			}
			buf = dt[:0]
		}
		if best < threshold {
			return 0, false
		}
		sum += best
	}
	return sum / float64(len(qs)), true
}

// ScoreFuzzy scores normalized document words against normalized query
// words: every query word needs a document word within maxEdits, and the
// result carries total edits with the mean best-word similarity as the
// ranking tiebreak. It reports false when any query word has no in-budget
// word or either side has no words.
func ScoreFuzzy(qWords, dWords []string, maxEdits int) (edits int, sim float64, ok bool) {
	return ScoreFuzzySets(PrepareQueryWords(qWords), dWords, maxEdits)
}

// ScoreFuzzySets is ScoreFuzzy over prepared query words.
func ScoreFuzzySets(qs []QueryWord, dWords []string, maxEdits int) (edits int, sim float64, ok bool) {
	if maxEdits < 0 {
		maxEdits = 0
	}
	if len(qs) == 0 || len(dWords) == 0 {
		return 0, 0, false
	}
	var buf []uint32
	total, simSum := 0, 0.0
	for _, qw := range qs {
		bestEdits, bestSim, found := 0, 0.0, false
		for _, dw := range dWords {
			d, within := Distance(qw.Word, dw, maxEdits)
			if !within {
				continue
			}
			dt := SortUnique(AppendWordTrigrams(dw, buf[:0]))
			s := Jaccard(qw.Tri, dt)
			buf = dt[:0]
			if !found || d < bestEdits || (d == bestEdits && s > bestSim) {
				bestEdits, bestSim, found = d, s, true
			}
		}
		if !found {
			return 0, 0, false
		}
		total += bestEdits
		simSum += bestSim
	}
	return total, simSum / float64(len(qs)), true
}
