package trigram

import (
	"math"
	"slices"
	"testing"
)

func TestNormalize(t *testing.T) {
	cases := map[string]string{
		"":                       "",
		"abc":                    "abc",
		"  Hello\tWORLD\n\nGo  ": "hello world go",
		"MiXeD  \t Spacing":      "mixed spacing",
		"already folded":         "already folded",
		"punctuation! Stays?":    "punctuation! stays?",
		"under_score-dash.123":   "under_score-dash.123",
	}
	for in, want := range cases {
		if got := Normalize(in); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPrepare(t *testing.T) {
	norm, words := Prepare("  The Database\tIS\nRunning  ")
	if norm != "the database is running" {
		t.Fatalf("norm = %q", norm)
	}
	want := []string{"the", "database", "is", "running"}
	if !slices.Equal(words, want) {
		t.Fatalf("words = %q, want %q", words, want)
	}
	if norm, words := Prepare(""); norm != "" || len(words) != 0 {
		t.Fatalf("empty Prepare = %q %q", norm, words)
	}
}

func TestExtractUnpadded(t *testing.T) {
	var got []uint32
	ExtractUnpadded("abcd", func(x uint32) { got = append(got, x) })
	want := []uint32{Pack('a', 'b', 'c'), Pack('b', 'c', 'd')}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	got = nil
	ExtractUnpadded("ab", func(x uint32) { got = append(got, x) })
	if len(got) != 0 {
		t.Fatalf("short input yielded %v", got)
	}
	// Windows span spaces: cross-word trigrams exist in the unpadded form.
	found := false
	ExtractUnpadded("database is", func(x uint32) {
		if x == Pack('e', ' ', 'i') {
			found = true
		}
	})
	if !found {
		t.Fatal("unpadded extraction missed cross-word trigram 'e i'")
	}
}

func TestExtractPadded(t *testing.T) {
	var got []uint32
	ExtractPadded("ab", func(x uint32) { got = append(got, x) })
	want := []uint32{Pack(' ', ' ', 'a'), Pack(' ', 'a', 'b'), Pack('a', 'b', ' '), Pack('b', ' ', ' ')}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	got = nil
	ExtractPadded("a", func(x uint32) { got = append(got, x) })
	if len(got) != 3 {
		t.Fatalf("single char yielded %d trigrams, want 3", len(got))
	}
	got = nil
	ExtractPadded("", func(x uint32) { got = append(got, x) })
	if len(got) != 0 {
		t.Fatalf("empty word yielded %v", got)
	}
}

func containsAll(hay []uint32, needles []uint32) bool {
	set := map[uint32]struct{}{}
	for _, x := range hay {
		set[x] = struct{}{}
	}
	for _, n := range needles {
		if _, ok := set[n]; !ok {
			return false
		}
	}
	return true
}

func TestFieldTrigramsUnion(t *testing.T) {
	norm, words := Prepare("The database is running")
	field := FieldTrigrams(norm, words)
	if !slices.IsSorted(field) {
		t.Fatal("FieldTrigrams not sorted")
	}
	for i := 1; i < len(field); i++ {
		if field[i] == field[i-1] {
			t.Fatal("FieldTrigrams contains duplicates")
		}
	}
	// Cross-word trigram comes only from the full-field unpadded source.
	if !containsAll(field, []uint32{Pack('e', ' ', 'i')}) {
		t.Fatal("field trigrams miss cross-word 'e i'")
	}
	// Boundary trigram comes only from per-word padding.
	if !containsAll(field, []uint32{Pack(' ', ' ', 'd')}) {
		t.Fatal("field trigrams miss padded boundary '  d'")
	}
	// Single-word query trigrams are a subset (the padding trap).
	if !containsAll(field, QueryTrigrams("database")) {
		t.Fatal("query trigrams for 'database' are not a field subset")
	}
	// Multi-word query trigrams are a subset too.
	if !containsAll(field, QueryTrigrams("database is")) {
		t.Fatal("query trigrams for 'database is' are not a field subset")
	}
}

func TestFieldTrigramSetMatchesUnion(t *testing.T) {
	norm, words := Prepare("The database is running")
	got := SortUnique(append([]uint32(nil), FieldTrigramSet(norm, words)...))
	if !slices.Equal(got, FieldTrigrams(norm, words)) {
		t.Fatal("FieldTrigramSet differs from FieldTrigrams")
	}
	seen := map[uint32]int{}
	for _, x := range FieldTrigramSet(norm, words) {
		seen[x]++
	}
	for x, n := range seen {
		if n != 1 {
			t.Fatalf("trigram %v appears %d times", x, n)
		}
	}
	if out := FieldTrigramSet("", nil); out != nil {
		t.Fatalf("empty set = %v", out)
	}
}

func TestJaccard(t *testing.T) {
	a := WordTrigrams("database")
	if got := Jaccard(a, a); got != 1 {
		t.Fatalf("identical Jaccard = %v, want 1", got)
	}
	if got := Jaccard(WordTrigrams("cat"), WordTrigrams("ran")); got != 0 {
		t.Fatalf("disjoint Jaccard = %v, want 0", got)
	}
	if got := Jaccard(nil, nil); got != 1 {
		t.Fatalf("empty Jaccard = %v, want 1", got)
	}
	// databse vs database share 7 of 12 union trigrams.
	got := Jaccard(WordTrigrams("databse"), WordTrigrams("database"))
	if math.Abs(got-7.0/12.0) > 1e-9 {
		t.Fatalf("Jaccard(databse, database) = %v, want 7/12", got)
	}
}

func TestDistance(t *testing.T) {
	cases := []struct {
		a, b string
		max  int
		d    int
		ok   bool
	}{
		{"kitten", "sitting", 3, 3, true},
		{"kitten", "sitting", 2, 0, false},
		{"cat", "ran", 2, 2, true},
		{"cat", "ran", 1, 0, false},
		{"same", "same", 0, 0, true},
		{"a", "b", 0, 0, false},
		{"", "", 0, 0, true},
		{"abc", "", 2, 0, false},
		{"abc", "", 3, 3, true},
		{"a", "abcdef", 2, 0, false}, // length-difference early exit
		{"a", "a", -1, 0, true},      // negative budget clamps to 0
		{"a", "b", -1, 0, false},
	}
	for _, c := range cases {
		d, ok := Distance(c.a, c.b, c.max)
		if d != c.d || ok != c.ok {
			t.Errorf("Distance(%q,%q,%d) = (%d,%v), want (%d,%v)",
				c.a, c.b, c.max, d, ok, c.d, c.ok)
		}
	}
}

func TestMinOverlap(t *testing.T) {
	if got := MinOverlap(5, 2); got != 0 {
		t.Fatalf("MinOverlap(5,2) = %d, want 0 (scan fallback)", got)
	}
	if got := MinOverlap(10, 2); got != 4 {
		t.Fatalf("MinOverlap(10,2) = %d, want 4", got)
	}
	if got := MinOverlap(8, 1); got != 5 {
		t.Fatalf("MinOverlap(8,1) = %d, want 5", got)
	}
}

func TestMinSimilarOverlap(t *testing.T) {
	cases := []struct {
		n    int
		th   float64
		want int
	}{
		{9, 0.3, 3},   // ceil(2.7/1.3)
		{5, 0.5, 2},   // ceil(2.5/1.5)
		{10, 1.0, 5},  // ceil(10/2)
		{10, 0.01, 1}, // tiny thresholds still need one shared trigram
		{10, 0, 0},    // zero threshold matches everything: scan
		{10, -1, 0},
		{0, 0.3, 0},
	}
	for _, c := range cases {
		if got := MinSimilarOverlap(c.n, c.th); got != c.want {
			t.Errorf("MinSimilarOverlap(%d,%v) = %d, want %d", c.n, c.th, got, c.want)
		}
	}
}

func TestPreparedSetsMatchWordScoring(t *testing.T) {
	qWords := []string{"databse", "connection"}
	dWords := []string{"the", "database", "is", "running", "connection"}
	qs := PrepareQueryWords(qWords)
	if len(qs) != len(qWords) || qs[0].Word != "databse" || len(qs[0].Tri) == 0 {
		t.Fatalf("prepared = %+v", qs)
	}
	wantScore, wantOK := ScoreSimilar(qWords, dWords, 0.3)
	gotScore, gotOK := ScoreSimilarSets(qs, dWords, 0.3)
	if gotScore != wantScore || gotOK != wantOK {
		t.Fatalf("similar sets = (%v,%v), want (%v,%v)", gotScore, gotOK, wantScore, wantOK)
	}
	wantEdits, wantSim, wantFOK := ScoreFuzzy(qWords, dWords, 2)
	gotEdits, gotSim, gotFOK := ScoreFuzzySets(qs, dWords, 2)
	if gotEdits != wantEdits || gotSim != wantSim || gotFOK != wantFOK {
		t.Fatalf("fuzzy sets = (%d,%v,%v), want (%d,%v,%v)", gotEdits, gotSim, gotFOK, wantEdits, wantSim, wantFOK)
	}
}

func TestBufferExtractionMatches(t *testing.T) {
	if got := SortUnique(AppendWordTrigrams("database", nil)); !slices.Equal(got, WordTrigrams("database")) {
		t.Fatal("buffered word extraction differs")
	}
	if got := SortUnique(AppendUnpadded("database is", nil)); !slices.Equal(got, QueryTrigrams("database is")) {
		t.Fatal("buffered unpadded extraction differs")
	}
	// Buffers reuse across words without leaking earlier content.
	var buf []uint32
	first := SortUnique(AppendWordTrigrams("database", buf[:0]))
	buf = first[:0]
	second := SortUnique(AppendWordTrigrams("cat", buf))
	if !slices.Equal(second, WordTrigrams("cat")) {
		t.Fatalf("reused buffer = %v", second)
	}
}

func TestScoreSimilar(t *testing.T) {
	doc := []string{"the", "database", "is", "running"}
	score, ok := ScoreSimilar([]string{"databse"}, doc, 0.3)
	if !ok {
		t.Fatal("databse should match database at threshold 0.3")
	}
	if math.Abs(score-7.0/12.0) > 1e-9 {
		t.Fatalf("score = %v, want 7/12", score)
	}
	if _, ok := ScoreSimilar([]string{"databse"}, doc, 0.7); ok {
		t.Fatal("databse should miss at threshold 0.7")
	}
	// AND semantics: every query word must meet the threshold, so a doc
	// holding only "network" cannot score 1.0 for "network connection".
	if _, ok := ScoreSimilar([]string{"network", "connection"}, []string{"network", "only"}, 0.3); ok {
		t.Fatal("partial multi-word match should be excluded")
	}
	if _, ok := ScoreSimilar(nil, doc, 0.3); ok {
		t.Fatal("empty query should not match")
	}
	if _, ok := ScoreSimilar([]string{"x"}, nil, 0.3); ok {
		t.Fatal("empty document should not match")
	}
}

func TestScoreSimilarBeatsWholeDocDilution(t *testing.T) {
	words := []string{"database"}
	for i := 0; i < 200; i++ {
		words = append(words, "filler"+string(rune('a'+i/26))+string(rune('a'+i%26)))
	}
	if _, ok := ScoreSimilar([]string{"databse"}, words, 0.3); !ok {
		t.Fatal("word-level score should survive a long document")
	}
	var docTrigrams []uint32
	seen := map[uint32]struct{}{}
	for _, w := range words {
		for _, x := range WordTrigrams(w) {
			if _, dup := seen[x]; !dup {
				seen[x] = struct{}{}
				docTrigrams = append(docTrigrams, x)
			}
		}
	}
	slices.Sort(docTrigrams)
	if got := Jaccard(WordTrigrams("databse"), docTrigrams); got >= 0.1 {
		t.Fatalf("whole-doc Jaccard = %v, want dilution below 0.1", got)
	}
}

func TestScoreFuzzy(t *testing.T) {
	doc := []string{"the", "database", "is", "running"}
	edits, sim, ok := ScoreFuzzy([]string{"databse"}, doc, 2)
	if !ok || edits != 1 {
		t.Fatalf("databse = (%d,%v,%v), want (1,7/12,true)", edits, sim, ok)
	}
	if math.Abs(sim-7.0/12.0) > 1e-9 {
		t.Fatalf("sim = %v, want 7/12", sim)
	}
	// Zero shared trigrams at distance 2: scoring still finds it; the
	// caller must use MinOverlap to decide scan vs index.
	edits, _, ok = ScoreFuzzy([]string{"cat"}, []string{"ran"}, 2)
	if !ok || edits != 2 {
		t.Fatalf("cat/ran = (%d,%v), want (2,true)", edits, ok)
	}
	if _, _, ok = ScoreFuzzy([]string{"cat"}, []string{"ran"}, 1); ok {
		t.Fatal("cat/ran should miss with max 1 edit")
	}
	if _, _, ok = ScoreFuzzy([]string{"network", "connection"}, []string{"network", "only"}, 2); ok {
		t.Fatal("partial multi-word fuzzy match should be excluded")
	}
	if _, _, ok = ScoreFuzzy(nil, doc, 2); ok {
		t.Fatal("empty query should not match")
	}
}

func intLess(a, b any) bool { return a.(int) < b.(int) }

func TestSortHits(t *testing.T) {
	similar := []Hit{{Key: 1, Score: 0.5}, {Key: 2, Score: 0.9}, {Key: 3, Score: 0.5}}
	SortHits(similar, Similar, intLess)
	var order []int
	for _, h := range similar {
		order = append(order, h.Key.(int))
	}
	if !slices.Equal(order, []int{2, 1, 3}) {
		t.Fatalf("similar order = %v", order)
	}
	fuzzy := []Hit{{Key: 1, Score: 0.1, Edits: 2}, {Key: 2, Score: 0.1, Edits: 1}, {Key: 3, Score: 0.9, Edits: 1}}
	SortHits(fuzzy, Fuzzy, intLess)
	order = order[:0]
	for _, h := range fuzzy {
		order = append(order, h.Key.(int))
	}
	if !slices.Equal(order, []int{3, 2, 1}) {
		t.Fatalf("fuzzy order = %v", order)
	}
	contains := []Hit{{Key: 2, Score: 1}, {Key: 1, Score: 1}}
	SortHits(contains, Contains, intLess)
	order = order[:0]
	for _, h := range contains {
		order = append(order, h.Key.(int))
	}
	if !slices.Equal(order, []int{1, 2}) {
		t.Fatalf("contains order = %v", order)
	}
}

func TestOptionsDefaults(t *testing.T) {
	o := Options{}.WithDefaults()
	if o.Threshold != DefaultThreshold || o.MaxEdits != DefaultMaxEdits || o.Limit != 0 {
		t.Fatalf("defaults = %+v", o)
	}
	o = Options{Threshold: 0.7, MaxEdits: 1, Limit: 5}.WithDefaults()
	if o.Threshold != 0.7 || o.MaxEdits != 1 || o.Limit != 5 {
		t.Fatalf("custom options changed to %+v", o)
	}
}
