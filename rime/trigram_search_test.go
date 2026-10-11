package rime_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/marcgauthier/murmur/rime"
	"github.com/marcgauthier/murmur/rime/trigram"
)

type trigramDoc struct {
	ID   string `rime:"primary"`
	Body string `rime:"trigram"`
}

func seedTrigramDocs(t *testing.T) (*rime.DB, *rime.Table[trigramDoc]) {
	t.Helper()
	db := rime.New()
	table, err := rime.Register[trigramDoc](db)
	if err != nil {
		t.Fatal(err)
	}
	docs := []trigramDoc{
		{ID: "d1", Body: "The database is running"},
		{ID: "d2", Body: "Network connection pooling"},
		{ID: "d3", Body: "A quick brown fox"},
		{ID: "d4", Body: "Schedule the databse backup"},
		{ID: "d6", Body: "lone network gadget"},
	}
	for i := range docs {
		if err := table.Insert(&docs[i]); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Close() })
	return db, table
}

func hitKeys(hits []rime.TrigramHit) []string {
	keys := make([]string, len(hits))
	for i, h := range hits {
		keys[i] = h.Key.(string)
	}
	return keys
}

func TestSearchTrigramContains(t *testing.T) {
	_, table := seedTrigramDocs(t)
	hits, err := table.SearchTrigram("Body", "database", trigram.Contains, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if keys := hitKeys(hits); len(keys) != 1 || keys[0] != "d1" {
		t.Fatalf("contains database = %v", keys)
	}
	if hits[0].Score != 1 {
		t.Fatalf("contains score = %v, want 1", hits[0].Score)
	}
	// Multi-word query needs the cross-word trigram "e i".
	hits, err = table.SearchTrigram("Body", "database is", trigram.Contains, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if keys := hitKeys(hits); len(keys) != 1 || keys[0] != "d1" {
		t.Fatalf("contains multi-word = %v", keys)
	}
	// Case-insensitive through the shared fold.
	hits, err = table.SearchTrigram("Body", "DATABASE", trigram.Contains, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if keys := hitKeys(hits); len(keys) != 1 || keys[0] != "d1" {
		t.Fatalf("contains folded = %v", keys)
	}
	hits, err = table.SearchTrigram("Body", "missing term xyz", trigram.Contains, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 0 {
		t.Fatalf("contains no-match = %v", hitKeys(hits))
	}
}

func TestSearchTrigramContainsShortQueryScans(t *testing.T) {
	_, table := seedTrigramDocs(t)
	hits, err := table.SearchTrigram("Body", "is", trigram.Contains, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if keys := hitKeys(hits); len(keys) != 1 || keys[0] != "d1" {
		t.Fatalf("short contains = %v", keys)
	}
	hits, err = table.SearchTrigram("Body", "e", trigram.Contains, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if keys := hitKeys(hits); len(keys) != 4 {
		t.Fatalf("single-char contains = %v, want 4 hits", keys)
	}
}

func TestSearchTrigramSimilar(t *testing.T) {
	_, table := seedTrigramDocs(t)
	hits, err := table.SearchTrigram("Body", "databse", trigram.Similar, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	keys := hitKeys(hits)
	if len(keys) != 2 || keys[0] != "d4" || keys[1] != "d1" {
		t.Fatalf("similar databse = %v, want [d4 d1]", keys)
	}
	if hits[0].Score != 1 || hits[1].Score >= 1 {
		t.Fatalf("similar scores = %v %v", hits[0].Score, hits[1].Score)
	}
	// AND semantics: a doc holding only "network" cannot match both words.
	hits, err = table.SearchTrigram("Body", "network connection", trigram.Similar, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if keys := hitKeys(hits); len(keys) != 1 || keys[0] != "d2" {
		t.Fatalf("similar multi-word = %v, want [d2]", keys)
	}
}

func TestSearchTrigramFuzzy(t *testing.T) {
	_, table := seedTrigramDocs(t)
	hits, err := table.SearchTrigram("Body", "databse", trigram.Fuzzy, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	keys := hitKeys(hits)
	if len(keys) != 2 || keys[0] != "d4" || keys[1] != "d1" {
		t.Fatalf("fuzzy databse = %v, want [d4 d1]", keys)
	}
	if hits[0].Edits != 0 || hits[1].Edits != 1 {
		t.Fatalf("fuzzy edits = %d %d, want 0 1", hits[0].Edits, hits[1].Edits)
	}
	hits, err = table.SearchTrigram("Body", "netwrok connection", trigram.Fuzzy, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if keys := hitKeys(hits); len(keys) != 1 || keys[0] != "d2" {
		t.Fatalf("fuzzy multi-word = %v, want [d2]", keys)
	}
	if hits[0].Edits != 2 {
		t.Fatalf("fuzzy multi-word edits = %d, want 2", hits[0].Edits)
	}
}

func TestSearchTrigramFuzzyZeroOverlapScans(t *testing.T) {
	db := rime.New()
	defer db.Close()
	table, err := rime.Register[trigramDoc](db)
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range []trigramDoc{
		{ID: "r1", Body: "ran away"},
		{ID: "r2", Body: "completely different"},
	} {
		if err := table.Insert(&doc); err != nil {
			t.Fatal(err)
		}
	}
	// "cat" shares no padded trigram with "ran" at distance 2, so the
	// lemma forces the scan fallback instead of dropping the match.
	hits, err := table.SearchTrigram("Body", "cat", trigram.Fuzzy, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if keys := hitKeys(hits); len(keys) != 1 || keys[0] != "r1" {
		t.Fatalf("fuzzy zero-overlap = %v, want [r1]", keys)
	}
	if hits[0].Edits != 2 {
		t.Fatalf("edits = %d, want 2", hits[0].Edits)
	}
}

func TestSearchTrigramLimit(t *testing.T) {
	_, table := seedTrigramDocs(t)
	hits, err := table.SearchTrigram("Body", "e", trigram.Contains, trigram.Options{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if keys := hitKeys(hits); len(keys) != 2 || keys[0] != "d1" || keys[1] != "d2" {
		t.Fatalf("limited = %v, want [d1 d2]", keys)
	}
}

func TestSearchTrigramErrors(t *testing.T) {
	_, table := seedTrigramDocs(t)
	if _, err := table.SearchTrigram("Nope", "x", trigram.Contains, trigram.Options{}); !errors.Is(err, rime.ErrBadSchema) {
		t.Fatalf("unknown field err = %v", err)
	}
	if _, err := table.SearchTrigram("ID", "x", trigram.Contains, trigram.Options{}); !errors.Is(err, rime.ErrBadSchema) {
		t.Fatalf("unindexed field err = %v", err)
	}
	if _, err := table.SearchTrigram("Body", "x", trigram.Mode(99), trigram.Options{}); !errors.Is(err, rime.ErrBadSchema) {
		t.Fatalf("bad mode err = %v", err)
	}
	for _, q := range []string{"", "   "} {
		hits, err := table.SearchTrigram("Body", q, trigram.Similar, trigram.Options{})
		if err != nil || len(hits) != 0 {
			t.Fatalf("empty query = %v, %v", hits, err)
		}
	}
}

func TestSearchTrigramReflectsWrites(t *testing.T) {
	db := rime.New()
	defer db.Close()
	table, err := rime.Register[trigramDoc](db)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(&trigramDoc{ID: "r1", Body: "alpha bravo"}); err != nil {
		t.Fatal(err)
	}
	if hits, _ := table.SearchTrigram("Body", "alpha", trigram.Contains, trigram.Options{}); len(hits) != 1 {
		t.Fatalf("insert not visible: %v", hitKeys(hits))
	}
	if err := table.Update("r1", func(d *trigramDoc) error { d.Body = "charlie delta"; return nil }); err != nil {
		t.Fatal(err)
	}
	if hits, _ := table.SearchTrigram("Body", "alpha", trigram.Contains, trigram.Options{}); len(hits) != 0 {
		t.Fatalf("stale term visible: %v", hitKeys(hits))
	}
	if hits, _ := table.SearchTrigram("Body", "charlie", trigram.Contains, trigram.Options{}); len(hits) != 1 {
		t.Fatalf("updated term missing: %v", hitKeys(hits))
	}
	if err := table.Delete("r1"); err != nil {
		t.Fatal(err)
	}
	if hits, _ := table.SearchTrigram("Body", "charlie", trigram.Contains, trigram.Options{}); len(hits) != 0 {
		t.Fatalf("deleted row visible: %v", hitKeys(hits))
	}
}

func TestSearchTrigramConcurrentWrites(t *testing.T) {
	db := rime.New()
	defer db.Close()
	table, err := rime.Register[trigramDoc](db)
	if err != nil {
		t.Fatal(err)
	}
	bodies := []string{"alpha bravo gamma", "delta epsilon zeta", "eta theta iota"}
	const rows = 8
	for i := 0; i < rows; i++ {
		if err := table.Insert(&trigramDoc{ID: fmt.Sprintf("r%d", i), Body: bodies[i%len(bodies)]}); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				id := fmt.Sprintf("r%d", (w+i)%rows)
				_ = table.Update(id, func(d *trigramDoc) error {
					d.Body = bodies[(w+i+1)%len(bodies)]
					return nil
				})
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			modes := []trigram.Mode{trigram.Contains, trigram.Similar, trigram.Fuzzy}
			queries := []string{"alpha", "alpah", "delta zeta"}
			for i := 0; i < 50; i++ {
				hits, err := table.SearchTrigram("Body", queries[(r+i)%len(queries)], modes[(r+i)%len(modes)], trigram.Options{})
				if err != nil {
					t.Errorf("search: %v", err)
					return
				}
				for _, h := range hits {
					if _, found, err := table.Lookup(h.Key); err != nil || !found {
						t.Errorf("hit %v unresolvable: found=%v err=%v", h.Key, found, err)
						return
					}
				}
			}
		}(r)
	}
	wg.Wait()
}
