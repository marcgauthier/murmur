package rime

import (
	"errors"
	"testing"

	"github.com/marcgauthier/murmur/rime/trigram"
)

type trigramRegRecord struct {
	ID   string `rime:"primary"`
	Body string `rime:"trigram"`
}

type trigramNamed string

type trigramRegNamed struct {
	ID   string       `rime:"primary"`
	Body trigramNamed `rime:"trigram"`
}

type trigramRegBad struct {
	ID    string `rime:"primary"`
	Count int    `rime:"trigram"`
}

func TestTrigramTagBuildsIndex(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[trigramRegRecord](db)
	if err != nil {
		t.Fatal(err)
	}
	ix := table.idx
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if _, ok := ix.trigram["Body"]; !ok {
		t.Fatal("trigram tag did not build an index")
	}
	if len(ix.trigramFields) != 1 || ix.trigramFields[0] != "Body" {
		t.Fatalf("trigramFields = %v", ix.trigramFields)
	}
}

func TestTrigramTagAcceptsNamedString(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[trigramRegNamed](db)
	if err != nil {
		t.Fatal(err)
	}
	table.idx.mu.RLock()
	defer table.idx.mu.RUnlock()
	if _, ok := table.idx.trigram["Body"]; !ok {
		t.Fatal("named string trigram tag did not build an index")
	}
}

func TestTrigramTagRejectsNonString(t *testing.T) {
	db := New()
	defer db.Close()
	if _, err := Register[trigramRegBad](db); !errors.Is(err, ErrBadSchema) {
		t.Fatalf("non-string trigram err = %v, want ErrBadSchema", err)
	}
}

func postKeys(s *slicePostings, tg uint32) []any {
	return s.appendKeys(tg, nil)
}

func TestSlicePostingsAddRemove(t *testing.T) {
	s := newSlicePostings()
	abc := trigram.Pack('a', 'b', 'c')
	bcd := trigram.Pack('b', 'c', 'd')
	s.add([]uint32{abc, bcd}, "k1")
	s.add([]uint32{abc}, "k2")
	if got := postKeys(s, abc); len(got) != 2 {
		t.Fatalf("abc postings = %v, want 2 keys", got)
	}
	if s.terms() != 2 || s.refs() != 3 {
		t.Fatalf("terms = %d refs = %d, want 2 and 3", s.terms(), s.refs())
	}
	s.remove([]uint32{abc, bcd}, "k1")
	if got := postKeys(s, abc); len(got) != 1 || got[0] != "k2" {
		t.Fatalf("abc postings after remove = %v", got)
	}
	if got := postKeys(s, bcd); len(got) != 0 {
		t.Fatalf("emptied posting not deleted: %v", got)
	}
	if s.terms() != 1 || s.refs() != 1 {
		t.Fatalf("terms = %d refs = %d, want 1 and 1", s.terms(), s.refs())
	}
	// Missing trigram and missing key are no-ops.
	s.remove([]uint32{trigram.Pack('x', 'y', 'z')}, "k2")
	s.remove([]uint32{abc}, "nobody")
	if got := postKeys(s, abc); len(got) != 1 {
		t.Fatalf("no-op removals changed postings: %v", got)
	}
}

func TestSlicePostingsPrepareAbort(t *testing.T) {
	s := newSlicePostings()
	abc := trigram.Pack('a', 'b', 'c')
	s.prepare([]uint32{abc}, 2)
	if s.terms() != 1 {
		t.Fatalf("prepared terms = %d, want 1", s.terms())
	}
	if got := postKeys(s, abc); len(got) != 0 {
		t.Fatalf("prepared posting visible: %v", got)
	}
	s.prune([]uint32{abc})
	if s.terms() != 0 {
		t.Fatalf("prune left %d terms", s.terms())
	}
	// Prepared capacity installs without reallocation failures.
	s.prepare([]uint32{abc}, 2)
	s.addPrepared([]uint32{abc}, "k1")
	s.addPrepared([]uint32{abc}, "k2")
	if got := postKeys(s, abc); len(got) != 2 {
		t.Fatalf("prepared install = %v, want 2 keys", got)
	}
	// Prune keeps non-empty postings.
	s.prune([]uint32{abc})
	if got := postKeys(s, abc); len(got) != 2 {
		t.Fatalf("prune dropped live posting: %v", got)
	}
}

func TestTrigramIdxAddRemove(t *testing.T) {
	idx := newTrigramIdx()
	idx.add("The database is running", "k1")
	if idx.store.terms() == 0 || idx.store.refs() == 0 {
		t.Fatal("add left the index empty")
	}
	idx.remove("The database is running", "k1")
	if idx.store.terms() != 0 || idx.store.refs() != 0 {
		t.Fatalf("remove left terms=%d refs=%d", idx.store.terms(), idx.store.refs())
	}
}

type trigramMaintRecord struct {
	ID   string `rime:"primary"`
	Body string `rime:"trigram"`
	Tag  string
}

func trigramKeys(t *testing.T, table *Table[trigramMaintRecord], tg uint32) []any {
	t.Helper()
	table.idx.mu.RLock()
	defer table.idx.mu.RUnlock()
	return table.idx.trigram["Body"].store.appendKeys(tg, nil)
}

func trigramRefs(t *testing.T, table *Table[trigramMaintRecord]) (terms int, refs int64) {
	t.Helper()
	table.idx.mu.RLock()
	defer table.idx.mu.RUnlock()
	store := table.idx.trigram["Body"].store
	return store.terms(), store.refs()
}

func hasKey(keys []any, want any) bool {
	for _, k := range keys {
		if k == want {
			return true
		}
	}
	return false
}

func TestTrigramMaintainedOnInsertUpdateDelete(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[trigramMaintRecord](db)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(&trigramMaintRecord{ID: "r1", Body: "alpha bravo", Tag: "x"}); err != nil {
		t.Fatal(err)
	}
	alp := trigram.Pack('a', 'l', 'p')
	if keys := trigramKeys(t, table, alp); !hasKey(keys, "r1") {
		t.Fatalf("insert left no posting: %v", keys)
	}
	_, refsAfterInsert := trigramRefs(t, table)
	if refsAfterInsert == 0 {
		t.Fatal("insert left zero refs")
	}
	// Updating an untracked field keeps every posting.
	if err := table.Update("r1", func(r *trigramMaintRecord) error { r.Tag = "y"; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, refs := trigramRefs(t, table); refs != refsAfterInsert {
		t.Fatalf("untracked update changed refs %d -> %d", refsAfterInsert, refs)
	}
	// Updating the tracked field swaps postings.
	if err := table.Update("r1", func(r *trigramMaintRecord) error { r.Body = "charlie delta"; return nil }); err != nil {
		t.Fatal(err)
	}
	if keys := trigramKeys(t, table, alp); hasKey(keys, "r1") {
		t.Fatalf("stale posting survived update: %v", keys)
	}
	if keys := trigramKeys(t, table, trigram.Pack('c', 'h', 'a')); !hasKey(keys, "r1") {
		t.Fatalf("new posting missing: %v", keys)
	}
	if err := table.Delete("r1"); err != nil {
		t.Fatal(err)
	}
	if terms, refs := trigramRefs(t, table); terms != 0 || refs != 0 {
		t.Fatalf("delete left terms=%d refs=%d", terms, refs)
	}
}

func TestTrigramMaintainedOverPreparedCommit(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[trigramMaintRecord](db)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := table.In(tx).Insert(&trigramMaintRecord{ID: "r1", Body: "prepared write"}); err != nil {
		t.Fatal(err)
	}
	prepared, err := tx.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Publish(); err != nil {
		t.Fatal(err)
	}
	if keys := trigramKeys(t, table, trigram.Pack('p', 'r', 'e')); !hasKey(keys, "r1") {
		t.Fatalf("prepared publish left no posting: %v", keys)
	}
	tx2, err := db.BeginTx(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := table.In(tx2).Delete("r1"); err != nil {
		t.Fatal(err)
	}
	prepared2, err := tx2.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared2.Publish(); err != nil {
		t.Fatal(err)
	}
	if terms, refs := trigramRefs(t, table); terms != 0 || refs != 0 {
		t.Fatalf("prepared delete left terms=%d refs=%d", terms, refs)
	}
}

func TestTrigramPreparedAbortPrunes(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[trigramMaintRecord](db)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := table.In(tx).Insert(&trigramMaintRecord{ID: "r1", Body: "aborted write"}); err != nil {
		t.Fatal(err)
	}
	prepared, err := tx.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Abort(); err != nil {
		t.Fatal(err)
	}
	if terms, refs := trigramRefs(t, table); terms != 0 || refs != 0 {
		t.Fatalf("abort left terms=%d refs=%d", terms, refs)
	}
	if _, found, err := table.Lookup("r1"); err != nil || found {
		t.Fatalf("aborted row visible: found=%v err=%v", found, err)
	}
}

func TestCompareTrigramKeys(t *testing.T) {
	if compareTrigramKeys(9, 10) >= 0 || compareTrigramKeys(10, 9) <= 0 {
		t.Fatal("ints do not compare numerically")
	}
	if compareTrigramKeys("b", "a") <= 0 || compareTrigramKeys("a", "a") != 0 {
		t.Fatal("strings misordered")
	}
	type named string
	if compareTrigramKeys(named("a"), "b") >= 0 {
		t.Fatal("named string not compared as string")
	}
	low, high := UUID{1}, UUID{2}
	if compareTrigramKeys(low, high) >= 0 || compareTrigramKeys(high, low) <= 0 {
		t.Fatal("UUIDs misordered")
	}
	if compareTrigramKeys(1, "a") == 0 {
		t.Fatal("mixed classes compare equal")
	}
	// Totality: antisymmetric and deterministic across kinds.
	pairs := [][2]any{{1, "a"}, {"a", 1}, {low, "a"}, {1.5, 2}, {nil, "a"}}
	for _, p := range pairs {
		if compareTrigramKeys(p[0], p[1]) != -compareTrigramKeys(p[1], p[0]) {
			t.Fatalf("not antisymmetric for %v %v", p[0], p[1])
		}
	}
}

func TestTrigramBatchInsert(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[trigramMaintRecord](db)
	if err != nil {
		t.Fatal(err)
	}
	rows := []*trigramMaintRecord{
		{ID: "r1", Body: "shared words here"},
		{ID: "r2", Body: "more shared words"},
		{ID: "r3", Body: "nothing alike"},
	}
	if err := table.InsertMany(rows); err != nil {
		t.Fatal(err)
	}
	keys := trigramKeys(t, table, trigram.Pack('s', 'h', 'a'))
	if !hasKey(keys, "r1") || !hasKey(keys, "r2") || hasKey(keys, "r3") {
		t.Fatalf("batch postings wrong: %v", keys)
	}
}
