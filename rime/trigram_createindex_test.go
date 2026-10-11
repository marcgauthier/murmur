package rime_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/marcgauthier/murmur/rime"
	"github.com/marcgauthier/murmur/rime/trigram"
)

type trigramCIPlain struct {
	ID    string `rime:"primary"`
	Body  string
	Count int
}

func seedCIPlain(t *testing.T, bodies ...string) (*rime.DB, *rime.Table[trigramCIPlain]) {
	t.Helper()
	db := rime.New()
	table, err := rime.Register[trigramCIPlain](db)
	if err != nil {
		t.Fatal(err)
	}
	for i, body := range bodies {
		if err := table.Insert(&trigramCIPlain{ID: fmt.Sprintf("r%d", i), Body: body, Count: i}); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { db.Close() })
	return db, table
}

func TestCreateIndexBuildsOverExistingData(t *testing.T) {
	db, table := seedCIPlain(t,
		"The database is running",
		"Network connection pooling",
		"A quick brown fox",
	)
	if _, err := table.SearchTrigram("Body", "database", trigram.Contains, trigram.Options{}); !errors.Is(err, rime.ErrBadSchema) {
		t.Fatalf("search before CreateIndex err = %v, want ErrBadSchema", err)
	}
	if err := db.CreateIndex("trigramCIPlain", "Body", rime.IndexTrigram); err != nil {
		t.Fatal(err)
	}
	hits, err := table.SearchTrigram("Body", "database", trigram.Contains, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Key != "r0" {
		t.Fatalf("contains = %v", hits)
	}
	hits, err = table.SearchTrigram("Body", "databse", trigram.Similar, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Key != "r0" {
		t.Fatalf("similar = %v", hits)
	}
}

func TestCreateIndexIdempotentRebuild(t *testing.T) {
	db, table := seedCIPlain(t, "alpha bravo", "charlie delta")
	for i := 0; i < 2; i++ {
		if err := db.CreateIndex("trigramCIPlain", "Body", rime.IndexTrigram); err != nil {
			t.Fatal(err)
		}
	}
	hits, err := table.SearchTrigram("Body", "alpha", trigram.Contains, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != 1 || hits[0].Key != "r0" {
		t.Fatalf("after rebuild = %v", hits)
	}
}

func TestCreateIndexErrors(t *testing.T) {
	db, _ := seedCIPlain(t, "hello")
	if err := db.CreateIndex("nope", "Body", rime.IndexTrigram); !errors.Is(err, rime.ErrBadSchema) {
		t.Fatalf("unknown table err = %v", err)
	}
	if err := db.CreateIndex("trigramCIPlain", "Nope", rime.IndexTrigram); !errors.Is(err, rime.ErrBadSchema) {
		t.Fatalf("unknown field err = %v", err)
	}
	if err := db.CreateIndex("trigramCIPlain", "Count", rime.IndexTrigram); !errors.Is(err, rime.ErrBadSchema) {
		t.Fatalf("non-string field err = %v", err)
	}
	if err := db.CreateIndex("trigramCIPlain", "Body", rime.IndexKind(99)); !errors.Is(err, rime.ErrBadSchema) {
		t.Fatalf("bad kind err = %v", err)
	}
	closed := rime.New()
	if _, err := rime.Register[trigramCIPlain](closed); err != nil {
		t.Fatal(err)
	}
	closed.Close()
	if err := closed.CreateIndex("trigramCIPlain", "Body", rime.IndexTrigram); !errors.Is(err, rime.ErrDBClosed) {
		t.Fatalf("closed db err = %v", err)
	}
}

func TestCreateIndexMaintainedAfter(t *testing.T) {
	db, table := seedCIPlain(t, "alpha bravo")
	if err := db.CreateIndex("trigramCIPlain", "Body", rime.IndexTrigram); err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(&trigramCIPlain{ID: "r1", Body: "charlie delta"}); err != nil {
		t.Fatal(err)
	}
	if hits, _ := table.SearchTrigram("Body", "charlie", trigram.Contains, trigram.Options{}); len(hits) != 1 {
		t.Fatalf("insert after CreateIndex missing: %v", hits)
	}
	if err := table.Delete("r0"); err != nil {
		t.Fatal(err)
	}
	if hits, _ := table.SearchTrigram("Body", "alpha", trigram.Contains, trigram.Options{}); len(hits) != 0 {
		t.Fatalf("deleted row still indexed: %v", hits)
	}
}

func TestCreateIndexConcurrentWriters(t *testing.T) {
	db := rime.New()
	defer db.Close()
	table, err := rime.Register[trigramCIPlain](db)
	if err != nil {
		t.Fatal(err)
	}
	bodies := []string{
		"alpha one " + strings.Repeat("filler text ", 100),
		"beta two " + strings.Repeat("filler text ", 100),
		"gamma three " + strings.Repeat("filler text ", 100),
	}
	const rows = 300
	for i := 0; i < rows; i++ {
		if err := table.Insert(&trigramCIPlain{ID: fmt.Sprintf("r%d", i), Body: bodies[i%len(bodies)]}); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				id := fmt.Sprintf("r%d", (w*37+i)%rows)
				_ = table.Update(id, func(d *trigramCIPlain) error {
					d.Body = bodies[(w+i+1)%len(bodies)]
					return nil
				})
			}
		}(w)
	}
	createErr := make(chan error, 1)
	go func() { createErr <- db.CreateIndex("trigramCIPlain", "Body", rime.IndexTrigram) }()
	wg.Wait()
	if err := <-createErr; err != nil {
		t.Fatal(err)
	}
	// The finished index must agree exactly with a full scan baseline.
	scan, err := table.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for _, rec := range scan {
		if strings.Contains(trigram.Normalize(rec.Body), "alpha") {
			want[rec.ID] = true
		}
	}
	hits, err := table.SearchTrigram("Body", "alpha", trigram.Contains, trigram.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != len(want) {
		t.Fatalf("index hits %d rows, baseline %d", len(hits), len(want))
	}
	for _, h := range hits {
		key := h.Key.(string)
		if !want[key] {
			t.Fatalf("index hit %q missing from baseline", key)
		}
		delete(want, key)
	}
}

func TestTrigramStats(t *testing.T) {
	_, table := seedTrigramDocs(t)
	st := table.TableStats()
	stat, ok := st.Indexes["Body"]
	if !ok {
		t.Fatalf("no Body index stat: %+v", st.Indexes)
	}
	if stat.Kind != "trigram" || stat.Entries == 0 || stat.Cardinality == 0 {
		t.Fatalf("Body stat = %+v", stat)
	}
	if st.IndexEntries == 0 {
		t.Fatal("zero total index entries")
	}
}
