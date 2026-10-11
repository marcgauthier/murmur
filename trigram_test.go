package murmur

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/rime"
)

type trigramArticle struct {
	ID    ids.RowID `rime:"ID"`
	Title string    `rime:"trigram"`
	Site  string    `rime:"index"`
}

type trigramPlain struct {
	ID   ids.RowID `rime:"ID"`
	Body string
	Rank int
}

type trigramV1 struct {
	ID   ids.RowID `rime:"ID"`
	Body string
}

type trigramV2 struct {
	ID   ids.RowID `rime:"ID"`
	Body string    `rime:"trigram"`
}

func openTrigramDB(t *testing.T, models ...any) *DB {
	t.Helper()
	cfg := modelTestConfig(t, models...)
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func trigramRegistrations(t *testing.T, exemplar any) map[string]any {
	t.Helper()
	out := map[string]any{"runtime": exemplar}
	switch model := exemplar.(type) {
	case trigramArticle:
		def, err := Model[trigramArticle]()
		if err != nil {
			t.Fatal(err)
		}
		_ = model
		out["typed"] = def
	case trigramPlain:
		def, err := Model[trigramPlain]()
		if err != nil {
			t.Fatal(err)
		}
		_ = model
		out["typed"] = def
	default:
		t.Fatalf("no typed registration for %T", exemplar)
	}
	return out
}

func seedTrigramArticles(t *testing.T, db *DB) []trigramArticle {
	t.Helper()
	ctx := context.Background()
	docs := []trigramArticle{
		{ID: ids.NewRowID(), Title: "The database is running", Site: "OTT"},
		{ID: ids.NewRowID(), Title: "Network connection pooling", Site: "MTL"},
		{ID: ids.NewRowID(), Title: "Schedule the databse backup", Site: "OTT"},
		{ID: ids.NewRowID(), Title: "A quick brown fox", Site: "YUL"},
	}
	for i := range docs {
		if err := db.InsertItem(ctx, &docs[i]); err != nil {
			t.Fatal(err)
		}
	}
	return docs
}

func TestTrigramSearchModesLive(t *testing.T) {
	for name, registration := range trigramRegistrations(t, trigramArticle{}) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := openTrigramDB(t, registration)
			docs := seedTrigramArticles(t, db)

			hits, err := db.SearchTrigram(ctx, trigramArticle{}, "Title", "database", TrigramContains, TrigramOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(hits) != 1 || hits[0].Key != any(docs[0].ID) {
				t.Fatalf("contains = %+v", hits)
			}
			// Hydrate the hit through the item API.
			key, ok := hits[0].Key.(ids.RowID)
			if !ok {
				t.Fatalf("hit key is %T, want ids.RowID", hits[0].Key)
			}
			var loaded trigramArticle
			loaded.ID = key
			if err := db.GetItem(ctx, &loaded); err != nil {
				t.Fatal(err)
			}
			if loaded.Title != docs[0].Title {
				t.Fatalf("hydrated title = %q", loaded.Title)
			}

			hits, err = db.SearchTrigram(ctx, trigramArticle{}, "Title", "databse", TrigramSimilar, TrigramOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(hits) != 2 || hits[0].Key != any(docs[2].ID) || hits[1].Key != any(docs[0].ID) {
				t.Fatalf("similar = %+v", hits)
			}

			hits, err = db.SearchTrigram(ctx, trigramArticle{}, "Title", "databse", TrigramFuzzy, TrigramOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(hits) != 2 || hits[0].Edits != 0 || hits[1].Edits != 1 {
				t.Fatalf("fuzzy = %+v", hits)
			}

			hits, err = db.SearchTrigram(ctx, trigramArticle{}, "Title", "missing term xyz", TrigramContains, TrigramOptions{})
			if err != nil || len(hits) != 0 {
				t.Fatalf("no-match = %+v, %v", hits, err)
			}
			if _, err := db.SearchTrigram(ctx, trigramArticle{}, "Nope", "x", TrigramContains, TrigramOptions{}); err == nil {
				t.Fatal("unknown field search succeeded")
			}
			if _, err := db.SearchTrigram(ctx, trigramArticle{}, "Site", "OTT", TrigramContains, TrigramOptions{}); !errors.Is(err, rime.ErrBadSchema) {
				t.Fatalf("unindexed field err = %v, want ErrBadSchema", err)
			}
		})
	}
}

func TestTrigramCreateIndexLive(t *testing.T) {
	for name, registration := range trigramRegistrations(t, trigramPlain{}) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := openTrigramDB(t, registration)
			doc := trigramPlain{ID: ids.NewRowID(), Body: "The database is running", Rank: 1}
			if err := db.InsertItem(ctx, &doc); err != nil {
				t.Fatal(err)
			}
			// Table names match case-insensitively, like the item API.
			if err := db.CreateIndex(ctx, "trigramplain", "Body", IndexTrigram); err != nil {
				t.Fatal(err)
			}
			hits, err := db.SearchTrigram(ctx, trigramPlain{}, "Body", "database", TrigramContains, TrigramOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(hits) != 1 || hits[0].Key != any(doc.ID) {
				t.Fatalf("search after CreateIndex = %+v", hits)
			}
			// Rebuild is idempotent.
			if err := db.CreateIndex(ctx, "trigramPlain", "Body", IndexTrigram); err != nil {
				t.Fatal(err)
			}
			if err := db.CreateIndex(ctx, "nope", "Body", IndexTrigram); err == nil {
				t.Fatal("unknown table CreateIndex succeeded")
			}
			if err := db.CreateIndex(ctx, "trigramPlain", "Rank", IndexTrigram); !errors.Is(err, rime.ErrBadSchema) {
				t.Fatalf("non-string CreateIndex err = %v", err)
			}
			if err := db.CreateIndex(ctx, "trigramPlain", "Body", IndexKind(99)); !errors.Is(err, rime.ErrBadSchema) {
				t.Fatalf("bad kind CreateIndex err = %v", err)
			}
		})
	}
}

func TestTrigramTagLeavesManifestUnchanged(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	v1def, err := Model[trigramV1](ModelOptions{Name: "TrigramDoc"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := testConfig(dir)
	cfg.Tables = nil
	cfg.Models = []any{v1def}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	docs := []trigramV1{
		{ID: ids.NewRowID(), Body: "The database is running"},
		{ID: ids.NewRowID(), Body: "Network connection pooling"},
	}
	for i := range docs {
		if err := db.InsertItem(ctx, &docs[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.CreateIndex(ctx, "TrigramDoc", "Body", IndexTrigram); err != nil {
		t.Fatal(err)
	}
	before, err := db.SearchTrigram(ctx, trigramV1{}, "Body", "database", TrigramContains, TrigramOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 1 {
		t.Fatalf("pre-close hits = %+v", before)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen with the trigram tag added: no migration may be required
	// because index declarations never enter the replicated manifest.
	v2def, err := Model[trigramV2](ModelOptions{Name: "TrigramDoc"})
	if err != nil {
		t.Fatal(err)
	}
	cfg2 := cfg
	cfg2.Tables = nil
	cfg2.Models = []any{v2def}
	db2, err := Open(ctx, cfg2)
	if err != nil {
		t.Fatalf("reopen with trigram tag: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })
	after, err := db2.SearchTrigram(ctx, trigramV2{}, "Body", "database", TrigramContains, TrigramOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) || after[0].Key != before[0].Key {
		t.Fatalf("post-reopen hits = %+v, want %+v", after, before)
	}
}

type trigramRep struct {
	ID   ids.RowID `rime:"primary"`
	Body string    `rime:"trigram"`
}

func trigramRepDefinition(t testing.TB) TableDefinition {
	t.Helper()
	definition, err := define[trigramRep]("trigrams", 81, RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Body": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func waitForTrigramItem(t *testing.T, db *DB, id ids.RowID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var doc trigramRep
		doc.ID = id
		if err := db.GetItem(context.Background(), &doc); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("row %s never converged", id)
}

func TestTrigramReplicationConverges(t *testing.T) {
	ctx := context.Background()
	nodeA, nodeB := NewNodeID(), NewNodeID()
	dbid := NewDBID()
	_, creds := testClusterCA(t, nodeA, nodeB)

	cfgA := replConfig(t.TempDir(), nodeA, dbid, creds[nodeA], nil)
	cfgA.Schema.Tables = nil
	cfgA.Tables = []TableDefinition{trigramRepDefinition(t)}
	dbA, err := openSignedFixture(ctx, cfgA)
	if err != nil {
		t.Fatal(err)
	}
	defer dbA.Close()
	addrA := waitForAddr(t, dbA, 5*time.Second)

	cfgB := replConfig(t.TempDir(), nodeB, dbid, creds[nodeB],
		[]Peer{{NodeID: nodeA, Addrs: []string{addrA}}})
	cfgB.Schema.Tables = nil
	cfgB.Tables = []TableDefinition{trigramRepDefinition(t)}
	dbB, err := openSignedFixture(ctx, cfgB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbB.Close()

	doc := trigramRep{ID: NewRowID(), Body: "The replicated database is running"}
	if err := dbA.InsertItem(ctx, &doc); err != nil {
		t.Fatal(err)
	}
	waitForTrigramItem(t, dbB, doc.ID, 15*time.Second)

	// Each replica builds its own local index from replicated rows.
	for name, db := range map[string]*DB{"A": dbA, "B": dbB} {
		hits, err := db.SearchTrigram(ctx, trigramRep{}, "Body", "replicated database", TrigramContains, TrigramOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if len(hits) != 1 || hits[0].Key != any(doc.ID) {
			t.Fatalf("node %s hits = %+v", name, hits)
		}
	}
	st := dbA.Status()
	if st.ConnectedPeers < 1 {
		t.Fatalf("A peers: %+v", st)
	}
}
