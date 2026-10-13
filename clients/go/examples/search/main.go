package main

import (
	"context"
	"errors"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
)

func run(ctx context.Context, e *demo.Env) error {
	db, err := e.Open("db", demo.Options())
	if err != nil {
		return err
	}
	if err = db.Migrate(murmur.NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT,embedding BLOB)")); err != nil {
		return err
	}
	if _, err = db.Exec("INSERT INTO docs VALUES(?,?,?),(?,?,?)", 1, "running SQLite", demo.Embedding(1, 0), 2, "other document", demo.Embedding(0, 1)); err != nil {
		return err
	}
	fts := murmur.FTS5Index("docs_fts", "docs", "title")
	fts.Tokenizer = "porter"
	indexes := []murmur.SearchIndex{fts}
	for i, metric := range []murmur.DistanceMetric{murmur.L2, murmur.Cosine, murmur.L1} {
		v := murmur.VectorIndex([]string{"vec_l2", "vec_cosine", "vec_l1"}[i], "docs", "embedding", 2)
		v.Distance = metric
		indexes = append(indexes, v)
	}
	if err = db.SetSearchIndexes(indexes); err != nil {
		return err
	}
	if err = db.RefreshIndexes(); err != nil {
		return err
	}
	n, err := demo.Int(db, "SELECT count(*) FROM docs_fts WHERE docs_fts MATCH 'run'")
	if err != nil {
		return err
	}
	if err = demo.Require(n == 1, "porter match failed"); err != nil {
		return err
	}
	for _, name := range []string{"vec_l2", "vec_cosine", "vec_l1"} {
		id, err := demo.Int(db, "SELECT rowid FROM "+name+" WHERE embedding MATCH ? AND k=1", demo.Embedding(1, 0))
		if err != nil {
			return err
		}
		if err = demo.Require(id == 1, "%s nearest neighbor mismatch", name); err != nil {
			return err
		}
	}
	if _, err = db.Exec("DELETE FROM docs_fts"); !errors.Is(err, murmur.ErrSQL) {
		return demo.Require(false, "managed virtual-table write was not rejected: %v", err)
	}
	state, err := db.ExportState()
	if err != nil {
		return err
	}
	if err = demo.Require(len(state.Schema.Tables) == 1, "derived table exported"); err != nil {
		return err
	}
	if err = db.Flush(); err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	opts := murmur.DefaultOptions()
	opts.SearchIndexes = indexes
	db, err = e.Open("db", opts)
	if err != nil {
		return err
	}
	if err = db.RefreshIndexes(); err != nil {
		return err
	}
	n, err = demo.Int(db, "SELECT count(*) FROM docs_fts WHERE docs_fts MATCH 'sqlite'")
	if err != nil {
		return err
	}
	e.Print("FTS stemming and L2/cosine/L1 neighbors verified; derived indexes rebuilt on reopen")
	return demo.Require(n == 1, "recovered FTS missing row")
}
func main() { demo.Main("search", run) }
