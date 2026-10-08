// Command prefix-search demonstrates RIME's indexed string-prefix query.
// Prefix matching is useful for known prefixes; it does not provide FTS5's
// tokenization or full-text ranking.
//
// Run it with:
//
//	go run ./examples/prefix-search
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
	"github.com/marcgauthier/murmur/ids"
)

type document struct {
	ID    ids.RowID `rime:"primary"`
	Title string    `rime:"prefix"`
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-prefix-search-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	definition, err := murmur.Define[document]("documents", 13, murmur.RecordOptions{
		PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Title": 2},
	})
	if err != nil {
		log.Fatal(err)
	}
	db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
		Path: dir, NodeID: murmur.NewNodeID(), Schema: murmur.SchemaConfig{Version: 1},
		Tables: []murmur.TableDefinition{definition}, Spool: murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "prefix-search-key"},
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	documents, err := murmur.TableOf[document](db, "documents")
	if err != nil {
		log.Fatal(err)
	}
	titles := []string{"replication basics", "replication recovery", "encrypted storage"}
	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		rows := make([]*document, len(titles))
		for i, title := range titles {
			rows[i] = &document{ID: murmur.NewRowID(), Title: title}
		}
		return documents.InsertMany(tx, rows)
	}); err != nil {
		log.Fatal(err)
	}
	title := murmur.StringFieldOf[document](documents, "Title")
	matches, err := documents.Where(title.StartsWith("replication")).OrderByAsc(murmur.FieldOf[document, string](documents, "Title")).Find()
	if err != nil {
		log.Fatal(err)
	}
	for _, match := range matches {
		fmt.Printf("prefix match: %s\n", match.Title)
	}
}
