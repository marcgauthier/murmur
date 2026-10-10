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
	"github.com/marcgauthier/murmur/q"
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
	definition, err := murmur.Model[document](murmur.ModelOptions{
		Name: "documents", TableID: 13,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Title": 2},
		},
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
	titles := []string{"replication basics", "replication recovery", "encrypted storage"}
	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		rows := make([]*document, len(titles))
		for i, title := range titles {
			rows[i] = &document{ID: murmur.NewRowID(), Title: title}
		}
		return tx.InsertMany(rows)
	}); err != nil {
		log.Fatal(err)
	}
	var matches []document
	if err := db.Query(document{}, q.StartsWith("Title", "replication")).OrderBy("Title").FindInto(&matches); err != nil {
		log.Fatal(err)
	}
	for _, match := range matches {
		fmt.Printf("prefix match: %s\n", match.Title)
	}
}
