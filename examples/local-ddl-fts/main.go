// Command local-ddl-fts shows node-local SQL objects: a secondary index,
// a standalone FTS5 full-text table, and a local-only cache table. All
// live in LocalDDL, so they are re-applied after every open and never
// replicated.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/local-ddl-fts
package main

import (
	"context"
	"fmt"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
	"log"
	"os"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-localddl-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
		Path:   dir,
		NodeID: murmur.NewNodeID(),
		Schema: murmur.SchemaConfig{
			Version: 1,
			Tables: []schema.TableSchema{{
				Name: "docs",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "title", Type: schema.ColText, Nullable: true},
				},
			}},
			LocalDDL: []string{
				`CREATE INDEX IF NOT EXISTS idx_docs_title ON docs(title)`,
				`CREATE VIRTUAL TABLE IF NOT EXISTS docs_fts USING fts5(title)`,
				`CREATE TABLE IF NOT EXISTS local_cache (key TEXT PRIMARY KEY, value BLOB)`,
			},
		},
		Pebble: murmur.DefaultPebbleConfig(),
		Encryption: murmur.EncryptionConfig{
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "localddl-key",
		},
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	titles := []string{"fast local sql", "encrypted replication", "full text search"}
	for _, title := range titles {
		id := murmur.NewRowID()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO docs (id, title) VALUES (?, ?)`, id[:], title); err != nil {
			log.Fatal(err)
		}
		// The FTS table is local-only: mirror searchable text into it.
		if _, err := db.ExecContext(ctx,
			`INSERT INTO docs_fts (title) VALUES (?)`, title); err != nil {
			log.Fatal(err)
		}
	}

	rows, err := db.QueryContext(ctx,
		`SELECT title FROM docs_fts WHERE docs_fts MATCH 'replication'`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var title string
		if err := rows.Scan(&title); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("fts match: %s\n", title)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	// The local cache table is plain scratch space: no capture, no wire cost.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO local_cache (key, value) VALUES (?, ?)`, "greeting", []byte("hello")); err != nil {
		log.Fatal(err)
	}
	var value []byte
	if err := db.QueryRowContext(ctx,
		`SELECT value FROM local_cache WHERE key = ?`, "greeting").Scan(&value); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("local cache: %s\n", value)
}
