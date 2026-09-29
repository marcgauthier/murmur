// Command pebble-tuning shows a custom storage configuration: a small
// block cache and disabled compression instead of the Zstd level 3
// defaults. Writes, reads, and reopen recovery all work the same.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/pebble-tuning
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	replicateddb "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "spedsql-pebble-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	nodeID := replicateddb.NewNodeID()

	pebble := replicateddb.DefaultPebbleConfig()
	pebble.CacheBytes = 8 << 20 // 8 MiB block cache instead of 256 MiB
	pebble.Compression = replicateddb.CompressionConfig{
		Algorithm: replicateddb.CompressionNone,
	}
	open := func() *replicateddb.DB {
		db, err := replicateddb.Open(ctx, replicateddb.Config{
			Path:   dir,
			NodeID: nodeID,
			Schema: replicateddb.SchemaConfig{
				Version: 1,
				Tables: []schema.TableSchema{{
					Name: "items",
					Columns: []schema.ColumnSchema{
						{Name: "id", Type: schema.ColBlob},
						{Name: "body", Type: schema.ColText, Nullable: true},
					},
				}},
			},
			Pebble: pebble,
			Encryption: replicateddb.EncryptionConfig{
				Key:   []byte("0123456789abcdef0123456789abcdef"),
				KeyID: "pebble-key",
			},
		})
		if err != nil {
			log.Fatal(err)
		}
		return db
	}

	db := open()
	for i := 0; i < 20; i++ {
		id := replicateddb.NewRowID()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO items (id, body) VALUES (?, ?)`,
			id[:], fmt.Sprintf("item-%d", i)); err != nil {
			log.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	db = open()
	defer db.Close()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM items`).Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after reopen with custom Pebble config: %d\n", n)
	if n != 20 {
		log.Fatalf("want 20 rows, got %d", n)
	}
}
