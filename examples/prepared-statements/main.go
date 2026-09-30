// Command prepared-statements shows explicit statement preparation:
// prepare once, execute many times with different arguments. The
// prepared-statement cache (Cache.StatementCacheEntries) keeps hot
// statements ready without re-parsing.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/prepared-statements
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	replicateddb "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "spedsql-prepared-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := replicateddb.Open(ctx, replicateddb.Config{
		Path:   dir,
		NodeID: replicateddb.NewNodeID(),
		Schema: replicateddb.SchemaConfig{
			Version: 1,
			Tables: []schema.TableSchema{{
				Name: "sensors",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "name", Type: schema.ColText, Nullable: true},
					{Name: "value", Type: schema.ColInteger, Nullable: true},
				},
			}},
		},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "prepared-key",
		},
		Cache: replicateddb.CacheConfig{StatementCacheEntries: 64},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	stmt, err := db.PrepareContext(ctx, `INSERT INTO sensors (id, name, value) VALUES (?, ?, ?)`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	for i := 0; i < 50; i++ {
		id := replicateddb.NewRowID()
		if _, err := stmt.ExecContext(ctx, id[:], fmt.Sprintf("sensor-%d", i%5), i); err != nil {
			log.Fatal(err)
		}
	}

	qry, err := db.PrepareContext(ctx, `SELECT count(*) FROM sensors WHERE name = ?`)
	if err != nil {
		log.Fatal(err)
	}
	defer qry.Close()
	var n int
	if err := qry.QueryRowContext(ctx, "sensor-2").Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows for sensor-2: %d\n", n)
	if n != 10 {
		log.Fatalf("want 10 rows, got %d", n)
	}
}
