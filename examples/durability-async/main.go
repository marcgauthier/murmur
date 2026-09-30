// Command durability-async shows opt-in scheduled disk syncs: with
// DurabilityAsync, commits acknowledge without a per-write fsync while
// Pebble still syncs about once per second (plus on graceful close).
// Trade a short loss window after a crash for much faster single-row
// writes.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/durability-async
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	replicateddb "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "spedsql-durability-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	nodeID := replicateddb.NewNodeID()

	open := func() *replicateddb.DB {
		db, err := replicateddb.Open(ctx, replicateddb.Config{
			Path:   dir,
			NodeID: nodeID,
			Schema: replicateddb.SchemaConfig{
				Version: 1,
				Tables: []schema.TableSchema{{
					Name: "events",
					Columns: []schema.ColumnSchema{
						{Name: "id", Type: schema.ColBlob},
						{Name: "body", Type: schema.ColText, Nullable: true},
					},
				}},
			},
			Pebble: replicateddb.DefaultPebbleConfig(),
			Encryption: replicateddb.EncryptionConfig{
				Key:   []byte("0123456789abcdef0123456789abcdef"),
				KeyID: "durability-key",
			},
			Durability: replicateddb.DurabilityConfig{
				Mode:         replicateddb.DurabilityAsync,
				SyncInterval: time.Second,
			},
		})
		if err != nil {
			log.Fatal(err)
		}
		return db
	}

	db := open()
	const rows = 200
	start := time.Now()
	for i := 0; i < rows; i++ {
		id := replicateddb.NewRowID()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO events (id, body) VALUES (?, ?)`,
			id[:], fmt.Sprintf("event-%d", i)); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("%d async commits in %v\n", rows, time.Since(start).Round(time.Millisecond))
	// Graceful close syncs before returning, so every acknowledged
	// row below is durable.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	db = open()
	defer db.Close()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM events`).Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after graceful close + reopen: %d\n", n)
	if n != rows {
		log.Fatalf("want %d durable rows, got %d", rows, n)
	}
}
