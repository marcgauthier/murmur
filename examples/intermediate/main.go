// Command intermediate shows one encrypted node with explicit cipher
// options, a local-only secondary index, an explicit multi-statement
// transaction, and close/reopen durability: rows written before Close
// are still there after Open.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/intermediate
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	replicateddb "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
)

func openNode(ctx context.Context, dir string, nodeID replicateddb.NodeID) *replicateddb.DB {
	db, err := replicateddb.Open(ctx, replicateddb.Config{
		Path: dir,
		// The NodeID must be stable across restarts: the data
		// directory is bound to the identity that created it.
		NodeID: nodeID,
		Schema: replicateddb.SchemaConfig{
			Version: 1,
			Tables: []schema.TableSchema{{
				Name: "orders",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "sku", Type: schema.ColText, Nullable: true},
					{Name: "qty", Type: schema.ColInteger, Nullable: true},
				},
			}},
			// LocalDDL objects live only on this node: they are
			// re-applied after every open and never replicated.
			LocalDDL: []string{
				`CREATE INDEX IF NOT EXISTS idx_orders_sku ON orders(sku)`,
			},
		},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			Algorithm:       replicateddb.AES256GCM,
			Key:             []byte("0123456789abcdef0123456789abcdef"),
			KeyID:           "intermediate-key",
			DataKeyRotation: 24 * time.Hour,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "spedsql-intermediate-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	nodeID := replicateddb.NewNodeID()
	db := openNode(ctx, dir, nodeID)

	// One explicit transaction with many statements commits atomically.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		id := replicateddb.NewRowID()
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO orders (id, sku, qty) VALUES (?, ?, ?)`,
			id[:], fmt.Sprintf("sku-%03d", i%10), i); err != nil {
			_ = tx.Rollback()
			log.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// The local index serves this lookup; it costs zero replication.
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM orders WHERE sku = ?`, "sku-003").Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows with sku-003: %d\n", n)
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Reopen the same directory with the same NodeID: the 100 rows
	// must survive the restart.
	db = openNode(ctx, dir, nodeID)
	defer db.Close()
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM orders`).Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after reopen: %d\n", n)
	if n != 100 {
		log.Fatalf("want 100 durable rows, got %d", n)
	}
}
