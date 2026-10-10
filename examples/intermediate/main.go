// Command intermediate shows an encrypted table with a local secondary
// index, an atomic batch transaction, and close/reopen durability.
//
// Run it with:
//
//	go run ./examples/intermediate
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/q"
)

type order struct {
	ID  ids.RowID `rime:"primary"`
	SKU string    `rime:"index"`
	Qty int
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-intermediate-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	nodeID := murmur.NewNodeID()
	definition, err := murmur.Model[order](murmur.ModelOptions{
		Name: "orders", TableID: 2,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "SKU": 2, "Qty": 3},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	openNode := func() *murmur.DB {
		db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
			Path: dir, NodeID: nodeID,
			Schema: murmur.SchemaConfig{Version: 1},
			Tables: []murmur.TableDefinition{definition},
			Spool:  murmur.DefaultSpoolConfig(),
			Encryption: murmur.EncryptionConfig{
				Algorithm: murmur.AES256GCM, Key: []byte("0123456789abcdef0123456789abcdef"),
				KeyID: "intermediate-key", DataKeyRotation: 24 * time.Hour,
			},
		}))
		if err != nil {
			log.Fatal(err)
		}
		return db
	}

	db := openNode()
	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		batch := make([]*order, 100)
		for i := range batch {
			batch[i] = &order{ID: murmur.NewRowID(), SKU: fmt.Sprintf("sku-%03d", i%10), Qty: i}
		}
		return tx.InsertMany(batch)
	}); err != nil {
		log.Fatal(err)
	}
	n, err := db.Count(ctx, order{}, q.Eq("SKU", "sku-003"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows with sku-003: %d\n", n)
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	db = openNode()
	defer db.Close()
	n, err = db.Count(ctx, order{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after reopen: %d\n", n)
	if n != 100 {
		log.Fatalf("want 100 durable rows, got %d", n)
	}
}
