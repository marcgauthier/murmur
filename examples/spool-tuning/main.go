// Command spool-tuning demonstrates a custom Spool configuration and record
// recovery after reopen.
//
// Run it with:
//
//	go run ./examples/spool-tuning
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

type item struct {
	ID   ids.RowID `rime:"primary"`
	Body string
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-spool-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	spoolCfg := murmur.DefaultSpoolConfig()
	spoolCfg.TargetBlockBytes = 1 << 20
	spoolCfg.Compression = murmur.CompressionNone
	definition, err := murmur.Model[item](murmur.ModelOptions{
		Name: "items", TableID: 5,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Body": 2},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	nodeID := murmur.NewNodeID()
	open := func() *murmur.DB {
		db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
			Path: dir, NodeID: nodeID, Schema: murmur.SchemaConfig{Version: 1},
			Tables: []murmur.TableDefinition{definition}, Spool: spoolCfg,
			Encryption: murmur.EncryptionConfig{Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "spool-key"},
		}))
		if err != nil {
			log.Fatal(err)
		}
		return db
	}
	db := open()
	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		batch := make([]*item, 20)
		for i := range batch {
			batch[i] = &item{ID: murmur.NewRowID(), Body: fmt.Sprintf("item-%d", i)}
		}
		return tx.InsertMany(batch)
	}); err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
	db = open()
	defer db.Close()
	n, err := db.Count(ctx, item{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after reopen with custom Spool config: %d\n", n)
	if n != 20 {
		log.Fatalf("want 20 rows, got %d", n)
	}
}
