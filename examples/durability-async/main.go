// Command durability-async shows opt-in scheduled Spool syncs with records.
// Graceful close syncs acknowledged writes before returning.
//
// Run it with:
//
//	go run ./examples/durability-async
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
)

type event struct {
	ID   ids.RowID `rime:"primary"`
	Body string
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-durability-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	nodeID := murmur.NewNodeID()
	definition, err := murmur.Model[event](murmur.ModelOptions{
		Name: "events", TableID: 21,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Body": 2},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	open := func() *murmur.DB {
		database, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
			Path: dir, NodeID: nodeID, Schema: murmur.SchemaConfig{Version: 1},
			Tables: []murmur.TableDefinition{definition}, Spool: murmur.DefaultSpoolConfig(),
			Encryption: murmur.EncryptionConfig{
				Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "durability-key",
			},
			Durability: murmur.DurabilityConfig{Mode: murmur.DurabilityAsync, SyncInterval: time.Second},
		}))
		if err != nil {
			log.Fatal(err)
		}
		return database
	}

	database := open()
	const rows = 200
	start := time.Now()
	for i := 0; i < rows; i++ {
		i := i
		if err := database.WriteTxContext(ctx, func(tx *murmur.Tx) error {
			return tx.InsertItem(&event{ID: murmur.NewRowID(), Body: fmt.Sprintf("event-%d", i)})
		}); err != nil {
			log.Fatal(err)
		}
	}
	fmt.Printf("%d async commits in %v\n", rows, time.Since(start).Round(time.Millisecond))
	if err := database.Close(); err != nil {
		log.Fatal(err)
	}

	database = open()
	defer database.Close()
	n, err := database.Count(ctx, event{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after graceful close + reopen: %d\n", n)
	if n != rows {
		log.Fatalf("want %d durable rows, got %d", rows, n)
	}
}
