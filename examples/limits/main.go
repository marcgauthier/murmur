// Command limits shows fail-closed value and batch limits with managed
// writes: oversized work is rejected and normal writes still pass.
//
// Run it with:
//
//	go run ./examples/limits
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
	"github.com/marcgauthier/murmur/ids"
)

type blob struct {
	ID      ids.RowID `rime:"primary"`
	Payload string
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-limits-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	definition, err := murmur.Model[blob](murmur.ModelOptions{
		Name: "blobs", TableID: 22,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Payload": 2},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
		Path: dir, NodeID: murmur.NewNodeID(),
		Schema: murmur.SchemaConfig{Version: 1}, Tables: []murmur.TableDefinition{definition},
		Spool: murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{
			Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "limits-key",
		},
		// The value cap clears per-mutation encoding overhead while still
		// rejecting the one-kilobyte payload below.
		MaxReplicatedValueBytes: 128,
		MaxBatchMutations:       10,
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// A one-kilobyte field exceeds the cap and leaves no row behind.
	err = db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		return tx.InsertItem(&blob{ID: murmur.NewRowID(), Payload: strings.Repeat("x", 1024)})
	})
	fmt.Printf("oversized value -> err=%v\n", err != nil)
	if err == nil {
		log.Fatal("oversized value was accepted, want rejection")
	}

	// Six two-field rows exceed the ten-mutation atomic batch cap.
	err = db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		rows := make([]*blob, 6)
		for i := range rows {
			rows[i] = &blob{ID: murmur.NewRowID(), Payload: "ok"}
		}
		return tx.InsertMany(rows)
	})
	fmt.Printf("oversized batch -> err=%v\n", err != nil)
	if err == nil {
		log.Fatal("oversized batch committed, want rejection")
	}

	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		return tx.InsertItem(&blob{ID: murmur.NewRowID(), Payload: "small"})
	}); err != nil {
		log.Fatal(err)
	}
	n, err := db.Count(ctx, blob{})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after limits demo: %d\n", n)
	if n != 1 {
		log.Fatalf("want 1 row, got %d", n)
	}
}
