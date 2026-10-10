// Command transactions shows managed typed transactions: an atomic batch and
// rollback when the application callback returns an error.
//
// Run it with:
//
//	go run ./examples/transactions
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
	"github.com/marcgauthier/murmur/ids"
)

type ledgerEntry struct {
	ID    ids.RowID `rime:"primary"`
	Entry string
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-transactions-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
		Path:   dir,
		NodeID: murmur.NewNodeID(),
		Schema: murmur.SchemaConfig{Version: 1},
		Models: []any{ledgerEntry{}},
		Spool:  murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{
			Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "transactions-key",
		},
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	count := func() int {
		n, err := db.Count(ctx, ledgerEntry{})
		if err != nil {
			log.Fatal(err)
		}
		return n
	}

	// The callback commits all three records together.
	if err := db.Transaction(ctx, func(tx *murmur.Tx) error {
		entries := []*ledgerEntry{
			{Entry: "debit"},
			{Entry: "credit"},
			{Entry: "fee"},
		}
		return tx.InsertMany(entries)
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after commit: %d\n", count())

	// Returning an error aborts every staged write in the callback.
	rollback := errors.New("demonstration rollback")
	err = db.Transaction(ctx, func(tx *murmur.Tx) error {
		if err := tx.InsertItem(&ledgerEntry{Entry: "abandoned"}); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		log.Fatalf("rollback callback returned %v, want %v", err, rollback)
	}
	fmt.Printf("rows after rollback: %d\n", count())
	if got := count(); got != 3 {
		log.Fatalf("want 3 rows after rollback, got %d", got)
	}
}
