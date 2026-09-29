// Command transactions shows explicit transactions: a multi-statement
// atomic commit, a rollback that leaves no trace, and the transaction ID.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/transactions
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	replicateddb "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
)

func count(ctx context.Context, db *replicateddb.DB) int {
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM ledger`).Scan(&n); err != nil {
		log.Fatal(err)
	}
	return n
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "spedsql-transactions-*")
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
				Name: "ledger",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "entry", Type: schema.ColText, Nullable: true},
				},
			}},
		},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "transactions-key",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Atomic commit: all three statements land together.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("tx id: %s\n", tx.TxID())
	for _, entry := range []string{"debit", "credit", "fee"} {
		id := replicateddb.NewRowID()
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO ledger (id, entry) VALUES (?, ?)`, id[:], entry); err != nil {
			_ = tx.Rollback()
			log.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after commit: %d\n", count(ctx, db))

	// Rollback: the insert never becomes visible.
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	id := replicateddb.NewRowID()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO ledger (id, entry) VALUES (?, ?)`, id[:], "abandoned"); err != nil {
		_ = tx.Rollback()
		log.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after rollback: %d\n", count(ctx, db))
	if got := count(ctx, db); got != 3 {
		log.Fatalf("want 3 rows after rollback, got %d", got)
	}
}
