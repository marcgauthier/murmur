// Command limits shows fail-closed write limits: tiny value and batch
// caps reject oversized work with an error while normal writes pass.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/limits
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	replicateddb "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "spedsql-limits-*")
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
				Name: "blobs",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "payload", Type: schema.ColText, Nullable: true},
				},
			}},
		},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "limits-key",
		},
		// Deliberately tiny caps for the demo (defaults are
		// 16 MiB per value and 100,000 mutations per batch).
		// The value cap must clear per-mutation encoding overhead,
		// so 128 still rejects the 1 KiB probe below.
		MaxReplicatedValueBytes: 128,
		MaxBatchMutations:       10,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// A 1 KiB value exceeds the 64-byte cap: rejected, nothing stored.
	id := replicateddb.NewRowID()
	_, err = db.ExecContext(ctx,
		`INSERT INTO blobs (id, payload) VALUES (?, ?)`, id[:], strings.Repeat("x", 1024))
	fmt.Printf("oversized value -> err=%v\n", err != nil)
	if err == nil {
		log.Fatal("oversized value was accepted, want rejection")
	}

	// A 20-statement transaction exceeds the 10-mutation batch cap.
	// The rejection may land on a statement or at commit; either way
	// nothing from the batch may survive.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	rejected := false
	for i := 0; i < 20 && !rejected; i++ {
		mid := replicateddb.NewRowID()
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO blobs (id, payload) VALUES (?, ?)`, mid[:], "ok"); err != nil {
			rejected = true
		}
	}
	if !rejected {
		if err := tx.Commit(); err != nil {
			rejected = true
		}
	} else {
		_ = tx.Rollback()
	}
	fmt.Printf("oversized batch -> err=%v\n", rejected)
	if !rejected {
		log.Fatal("oversized batch committed, want rejection")
	}

	// Normal writes still pass and the rejects left no trace.
	id = replicateddb.NewRowID()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO blobs (id, payload) VALUES (?, ?)`, id[:], "small"); err != nil {
		log.Fatal(err)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM blobs`).Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after limits demo: %d\n", n)
	if n != 1 {
		log.Fatalf("want 1 row, got %d", n)
	}
}
