// Command key-rotation shows data-key rotation on a live node: write
// rows, rotate the active data key, and prove every row still reads
// while encryption status reflects the rotation.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/key-rotation
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
	dir, err := os.MkdirTemp("", "spedsql-rotation-*")
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
				Name: "secrets",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "body", Type: schema.ColText, Nullable: true},
				},
			}},
		},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "rotation-key",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 3; i++ {
		id := replicateddb.NewRowID()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO secrets (id, body) VALUES (?, ?)`,
			id[:], fmt.Sprintf("secret-%d", i)); err != nil {
			log.Fatal(err)
		}
	}
	before := db.EncryptionStatus()
	fmt.Printf("before: algorithm=%s appKey=%s dataKey=%s phase=%s\n",
		before.Algorithm, before.ApplicationKeyID, before.ActiveDataKeyID, before.Phase)

	if err := db.RotateDataKey(ctx); err != nil {
		log.Fatal(err)
	}
	after := db.EncryptionStatus()
	fmt.Printf("after:  algorithm=%s appKey=%s dataKey=%s phase=%s\n",
		after.Algorithm, after.ApplicationKeyID, after.ActiveDataKeyID, after.Phase)
	if after.ActiveDataKeyID == before.ActiveDataKeyID {
		log.Fatal("active data key did not change across rotation")
	}

	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM secrets`).Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows readable after rotation: %d\n", n)
	if n != 3 {
		log.Fatalf("want 3 rows after rotation, got %d", n)
	}
}
