// Command driver-sql shows the database/sql wrapper: register an open
// *DB under a name, then use the standard library sql package against
// it. Closing the sql handle never closes the underlying *DB.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/driver-sql
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	replicateddb "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "spedsql-driver-*")
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
				Name: "widgets",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "label", Type: schema.ColText, Nullable: true},
				},
			}},
		},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "driver-key",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	replicateddb.RegisterDriverDB("primary", db)
	sqldb, err := sql.Open("replicateddb", "primary")
	if err != nil {
		log.Fatal(err)
	}

	id := replicateddb.NewRowID()
	if _, err := sqldb.ExecContext(ctx,
		`INSERT INTO widgets (id, label) VALUES (?, ?)`, id[:], "sprocket"); err != nil {
		log.Fatal(err)
	}
	var label string
	if err := sqldb.QueryRowContext(ctx,
		`SELECT label FROM widgets WHERE id = ?`, id[:]).Scan(&label); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("label via database/sql: %s\n", label)
	if err := sqldb.Close(); err != nil {
		log.Fatal(err)
	}

	// The underlying *DB is still open and usable.
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM widgets`).Scan(&n); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows after sql handle close: %d\n", n)
}
