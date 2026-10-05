// Command basic opens a single embedded Murmur-SQL node, inserts two rows,
// and queries them back. No networking, no encryption options beyond the
// mandatory key: the smallest possible program.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/basic
package main

import (
	"context"
	"fmt"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
	"log"
	"os"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
)

func main() {
	ctx := context.Background()

	// Every node needs a directory for its durable Pebble store.
	dir, err := os.MkdirTemp("", "murmur-basic-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
		Path:   dir,
		NodeID: murmur.NewNodeID(),
		Schema: murmur.SchemaConfig{
			Version: 1,
			Tables: []schema.TableSchema{{
				Name: "contacts",
				Columns: []schema.ColumnSchema{
					// Every replicated table needs an explicit BLOB(16)
					// primary key supplied by the application.
					{Name: "id", Type: schema.ColBlob},
					{Name: "name", Type: schema.ColText, Nullable: true},
				},
			}},
		},
		Pebble: murmur.DefaultPebbleConfig(),
		Encryption: murmur.EncryptionConfig{
			// Demo key. Production deployments load key material
			// from a file, environment, or KMS via a KeyProvider.
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "basic-key",
		},
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for _, name := range []string{"ann", "bob"} {
		id := murmur.NewRowID()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO contacts (id, name) VALUES (?, ?)`,
			id[:], name); err != nil {
			log.Fatal(err)
		}
	}

	rows, err := db.QueryContext(ctx, `SELECT name FROM contacts ORDER BY name`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Fatal(err)
		}
		fmt.Println(name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	st := db.Status()
	fmt.Printf("state=%s rows durable in %s\n", st.State, dir)
}
