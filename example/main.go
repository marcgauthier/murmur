// Command example runs a single embedded node: write rows, query them back,
// and print the status. It uses a temporary directory and no networking.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	replicateddb "github.com/nomadsql/replicateddb"
	"github.com/nomadsql/replicateddb/schema"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "replicateddb-example-*")
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
				Name: "contacts",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "name", Type: schema.ColText, Nullable: true},
					{Name: "phone", Type: schema.ColText, Nullable: true},
				},
			}},
		},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			// Demo key. Production deployments load key material from a
			// file, environment, or KMS via crypto providers.
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "example-key",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for _, c := range [][2]string{{"ann", "111"}, {"bob", "222"}} {
		id := replicateddb.NewRowID()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
			id[:], c[0], c[1]); err != nil {
			log.Fatal(err)
		}
	}

	rows, err := db.QueryContext(ctx, `SELECT name, phone FROM contacts ORDER BY name`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, phone string
		if err := rows.Scan(&name, &phone); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s: %s\n", name, phone)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	st := db.Status()
	fmt.Printf("state=%s node=%s gen=%d hlc=%d\n",
		st.State, st.NodeID, st.StateGeneration, st.HLC)
}
