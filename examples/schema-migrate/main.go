// Command schema-migrate shows additive schema evolution: open on v1,
// write rows, migrate to v2 with a new column, and prove old rows are
// intact while new writes can use the new column.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/schema-migrate
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	replicateddb "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
)

func v1Tables() []schema.TableSchema {
	return []schema.TableSchema{{
		Name: "contacts",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}}
}

func v2Tables() []schema.TableSchema {
	return []schema.TableSchema{{
		Name: "contacts",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
			{Name: "phone", Type: schema.ColText, Nullable: true},
		},
	}}
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "spedsql-migrate-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := replicateddb.Open(ctx, replicateddb.Config{
		Path:   dir,
		NodeID: replicateddb.NewNodeID(),
		Schema: replicateddb.SchemaConfig{Version: 1, Tables: v1Tables()},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "migrate-key",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	id := replicateddb.NewRowID()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "ann"); err != nil {
		log.Fatal(err)
	}

	// Additive evolution only: new columns are appended, never renamed
	// or removed.
	if err := db.Migrate(ctx, v2Tables()); err != nil {
		log.Fatal(err)
	}
	fmt.Println("migrated to v2 (added phone column)")

	// Old rows survive with NULL in the new column; new writes use it.
	var name string
	var phone *string
	if err := db.QueryRowContext(ctx,
		`SELECT name, phone FROM contacts WHERE id = ?`, id[:]).Scan(&name, &phone); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("old row: name=%s phone=%v\n", name, phone)

	id2 := replicateddb.NewRowID()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO contacts (id, name, phone) VALUES (?, ?, ?)`,
		id2[:], "bob", "613-555-0100"); err != nil {
		log.Fatal(err)
	}
	var got string
	if err := db.QueryRowContext(ctx,
		`SELECT phone FROM contacts WHERE id = ?`, id2[:]).Scan(&got); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("new row phone: %s\n", got)
}
