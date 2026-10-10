// Command schema-migrate demonstrates additive schema evolution: it
// opens v1, writes a record, migrates to v2, and reads/writes the new field.
//
// Run it with:
//
//	go run ./examples/schema-migrate
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/q"
)

type contactV1 struct {
	ID   ids.RowID `rime:"primary"`
	Name string
}

type contactV2 struct {
	ID    ids.RowID `rime:"primary"`
	Name  string
	Phone *string
}

func v1Definition() murmur.TableDefinition {
	definition, err := murmur.Model[contactV1](murmur.ModelOptions{
		Name: "contacts", TableID: 1,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Name": 2},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	return definition
}

func v2Definition() murmur.TableDefinition {
	definition, err := murmur.Model[contactV2](murmur.ModelOptions{
		Name: "contacts", TableID: 1,
		RecordOptions: murmur.RecordOptions{
			FieldIDs: map[string]uint32{"ID": 1, "Name": 2, "Phone": 3},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	return definition
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-migrate-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
		Path:   dir,
		NodeID: murmur.NewNodeID(),
		Schema: murmur.SchemaConfig{Version: 1},
		Tables: []murmur.TableDefinition{v1Definition()},
		Spool:  murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{
			Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "migrate-key",
		},
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	id := murmur.NewRowID()
	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		return tx.InsertItem(&contactV1{ID: id, Name: "ann"})
	}); err != nil {
		log.Fatal(err)
	}

	// Additive evolution preserves old data and assigns a stable ID to Phone.
	if err := db.MigrateModels(ctx, []any{v2Definition()}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("migrated to v2 (added optional phone field)")

	var old contactV2
	old.ID = id
	if err := db.GetItem(ctx, &old); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("old row: name=%s phone=%v\n", old.Name, old.Phone)

	phone := "613-555-0100"
	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		return tx.InsertItem(&contactV2{ID: murmur.NewRowID(), Name: "bob", Phone: &phone})
	}); err != nil {
		log.Fatal(err)
	}
	var rows []contactV2
	if err := db.Find(ctx, &rows, q.Eq("Name", "bob")); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("new row phone: %s\n", *rows[0].Phone)
}
