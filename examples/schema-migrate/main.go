// Command schema-migrate demonstrates additive typed schema evolution: it
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
	definition, err := murmur.Define[contactV1]("contacts", 1, murmur.RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2},
	})
	if err != nil {
		log.Fatal(err)
	}
	return definition
}

func v2Definition() murmur.TableDefinition {
	definition, err := murmur.Define[contactV2]("contacts", 1, murmur.RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2, "Phone": 3},
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

	oldTable, err := murmur.TableOf[contactV1](db, "contacts")
	if err != nil {
		log.Fatal(err)
	}
	id := murmur.NewRowID()
	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		return oldTable.Insert(tx, &contactV1{ID: id, Name: "ann"})
	}); err != nil {
		log.Fatal(err)
	}

	// Additive evolution preserves old data and assigns a stable ID to Phone.
	if err := db.MigrateRecords(ctx, []murmur.TableDefinition{v2Definition()}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("migrated to v2 (added optional phone field)")

	contacts, err := murmur.TableOf[contactV2](db, "contacts")
	if err != nil {
		log.Fatal(err)
	}
	old, err := contacts.Get(id)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("old row: name=%s phone=%v\n", old.Name, old.Phone)

	phone := "613-555-0100"
	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		return contacts.Insert(tx, &contactV2{ID: murmur.NewRowID(), Name: "bob", Phone: &phone})
	}); err != nil {
		log.Fatal(err)
	}
	rows, err := contacts.Where(murmur.FieldOf[contactV2, string](contacts, "Name").Eq("bob")).Find()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("new row phone: %s\n", *rows[0].Phone)
}
