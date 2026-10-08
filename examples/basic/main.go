// Command basic opens a single embedded Murmur node, writes two native Go
// records, and queries them back through the managed RIME facade.
//
// Run it with:
//
//	go run ./examples/basic
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

type contact struct {
	ID   ids.RowID `rime:"primary"`
	Name string
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-basic-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	definition, err := murmur.Define[contact]("contacts", 1, murmur.RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Name": 2},
	})
	if err != nil {
		log.Fatal(err)
	}
	db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
		Path:   dir,
		NodeID: murmur.NewNodeID(),
		Schema: murmur.SchemaConfig{Version: 1},
		Tables: []murmur.TableDefinition{definition},
		Spool:  murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{
			// Demo key. Production applications load key material from a
			// file, environment, or KMS through a KeyProvider.
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "basic-key",
		},
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	contacts, err := murmur.TableOf[contact](db, "contacts")
	if err != nil {
		log.Fatal(err)
	}
	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		for _, name := range []string{"ann", "bob"} {
			if err := contacts.Insert(tx, &contact{ID: murmur.NewRowID(), Name: name}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	rows, err := contacts.Where().OrderByAsc(murmur.FieldOf[contact, string](contacts, "Name")).Find()
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		fmt.Println(row.Name)
	}

	st := db.Status()
	fmt.Printf("state=%s rows durable in %s\n", st.State, dir)
}
