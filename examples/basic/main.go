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
	"github.com/marcgauthier/murmur/q"
)

type contact struct {
	ID     ids.RowID `rime:"ID"`
	Name   string    `rime:"primary"`
	Online bool
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-basic-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
		Path:   dir,
		NodeID: murmur.NewNodeID(),
		Schema: murmur.SchemaConfig{Version: 1},
		Models: []any{contact{}},
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

	contacts := []contact{{Name: "ann"}, {Name: "bob"}}
	if err := db.InsertMany(ctx, &contacts); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(ctx, &contacts[0], murmur.Set("Online", true)); err != nil {
		log.Fatal(err)
	}
	var rows []contact
	if err := db.Find(ctx, &rows, q.Eq("Online", true)); err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		fmt.Println(row.Name, row.ID)
	}

	st := db.Status()
	fmt.Printf("state=%s rows durable in %s\n", st.State, dir)
}
