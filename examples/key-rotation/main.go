// Command key-rotation shows data-key rotation on a live typed database.
//
// Run it with:
//
//	go run ./examples/key-rotation
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

type secret struct {
	ID   ids.RowID `rime:"primary"`
	Body string
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-rotation-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	definition, err := murmur.Define[secret]("secrets", 3, murmur.RecordOptions{
		PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Body": 2},
	})
	if err != nil {
		log.Fatal(err)
	}
	db, err := murmur.Open(ctx, demoidentity.Configure(murmur.Config{
		Path: dir, NodeID: murmur.NewNodeID(), Schema: murmur.SchemaConfig{Version: 1},
		Tables: []murmur.TableDefinition{definition}, Spool: murmur.DefaultSpoolConfig(),
		Encryption: murmur.EncryptionConfig{Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "rotation-key"},
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	secrets, err := murmur.TableOf[secret](db, "secrets")
	if err != nil {
		log.Fatal(err)
	}
	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		rows := make([]*secret, 3)
		for i := range rows {
			rows[i] = &secret{ID: murmur.NewRowID(), Body: fmt.Sprintf("secret-%d", i)}
		}
		return secrets.InsertMany(tx, rows)
	}); err != nil {
		log.Fatal(err)
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
	n, err := secrets.Where().Count()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows readable after rotation: %d\n", n)
	if n != 3 {
		log.Fatalf("want 3 rows after rotation, got %d", n)
	}
}
