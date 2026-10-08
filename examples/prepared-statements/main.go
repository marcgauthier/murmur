// Command prepared-statements shows reusable compiled typed queries with
// positional parameters.
//
// Run it with:
//
//	go run ./examples/prepared-statements
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/rime"
)

type sensor struct {
	ID    ids.RowID `rime:"primary"`
	Name  string
	Value int
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-prepared-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	definition, err := murmur.Define[sensor]("sensors", 20, murmur.RecordOptions{
		PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Name": 2, "Value": 3},
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
			Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "prepared-key",
		},
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sensors, err := murmur.TableOf[sensor](db, "sensors")
	if err != nil {
		log.Fatal(err)
	}
	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		rows := make([]*sensor, 50)
		for i := range rows {
			rows[i] = &sensor{ID: murmur.NewRowID(), Name: fmt.Sprintf("sensor-%d", i%5), Value: i}
		}
		return sensors.InsertMany(tx, rows)
	}); err != nil {
		log.Fatal(err)
	}

	// Compile once, then bind a different value at each execution.
	byName := sensors.Compile(murmur.FieldOf[sensor, string](sensors, "Name").Eq(rime.Param[string]()))
	n, err := byName.Count("sensor-2")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows for sensor-2: %d\n", n)
	if n != 10 {
		log.Fatalf("want 10 rows, got %d", n)
	}
}
