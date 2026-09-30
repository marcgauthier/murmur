// Command subscriptions shows reactive queries: subscribe to a SELECT,
// write rows, and receive an event each time the result changes.
//
// Run it:
//
//	go run -tags "sqlite_preupdate_hook sqlite_fts5" ./examples/subscriptions
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	replicateddb "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "spedsql-subscriptions-*")
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
				Name: "tasks",
				Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "title", Type: schema.ColText, Nullable: true},
				},
			}},
		},
		Pebble: replicateddb.DefaultPebbleConfig(),
		Encryption: replicateddb.EncryptionConfig{
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "subscriptions-key",
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	sub, err := db.Subscribe(ctx, `SELECT title FROM tasks ORDER BY title`)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	for _, title := range []string{"first", "second"} {
		id := replicateddb.NewRowID()
		if _, err := db.ExecContext(ctx,
			`INSERT INTO tasks (id, title) VALUES (?, ?)`, id[:], title); err != nil {
			log.Fatal(err)
		}
	}

	// Events carry the full new result; wait for the two-row state.
	deadline := time.Now().Add(15 * time.Second)
	for {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			log.Fatal("timed out waiting for the two-row event")
		}
		select {
		case ev := <-sub.Events():
			fmt.Printf("event type=%s cursor=%d rows=%d\n", ev.Type, ev.Cursor, len(ev.Rows))
			if len(ev.Rows) == 2 {
				for _, row := range ev.Rows {
					fmt.Printf("  row: %v\n", row.Values)
				}
				fmt.Printf("subscription cursor now %d\n", sub.Cursor())
				return
			}
		case <-time.After(timeout):
			log.Fatal("timed out waiting for the two-row event")
		}
	}
}
