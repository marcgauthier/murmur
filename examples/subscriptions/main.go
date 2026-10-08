// Command subscriptions shows a typed query subscription receiving an initial
// snapshot and an update after a durable record batch.
//
// Run it with:
//
//	go run ./examples/subscriptions
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/examples/internal/demoidentity"
	"github.com/marcgauthier/murmur/ids"
)

type task struct {
	ID    ids.RowID `rime:"primary"`
	Title string
}

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "murmur-subscriptions-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	definition, err := murmur.Define[task]("tasks", 19, murmur.RecordOptions{
		PrimaryField: "ID", FieldIDs: map[string]uint32{"ID": 1, "Title": 2},
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
			Key: []byte("0123456789abcdef0123456789abcdef"), KeyID: "subscriptions-key",
		},
	}))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	tasks, err := murmur.TableOf[task](db, "tasks")
	if err != nil {
		log.Fatal(err)
	}
	sub, err := tasks.Subscribe(ctx, murmur.RecordSubscriptionOptions{})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()
	initial := receive(sub, 15*time.Second)
	fmt.Printf("event type=%s cursor=%d rows=%d\n", initial.Type, initial.Cursor, len(initial.Rows))

	if err := db.WriteTxContext(ctx, func(tx *murmur.Tx) error {
		return tasks.InsertMany(tx, []*task{
			{ID: murmur.NewRowID(), Title: "first"},
			{ID: murmur.NewRowID(), Title: "second"},
		})
	}); err != nil {
		log.Fatal(err)
	}
	deadline := time.Now().Add(15 * time.Second)
	for {
		update := receive(sub, time.Until(deadline))
		fmt.Printf("event type=%s cursor=%d rows=%d\n", update.Type, update.Cursor, len(update.Rows))
		if len(update.Rows) == 2 {
			for _, row := range update.Rows {
				fmt.Printf("  row: %s\n", row.Title)
			}
			fmt.Printf("subscription cursor now %d\n", sub.Cursor())
			return
		}
		if time.Now().After(deadline) {
			log.Fatal("timed out waiting for the two-row event")
		}
	}
}

func receive(sub *murmur.RecordSubscription[task], timeout time.Duration) murmur.RecordSubscriptionEvent[task] {
	if timeout <= 0 {
		log.Fatal("timed out waiting for subscription event")
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case event, ok := <-sub.Events():
		if !ok {
			log.Fatal("subscription closed before delivering an event")
		}
		if event.Err != nil {
			log.Fatal(event.Err)
		}
		return event
	case <-timer.C:
		log.Fatal("timed out waiting for subscription event")
	}
	return murmur.RecordSubscriptionEvent[task]{}
}
