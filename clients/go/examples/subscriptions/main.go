package main

import (
	"context"
	"encoding/binary"
	"errors"
	murmur "github.com/marcgauthier/murmur/clients/go"
	"github.com/marcgauthier/murmur/clients/go/examples/internal/demo"
)

type Document struct {
	ID    int64
	Title string
}

func run(ctx context.Context, e *demo.Env) error {
	db, err := e.Open("db", demo.Options())
	if err != nil {
		return err
	}
	if err = db.Migrate(murmur.NewMigration("CREATE TABLE docs(id INTEGER PRIMARY KEY,title TEXT)")); err != nil {
		return err
	}
	sub, err := db.SubscribeRows("SELECT id,title FROM docs ORDER BY id", nil, []int{0}, murmur.DefaultSubscriptionOptions())
	if err != nil {
		return err
	}
	e.Defer(sub.Close)
	initial, err := sub.RecvContext(ctx)
	if err != nil {
		return err
	}
	if err = demo.Require(initial.Kind == "Initial" && len(initial.Rows) == 0, "initial snapshot mismatch"); err != nil {
		return err
	}
	codec := murmur.JSONRowCodec[Document]()
	typed, err := murmur.Subscribe(db, func(s *murmur.Sql) ([]Document, error) {
		rows, err := s.Query("SELECT id,title FROM docs ORDER BY id")
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var docs []Document
		for rows.Next() {
			var d Document
			if err := rows.Scan(&d.ID, &d.Title); err != nil {
				return nil, err
			}
			docs = append(docs, d)
		}
		return docs, rows.Err()
	}, func(d Document) []byte {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(d.ID))
		return b
	}, murmur.DefaultSubscriptionOptions(), codec)
	if err != nil {
		return err
	}
	e.Defer(typed.Close)
	if _, err = typed.RecvContext(ctx); err != nil {
		return err
	}
	if _, err = db.Exec("INSERT INTO docs VALUES(1,'reactive')"); err != nil {
		return err
	}
	if err = db.Flush(); err != nil {
		return err
	}
	event, err := sub.RecvContext(ctx)
	if err != nil {
		return err
	}
	typedEvent, err := typed.RecvContext(ctx)
	if err != nil {
		return err
	}
	if err = demo.Require(event.Kind == "Update" && len(event.Changes) == 1 && event.Changes[0].ChangeType == murmur.Added && len(typedEvent.Rows) == 1 && typedEvent.Rows[0].Title == "reactive", "subscription update mismatch"); err != nil {
		return err
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = sub.RecvContext(cancelled); !errors.Is(err, context.Canceled) {
		return demo.Require(false, "receive cancellation: %v", err)
	}
	opts := murmur.DefaultSubscriptionOptions()
	opts.ResumeFrom = &event.Cursor
	resumed, err := db.SubscribeRows("SELECT id,title FROM docs", nil, []int{0}, opts)
	if err != nil {
		return err
	}
	e.Defer(resumed.Close)
	current, err := resumed.RecvContext(ctx)
	if err != nil {
		return err
	}
	if err = demo.Require(current.Kind == "Update" && len(current.Rows) == 1, "valid resume did not return current snapshot"); err != nil {
		return err
	}
	expired := event.Cursor
	expired.Epoch++
	opts.ResumeFrom = &expired
	invalid, err := db.SubscribeRows("SELECT id FROM docs", nil, []int{0}, opts)
	if invalid != nil {
		invalid.Close()
	}
	if !errors.Is(err, murmur.ErrInvalid) {
		return demo.Require(false, "expired cursor: %v", err)
	}
	// Leave the initial event queued in a one-event buffer. The next publication
	// overflows it, yielding Reset instead of silently dropping a row update.
	opts = murmur.DefaultSubscriptionOptions()
	opts.BufferSize = 1
	slow, err := db.SubscribeRows("SELECT id,title FROM docs", nil, []int{0}, opts)
	if err != nil {
		return err
	}
	e.Defer(slow.Close)
	if _, err = db.Exec("UPDATE docs SET title='overflow' WHERE id=1"); err != nil {
		return err
	}
	if err = db.Flush(); err != nil {
		return err
	}
	// Wait for another subscriber to observe the publication before inspecting
	// the slow subscriber; scheduling can still differ, so migration provides a
	// deterministic reset if the overflow publication has not arrived yet.
	if _, err = typed.RecvContext(ctx); err != nil {
		return err
	}
	if err = db.Migrate(murmur.NewMigration("CREATE INDEX docs_title ON docs(title)")); err != nil {
		return err
	}
	reset, err := slow.RecvContext(ctx)
	if err != nil {
		return err
	}
	if err = demo.Require(reset.Kind == "Reset", "expected reset, got %s", reset.Kind); err != nil {
		return err
	}
	if err = slow.Close(); err != nil {
		return err
	}
	fresh, err := db.SubscribeRows("SELECT id,title FROM docs", nil, []int{0}, murmur.DefaultSubscriptionOptions())
	if err != nil {
		return err
	}
	e.Defer(fresh.Close)
	snapshot, err := fresh.RecvContext(ctx)
	if err != nil {
		return err
	}
	e.Print("Typed/SQL updates, cancellation, valid/expired cursors, and reset recovery verified (%s)", reset.Reason)
	return demo.Require(snapshot.Kind == "Initial" && len(snapshot.Rows) == 1 && snapshot.Rows[0][1].Text == "overflow", "fresh snapshot mismatch")
}
func main() { demo.Main("subscriptions", run) }
