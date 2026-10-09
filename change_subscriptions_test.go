package murmur

import (
	"context"
	"testing"
	"time"
)

func TestChangeSubscriptionSignalsTypedCommit(t *testing.T) {
	db, err := Open(context.Background(), testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sub, err := db.SubscribeChanges(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	table, err := TableOf[testContactRecord](db, "contacts")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTxContext(context.Background(), func(tx *Tx) error {
		return table.Insert(tx, &testContactRecord{ID: NewRowID(), Name: "notify"})
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case cursor, ok := <-sub.Events():
		if !ok || cursor == 0 {
			t.Fatalf("invalid change notification: cursor=%d open=%t", cursor, ok)
		}
	case <-time.After(time.Second):
		t.Fatal("commit notification timed out")
	}
}

func TestChangeSubscriptionClosesWithContext(t *testing.T) {
	db, err := Open(context.Background(), testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	sub, err := db.SubscribeChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Fatal("expected context cancellation to close the subscription")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription did not close after context cancellation")
	}
}
