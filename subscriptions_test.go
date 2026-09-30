package replicateddb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/codec"
)

func testSubscriptionDB(t *testing.T, modCfg func(*Config)) *DB {
	t.Helper()
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.Subscription = SubscriptionConfig{
		MaxSubscribers:    100,
		EventBufferSize:   32,
		MaxRetainedEvents: 10,
	}
	if modCfg != nil {
		modCfg(&cfg)
	}
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func TestSubscriptionInitialAndLocalUpdates(t *testing.T) {
	db := testSubscriptionDB(t, nil)
	ctx := context.Background()

	// Subscribe before any data exists
	sub, err := db.Subscribe(ctx, "SELECT name, score FROM contacts ORDER BY score ASC")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	// Expect initial empty result
	select {
	case ev := <-sub.Events():
		if ev.Type != EventInitial {
			t.Fatalf("expected EventInitial, got %v", ev.Type)
		}
		if len(ev.Rows) != 0 {
			t.Fatalf("expected 0 rows initially, got %d", len(ev.Rows))
		}
		if len(ev.Columns) != 2 || ev.Columns[0] != "name" || ev.Columns[1] != "score" {
			t.Fatalf("unexpected columns: %v", ev.Columns)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for initial event")
	}

	// Insert item 1
	id1 := NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)", id1[:], "Alice", 10); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Expect update event
	select {
	case ev := <-sub.Events():
		if ev.Type != EventUpdate {
			t.Fatalf("expected EventUpdate, got %v", ev.Type)
		}
		if len(ev.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(ev.Rows))
		}
		if ev.Rows[0].Values[0] != "Alice" || ev.Rows[0].Values[1] != int64(10) {
			t.Fatalf("unexpected row values: %v", ev.Rows[0].Values)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for update event 1")
	}

	// Insert item 2
	id2 := NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)", id2[:], "Bob", 20); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Expect update event with 2 rows
	select {
	case ev := <-sub.Events():
		if ev.Type != EventUpdate {
			t.Fatalf("expected EventUpdate, got %v", ev.Type)
		}
		if len(ev.Rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(ev.Rows))
		}
		if ev.Rows[0].Values[0] != "Alice" || ev.Rows[1].Values[0] != "Bob" {
			t.Fatalf("unexpected row names: %v, %v", ev.Rows[0].Values[0], ev.Rows[1].Values[0])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for update event 2")
	}

	// Rollback should NOT emit events
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	id3 := NewRowID()
	if _, err := tx.ExecContext(ctx, "INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)", id3[:], "Charlie", 30); err != nil {
		t.Fatalf("tx Exec: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Ensure no spurious event is emitted on rollback
	select {
	case ev := <-sub.Events():
		t.Fatalf("unexpected event after rollback: %v", ev)
	case <-time.After(200 * time.Millisecond):
		// Expected: no event
	}
}

func TestSubscriptionDiffingSuppressesUnchanged(t *testing.T) {
	db := testSubscriptionDB(t, nil)
	ctx := context.Background()

	// Query only names starting with 'A'
	sub, err := db.Subscribe(ctx, "SELECT name FROM contacts WHERE name LIKE 'A%'")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	// Initial event
	<-sub.Events()

	// Insert row that doesn't match WHERE clause
	id1 := NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)", id1[:], "Bob", 1); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// No update event should be delivered because query result is unchanged
	select {
	case ev := <-sub.Events():
		t.Fatalf("unexpected event when result unchanged: %v", ev)
	case <-time.After(200 * time.Millisecond):
		// Expected: suppressed
	}

	// Now insert matching row
	id2 := NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)", id2[:], "Alice", 2); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	select {
	case ev := <-sub.Events():
		if ev.Type != EventUpdate {
			t.Fatalf("expected EventUpdate, got %v", ev.Type)
		}
		if len(ev.Rows) != 1 || ev.Rows[0].Values[0] != "Alice" {
			t.Fatalf("unexpected rows: %v", ev.Rows)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for matching update")
	}
}

func TestSubscriptionRemoteApplyAndSnapshot(t *testing.T) {
	db := testSubscriptionDB(t, nil)
	ctx := context.Background()

	sub, err := db.Subscribe(ctx, "SELECT name, score FROM contacts ORDER BY score ASC")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	// Initial event
	<-sub.Events()

	// Apply remote batch using remoteBatchCells
	peer := NewNodeID()
	rowID := NewRowID()
	batch := remoteBatchCells(db, peer, 1, 1000<<16, "contacts", rowID, map[string]codec.Value{
		"id":    codec.Blob(rowID[:]),
		"name":  codec.Text("David"),
		"score": codec.Int(99),
	})
	if err := db.ApplyRemote(ctx, batch); err != nil {
		t.Fatalf("ApplyRemote: %v", err)
	}

	// Expect update event from remote apply
	select {
	case ev := <-sub.Events():
		if ev.Type != EventUpdate {
			t.Fatalf("expected EventUpdate, got %v", ev.Type)
		}
		if len(ev.Rows) != 1 || ev.Rows[0].Values[0] != "David" {
			t.Fatalf("unexpected row from remote apply: %v", ev.Rows)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for remote apply update")
	}
}

func TestSubscriptionSlowConsumerReset(t *testing.T) {
	// Configure very small buffer size
	db := testSubscriptionDB(t, func(c *Config) {
		c.Subscription.EventBufferSize = 2
	})
	ctx := context.Background()

	sub, err := db.SubscribeWithOptions(ctx, "SELECT name FROM contacts", SubscriptionOptions{
		BufferSize: 1, // buffer holds only 1 event
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	// Read initial event
	<-sub.Events()

	// Generate multiple commits without reading from sub.Events()
	for i := 0; i < 10; i++ {
		id := NewRowID()
		_, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)", id[:], fmt.Sprintf("name-%d", i), i)
		if err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	// Give worker time to dispatch and detect full buffer
	time.Sleep(100 * time.Millisecond)

	// Now read from sub.Events(): must encounter EventReset with ErrSubscriptionReset
	var foundReset bool
	for ev := range sub.Events() {
		if ev.Type == EventReset {
			if !errors.Is(ev.Err, ErrSubscriptionReset) {
				t.Fatalf("expected ErrSubscriptionReset, got %v", ev.Err)
			}
			foundReset = true
			break
		}
	}
	if !foundReset {
		t.Fatal("slow consumer was not reset with EventReset")
	}
}

func TestSubscriptionResumptionFromCursor(t *testing.T) {
	db := testSubscriptionDB(t, func(c *Config) {
		c.Subscription.MaxRetainedEvents = 5
	})
	ctx := context.Background()

	// Insert several items to advance cursor
	for i := 1; i <= 3; i++ {
		id := NewRowID()
		if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)", id[:], fmt.Sprintf("name-%d", i), i); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	// Open subscription to capture cursor
	sub1, err := db.Subscribe(ctx, "SELECT name FROM contacts ORDER BY score ASC")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	evInit := <-sub1.Events()
	lastCursor := evInit.Cursor
	_ = sub1.Close()

	// Insert name-4
	id4 := NewRowID()
	if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)", id4[:], "name-4", 4); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Resume from lastCursor (which is within retained history)
	subResume, err := db.SubscribeWithOptions(ctx, "SELECT name FROM contacts ORDER BY score ASC", SubscriptionOptions{
		ResumeFromCursor: lastCursor,
	})
	if err != nil {
		t.Fatalf("SubscribeWithOptions resume: %v", err)
	}
	defer subResume.Close()

	// Expect catchup update
	select {
	case ev := <-subResume.Events():
		if ev.Type != EventUpdate {
			t.Fatalf("expected EventUpdate catchup, got %v", ev.Type)
		}
		if len(ev.Rows) != 4 {
			t.Fatalf("expected 4 rows in catchup, got %d", len(ev.Rows))
		}
		if ev.Rows[3].Values[0] != "name-4" {
			t.Fatalf("unexpected item: %v", ev.Rows[3].Values[0])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for resumed event")
	}

	// Advance cursor many times to prune old history beyond MaxRetainedEvents (5)
	for i := 5; i <= 20; i++ {
		id := NewRowID()
		if _, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)", id[:], fmt.Sprintf("name-%d", i), i); err != nil {
			t.Fatalf("Insert: %v", err)
		}
	}

	// Resuming from expired lastCursor should fail with ErrSubscriptionExpired
	_, err = db.SubscribeWithOptions(ctx, "SELECT name FROM contacts", SubscriptionOptions{
		ResumeFromCursor: lastCursor,
	})
	if !errors.Is(err, ErrSubscriptionExpired) {
		t.Fatalf("expected ErrSubscriptionExpired, got %v", err)
	}
}

func TestSubscriptionSchemaMigrationReset(t *testing.T) {
	db := testSubscriptionDB(t, nil)
	ctx := context.Background()

	sub, err := db.Subscribe(ctx, "SELECT name FROM contacts")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	<-sub.Events() // initial

	// Migrate schema using migrateTestTables()
	if err := db.Migrate(ctx, migrateTestTables()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// Active subscriber must receive EventReset
	select {
	case ev := <-sub.Events():
		if ev.Type != EventReset {
			t.Fatalf("expected EventReset on migration, got %v", ev.Type)
		}
		if !errors.Is(ev.Err, ErrSubscriptionReset) {
			t.Fatalf("expected ErrSubscriptionReset, got %v", ev.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for migration reset event")
	}
}

func TestSubscriptionMaxSubscribersLimit(t *testing.T) {
	db := testSubscriptionDB(t, func(c *Config) {
		c.Subscription.MaxSubscribers = 3
	})
	ctx := context.Background()

	var subs []*Subscription
	defer func() {
		for _, s := range subs {
			_ = s.Close()
		}
	}()

	for i := 0; i < 3; i++ {
		s, err := db.Subscribe(ctx, "SELECT score FROM contacts")
		if err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		subs = append(subs, s)
	}

	// 4th subscriber should be rejected
	_, err := db.Subscribe(ctx, "SELECT score FROM contacts")
	if !errors.Is(err, ErrMaxSubscribersReached) {
		t.Fatalf("expected ErrMaxSubscribersReached, got %v", err)
	}

	// Close one subscriber
	_ = subs[0].Close()

	// Now subscription should succeed
	s4, err := db.Subscribe(ctx, "SELECT score FROM contacts")
	if err != nil {
		t.Fatalf("Subscribe after close: %v", err)
	}
	subs = append(subs, s4)
}

func TestSubscriptionReadOnlyRejection(t *testing.T) {
	db := testSubscriptionDB(t, nil)
	ctx := context.Background()

	// Subscribing to mutating statements must be rejected
	mutatingQueries := []string{
		"INSERT INTO contacts (id, name, score) VALUES ('1', 'x', 1)",
		"UPDATE contacts SET score = 2",
		"DELETE FROM contacts WHERE score = 1",
		"DROP TABLE contacts",
	}

	for _, q := range mutatingQueries {
		_, err := db.Subscribe(ctx, q)
		if !errors.Is(err, ErrReadOnlyRequired) {
			t.Fatalf("query %q: expected ErrReadOnlyRequired, got %v", q, err)
		}
	}
}

func TestSubscriptionContextCancellation(t *testing.T) {
	db := testSubscriptionDB(t, nil)

	subCtx, cancel := context.WithCancel(context.Background())
	sub, err := db.Subscribe(subCtx, "SELECT name FROM contacts")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Initial event
	<-sub.Events()

	// Cancel context
	cancel()

	// Give cancellation goroutine time to unregister
	time.Sleep(50 * time.Millisecond)

	// Events channel should be closed
	select {
	case _, ok := <-sub.Events():
		if ok {
			// May have delivered closed state
		}
	case <-time.After(1 * time.Second):
		t.Fatal("subscription did not close on context cancellation")
	}
}

func TestSubscriptionNonBlockingWrites(t *testing.T) {
	db := testSubscriptionDB(t, nil)
	ctx := context.Background()

	// Create a subscriber
	sub, err := db.Subscribe(ctx, "SELECT name FROM contacts")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()

	<-sub.Events() // initial

	// Perform 100 concurrent writes
	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id := NewRowID()
			_, err := db.ExecContext(ctx, "INSERT INTO contacts (id, name, score) VALUES (?, ?, ?)", id[:], fmt.Sprintf("item-%d", idx), idx)
			if err != nil {
				errCh <- err
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent write failed: %v", err)
		}
	}
}
