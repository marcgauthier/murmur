package murmur

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/q"
)

func receiveItemEvent(t *testing.T, sub *ItemSubscription) ItemSubscriptionEvent {
	t.Helper()
	select {
	case event, ok := <-sub.Events():
		if !ok {
			t.Fatal("item subscription closed before delivering event")
		}
		return event
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for item subscription event")
		return ItemSubscriptionEvent{}
	}
}

func itemEventRows(t *testing.T, event ItemSubscriptionEvent) []*itemDevice {
	t.Helper()
	rows := make([]*itemDevice, len(event.Rows))
	for i, row := range event.Rows {
		device, ok := row.(*itemDevice)
		if !ok {
			t.Fatalf("event row %d = %T, want *itemDevice", i, row)
		}
		rows[i] = device
	}
	return rows
}

func TestItemSubscribeLive(t *testing.T) {
	db := openItemTestDB(t)
	seeded := seedItemDevices(t, db)
	ctx := context.Background()

	sub, err := db.Subscribe(ctx, itemDevice{}, ItemSubscriptionOptions{}, q.Eq("Site", "OTT"))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	initial := receiveItemEvent(t, sub)
	if initial.Type != EventInitial || len(initial.Rows) != 2 {
		t.Fatalf("initial event = %+v; want two OTT records", initial)
	}
	rows := itemEventRows(t, initial)
	rows[0].Hostname = "subscriber mutation"
	probe := itemDevice{ID: rows[0].ID}
	if err := db.GetItem(ctx, &probe); err != nil || probe.Hostname == "subscriber mutation" {
		t.Fatalf("subscription exposed published record ownership: %+v, %v", probe, err)
	}

	target := seeded[0].ID // edge-01, OTT.
	if err := db.Update(ctx, &itemDevice{ID: target}, Set("Status", 9)); err != nil {
		t.Fatal(err)
	}
	update := receiveItemEvent(t, sub)
	if update.Type != EventUpdate || len(update.Rows) != 2 {
		t.Fatalf("update event = %+v; want two records", update)
	}
	if len(update.Changes) != 1 || update.Changes[0].Type != ItemUpdated || update.Changes[0].Key != target {
		t.Fatalf("keyed diff = %+v; want one update for %s", update.Changes, target)
	}
	changed, ok := update.Changes[0].Row.(*itemDevice)
	if !ok || changed.Status != 9 {
		t.Fatalf("changed row = %+v; want status 9", update.Changes[0].Row)
	}
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}

	// Resuming from the initial cursor replays the latest snapshot.
	resumed, err := db.Subscribe(ctx, itemDevice{}, ItemSubscriptionOptions{ResumeFrom: initial.ResumeCursor}, q.Eq("Site", "OTT"))
	if err != nil {
		t.Fatalf("resume subscription: %v", err)
	}
	defer resumed.Close()
	replayed := receiveItemEvent(t, resumed)
	if replayed.Type != EventUpdate || replayed.Cursor != update.Cursor || len(replayed.Rows) != 2 {
		t.Fatalf("resumed event = %+v; want latest snapshot at cursor %d", replayed, update.Cursor)
	}

	future := update.ResumeCursor
	future.Sequence++
	if _, err := db.Subscribe(ctx, itemDevice{}, ItemSubscriptionOptions{ResumeFrom: future}); !errors.Is(err, ErrSubscriptionExpired) {
		t.Fatalf("future cursor error = %v; want ErrSubscriptionExpired", err)
	}
}

func TestItemSubscribeMembershipLive(t *testing.T) {
	db := openItemTestDB(t)
	seeded := seedItemDevices(t, db)
	ctx := context.Background()

	membership, err := db.Subscribe(ctx, itemDevice{}, ItemSubscriptionOptions{}, q.Eq("Hostname", "edge-01"))
	if err != nil {
		t.Fatal(err)
	}
	defer membership.Close()
	if initial := receiveItemEvent(t, membership); len(initial.Rows) != 1 {
		t.Fatalf("membership initial rows = %d, want 1", len(initial.Rows))
	}
	target := seeded[0].ID
	if err := db.Update(ctx, &itemDevice{ID: target}, Set("Hostname", "renamed")); err != nil {
		t.Fatal(err)
	}
	removed := receiveItemEvent(t, membership)
	if len(removed.Changes) != 1 || removed.Changes[0].Type != ItemRemoved || removed.Changes[0].Key != target || removed.Changes[0].Row != nil {
		t.Fatalf("membership removal = %+v", removed.Changes)
	}
	if err := db.Update(ctx, &itemDevice{ID: target}, Set("Hostname", "edge-01")); err != nil {
		t.Fatal(err)
	}
	added := receiveItemEvent(t, membership)
	if len(added.Changes) != 1 || added.Changes[0].Type != ItemAdded || added.Changes[0].Key != target || added.Changes[0].Row == nil {
		t.Fatalf("membership addition = %+v", added.Changes)
	}
}

func TestItemSubscribeModelsLive(t *testing.T) {
	db := openItemTestDB(t, itemCRDTDefinition(t))
	ctx := context.Background()

	id := ids.NewRowID()
	if err := db.InsertItem(ctx, &itemCRDT{ID: id}); err != nil {
		t.Fatalf("InsertItem: %v", err)
	}
	sub, err := db.Subscribe(ctx, itemCRDT{}, ItemSubscriptionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if initial := receiveItemEvent(t, sub); initial.Type != EventInitial || len(initial.Rows) != 1 {
		t.Fatalf("initial event = %+v; want one record", initial)
	}
	if err := db.CounterAdd(ctx, &itemCRDT{ID: id}, "Count", 4); err != nil {
		t.Fatal(err)
	}
	update := receiveItemEvent(t, sub)
	if update.Type != EventUpdate || len(update.Changes) != 1 || update.Changes[0].Type != ItemUpdated {
		t.Fatalf("update event = %+v; want one update", update)
	}
	changed, ok := update.Changes[0].Row.(*itemCRDT)
	if !ok || changed.Count != 4 {
		t.Fatalf("changed row = %+v; want count 4", update.Changes[0].Row)
	}
}

func TestItemSubscribeRuntimeModelsLive(t *testing.T) {
	cfg := modelTestConfig(t, modelDevice{})
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx := context.Background()

	device := modelDevice{Name: "router-01", Site: "OTT", Status: 3}
	if err := db.InsertItem(ctx, &device); err != nil {
		t.Fatalf("InsertItem: %v", err)
	}
	sub, err := db.Subscribe(ctx, modelDevice{}, ItemSubscriptionOptions{}, q.Eq("Site", "OTT"))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if initial := receiveItemEvent(t, sub); initial.Type != EventInitial || len(initial.Rows) != 1 {
		t.Fatalf("initial event = %+v; want one record", initial)
	}
	if err := db.Update(ctx, &modelDevice{Name: "router-01"}, Set("Status", 4)); err != nil {
		t.Fatal(err)
	}
	update := receiveItemEvent(t, sub)
	if update.Type != EventUpdate || len(update.Changes) != 1 || update.Changes[0].Type != ItemUpdated {
		t.Fatalf("update event = %+v; want one update", update)
	}
	changed, ok := update.Changes[0].Row.(*modelDevice)
	if !ok || changed.Status != 4 {
		t.Fatalf("changed row = %+v; want status 4", update.Changes[0].Row)
	}
}

func TestItemSubscribeErrorsLive(t *testing.T) {
	db := openItemTestDB(t)
	seedItemDevices(t, db)
	ctx := context.Background()

	if _, err := db.Subscribe(ctx, itemDevice{}, ItemSubscriptionOptions{}, q.Eq("Nope", 1)); err == nil || !strings.Contains(err.Error(), `unknown field "Nope"`) {
		t.Fatalf("unknown field = %v, want field error", err)
	}
	if _, err := db.Subscribe(ctx, itemDevice{}, ItemSubscriptionOptions{}, q.Eq("Site", q.Param())); err == nil || !strings.Contains(err.Error(), "parameters") {
		t.Fatalf("placeholder filter = %v, want params error", err)
	}
	if _, err := db.Subscribe(ctx, 42, ItemSubscriptionOptions{}); err == nil {
		t.Fatal("unregistered model succeeded")
	}
	if _, err := db.Subscribe(ctx, itemDevice{}, ItemSubscriptionOptions{BufferSize: -1}); err == nil || !strings.Contains(err.Error(), "buffer size") {
		t.Fatalf("negative buffer = %v, want bounds error", err)
	}
	var nilDB *DB
	if _, err := nilDB.Subscribe(ctx, itemDevice{}, ItemSubscriptionOptions{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil DB = %v, want ErrClosed", err)
	}
	var nilSub *ItemSubscription
	if nilSub.Events() != nil {
		t.Fatal("nil Events is not nil")
	}
	if nilSub.Cursor() != 0 {
		t.Fatal("nil Cursor is not zero")
	}
	if err := nilSub.Close(); err != nil {
		t.Fatalf("nil Close = %v", err)
	}
}
