package murmur

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/q"
	"github.com/marcgauthier/murmur/rime"
)

// itemDevice exercises the generic item API across string, int, and bool
// fields with secondary indexes for the query bridge.
type itemDevice struct {
	ID       ids.RowID `rime:"primary"`
	Hostname string    `rime:"index"`
	Site     string    `rime:"index"`
	Status   int       `rime:"ordered"`
	Online   bool
}

func itemDeviceDefinition(t testing.TB) TableDefinition {
	t.Helper()
	definition, err := define[itemDevice]("devices", 80, RecordOptions{
		PrimaryField: "ID",
		FieldIDs: map[string]uint32{
			"ID": 1, "Hostname": 2, "Site": 3, "Status": 4, "Online": 5,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func openItemTestDB(t testing.TB, definitions ...TableDefinition) *DB {
	t.Helper()
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	if len(definitions) == 0 {
		definitions = []TableDefinition{itemDeviceDefinition(t)}
	}
	cfg.Tables = definitions
	db, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func seedItemDevices(t testing.TB, db *DB) []itemDevice {
	t.Helper()
	ctx := context.Background()
	devices := []itemDevice{
		{ID: ids.NewRowID(), Hostname: "edge-01", Site: "OTT", Status: 3, Online: true},
		{ID: ids.NewRowID(), Hostname: "edge-02", Site: "OTT", Status: 1, Online: false},
		{ID: ids.NewRowID(), Hostname: "core-01", Site: "YUL", Status: 5, Online: true},
		{ID: ids.NewRowID(), Hostname: "core-02", Site: "YUL", Status: 2, Online: false},
	}
	for i := range devices {
		if err := db.InsertItem(ctx, &devices[i]); err != nil {
			t.Fatalf("InsertItem %s: %v", devices[i].Hostname, err)
		}
	}
	return devices
}

func TestItemCRUDLive(t *testing.T) {
	db := openItemTestDB(t)
	ctx := context.Background()

	id := ids.NewRowID()
	original := &itemDevice{ID: id, Hostname: "edge-09", Site: "OTT", Status: 4, Online: true}
	if err := db.InsertItem(ctx, original); err != nil {
		t.Fatalf("InsertItem: %v", err)
	}
	// Duplicate insert must fail like the typed API.
	if err := db.InsertItem(ctx, &itemDevice{ID: id, Hostname: "dup"}); err == nil {
		t.Fatal("duplicate InsertItem succeeded")
	}

	var got itemDevice
	got.ID = id
	if err := db.GetItem(ctx, &got); err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got != *original {
		t.Fatalf("GetItem = %+v, want %+v", got, *original)
	}

	updated := &itemDevice{ID: id, Hostname: "edge-10", Site: "YUL", Status: 7, Online: false}
	if err := db.SaveItem(ctx, updated); err != nil {
		t.Fatalf("SaveItem: %v", err)
	}
	var after itemDevice
	after.ID = id
	if err := db.GetItem(ctx, &after); err != nil {
		t.Fatalf("GetItem after save: %v", err)
	}
	if after != *updated {
		t.Fatalf("GetItem after save = %+v, want %+v", after, *updated)
	}

	if err := db.DeleteItem(ctx, &after); err != nil {
		t.Fatalf("DeleteItem: %v", err)
	}
	var missing itemDevice
	missing.ID = id
	if err := db.GetItem(ctx, &missing); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("GetItem after delete = %v, want ErrNotFound", err)
	}
}

func TestItemUpdateFieldsLive(t *testing.T) {
	db := openItemTestDB(t)
	ctx := context.Background()

	id := ids.NewRowID()
	if err := db.InsertItem(ctx, &itemDevice{ID: id, Hostname: "edge-01", Site: "OTT", Status: 5, Online: true}); err != nil {
		t.Fatalf("InsertItem: %v", err)
	}
	device := itemDevice{ID: id}
	if err := db.GetItem(ctx, &device); err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	// Zero values are explicit updates, not skips.
	if err := db.UpdateFields(ctx, &device, map[string]any{"Status": 0, "Online": false}); err != nil {
		t.Fatalf("UpdateFields: %v", err)
	}
	if device.Status != 0 || device.Online {
		t.Fatalf("in-memory item = %+v, want zeroed Status/Online", device)
	}
	var reread itemDevice
	reread.ID = id
	if err := db.GetItem(ctx, &reread); err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if reread.Status != 0 || reread.Online || reread.Hostname != "edge-01" || reread.Site != "OTT" {
		t.Fatalf("partial update = %+v, want only Status/Online changed", reread)
	}
	// Numeric widths coerce to the field type.
	if err := db.UpdateFields(ctx, &device, map[string]any{"Status": int64(9)}); err != nil {
		t.Fatalf("UpdateFields int64: %v", err)
	}
	if device.Status != 9 {
		t.Fatalf("Status = %d, want 9", device.Status)
	}
	// Unknown fields, primary keys, and bad types fail before any write.
	if err := db.UpdateFields(ctx, &device, map[string]any{"Nope": 1}); err == nil {
		t.Fatal("UpdateFields with unknown field succeeded")
	}
	if err := db.UpdateFields(ctx, &device, map[string]any{"ID": ids.NewRowID()}); err == nil {
		t.Fatal("UpdateFields with primary field succeeded")
	}
	if err := db.UpdateFields(ctx, &device, map[string]any{"Status": "high"}); err == nil {
		t.Fatal("UpdateFields with bad type succeeded")
	}
	// Empty updates are a no-op, even for missing rows.
	ghost := itemDevice{ID: ids.NewRowID()}
	if err := db.UpdateFields(ctx, &ghost, nil); err != nil {
		t.Fatalf("empty UpdateFields: %v", err)
	}
	// Partial update of a missing row reports NotFound.
	if err := db.UpdateFields(ctx, &ghost, map[string]any{"Status": 1}); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("UpdateFields missing = %v, want ErrNotFound", err)
	}
}

func TestItemQueryLive(t *testing.T) {
	db := openItemTestDB(t)
	seeded := seedItemDevices(t, db)

	t.Run("EqualityAndRange", func(t *testing.T) {
		var devices []itemDevice
		err := db.Query(itemDevice{},
			q.Eq("Site", "OTT"),
			q.Gte("Status", 2),
		).OrderBy("Hostname").Limit(100).FindInto(&devices)
		if err != nil {
			t.Fatalf("FindInto: %v", err)
		}
		if len(devices) != 1 || devices[0].Hostname != "edge-01" {
			t.Fatalf("FindInto = %+v, want [edge-01]", devices)
		}
		// Cross-width numeric literals coerce to the field type.
		var widened []itemDevice
		if err := db.Query(itemDevice{}, q.Gte("Status", int64(2))).FindInto(&widened); err != nil {
			t.Fatalf("FindInto widened: %v", err)
		}
		if len(widened) != 3 {
			t.Fatalf("widened Gte = %d rows, want 3", len(widened))
		}
	})

	t.Run("PointerSlicesAndFirst", func(t *testing.T) {
		var devices []*itemDevice
		if err := db.Query(&itemDevice{}, q.Eq("Online", true)).OrderBy("Hostname").FindInto(&devices); err != nil {
			t.Fatalf("FindInto pointers: %v", err)
		}
		if len(devices) != 2 || devices[0].Hostname != "core-01" || devices[1].Hostname != "edge-01" {
			t.Fatalf("pointer FindInto hostnames = %v, want [core-01 edge-01]", hostnames(devices))
		}
		var first itemDevice
		if err := db.Query(itemDevice{}, q.Eq("Site", "YUL")).OrderByDescending("Status").FirstInto(&first); err != nil {
			t.Fatalf("FirstInto: %v", err)
		}
		if first.Hostname != "core-01" {
			t.Fatalf("FirstInto = %s, want core-01", first.Hostname)
		}
		var missing itemDevice
		if err := db.Query(itemDevice{}, q.Eq("Site", "XXX")).FirstInto(&missing); !errors.Is(err, rime.ErrNotFound) {
			t.Fatalf("FirstInto missing = %v, want ErrNotFound", err)
		}
	})

	t.Run("Combinators", func(t *testing.T) {
		var devices []itemDevice
		err := db.Query(itemDevice{},
			q.Or(q.Eq("Site", "OTT"), q.Lt("Status", 2)),
			q.Ne("Hostname", "edge-02"),
		).FindInto(&devices)
		if err != nil {
			t.Fatalf("FindInto Or: %v", err)
		}
		if len(devices) != 1 || devices[0].Hostname != "edge-01" {
			t.Fatalf("Or/Ne = %+v, want [edge-01]", devices)
		}
		var in []itemDevice
		err = db.Query(itemDevice{}, q.In("Site", "OTT", "YUL"), q.Not(q.Eq("Online", true))).OrderBy("Hostname").FindInto(&in)
		if err != nil {
			t.Fatalf("FindInto In/Not: %v", err)
		}
		if len(in) != 2 || in[0].Hostname != "core-02" || in[1].Hostname != "edge-02" {
			t.Fatalf("In/Not hostnames = %v, want [core-02 edge-02]", flatHostnames(in))
		}
		var anded []itemDevice
		err = db.Query(itemDevice{}).Where(
			q.And(q.Eq("Site", "YUL"), q.Gte("Status", 2), q.Lte("Status", 5)),
		).FindInto(&anded)
		if err != nil {
			t.Fatalf("FindInto And: %v", err)
		}
		if len(anded) != 2 {
			t.Fatalf("And = %d rows, want 2", len(anded))
		}
	})

	t.Run("Pagination", func(t *testing.T) {
		var page []itemDevice
		err := db.Query(itemDevice{}).OrderBy("Hostname").Offset(1).Limit(2).FindInto(&page)
		if err != nil {
			t.Fatalf("FindInto page: %v", err)
		}
		if len(page) != 2 || page[0].Hostname != "core-02" || page[1].Hostname != "edge-01" {
			t.Fatalf("page = %v, want [core-02 edge-01]", flatHostnames(page))
		}
	})

	t.Run("Aggregates", func(t *testing.T) {
		count, err := db.Query(itemDevice{}, q.Eq("Site", "OTT")).Count()
		if err != nil || count != 2 {
			t.Fatalf("Count = %d, %v; want 2", count, err)
		}
		exists, err := db.Query(itemDevice{}, q.Eq("Hostname", "core-01")).Exists()
		if err != nil || !exists {
			t.Fatalf("Exists = %v, %v; want true", exists, err)
		}
		exists, err = db.Query(itemDevice{}, q.Eq("Hostname", "ghost")).Exists()
		if err != nil || exists {
			t.Fatalf("Exists ghost = %v, %v; want false", exists, err)
		}
		// RowID equality addresses the seeded primary keys.
		count, err = db.Query(itemDevice{}, q.Eq("ID", seeded[0].ID)).Count()
		if err != nil || count != 1 {
			t.Fatalf("Count by ID = %d, %v; want 1", count, err)
		}
	})
}

func TestItemTransactionsLive(t *testing.T) {
	db := openItemTestDB(t)
	ctx := context.Background()

	id := ids.NewRowID()
	tx, err := db.BeginTx(ctx)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	device := &itemDevice{ID: id, Hostname: "tx-01", Site: "OTT", Status: 1, Online: false}
	if err := tx.InsertItem(device); err != nil {
		t.Fatalf("tx.InsertItem: %v", err)
	}
	var staged itemDevice
	staged.ID = id
	if err := tx.GetItem(&staged); err != nil {
		t.Fatalf("tx.GetItem: %v", err)
	}
	if staged.Hostname != "tx-01" {
		t.Fatalf("tx.GetItem = %+v", staged)
	}
	if err := tx.UpdateFields(&staged, map[string]any{"Status": 6, "Online": true}); err != nil {
		t.Fatalf("tx.UpdateFields: %v", err)
	}
	var queried []itemDevice
	if err := db.Query(itemDevice{}, q.Eq("Site", "OTT")).In(tx).FindInto(&queried); err != nil {
		t.Fatalf("In(tx) FindInto: %v", err)
	}
	if len(queried) != 1 || queried[0].Status != 6 || !queried[0].Online {
		t.Fatalf("In(tx) = %+v, want staged update", queried)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	read, err := db.readTxContext(ctx)
	if err != nil {
		t.Fatalf("readTxContext: %v", err)
	}
	defer read.Close()
	var pinned itemDevice
	pinned.ID = id
	if err := read.GetItem(&pinned); err != nil {
		t.Fatalf("read.GetItem: %v", err)
	}
	if pinned.Status != 6 || !pinned.Online {
		t.Fatalf("read.GetItem = %+v", pinned)
	}
	count, err := db.Query(itemDevice{}, q.Gte("Status", 6)).inRead(read).Count()
	if err != nil || count != 1 {
		t.Fatalf("inRead Count = %d, %v; want 1", count, err)
	}

	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		if err := tx.SaveItem(&itemDevice{ID: id, Hostname: "tx-02", Site: "YUL", Status: 2, Online: false}); err != nil {
			return err
		}
		return tx.DeleteItem(&itemDevice{ID: id})
	}); err != nil {
		t.Fatalf("WriteTxContext save+delete: %v", err)
	}
	var gone itemDevice
	gone.ID = id
	if err := db.GetItem(ctx, &gone); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("GetItem after tx delete = %v, want ErrNotFound", err)
	}
}

func TestItemErrorsLive(t *testing.T) {
	db := openItemTestDB(t)
	ctx := context.Background()

	type ghost struct {
		ID ids.RowID `rime:"primary"`
	}
	if err := db.InsertItem(ctx, &ghost{ID: ids.NewRowID()}); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("InsertItem unregistered = %v, want ErrUnsupportedSchema", err)
	}
	if err := db.Query(ghost{}).FindInto(&[]ghost{}); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("Query unregistered = %v, want ErrUnsupportedSchema", err)
	}
	var device itemDevice
	if err := db.GetItem(ctx, &device); err == nil {
		t.Fatal("GetItem zero key succeeded")
	}
	if err := db.GetItem(ctx, itemDevice{}); err == nil {
		t.Fatal("GetItem value succeeded")
	}
	var devices []itemDevice
	if err := db.Query(itemDevice{}, q.Eq("Nope", 1)).FindInto(&devices); err == nil {
		t.Fatal("query with unknown field succeeded")
	}
	if err := db.Query(itemDevice{}, q.Gt("Online", true)).FindInto(&devices); err == nil {
		t.Fatal("range over bool succeeded")
	}
	if err := db.Query(itemDevice{}, q.Eq("Status", "high")).FindInto(&devices); err == nil {
		t.Fatal("query with bad value type succeeded")
	}
	if err := db.Query(itemDevice{}).OrderBy("Nope").FindInto(&devices); err == nil {
		t.Fatal("OrderBy unknown field succeeded")
	}
	if err := db.Query(nil).FindInto(&devices); err == nil {
		t.Fatal("Query nil model succeeded")
	}
	type other struct {
		ID ids.RowID `rime:"primary"`
	}
	if err := db.Query(itemDevice{}).FindInto(&[]other{}); err == nil {
		t.Fatal("FindInto mismatched slice succeeded")
	}
	var one itemDevice
	if err := db.Query(itemDevice{}).FirstInto(&[]itemDevice{}); err == nil {
		t.Fatal("FirstInto slice succeeded")
	}
	_ = one
}

func TestItemAmbiguousTypeLive(t *testing.T) {
	definition := itemDeviceDefinition(t)
	second, err := define[itemDevice]("devices_shadow", 81, RecordOptions{
		PrimaryField: "ID",
		FieldIDs: map[string]uint32{
			"ID": 1, "Hostname": 2, "Site": 3, "Status": 4, "Online": 5,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	db := openItemTestDB(t, definition, second)
	if err := db.Query(itemDevice{}).FindInto(&[]itemDevice{}); err == nil || !strings.Contains(err.Error(), "registered for tables") {
		t.Fatalf("ambiguous Query = %v, want multiple-tables error", err)
	}
	if err := db.InsertItem(context.Background(), &itemDevice{ID: ids.NewRowID()}); err == nil {
		t.Fatal("ambiguous InsertItem succeeded")
	}
	// The typed API still disambiguates by table name.
	first, err := tableOf[itemDevice](db, "devices")
	if err != nil {
		t.Fatalf("tableOf devices: %v", err)
	}
	shadow, err := tableOf[itemDevice](db, "devices_shadow")
	if err != nil {
		t.Fatalf("tableOf shadow: %v", err)
	}
	if first == nil || shadow == nil {
		t.Fatal("typed handles are nil")
	}
}

func hostnames(devices []*itemDevice) []string {
	out := make([]string, len(devices))
	for i, device := range devices {
		out[i] = device.Hostname
	}
	return out
}

func flatHostnames(devices []itemDevice) []string {
	out := make([]string, len(devices))
	for i := range devices {
		out[i] = devices[i].Hostname
	}
	return out
}
