package murmur

import (
	"context"
	"testing"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/q"
)

// TestItemBatchWrappersLive exercises the batch and update wrappers that the
// main suites only reach through other entry points.
func TestItemBatchWrappersLive(t *testing.T) {
	db := openItemTestDB(t)
	ctx := context.Background()

	seeded := seedItemDevices(t, db)
	if err := db.SaveMany(ctx, []itemDevice{
		{ID: ids.NewRowID(), Hostname: "agg-01", Site: "YOW", Status: 7, Online: true},
		seeded[0],
	}); err != nil {
		t.Fatalf("SaveMany: %v", err)
	}
	updated := seeded[1]
	updated.Status = 9
	if err := db.UpdateItem(ctx, &updated); err != nil {
		t.Fatalf("UpdateItem: %v", err)
	}
	ghost := itemDevice{ID: ids.NewRowID(), Hostname: "ghost"}
	if err := db.UpdateItem(ctx, &ghost); err == nil {
		t.Fatal("UpdateItem missing row succeeded")
	}
	if err := db.Transaction(ctx, func(tx *Tx) error {
		if err := tx.InsertMany([]itemDevice{
			{ID: ids.NewRowID(), Hostname: "tx-01", Site: "YOW", Status: 1},
		}); err != nil {
			return err
		}
		save := updated
		save.Online = true
		if err := tx.SaveMany([]*itemDevice{&save}); err != nil {
			return err
		}
		if err := tx.UpdateMany([]itemDevice{save}); err != nil {
			return err
		}
		return tx.DeleteMany([]itemDevice{{ID: save.ID}})
	}); err != nil {
		t.Fatalf("tx batches: %v", err)
	}
	n, err := db.Query(itemDevice{}).Count()
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	// 4 seeded + 1 saved - 1 deleted + 1 tx-inserted = 5.
	if n != 5 {
		t.Fatalf("Count = %d, want 5", n)
	}
}

// TestItemTxExtremaLive stages MIN/MAX updates inside one transaction.
func TestItemTxExtremaLive(t *testing.T) {
	db := openItemTestDB(t, itemCRDTDefinition(t))
	ctx := context.Background()

	id := ids.NewRowID()
	if err := db.InsertItem(ctx, &itemCRDT{ID: id, High: 10, Low: 100}); err != nil {
		t.Fatalf("InsertItem: %v", err)
	}
	if err := db.Transaction(ctx, func(tx *Tx) error {
		if err := tx.Max(&itemCRDT{ID: id}, "High", 40); err != nil {
			return err
		}
		return tx.Min(&itemCRDT{ID: id}, "Low", 25)
	}); err != nil {
		t.Fatalf("tx extrema: %v", err)
	}
	var got itemCRDT
	got.ID = id
	if err := db.GetItem(ctx, &got); err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got.High != 40 || got.Low != 25 {
		t.Fatalf("extrema = (%d, %v), want (40, 25)", got.High, got.Low)
	}
}

// TestItemCompiledWithContextLive binds contexts on the compiled handle
// itself rather than on the builder query.
func TestItemCompiledWithContextLive(t *testing.T) {
	db := openItemTestDB(t)
	seedItemDevices(t, db)
	ctx := context.Background()

	cq := db.Query(itemDevice{}, q.Eq("Site", q.Param())).Compile().WithContext(ctx)
	var results []itemDevice
	if err := cq.FindInto(&results, "OTT"); err != nil {
		t.Fatalf("FindInto: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("FindInto OTT = %d rows, want 2", len(results))
	}
	// A nil context keeps the previous binding instead of failing.
	if n, err := cq.WithContext(nil).Count("YUL"); err != nil || n != 2 {
		t.Fatalf("Count YUL = %d, %v; want 2", n, err)
	}
}

// TestItemOrderByFoldLive checks case-insensitive ordering in both
// directions.
func TestItemOrderByFoldLive(t *testing.T) {
	db := openItemTestDB(t)
	ctx := context.Background()
	for _, hostname := range []string{"bravo", "Alpha", "CHARLIE"} {
		if err := db.InsertItem(ctx, &itemDevice{ID: ids.NewRowID(), Hostname: hostname}); err != nil {
			t.Fatalf("InsertItem %s: %v", hostname, err)
		}
	}
	var asc []itemDevice
	if err := db.Query(itemDevice{}).WithContext(ctx).OrderByFold("Hostname").FindInto(&asc); err != nil {
		t.Fatalf("OrderByFold: %v", err)
	}
	var desc []itemDevice
	if err := db.Query(itemDevice{}).WithContext(ctx).OrderByFoldDescending("Hostname").FindInto(&desc); err != nil {
		t.Fatalf("OrderByFoldDescending: %v", err)
	}
	if len(asc) != 3 || len(desc) != 3 {
		t.Fatalf("orders = %d/%d rows, want 3/3", len(asc), len(desc))
	}
	for i := range asc {
		if asc[i].Hostname != desc[2-i].Hostname {
			t.Fatalf("asc = %v, desc = %v; want mirrors", asc, desc)
		}
	}
	if asc[0].Hostname != "Alpha" || asc[1].Hostname != "bravo" || asc[2].Hostname != "CHARLIE" {
		t.Fatalf("asc = %v, want Alpha/bravo/CHARLIE", asc)
	}
}
