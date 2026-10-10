package murmur

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/rime"
)

// itemCRDT exercises item-level CRDT operations across counter, set, and
// extrema merge policies with one plain field for rejection cases.
type itemCRDT struct {
	ID    ids.RowID `rime:"primary"`
	Note  string
	Count int64
	Tags  []string
	High  int64
	Low   float64
}

func itemCRDTDefinition(t testing.TB) TableDefinition {
	t.Helper()
	definition, err := Model[itemCRDT](ModelOptions{
		RecordOptions: RecordOptions{
			MergePolicies: map[string]RecordMergePolicy{
				"Count": RecordMergeCounter,
				"Tags":  RecordMergeORSet,
				"High":  RecordMergeMax,
				"Low":   RecordMergeMin,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func TestItemCounterLive(t *testing.T) {
	db := openItemTestDB(t, itemCRDTDefinition(t))
	ctx := context.Background()

	id := ids.NewRowID()
	if err := db.InsertItem(ctx, &itemCRDT{ID: id, Note: "n"}); err != nil {
		t.Fatalf("InsertItem: %v", err)
	}
	if err := db.CounterAdd(ctx, &itemCRDT{ID: id}, "Count", 5); err != nil {
		t.Fatalf("CounterAdd: %v", err)
	}
	// Value forms also identify the row.
	if err := db.CounterAdd(ctx, itemCRDT{ID: id}, "Count", -2); err != nil {
		t.Fatalf("CounterAdd value: %v", err)
	}
	if err := db.Transaction(ctx, func(tx *Tx) error {
		return tx.CounterAdd(&itemCRDT{ID: id}, "Count", 7)
	}); err != nil {
		t.Fatalf("tx CounterAdd: %v", err)
	}
	var got itemCRDT
	got.ID = id
	if err := db.GetItem(ctx, &got); err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got.Count != 10 {
		t.Fatalf("Count = %d, want 10", got.Count)
	}
	// A missing row reports NotFound; a zero delta stays a no-op.
	ghost := &itemCRDT{ID: ids.NewRowID()}
	if err := db.CounterAdd(ctx, ghost, "Count", 1); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("CounterAdd missing = %v, want ErrNotFound", err)
	}
	if err := db.CounterAdd(ctx, ghost, "Count", 0); err != nil {
		t.Fatalf("CounterAdd zero delta: %v", err)
	}
	// LWW fields, wrong policies, and keys fail before any write.
	if err := db.CounterAdd(ctx, &itemCRDT{ID: id}, "Note", 1); err == nil || !strings.Contains(err.Error(), "last-write-wins") {
		t.Fatalf("CounterAdd LWW = %v, want policy error", err)
	}
	if err := db.CounterAdd(ctx, &itemCRDT{ID: id}, "Tags", 1); err == nil {
		t.Fatal("CounterAdd on set field succeeded")
	}
	if err := db.CounterAdd(ctx, &itemCRDT{ID: id}, "ID", 1); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("CounterAdd key = %v, want immutable error", err)
	}
	if err := db.CounterAdd(ctx, &itemCRDT{ID: id}, "Nope", 1); err == nil || !strings.Contains(err.Error(), `unknown field "Nope"`) {
		t.Fatalf("CounterAdd unknown = %v, want field error", err)
	}
	// Partial updates keep pointing CRDT fields at the new operations.
	if err := db.UpdateFields(ctx, &itemCRDT{ID: id}, map[string]any{"Count": int64(1)}); err == nil || !strings.Contains(err.Error(), "CounterAdd") {
		t.Fatalf("UpdateFields counter = %v, want redirect error", err)
	}
}

func TestItemSetLive(t *testing.T) {
	db := openItemTestDB(t, itemCRDTDefinition(t))
	ctx := context.Background()

	id := ids.NewRowID()
	if err := db.InsertItem(ctx, &itemCRDT{ID: id}); err != nil {
		t.Fatalf("InsertItem: %v", err)
	}
	if err := db.SetAdd(ctx, &itemCRDT{ID: id}, "Tags", "a"); err != nil {
		t.Fatalf("SetAdd: %v", err)
	}
	if err := db.Transaction(ctx, func(tx *Tx) error {
		if err := tx.SetAdd(&itemCRDT{ID: id}, "Tags", "b"); err != nil {
			return err
		}
		return tx.SetRemove(&itemCRDT{ID: id}, "Tags", "a")
	}); err != nil {
		t.Fatalf("tx set ops: %v", err)
	}
	// Re-adding a present value and removing an absent one are no-ops.
	if err := db.SetAdd(ctx, &itemCRDT{ID: id}, "Tags", "b"); err != nil {
		t.Fatalf("SetAdd duplicate: %v", err)
	}
	if err := db.SetRemove(ctx, &itemCRDT{ID: id}, "Tags", "missing"); err != nil {
		t.Fatalf("SetRemove absent: %v", err)
	}
	var got itemCRDT
	got.ID = id
	if err := db.GetItem(ctx, &got); err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if len(got.Tags) != 1 || got.Tags[0] != "b" {
		t.Fatalf("Tags = %v, want [b]", got.Tags)
	}
	ghost := &itemCRDT{ID: ids.NewRowID()}
	if err := db.SetAdd(ctx, ghost, "Tags", "x"); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("SetAdd missing = %v, want ErrNotFound", err)
	}
	if err := db.SetAdd(ctx, &itemCRDT{ID: id}, "Count", "x"); err == nil {
		t.Fatal("SetAdd on counter field succeeded")
	}
}

func TestItemExtremaLive(t *testing.T) {
	db := openItemTestDB(t, itemCRDTDefinition(t))
	ctx := context.Background()

	id := ids.NewRowID()
	if err := db.InsertItem(ctx, &itemCRDT{ID: id, Low: 100}); err != nil {
		t.Fatalf("InsertItem: %v", err)
	}
	// int values coerce to the int64 field.
	if err := db.Max(ctx, &itemCRDT{ID: id}, "High", 10); err != nil {
		t.Fatalf("Max: %v", err)
	}
	if err := db.Max(ctx, &itemCRDT{ID: id}, "High", 5); err != nil {
		t.Fatalf("Max lower: %v", err)
	}
	if err := db.Transaction(ctx, func(tx *Tx) error {
		if err := tx.Min(&itemCRDT{ID: id}, "Low", 3.5); err != nil {
			return err
		}
		return tx.Min(&itemCRDT{ID: id}, "Low", 9.25)
	}); err != nil {
		t.Fatalf("tx extrema: %v", err)
	}
	var got itemCRDT
	got.ID = id
	if err := db.GetItem(ctx, &got); err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got.High != 10 || got.Low != 3.5 {
		t.Fatalf("extrema = (%d, %v), want (10, 3.5)", got.High, got.Low)
	}
	ghost := &itemCRDT{ID: ids.NewRowID()}
	if err := db.Max(ctx, ghost, "High", 1); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("Max missing = %v, want ErrNotFound", err)
	}
	if err := db.Max(ctx, &itemCRDT{ID: id}, "High", "x"); err == nil || !strings.Contains(err.Error(), `field "High"`) {
		t.Fatalf("Max bad type = %v, want field error", err)
	}
	if err := db.Max(ctx, &itemCRDT{ID: id}, "Count", 1); err == nil {
		t.Fatal("Max on counter field succeeded")
	}
	if err := db.Min(ctx, &itemCRDT{ID: id}, "High", 1); err == nil {
		t.Fatal("Min on max field succeeded")
	}
}
