package murmur

import (
	"context"
	"testing"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/q"
)

type nullableItem struct {
	ID     ids.RowID `rime:"primary"`
	Label  *string   `rime:"index"`
	Number *int64    `rime:"index"`
}

func nullableItemFixture(t *testing.T) (*DB, []nullableItem) {
	t.Helper()
	definition, err := ModelFor(&nullableItem{}, ModelOptions{Name: "nullable_items", TableID: 801, RecordOptions: RecordOptions{PrimaryField: "ID"}})
	if err != nil {
		t.Fatal(err)
	}
	db := openItemTestDB(t, definition)
	alpha, beta := "Alpha", "beta"
	zero, two := int64(0), int64(2)
	rows := []nullableItem{{ID: ids.NewRowID()}, {ID: ids.NewRowID(), Label: &beta, Number: &zero}, {ID: ids.NewRowID(), Label: &alpha, Number: &two}}
	if err := db.InsertMany(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	return db, rows
}

func TestRuntimeModelNullablePredicates(t *testing.T) {
	db, rows := nullableItemFixture(t)
	for _, test := range []struct {
		name    string
		matcher q.Matcher
		want    ids.RowID
	}{
		{"null", q.IsNull("Number"), rows[0].ID},
		{"zero", q.Eq("Number", int64(0)), rows[1].ID},
		{"range", q.Gt("Number", int64(0)), rows[2].ID},
		{"membership", q.In("Number", int64(2)), rows[2].ID},
		{"fold equality", q.EqFold("Label", "ALPHA"), rows[2].ID},
		{"fold contains", q.ContainsFold("Label", "ET"), rows[1].ID},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got []nullableItem
			if err := db.Find(context.Background(), &got, test.matcher); err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0].ID != test.want {
				t.Fatalf("rows=%+v, want %s", got, test.want)
			}
		})
	}
}

func TestRuntimeModelNullableCompiledOrderingAndTransaction(t *testing.T) {
	db, rows := nullableItemFixture(t)
	compiled := db.Query(&nullableItem{}, q.IsNotNull("Number"), q.Gte("Number", q.Param())).OrderByFold("Label").Limit(1).Compile()
	var got []nullableItem
	if err := compiled.FindInto(&got, int64(0)); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != rows[2].ID {
		t.Fatalf("folded page=%+v", got)
	}
	if err := db.Transaction(context.Background(), func(tx *Tx) error {
		value := int64(3)
		if err := tx.UpdateFields(&rows[0], map[string]any{"Number": &value}); err != nil {
			return err
		}
		var staged []nullableItem
		if err := db.Query(&nullableItem{}, q.Gte("Number", int64(3))).In(tx).FindInto(&staged); err != nil {
			return err
		}
		if len(staged) != 1 || staged[0].ID != rows[0].ID {
			t.Fatalf("staged rows=%+v", staged)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestNativeReadSnapshotPreservesRowsAcrossUpdates(t *testing.T) {
	db, rows := nullableItemFixture(t)
	snapshot, err := db.ReadTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	number := int64(10)
	if err := db.UpdateFields(context.Background(), &rows[1], map[string]any{"Number": &number}); err != nil {
		t.Fatal(err)
	}
	var before, after []nullableItem
	if err := snapshot.Query(&nullableItem{}, q.Eq("Number", int64(0))).FindInto(&before); err != nil {
		t.Fatal(err)
	}
	if err := db.Query(&nullableItem{}, q.Eq("Number", int64(0))).FindInto(&after); err != nil {
		t.Fatal(err)
	}
	if len(before) != 1 || before[0].ID != rows[1].ID || len(after) != 0 {
		t.Fatalf("snapshot=%+v current=%+v", before, after)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Query(&nullableItem{}).FindInto(&before); err == nil {
		t.Fatal("closed snapshot accepted")
	}
}
