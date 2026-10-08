package murmur

import (
	"testing"

	"github.com/marcgauthier/murmur/schema"
)

func TestOrderRegistryLikeLocalPreservesPhysicalOrder(t *testing.T) {
	current, err := schema.BuildRegistry(1, []schema.TableSchema{{
		Name: "mc_rows",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "name", Type: schema.ColText, Nullable: true},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := schema.BuildRegistry(2, []schema.TableSchema{{
		Name: "mc_rows",
		Columns: []schema.ColumnSchema{
			{Name: "name", Type: schema.ColText, Nullable: true},
			{Name: "id", Type: schema.ColBlob},
			{Name: "score", Type: schema.ColInteger, Nullable: true},
		},
	}, {
		Name: "new_table",
		Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	orderRegistryLikeLocal(current, wire)
	got := wire.Tables[0].Columns
	want := []string{"id", "name", "score"}
	if len(got) != len(want) {
		t.Fatalf("columns = %d, want %d", len(got), len(want))
	}
	for i, name := range want {
		if got[i].Name != name {
			gotNames := []string{got[0].Name, got[1].Name, got[2].Name}
			t.Fatalf("column order = %v, want %v", gotNames, want)
		}
	}
	// Unknown tables keep manifest order.
	if wire.Tables[1].Name != "new_table" || wire.Tables[1].Columns[0].Name != "id" {
		t.Fatalf("new table mangled: %+v", wire.Tables[1])
	}
	// Nil-safe.
	orderRegistryLikeLocal(nil, wire)
	orderRegistryLikeLocal(current, nil)
}
