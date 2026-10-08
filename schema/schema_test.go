package schema

import (
	"errors"
	"testing"
)

func validTables() []TableSchema {
	return []TableSchema{
		{
			Name: "contacts",
			Columns: []ColumnSchema{
				{Name: "id", Type: ColBlob},
				{Name: "name", Type: ColText, Nullable: true},
			},
		},
	}
}

func TestBuildRegistryValid(t *testing.T) {
	reg, err := BuildRegistry(1, validTables())
	if err != nil {
		t.Fatal(err)
	}
	tb := reg.Table("contacts")
	if tb == nil || tb.PKColumn() == nil || tb.PKColumn().Name != "id" {
		t.Fatalf("bad table: %+v", tb)
	}
	if reg.TableByID(tb.ID) != tb {
		t.Fatal("TableByID mismatch")
	}
	if reg.Hash == ([32]byte{}) {
		t.Fatal("empty hash")
	}
}

func TestBuildRegistryRejects(t *testing.T) {
	cases := []struct {
		name   string
		tables []TableSchema
	}{
		{"no name", []TableSchema{{Columns: []ColumnSchema{{Name: "id", Type: ColBlob}}}}},
		{"no columns", []TableSchema{{Name: "t"}}},
		{"dup table", append(validTables(), validTables()[0])},
		{"dup column", []TableSchema{{Name: "t", Columns: []ColumnSchema{
			{Name: "id", Type: ColBlob}, {Name: "ID", Type: ColText, Nullable: true}}}}},
		{"int pk", []TableSchema{{Name: "t", PK: 1, Columns: []ColumnSchema{
			{ID: 1, Name: "id", Type: ColInteger}}}}},
		{"nullable pk", []TableSchema{{Name: "t", PK: 1, Columns: []ColumnSchema{
			{ID: 1, Name: "id", Type: ColBlob, Nullable: true}}}}},
		{"missing pk", []TableSchema{{Name: "t", PK: 99, Columns: []ColumnSchema{
			{ID: 1, Name: "id", Type: ColBlob}}}}},
		{"no type", []TableSchema{{Name: "t", Columns: []ColumnSchema{
			{Name: "id", Type: ColBlob}, {Name: "x"}}}}},
	}
	for _, c := range cases {
		if _, err := BuildRegistry(1, c.tables); !errors.Is(err, ErrUnsupportedSchema) {
			t.Fatalf("%s: expected ErrUnsupportedSchema, got %v", c.name, err)
		}
	}
}

func TestStableIDsAcrossReorder(t *testing.T) {
	mk := func(cols ...ColumnSchema) []TableSchema {
		return []TableSchema{{Name: "t", Columns: cols}}
	}
	a, err := BuildRegistry(1, mk(
		ColumnSchema{Name: "id", Type: ColBlob},
		ColumnSchema{Name: "x", Type: ColText, Nullable: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	// Reordered + extended: existing IDs must not change.
	b, err := BuildRegistry(1, mk(
		ColumnSchema{Name: "y", Type: ColInteger, Nullable: true},
		ColumnSchema{Name: "x", Type: ColText, Nullable: true},
		ColumnSchema{Name: "id", Type: ColBlob},
	))
	if err != nil {
		t.Fatal(err)
	}
	if a.Tables[0].ID != b.Tables[0].ID {
		t.Fatal("table ID changed across reorder")
	}
	ida := func(name string) uint32 {
		for _, c := range a.Tables[0].Columns {
			if c.Name == name {
				return c.ID
			}
		}
		return 0
	}
	idb := func(name string) uint32 {
		for _, c := range b.Tables[0].Columns {
			if c.Name == name {
				return c.ID
			}
		}
		return 0
	}
	if ida("id") != idb("id") || ida("x") != idb("x") {
		t.Fatal("column IDs changed across reorder")
	}
	// Explicit IDs are honored.
	c, err := BuildRegistry(1, []TableSchema{{
		ID: 77, Name: "t", PK: 78,
		Columns: []ColumnSchema{{ID: 78, Name: "id", Type: ColBlob}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if c.Tables[0].ID != 77 || c.Tables[0].PK != 78 {
		t.Fatal("explicit IDs not honored")
	}
}

func TestParseColumnType(t *testing.T) {
	for in, want := range map[string]ColumnType{
		"INTEGER": ColInteger, "int": ColInteger, "REAL": ColReal,
		"text": ColText, "VARCHAR": ColText, "blob": ColBlob,
	} {
		got, err := ParseColumnType(in)
		if err != nil || got != want {
			t.Fatalf("%q: %v %v", in, got, err)
		}
	}
	if _, err := ParseColumnType("VECTOR"); err == nil {
		t.Fatal("expected error for VECTOR")
	}
}
