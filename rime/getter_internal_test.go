package rime

import (
	"reflect"
	"testing"
)

type getterRow struct {
	ID     string      `rime:"primary"`
	Name   stringNamed `rime:"index"`
	Age    int         `rime:"index"`
	Big    int64       `rime:"ordered"`
	U      uint64      `rime:"index"`
	F      float64     `rime:"ordered"`
	OK     bool        `rime:"index"`
	Raw    [16]byte    `rime:"index"`
	Uuid   UUID        `rime:"unique"`
	Ptr    *int
	Blob   []byte
	Labels map[string]string
	Any    any
}

type stringNamed string

// TestTypedGetterParity checks the unsafe offset fast path against the
// reflective getter for every supported field kind.
func TestTypedGetterParity(t *testing.T) {
	s, err := buildSchema("getterRow", reflect.TypeOf(getterRow{}))
	if err != nil {
		t.Fatal(err)
	}
	n := 7
	rec := &getterRow{
		ID: "k1", Name: "n1", Age: -3, Big: 1 << 60, U: 1<<64 - 1,
		F: 2.5, OK: true, Raw: [16]byte{1, 2, 3}, Uuid: UUID{9: 9},
		Ptr: &n, Blob: []byte{4, 5}, Labels: map[string]string{"a": "b"},
		Any: "boxed",
	}
	type tc struct {
		name string
		fast any // func(*getterRow) V result
		want any // reflective getter result
	}
	slow := func(name string) any {
		get, _, err := getter[getterRow](s, name)
		if err != nil {
			t.Fatalf("getter %s: %v", name, err)
		}
		return get(rec)
	}
	cases := []tc{
		{"ID", typedGetter[getterRow, string](s, "ID")(rec), slow("ID")},
		{"Age", typedGetter[getterRow, int](s, "Age")(rec), slow("Age")},
		{"Big", typedGetter[getterRow, int64](s, "Big")(rec), slow("Big")},
		{"U", typedGetter[getterRow, uint64](s, "U")(rec), slow("U")},
		{"F", typedGetter[getterRow, float64](s, "F")(rec), slow("F")},
		{"OK", typedGetter[getterRow, bool](s, "OK")(rec), slow("OK")},
		{"Raw", typedGetter[getterRow, [16]byte](s, "Raw")(rec), slow("Raw")},
		{"Uuid", typedGetter[getterRow, UUID](s, "Uuid")(rec), slow("Uuid")},
		{"Ptr", typedGetter[getterRow, *int](s, "Ptr")(rec), slow("Ptr")},
		{"Blob", typedGetter[getterRow, []byte](s, "Blob")(rec), slow("Blob")},
		{"Labels", typedGetter[getterRow, map[string]string](s, "Labels")(rec), slow("Labels")},
		// Named field type requested as its own type (exact match path).
		{"Name", typedGetter[getterRow, stringNamed](s, "Name")(rec), slow("Name")},
		// Interface field requested with a concrete type exercises the
		// reflective fallback path (assertion, not offset load).
		{"AnyAsString", typedGetter[getterRow, string](s, "Any")(rec), slow("Any")},
	}
	for _, c := range cases {
		if !reflect.DeepEqual(c.fast, c.want) {
			t.Errorf("%s: fast=%#v want=%#v", c.name, c.fast, c.want)
		}
	}
}

// TestTypedGetterZeroAlloc locks the fast path's allocation-free property.
func TestTypedGetterZeroAlloc(t *testing.T) {
	s, err := buildSchema("getterRow", reflect.TypeOf(getterRow{}))
	if err != nil {
		t.Fatal(err)
	}
	rec := &getterRow{ID: "k", Age: 1, F: 1.5}
	getID := typedGetter[getterRow, string](s, "ID")
	getAge := typedGetter[getterRow, int](s, "Age")
	if a := testing.AllocsPerRun(200, func() {
		_ = getID(rec)
	}); a != 0 {
		t.Errorf("string getter allocs = %v, want 0", a)
	}
	if a := testing.AllocsPerRun(200, func() {
		_ = getAge(rec)
	}); a != 0 {
		t.Errorf("int getter allocs = %v, want 0", a)
	}
}

// TestTypedGetterPanics preserves fail-fast handle construction.
func TestTypedGetterPanics(t *testing.T) {
	s, err := buildSchema("getterRow", reflect.TypeOf(getterRow{}))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		fn   func()
	}{
		{"unknown", func() { typedGetter[getterRow, string](s, "Nope") }},
		{"mismatch", func() { typedGetter[getterRow, int](s, "ID") }},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: expected panic", tc.name)
				}
			}()
			tc.fn()
		}()
	}
}
