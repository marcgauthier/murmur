package rime

import (
	"strings"
	"testing"
)

type opaquePredRow struct {
	ID   string `rime:"primary"`
	Name string `rime:"index"`
}

// TestOpaquePredicatesPlanAsFilters locks planner tolerance for kPred
// expressions without planner metadata (nullable/folded matchers): they
// must fall back to full scans with residual filters instead of panicking,
// alone and combined with ordinary indexed predicates.
func TestOpaquePredicatesPlanAsFilters(t *testing.T) {
	db := New()
	defer db.Close()
	tab, err := Register[opaquePredRow](db)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []opaquePredRow{{ID: "1", Name: "alpha"}, {ID: "2", Name: "beta"}} {
		row := row
		if err := tab.Insert(&row); err != nil {
			t.Fatal(err)
		}
	}
	opaque := func(want string) Expr[opaquePredRow] {
		return &expr[opaquePredRow]{k: kPred, desc: "Name OPAQUE",
			fn: func(rec *opaquePredRow) bool { return rec.Name == want }}
	}

	// Opaque-only: full scan, correct rows.
	q := tab.Where(opaque("alpha"))
	if got := q.Explain(); !strings.Contains(got, "FULL SCAN") {
		t.Fatalf("opaque explain = %q, want FULL SCAN", got)
	}
	rows, err := q.Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "1" {
		t.Fatalf("opaque rows = %+v, want ID 1", rows)
	}

	// Opaque conjunct beside an indexed equality: the indexed side may
	// still seek while the opaque side stays a residual filter.
	name := StringFieldOf[opaquePredRow](tab, "Name")
	mixed := tab.Where(name.Eq("beta"), opaque("beta"))
	if got := mixed.Explain(); !strings.Contains(got, "INDEX SEEK") {
		t.Fatalf("mixed explain = %q, want INDEX SEEK", got)
	}
	rows, err = mixed.Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "2" {
		t.Fatalf("mixed rows = %+v, want ID 2", rows)
	}

	// Compiled templates tolerate opaque leaves (no params to collect).
	compiled := tab.Compile(opaque("alpha"))
	rows, err = compiled.Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "1" {
		t.Fatalf("compiled opaque rows = %+v, want ID 1", rows)
	}
}
