package rime

import "testing"

type joinOpRow struct {
	ID string      `rime:"primary"`
	S  string      `rime:"unique"`
	I  int         `rime:"index"`
	U  uint        `rime:"index"`
	F  float64     `rime:"index"`
	B  bool        `rime:"index"`
	N  joinOpNamed `rime:"index"`
}

type joinOpNamed int

// TestResolveEqOp locks indexed-join probe selection: exact-kind keys take
// typed ops, named and exotic keys keep the scratch path.
func TestResolveEqOp(t *testing.T) {
	db := New()
	defer db.Close()
	tab, err := Register[joinOpRow](db)
	if err != nil {
		t.Fatal(err)
	}
	ix := tab.idx
	cases := []struct {
		name  string
		op    eqOp
		field string
		want  eqOp
	}{
		{"uniqueStr", resolveEqOp[string](ix, "S"), "S", eqUniqueStr},
		{"hashI64", resolveEqOp[int](ix, "I"), "I", eqHashI64},
		{"hashU64", resolveEqOp[uint](ix, "U"), "U", eqHashU64},
		{"hashF64", resolveEqOp[float64](ix, "F"), "F", eqHashF64},
		{"hashF32", resolveEqOp[float32](ix, "F"), "F", eqHashF64},
		{"hashBool", resolveEqOp[bool](ix, "B"), "B", eqHashBool},
		{"named", resolveEqOp[joinOpNamed](ix, "N"), "N", eqScratch},
		{"crossKind", resolveEqOp[string](ix, "I"), "I", eqScratch},
		{"primaryKey", resolveEqOp[string](ix, "ID"), "ID", eqUniqueStr},
		{"unknown", resolveEqOp[string](ix, "Nope"), "Nope", eqScratch},
	}
	for _, c := range cases {
		if c.op != c.want {
			t.Errorf("%s: op=%d want %d", c.name, c.op, c.want)
		}
	}
}
