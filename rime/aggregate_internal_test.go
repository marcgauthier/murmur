package rime

import (
	"testing"
)

type numRow struct {
	ID  string   `rime:"primary"`
	I8  int8     `rime:"ordered"`
	I16 int16    `rime:"ordered"`
	I32 int32    `rime:"ordered"`
	I64 int64    `rime:"ordered"`
	U8  uint8    `rime:"ordered"`
	U16 uint16   `rime:"ordered"`
	U32 uint32   `rime:"ordered"`
	U64 uint64   `rime:"ordered"`
	F32 float32  `rime:"ordered"`
	F64 float64  `rime:"ordered"`
	NI  namedInt `rime:"ordered"`
}

type namedInt int64

// TestNumAtWidths verifies direct offset loads for every numeric width,
// including named types, against plain field reads.
func TestNumAtWidths(t *testing.T) {
	db := New()
	defer db.Close()
	tab, err := Register[numRow](db)
	if err != nil {
		t.Fatal(err)
	}
	rec := &numRow{I8: -8, I16: -1600, I32: -320000, I64: -(1 << 60),
		U8: 8, U16: 1600, U32: 320000, U64: 1 << 60,
		F32: 1.5, F64: -2.25, NI: 42}
	cases := []struct {
		name string
		agg  Agg[numRow]
		want float64
	}{
		{"I8", SumOf(OF[numRow, int8](tab, "I8")), float64(rec.I8)},
		{"I16", SumOf(OF[numRow, int16](tab, "I16")), float64(rec.I16)},
		{"I32", SumOf(OF[numRow, int32](tab, "I32")), float64(rec.I32)},
		{"I64", SumOf(OF[numRow, int64](tab, "I64")), float64(rec.I64)},
		{"U8", SumOf(OF[numRow, uint8](tab, "U8")), float64(rec.U8)},
		{"U16", SumOf(OF[numRow, uint16](tab, "U16")), float64(rec.U16)},
		{"U32", SumOf(OF[numRow, uint32](tab, "U32")), float64(rec.U32)},
		{"U64", SumOf(OF[numRow, uint64](tab, "U64")), float64(rec.U64)},
		{"F32", AvgOf(OF[numRow, float32](tab, "F32")), float64(rec.F32)},
		{"F64", AvgOf(OF[numRow, float64](tab, "F64")), rec.F64},
		{"NI", SumOf(OF[numRow, namedInt](tab, "NI")), float64(rec.NI)},
	}
	for _, c := range cases {
		if c.agg.nk == numNone {
			t.Errorf("%s: expected direct load, got fallback", c.name)
		}
		if got := c.agg.numAt(rec); got != c.want {
			t.Errorf("%s: numAt=%v want %v", c.name, got, c.want)
		}
		// run() over one row must agree (SUM and AVG of a single row).
		if got := c.agg.run([]*numRow{rec}).(float64); got != c.want {
			t.Errorf("%s: run=%v want %v", c.name, got, c.want)
		}
	}
	// Fallback path: hand-built Agg without offset info uses num.
	fb := Agg[numRow]{kind: aggSum, num: func(r *numRow) float64 { return float64(r.I32) * 2 }}
	if got := fb.numAt(rec); got != float64(rec.I32)*2 {
		t.Errorf("fallback numAt=%v want %v", got, float64(rec.I32)*2)
	}
}
