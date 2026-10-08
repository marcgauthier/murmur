package murmur

import (
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/schema"
)

func TestBridgeShadowCodec(t *testing.T) {
	lim := codec.DefaultLimits()
	for _, v := range []codec.Value{
		codec.Text("hi"), codec.Int(-7), codec.Blob([]byte{1, 2, 3}),
		codec.Blob(nil), {Type: codec.TypeNull},
	} {
		set, got, err := decodeBridgeShadow(codec.CellState{Value: encodeBridgeShadowSet(v)}, lim)
		if err != nil || !set || got.Type != v.Type || got.S != v.S || got.I != v.I || string(got.B) != string(v.B) {
			t.Fatalf("set round trip of %+v: set=%v got=%+v err=%v", v, set, got, err)
		}
	}
	set, _, err := decodeBridgeShadow(codec.CellState{Value: encodeBridgeShadowClear()}, lim)
	if err != nil || set {
		t.Fatalf("clear decode: set=%v err=%v", set, err)
	}
	bad := []codec.Value{
		{Type: codec.TypeNull},
		codec.Text("x"),
		codec.Blob(nil),
		codec.Blob([]byte{0x7f}),
		codec.Blob([]byte{bridgeShadowClear, 0x00}),
		codec.Blob([]byte{bridgeShadowSet}),
		codec.Blob([]byte{bridgeShadowSet, 0xde, 0xad}),
	}
	for i, v := range bad {
		if _, _, err := decodeBridgeShadow(codec.CellState{Value: v}, lim); err == nil {
			t.Fatalf("malformed %d decoded without error", i)
		}
	}
}

func TestBridgeShadowMapping(t *testing.T) {
	if s, ok := bridgeShadowColumn(5); !ok || s != 5^0x80000000 {
		t.Fatalf("mapping 5 -> %d,%v", s, ok)
	}
	if _, ok := bridgeShadowColumn(0x80000000); ok {
		t.Fatal("0x80000000 must not map (lands on 0)")
	}
	if _, ok := bridgeShadowColumn(0x7FFFFFFF); ok {
		t.Fatal("0x7FFFFFFF must not map (lands on ColumnTombstone)")
	}
	cols := map[uint32]bool{3: true, 9: true}
	if c, ok := bridgeShadowOf(3^0x80000000, cols); !ok || c != 3 {
		t.Fatalf("inverse: %d,%v", c, ok)
	}
	if _, ok := bridgeShadowOf(3, cols); ok {
		t.Fatal("app column misread as shadow")
	}
	if _, ok := bridgeShadowOf(77, cols); ok {
		t.Fatal("unknown column misread as shadow")
	}
	// Legacy ambiguous pair: the app column wins.
	amb := map[uint32]bool{3: true, 3 ^ 0x80000000: true}
	if _, ok := bridgeShadowOf(3^0x80000000, amb); ok {
		t.Fatal("ambiguous column must read as app column")
	}
}

func TestBridgeShadowResolveRow(t *testing.T) {
	lim := codec.DefaultLimits()
	cols := map[uint32]bool{1: true, 2: true}
	raw := map[uint32]codec.CellState{
		1:              {Value: codec.Text("low")},
		2:              {Value: codec.Text("low2")},
		1 ^ 0x80000000: {Value: encodeBridgeShadowSet(codec.Text("high"))},
		2 ^ 0x80000000: {Value: encodeBridgeShadowClear()},
	}
	eff, err := resolveBridgeRow(raw, cols, lim)
	if err != nil {
		t.Fatal(err)
	}
	if len(eff) != 2 || eff[1].Value.S != "high" || eff[2].Value.S != "low2" {
		t.Fatalf("effective=%+v", eff)
	}
	shadowed, err := bridgeRowShadowed(raw, cols, lim)
	if err != nil || !shadowed {
		t.Fatalf("shadowed=%v err=%v", shadowed, err)
	}
	delete(raw, 1^0x80000000)
	shadowed, err = bridgeRowShadowed(raw, cols, lim)
	if err != nil || shadowed {
		t.Fatalf("cleared row shadowed=%v err=%v", shadowed, err)
	}
	raw[1^0x80000000] = codec.CellState{Value: codec.Blob([]byte{0x7f})}
	if _, err := resolveBridgeRow(raw, cols, lim); err == nil {
		t.Fatal("malformed shadow resolved without error")
	}
}

func TestBridgeShadowSchemaGuards(t *testing.T) {
	// Ambiguous high-bit pairs and reserved mappings are rejected.
	mkcol := func(name string, id uint32) schema.ColumnSchema {
		return schema.ColumnSchema{Name: name, ID: id, Type: schema.ColText, Nullable: true}
	}
	bad := [][]schema.TableSchema{
		{{Name: "t", Columns: []schema.ColumnSchema{mkcol("a", 7), mkcol("b", 7^0x80000000)}}},
		{{Name: "t", Columns: []schema.ColumnSchema{mkcol("a", 0x80000000)}}},
		{{Name: "t", Columns: []schema.ColumnSchema{mkcol("a", 0x7FFFFFFF)}}},
	}
	for i, tables := range bad {
		if _, err := schema.BuildRegistry(1, tables); err == nil {
			t.Fatalf("schema %d accepted despite shadow ambiguity", i)
		}
	}
	// The reserved file IDs are shadow-safe: no reserved mappings, no
	// pairs, and no shadow lands on a file column.
	fids, err := resolveFileIDs()
	if err != nil {
		t.Fatal(err)
	}
	fcols := []uint32{fids.id, fids.name, fids.digest, fids.size}
	seen := map[uint32]bool{}
	for _, c := range fcols {
		s, ok := bridgeShadowColumn(c)
		if !ok {
			t.Fatalf("file column %d has no shadow mapping", c)
		}
		seen[c] = true
		if seen[s] {
			t.Fatalf("file shadow %d collides", s)
		}
	}
	for _, c := range fcols {
		s, _ := bridgeShadowColumn(c)
		for _, d := range fcols {
			if s == d {
				t.Fatalf("file shadow %d lands on file column %d", s, d)
			}
		}
	}
}
