package codec

import (
	"bytes"
	"testing"

	"github.com/nomadsql/replicateddb/crdt"
	"github.com/nomadsql/replicateddb/ids"
)

func TestValueRoundTrip(t *testing.T) {
	vals := []Value{
		Null(), Int(0), Int(-1), Int(1 << 62), Real(0), Real(-3.14),
		Text(""), Text("hello"), Blob(nil), Blob([]byte{0, 1, 2, 255}),
	}
	for i, v := range vals {
		enc := AppendValue(nil, v)
		got, rest, err := ConsumeValue(enc, 1<<20)
		if err != nil {
			t.Fatalf("case %d: %v", i, err)
		}
		if len(rest) != 0 {
			t.Fatalf("case %d: %d trailing bytes", i, len(rest))
		}
		if !got.Equal(v) {
			t.Fatalf("case %d: got %+v want %+v", i, got, v)
		}
	}
}

func TestValueBounds(t *testing.T) {
	enc := AppendValue(nil, Text("toolong"))
	if _, _, err := ConsumeValue(enc, 3); err == nil {
		t.Fatal("expected oversize rejection")
	}
	if _, _, err := ConsumeValue([]byte{byte(TypeText)}, 1<<20); err == nil {
		t.Fatal("expected truncation error")
	}
	if _, _, err := ConsumeValue([]byte{99}, 1<<20); err == nil {
		t.Fatal("expected unknown type error")
	}
}

func TestBatchRoundTrip(t *testing.T) {
	b := &MutationBatch{
		ProtocolVersion: 1,
		TxID:            ids.NewTxID(),
		OriginNode:      ids.NewNodeID(),
		Sequence:        42,
		HLC:             123456,
		SchemaEpoch:     7,
		Mutations: []Mutation{
			{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: Text("x")},
			{TableID: 1, RowID: ids.NewRowID(), ColumnID: ColumnTombstone, Flags: FlagTombstone},
			{TableID: 9, RowID: ids.NewRowID(), ColumnID: 3, Value: Blob([]byte{1, 2, 3})},
		},
	}
	enc := EncodeBatch(nil, b)
	got, rest, err := DecodeBatch(enc, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 {
		t.Fatalf("%d trailing bytes", len(rest))
	}
	if got.Sequence != b.Sequence || got.HLC != b.HLC || got.TxID != b.TxID ||
		got.OriginNode != b.OriginNode || got.SchemaEpoch != b.SchemaEpoch {
		t.Fatalf("header mismatch: %+v", got)
	}
	if len(got.Mutations) != 3 {
		t.Fatalf("got %d mutations", len(got.Mutations))
	}
	if !got.Mutations[0].Value.Equal(b.Mutations[0].Value) {
		t.Fatal("value mismatch")
	}
	if !got.Mutations[1].IsTombstone() {
		t.Fatal("tombstone flag lost")
	}
}

func TestBatchLimits(t *testing.T) {
	b := &MutationBatch{Mutations: []Mutation{{Value: Text("0123456789")}}}
	enc := EncodeBatch(nil, b)
	if _, _, err := DecodeBatch(enc, Limits{MaxValueBytes: 4, MaxMutations: 100}); err == nil {
		t.Fatal("expected value limit error")
	}
	if _, _, err := DecodeBatch(enc, Limits{MaxValueBytes: 1 << 20, MaxMutations: 0}); err == nil {
		t.Fatal("expected mutation count error")
	}
	if _, _, err := DecodeBatch(enc[:10], DefaultLimits()); err == nil {
		t.Fatal("expected truncation error")
	}
}

func TestCellStateRoundTrip(t *testing.T) {
	st := CellState{Version: testVersion(), Value: Real(1.5)}
	enc := EncodeCellState(nil, st)
	got, err := DecodeCellState(enc, DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != st.Version || !got.Value.Equal(st.Value) {
		t.Fatalf("mismatch: %+v", got)
	}
	if _, err := DecodeCellState(enc[:10], DefaultLimits()); err == nil {
		t.Fatal("expected truncation error")
	}
}

func TestManifestRoundTrip(t *testing.T) {
	m := &SnapshotManifest{
		SnapshotID:  ids.NewTxID(),
		DBID:        ids.NewDBID(),
		SchemaEpoch: 3,
		CreatedHLC:  999,
		Watermarks: []OriginWatermark{
			{Origin: ids.NewNodeID(), Sequence: 10},
			{Origin: ids.NewNodeID(), Sequence: 20},
		},
	}
	m.SchemaHash[0] = 0xAB
	enc := EncodeManifest(nil, m)
	got, rest, err := DecodeManifest(enc)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 || got.SchemaEpoch != 3 || got.CreatedHLC != 999 ||
		len(got.Watermarks) != 2 || got.SnapshotID != m.SnapshotID {
		t.Fatalf("mismatch: %+v rest=%d", got, len(rest))
	}
	if !bytes.Equal(got.SchemaHash[:], m.SchemaHash[:]) {
		t.Fatal("hash mismatch")
	}
}

func TestSnapshotCellsRoundTrip(t *testing.T) {
	cells := []SnapshotCell{
		{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Version: testVersion(), Value: Int(7)},
		{TableID: 1, RowID: ids.NewRowID(), ColumnID: ColumnTombstone, Version: testVersion()},
	}
	enc := EncodeSnapshotCells(nil, cells)
	got, rest, err := DecodeSnapshotCells(enc, DefaultLimits(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 0 || len(got) != 2 {
		t.Fatalf("mismatch: %+v rest=%d", got, len(rest))
	}
	if _, _, err := DecodeSnapshotCells(enc, DefaultLimits(), 1); err == nil {
		t.Fatal("expected cell count error")
	}
}

func testVersion() crdt.Version {
	return crdt.Version{HLC: 4242, NodeID: ids.NewNodeID()}
}
