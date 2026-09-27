package codec

import (
	"testing"

	"github.com/nomadsql/replicateddb/ids"
)

// FuzzValue decodes arbitrary bytes as a Value: must never panic.
func FuzzValue(f *testing.F) {
	seeds := [][]byte{
		AppendValue(nil, Null()),
		AppendValue(nil, Int(-12345)),
		AppendValue(nil, Real(3.14)),
		AppendValue(nil, Text("hello")),
		AppendValue(nil, Blob([]byte{0, 1, 2})),
		{byte(TypeText), 5, 'a'},
		{99},
		{},
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		v, rest, err := ConsumeValue(b, 1<<20)
		if err != nil {
			return
		}
		// Round-trip: re-encoding must decode to an equal value.
		enc := AppendValue(nil, v)
		v2, rest2, err := ConsumeValue(enc, 1<<20)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if len(rest2) != 0 {
			t.Fatalf("%d trailing bytes", len(rest2))
		}
		if !v.Equal(v2) {
			t.Fatalf("round-trip mismatch: %+v vs %+v", v, v2)
		}
		_ = rest
	})
}

// FuzzBatch decodes arbitrary bytes as a MutationBatch: must never panic,
// and valid decodes must re-encode identically.
func FuzzBatch(f *testing.F) {
	b := &MutationBatch{
		ProtocolVersion: 1,
		TxID:            ids.NewTxID(),
		OriginNode:      ids.NewNodeID(),
		Sequence:        1,
		HLC:             100,
		SchemaEpoch:     1,
		Mutations: []Mutation{
			{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: Text("v")},
			{TableID: 1, RowID: ids.NewRowID(), ColumnID: ColumnTombstone, Flags: FlagTombstone},
		},
	}
	f.Add(EncodeBatch(nil, b))
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		got, _, err := DecodeBatch(data, DefaultLimits())
		if err != nil {
			return
		}
		// Canonical re-encoding must decode to the same batch.
		enc := EncodeBatch(nil, got)
		got2, rest, err := DecodeBatch(enc, DefaultLimits())
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if len(rest) != 0 {
			t.Fatalf("%d trailing bytes", len(rest))
		}
		if len(got2.Mutations) != len(got.Mutations) {
			t.Fatal("mutation count changed")
		}
	})
}

// FuzzSnapshotCells decodes arbitrary snapshot-chunk payloads.
func FuzzSnapshotCells(f *testing.F) {
	cells := []SnapshotCell{
		{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: Int(7)},
		{TableID: 2, RowID: ids.NewRowID(), ColumnID: ColumnTombstone},
	}
	f.Add(EncodeSnapshotCells(nil, cells))
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		got, _, err := DecodeSnapshotCells(data, DefaultLimits(), 10000)
		if err != nil {
			return
		}
		enc := EncodeSnapshotCells(nil, got)
		got2, rest, err := DecodeSnapshotCells(enc, DefaultLimits(), 10000)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if len(rest) != 0 || len(got2) != len(got) {
			t.Fatal("round-trip mismatch")
		}
	})
}

// FuzzCellState decodes arbitrary cell-state payloads.
func FuzzCellState(f *testing.F) {
	f.Add(EncodeCellState(nil, CellState{Value: Text("x")}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		st, err := DecodeCellState(data, DefaultLimits())
		if err != nil {
			return
		}
		enc := EncodeCellState(nil, st)
		st2, err := DecodeCellState(enc, DefaultLimits())
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if st2.Version != st.Version || !st2.Value.Equal(st.Value) {
			t.Fatal("round-trip mismatch")
		}
	})
}
