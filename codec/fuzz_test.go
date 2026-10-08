package codec

import (
	"crypto/ed25519"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
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

// FuzzTransactionChunk decodes arbitrary bytes as a transaction chunk: must
// never panic, and valid decodes must round-trip through the canonical
// encoder.
func FuzzTransactionChunk(f *testing.F) {
	b := &MutationBatch{
		ProtocolVersion: 1,
		TxID:            ids.NewTxID(),
		OriginNode:      ids.NewNodeID(),
		Sequence:        1,
		HLC:             100,
		SchemaEpoch:     1,
		Mutations:       []Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: Text(strings.Repeat("v", 70<<10))}},
	}
	if frames, err := EncodeTransactionChunks(b, DefaultLimits().MaxTransactionBytes); err == nil {
		for _, frame := range frames {
			f.Add(frame)
		}
	}
	f.Add([]byte{})
	f.Add([]byte{0, 1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		maxBytes := DefaultLimits().MaxTransactionBytes
		got, err := DecodeTransactionChunk(data, maxBytes)
		if err != nil {
			return
		}
		enc, err := EncodeTransactionChunk(nil, got, maxBytes)
		if err != nil {
			t.Fatalf("re-encode failed: %v", err)
		}
		got2, err := DecodeTransactionChunk(enc, maxBytes)
		if err != nil {
			t.Fatalf("re-decode failed: %v", err)
		}
		if got2.Index != got.Index || got2.Count != got.Count || string(got2.Data) != string(got.Data) {
			t.Fatal("round-trip mismatch")
		}
	})
}

// FuzzOriginSignatureRoundTrip fuzzes the signature canonical
// representation: every generated batch must sign, survive encode/decode,
// and verify with an identical digest and identical signature input.
func FuzzOriginSignatureRoundTrip(f *testing.F) {
	key := ed25519.NewKeyFromSeed(make([]byte, 32))
	pub := key.Public().(ed25519.PublicKey)
	dbid := ids.DBID{3}
	f.Add(uint32(8), uint32(10), uint32(0), uint8(0), []byte("hello"), uint8(1), uint8(4))
	f.Add(uint32(0), uint32(0), uint32(0), uint8(0), []byte{}, uint8(0), uint8(0))
	f.Fuzz(func(t *testing.T, tableID, columnID, flags uint32, policy uint8, value []byte, extra, typ uint8) {
		if len(value) > 2048 {
			return
		}
		var v Value
		switch typ % 5 {
		case 0:
			v = Null()
		case 1:
			v = Int(int64(len(value)) | int64(tableID)<<32)
		case 2:
			v = Real(float64(len(value)) + 0.5)
		case 3:
			v = Text(string(value))
		default:
			v = Blob(value)
		}
		var records []CRDTRecord
		if extra&1 != 0 {
			records = append(records, CRDTRecord{Key: value, Data: []byte{extra}})
		}
		if extra&2 != 0 {
			records = append(records, CRDTRecord{Key: []byte{byte(tableID), byte(columnID)}, Data: value})
		}
		// Mask out the tombstone bit: a tombstone with any other ColumnID
		// is legitimately rejected by the decoder, and the fixed second
		// mutation already covers the tombstone path.
		flags &^= uint32(FlagTombstone)
		b := &MutationBatch{
			ProtocolVersion: 5,
			OriginNode:      ids.NodeID{2},
			Sequence:        7,
			TxID:            ids.TxID{9},
			HLC:             8,
			SchemaEpoch:     1,
			Mutations: []Mutation{
				{TableID: tableID, RowID: ids.RowID{9}, ColumnID: columnID, Value: v, Flags: MutationFlags(flags), Policy: schema.MergePolicy(policy), Records: records},
				{TableID: 1, RowID: ids.RowID{10}, ColumnID: ColumnTombstone, Value: Null(), Flags: FlagTombstone, Policy: schema.LWW},
			},
		}
		if err := SignOrigin(b, dbid, key); err != nil {
			t.Fatalf("sign: %v", err)
		}
		decoded, rest, err := DecodeBatch(EncodeBatch(nil, b), DefaultLimits())
		if err != nil {
			t.Fatalf("round-trip decode: %v", err)
		}
		if len(rest) != 0 {
			t.Fatalf("%d trailing bytes", len(rest))
		}
		if MutationDigest(decoded) != b.MutationDigest {
			t.Fatal("digest changed across round-trip")
		}
		if string(OriginSigningBytes(decoded)) != string(OriginSigningBytes(b)) {
			t.Fatal("signature input changed across round-trip")
		}
		if err := VerifyOrigin(decoded, dbid, pub); err != nil {
			t.Fatalf("decoded batch rejected: %v", err)
		}
	})
}
