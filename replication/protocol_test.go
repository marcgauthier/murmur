package replication

import (
	"bytes"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

func TestFrameRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MsgPing, 0, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	f, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if f.Type != MsgPing || !bytes.Equal(f.Payload, []byte{1, 2, 3}) {
		t.Fatalf("mismatch: %+v", f)
	}
}

func TestFrameRejects(t *testing.T) {
	// Bad magic.
	if _, err := ReadFrame(bytes.NewReader(make([]byte, 12))); err == nil {
		t.Fatal("expected bad magic error")
	}
	// Oversize length prefix (no 4GB allocation: must fail fast).
	hdr := []byte{0x52, 0x44, 0, 1, 0, 3, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF}
	if _, err := ReadFrame(bytes.NewReader(hdr)); err == nil {
		t.Fatal("expected oversize error")
	}
	// Truncated payload.
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MsgPing, 0, []byte{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrame(bytes.NewReader(buf.Bytes()[:len(buf.Bytes())-2])); err == nil {
		t.Fatal("expected truncation error")
	}
}

func TestHelloRoundTrip(t *testing.T) {
	h := &Hello{
		ProtocolVersion: ProtocolVersion, MinProtocolVersion: MinProtocolVersion,
		NodeID: ids.NewNodeID(), DBID: fixtureDBID,
		SchemaEpoch: 9, SchemaAuthorNode: ids.NewNodeID(), SchemaTimeCreated: 77,
		Capabilities: CapMergePolicies | CapOriginSignatures | (CapZstd),
		Have:         []codec.OriginWatermark{{Origin: ids.NewNodeID(), Sequence: 12}},
	}
	h.SchemaHash[31] = 7
	got, err := DecodeHello(EncodeHello(nil, h))
	if err != nil {
		t.Fatal(err)
	}
	if got.NodeID != h.NodeID || got.SchemaEpoch != 9 || len(got.Have) != 1 ||
		got.Have[0].Sequence != 12 || got.SchemaHash != h.SchemaHash ||
		got.SchemaAuthorNode != h.SchemaAuthorNode || got.SchemaTimeCreated != 77 {
		t.Fatalf("mismatch: %+v", got)
	}
}

func TestSchemaMessagesRoundTrip(t *testing.T) {
	req := &SchemaRequest{WantCurrent: true, WantIDs: [][32]byte{{1}, {2}}}
	gotReq, err := DecodeSchemaRequest(EncodeSchemaRequest(nil, req))
	if err != nil {
		t.Fatal(err)
	}
	if !gotReq.WantCurrent || len(gotReq.WantIDs) != 2 || gotReq.WantIDs[1] != [32]byte{2} {
		t.Fatalf("request mismatch: %+v", gotReq)
	}
	if _, err := DecodeSchemaRequest([]byte{1, 2}); err == nil {
		t.Fatal("truncated schema request accepted")
	}

	ack := &SchemaAck{Version: 5, Hash: [32]byte{9}}
	gotAck, err := DecodeSchemaAck(EncodeSchemaAck(nil, ack))
	if err != nil {
		t.Fatal(err)
	}
	if *gotAck != *ack {
		t.Fatalf("ack mismatch: %+v", gotAck)
	}
	if _, err := DecodeSchemaAck([]byte{1, 2, 3}); err == nil {
		t.Fatal("malformed schema ack accepted")
	}

	// Manifest messages authenticate every revision through schema.Decode.
	empty := EncodeSchemaManifest(nil, &SchemaManifestMsg{})
	if _, err := DecodeSchemaManifest(empty); err == nil {
		t.Fatal("empty manifest message accepted")
	}
}

func TestBatchesRoundTrip(t *testing.T) {
	batches := []*codec.MutationBatch{
		{
			ProtocolVersion: 1, TxID: ids.NewTxID(), OriginNode: ids.NewNodeID(),
			Sequence: 1, HLC: 100, SchemaEpoch: 1,
			Mutations: []codec.Mutation{{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Text("v")}},
		},
	}
	got, err := DecodeBatches(encodeBatchesFixture(nil, batches), codec.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Sequence != 1 {
		t.Fatalf("mismatch: %+v", got)
	}
}

func TestNeedAndErrorRoundTrip(t *testing.T) {
	n := Need{Origin: ids.NewNodeID(), FromSeq: 77}
	got, err := DecodeNeed(EncodeNeed(nil, n))
	if err != nil || got != n {
		t.Fatalf("need mismatch: %+v %v", got, err)
	}
	code, msg, err := DecodeError(EncodeError(nil, ErrSnapshotRequired, "gone"))
	if err != nil || code != ErrSnapshotRequired || msg != "gone" {
		t.Fatalf("error mismatch: %d %q %v", code, msg, err)
	}
}

func TestSnapshotChunkRoundTrip(t *testing.T) {
	c := &SnapshotChunk{Index: 7, Last: true, Cells: []codec.SnapshotCell{
		{TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: codec.Int(3)},
	}}
	got, err := DecodeSnapshotChunk(EncodeSnapshotChunk(nil, c), codec.DefaultLimits(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Last || got.Index != 7 || len(got.Cells) != 1 {
		t.Fatalf("mismatch: %+v", got)
	}
	if ProtocolVersion != 5 || MinProtocolVersion != 5 {
		t.Fatal("snapshot format requires replication protocol v5")
	}
}
