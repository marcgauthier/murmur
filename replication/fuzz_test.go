package replication

import (
	"bytes"
	"io"
	"testing"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
)

// FuzzFrame feeds arbitrary bytes to the frame reader: must never panic and
// must never allocate beyond MaxFrameBytes for one frame.
func FuzzFrame(f *testing.F) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, MsgPing, 0, []byte{1, 2, 3}); err != nil {
		f.Fatal(err)
	}
	f.Add(buf.Bytes())
	f.Add([]byte{})
	f.Add([]byte{0x52, 0x44, 0, 1, 0, 3, 0, 0, 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'})
	f.Add([]byte{0x52, 0x44, 0, 1, 0, 3, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Fuzz(func(t *testing.T, data []byte) {
		fr, err := ReadFrame(bytes.NewReader(data))
		if err != nil {
			return
		}
		if len(fr.Payload) > MaxFrameBytes {
			t.Fatalf("payload %d exceeds max", len(fr.Payload))
		}
		// Dispatch-level decoders must also not panic on the payload.
		switch fr.Type {
		case MsgHello, MsgWelcome:
			_, _ = DecodeHello(fr.Payload)
		case MsgBatches:
			_, _ = DecodeBatches(fr.Payload, codec.DefaultLimits())
		case MsgAck:
			_, _ = DecodeWatermarks(fr.Payload)
		case MsgNeed:
			_, _ = DecodeNeed(fr.Payload)
		case MsgSnapshotManifest:
			_, _, _ = codec.DecodeManifest(fr.Payload)
		case MsgSnapshotChunk:
			_, _ = DecodeSnapshotChunk(fr.Payload, codec.DefaultLimits(), 10000)
		case MsgError:
			_, _, _ = DecodeError(fr.Payload)
		case MsgSchemaRequest:
			_, _ = DecodeSchemaRequest(fr.Payload)
		case MsgSchemaManifest:
			_, _ = DecodeSchemaManifest(fr.Payload)
		case MsgSchemaAck:
			_, _ = DecodeSchemaAck(fr.Payload)
		}
	})
}

// FuzzHello decodes arbitrary Hello payloads.
func FuzzHello(f *testing.F) {
	h := &Hello{
		ProtocolVersion: 1, MinProtocolVersion: 1,
		NodeID: ids.NewNodeID(), DBID: ids.NewDBID(),
		Have: []codec.OriginWatermark{{Origin: ids.NewNodeID(), Sequence: 3}},
	}
	f.Add(EncodeHello(nil, h))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if _, err := DecodeHello(data); err != nil {
			return
		}
	})
}

// FuzzBatchesMessage decodes arbitrary Batches payloads.
func FuzzBatchesMessage(f *testing.F) {
	b := &codec.MutationBatch{
		ProtocolVersion: 1, TxID: ids.NewTxID(), OriginNode: ids.NewNodeID(),
		Sequence: 1, HLC: 1, SchemaEpoch: 1,
		Mutations: []codec.Mutation{{Value: codec.Int(1)}},
	}
	f.Add(EncodeBatches(nil, []*codec.MutationBatch{b}))
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if _, err := DecodeBatches(data, codec.DefaultLimits()); err != nil {
			return
		}
	})
}

// FuzzSnapshotChunkMessage decodes arbitrary snapshot-chunk payloads,
// including the zstd flag path (decompression is size-bounded).
func FuzzSnapshotChunkMessage(f *testing.F) {
	c := &SnapshotChunk{Last: true, Cells: []codec.SnapshotCell{{Value: codec.Int(1)}}}
	f.Add(EncodeSnapshotChunk(nil, c))
	f.Add([]byte{})
	f.Add([]byte{1, 0, 0, 0, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if _, err := DecodeSnapshotChunk(data, codec.DefaultLimits(), 10000); err != nil {
			return
		}
	})
}

var _ = io.EOF
