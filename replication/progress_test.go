package replication

import (
	"testing"

	"github.com/marcgauthier/murmur/ids"
)

func TestProgressRequestAndPageCodec(t *testing.T) {
	after := ids.NodeID{1, 2, 3}
	got, err := DecodeProgressRequest(EncodeProgressRequest(nil, after))
	if err != nil || got != after {
		t.Fatalf("request round trip = %v, %v", got, err)
	}
	if _, err := DecodeProgressRequest([]byte{1}); err == nil {
		t.Fatal("accepted truncated cursor")
	}

	page := ProgressPage{More: true, Items: []ProgressItem{{Origin: ids.NodeID{1}, Applied: 8, Observed: 8, RetainedFrom: 4, RetainedThrough: 8}}}
	gotPage, err := DecodeProgressPage(EncodeProgressPage(nil, page))
	if err != nil || len(gotPage.Items) != 1 || !gotPage.More || gotPage.Items[0] != page.Items[0] {
		t.Fatalf("page round trip = %#v, %v", gotPage, err)
	}
	page.Items[0].Observed = 7
	if _, err := DecodeProgressPage(EncodeProgressPage(nil, page)); err == nil {
		t.Fatal("accepted observed below applied")
	}
}

func TestChunkNeedCodec(t *testing.T) {
	if caps, err := NegotiateCapabilities(CapTransactionChunks | CapProgressPages); err != nil || caps&(CapTransactionChunks|CapProgressPages) != CapTransactionChunks|CapProgressPages {
		t.Fatalf("chunk/progress capability negotiation: %#x %v", caps, err)
	}
	n := ChunkNeed{Origin: ids.NodeID{1}, Sequence: 9, TxID: ids.TxID{2}, Missing: []uint32{1, 4, 7}}
	got, err := DecodeChunkNeed(EncodeChunkNeed(nil, n))
	if err != nil || got.Origin != n.Origin || got.Sequence != n.Sequence || got.TxID != n.TxID || len(got.Missing) != len(n.Missing) || got.Missing[2] != 7 {
		t.Fatalf("chunk need round trip: %+v, %v", got, err)
	}
	if _, err := DecodeChunkNeed(EncodeChunkNeed(nil, ChunkNeed{Origin: n.Origin, Sequence: n.Sequence, TxID: n.TxID, Missing: []uint32{2, 2}})); err == nil {
		t.Fatal("accepted duplicate missing chunk")
	}
}

func TestChunkAvailabilityPageCodec(t *testing.T) {
	item := ChunkAvailability{TxID: ids.TxID{1}, Origin: ids.NodeID{2}, Sequence: 4, ChunkCount: 10, Received: []bool{true, false, true, false, false, false, false, false, true, true}}
	page := ChunkAvailabilityPage{More: true, Items: []ChunkAvailability{item}}
	got, err := DecodeChunkAvailabilityPage(EncodeChunkAvailabilityPage(nil, page))
	if err != nil || !got.More || len(got.Items) != 1 || len(got.Items[0].Received) != 10 || !got.Items[0].Received[8] || got.Items[0].Received[7] {
		t.Fatalf("availability page roundtrip %+v %v", got, err)
	}
}
