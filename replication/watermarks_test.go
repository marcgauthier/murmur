package replication

import (
	"encoding/binary"
	"testing"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
)

func TestWatermarksRoundTripAndRejectMalformedPayloads(t *testing.T) {
	want := []codec.OriginWatermark{
		{Origin: ids.NewNodeID(), Sequence: 1},
		{Origin: ids.NewNodeID(), Sequence: 1<<40 + 7},
	}
	encoded := EncodeWatermarks(nil, want)
	got, err := DecodeWatermarks(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("decoded %d watermarks, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("watermark[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}

	for name, payload := range map[string][]byte{
		"missing count":   nil,
		"truncated entry": {0, 0, 0, 1, 1, 2, 3},
		"absurd count": func() []byte {
			b := make([]byte, 4)
			binary.BigEndian.PutUint32(b, 4097)
			return b
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeWatermarks(payload); err == nil {
				t.Fatal("expected malformed payload to be rejected")
			}
		})
	}
}
