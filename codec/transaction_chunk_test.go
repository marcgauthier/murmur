package codec

import (
	"bytes"
	"testing"

	"github.com/marcgauthier/spedsql/ids"
)

func chunkTestBatch(payloadBytes int) *MutationBatch {
	payload := bytes.Repeat([]byte("chunk-payload/"), payloadBytes/14+1)[:payloadBytes]
	return &MutationBatch{
		ProtocolVersion: 1,
		TxID:            ids.NewTxID(),
		OriginNode:      ids.NewNodeID(),
		Sequence:        17,
		HLC:             12345,
		SchemaEpoch:     4,
		SchemaHash:      [32]byte{1, 2, 3},
		Mutations: []Mutation{{
			TableID: 1, RowID: ids.NewRowID(), ColumnID: 2, Value: Blob(payload),
		}},
	}
}

func chunkTestLimits() Limits {
	return Limits{MaxValueBytes: 1 << 20, MaxMutations: 100, MaxTransactionBytes: 1 << 20}
}

func decodeFrames(t *testing.T, frames [][]byte, max int64) []*TransactionChunk {
	t.Helper()
	chunks := make([]*TransactionChunk, len(frames))
	for i, frame := range frames {
		chunk, err := DecodeTransactionChunk(frame, max)
		if err != nil {
			t.Fatalf("decode chunk %d: %v", i, err)
		}
		chunks[i] = chunk
	}
	return chunks
}

func cloneChunks(src []*TransactionChunk) []*TransactionChunk {
	out := make([]*TransactionChunk, len(src))
	for i, c := range src {
		copy := *c
		copy.Data = append([]byte(nil), c.Data...)
		out[i] = &copy
	}
	return out
}

func TestTransactionChunkRoundTripAndCanonicalOrder(t *testing.T) {
	limits := chunkTestLimits()
	batch := chunkTestBatch(250_000)
	frames, err := EncodeTransactionChunks(batch, limits.MaxTransactionBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) < 2 {
		t.Fatalf("chunk count=%d, want multiple frames", len(frames))
	}
	chunks := decodeFrames(t, frames, limits.MaxTransactionBytes)
	for i, chunk := range chunks {
		if chunk.Index != uint32(i) || chunk.Count != uint32(len(chunks)) || len(chunk.Data) > TransactionChunkSize {
			t.Fatalf("chunk %d metadata = index %d count %d bytes %d", i, chunk.Index, chunk.Count, len(chunk.Data))
		}
	}
	for i, j := 0, len(chunks)-1; i < j; i, j = i+1, j-1 {
		chunks[i], chunks[j] = chunks[j], chunks[i]
	}
	got, err := AssembleTransactionChunks(chunks, limits)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(EncodeBatch(nil, got), EncodeBatch(nil, batch)) {
		t.Fatal("assembled transaction differs from original canonical batch")
	}
}

func TestVisitTransactionChunksMatchesBufferedEncoding(t *testing.T) {
	batch := chunkTestBatch(300_000)
	want, err := EncodeTransactionChunks(batch, chunkTestLimits().MaxTransactionBytes)
	if err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	err = VisitTransactionChunks(batch, chunkTestLimits().MaxTransactionBytes, func(index uint32, frame []byte) error {
		if index != uint32(len(got)) {
			t.Fatalf("visitor index=%d after %d frames", index, len(got))
		}
		got = append(got, append([]byte(nil), frame...))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("streaming emitted %d chunks, buffered emitted %d", len(got), len(want))
	}
	for i := range got {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("chunk %d differs", i)
		}
	}
}

var transactionChunkBenchmarkSink [][]byte

func BenchmarkTransactionChunkEncoding(b *testing.B) {
	batch := chunkTestBatch(16 << 20)
	b.Run("buffered", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(16 << 20)
		for i := 0; i < b.N; i++ {
			frames, err := EncodeTransactionChunks(batch, 32<<20)
			if err != nil {
				b.Fatal(err)
			}
			transactionChunkBenchmarkSink = frames
		}
	})
	b.Run("visited", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(16 << 20)
		for i := 0; i < b.N; i++ {
			if err := VisitTransactionChunks(batch, 32<<20, func(uint32, []byte) error { return nil }); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestTransactionChunkRejectsMalformedAndOverBudgetFrames(t *testing.T) {
	frames, err := EncodeTransactionChunks(chunkTestBatch(100_000), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeTransactionChunk(frames[0], 100); err == nil {
		t.Fatal("over-budget transaction chunk accepted")
	}
	if _, err := DecodeTransactionChunk(frames[0][:transactionChunkHeaderSize-1], 1<<20); err == nil {
		t.Fatal("truncated chunk header accepted")
	}
	trailing := append(append([]byte(nil), frames[0]...), 0)
	if _, err := DecodeTransactionChunk(trailing, 1<<20); err == nil {
		t.Fatal("chunk with trailing payload accepted")
	}
	malformed := append([]byte(nil), frames[0]...)
	malformed[transactionChunkHeaderSize-4] ^= 0x80 // corrupt declared payload length
	if _, err := DecodeTransactionChunk(malformed, 1<<20); err == nil {
		t.Fatal("payload length mismatch accepted")
	}
}

func TestTransactionChunkAssemblyRejectsIncompleteDuplicateMetadataAndDigest(t *testing.T) {
	limits := chunkTestLimits()
	frames, err := EncodeTransactionChunks(chunkTestBatch(180_000), limits.MaxTransactionBytes)
	if err != nil {
		t.Fatal(err)
	}
	chunks := decodeFrames(t, frames, limits.MaxTransactionBytes)
	if _, err := AssembleTransactionChunks(chunks[:len(chunks)-1], limits); err == nil {
		t.Fatal("incomplete chunk set accepted")
	}
	duplicate := cloneChunks(chunks)
	duplicate[1] = duplicate[0]
	if _, err := AssembleTransactionChunks(duplicate, limits); err == nil {
		t.Fatal("duplicate chunk index accepted")
	}
	metadata := cloneChunks(chunks)
	metadata[1].Sequence++
	if _, err := AssembleTransactionChunks(metadata, limits); err == nil {
		t.Fatal("inconsistent transaction identity accepted")
	}
	corrupt := cloneChunks(chunks)
	corrupt[0].Data[0] ^= 1
	if _, err := AssembleTransactionChunks(corrupt, limits); err == nil {
		t.Fatal("corrupt transaction digest accepted")
	}
}
