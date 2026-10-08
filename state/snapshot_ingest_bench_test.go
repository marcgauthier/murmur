package state

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
)

func BenchmarkSnapshotChunkMergeEncrypted(batch *testing.B) {
	const rows = 12_000
	payload := make([]byte, 900)
	for i := range payload {
		payload[i] = byte(i * 31)
	}
	var sequence uint64
	makeChunk := func() []byte {
		sequence++
		cells := make([]codec.SnapshotCell, rows)
		origin := ids.NewNodeID()
		base := sequence << 32
		for i := range cells {
			var row ids.RowID
			binary.BigEndian.PutUint64(row[8:], base+uint64(i))
			cells[i] = codec.SnapshotCell{TableID: 1, RowID: row, ColumnID: 1, Version: crdt.Version{HLC: uint64(i + 1), NodeID: origin}, Value: codec.Blob(payload)}
		}
		return codec.EncodeSnapshotCells(nil, cells)
	}
	for _, mode := range []string{"batch", "chunk-commit"} {
		batch.Run(mode, func(b *testing.B) {
			dir := b.TempDir()
			dbID, node := ids.NewDBID(), ids.NewNodeID()
			s, err := openSignedFixture(dir+"/data", node, dbID, Options{Limits: codec.DefaultLimits()})
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			b.ReportAllocs()
			b.SetBytes(rows * 950)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				raw := makeChunk()
				if mode == "batch" {
					writeBatch := s.mem.newBatch()
					_, _, err := s.mergeSnapshotChunk(writeBatch, raw, nil)
					if err == nil {
						err = s.commitBatch(writeBatch, true)
					}
					_ = writeBatch.Close()
					if err != nil {
						b.Fatal(fmt.Errorf("batch merge: %w", err))
					}
				} else if _, _, err := s.commitSnapshotChunk(raw, nil); err != nil {
					b.Fatal(fmt.Errorf("chunk commit: %w", err))
				}
			}
		})
	}
}
