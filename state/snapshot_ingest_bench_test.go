package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/crdt"
	spedsqlcrypto "github.com/marcgauthier/spedsql/crypto"
	"github.com/marcgauthier/spedsql/ids"
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
	for _, mode := range []string{"batch", "sstable-ingest"} {
		batch.Run(mode, func(b *testing.B) {
			dir := b.TempDir()
			dbID, node := ids.NewDBID(), ids.NewNodeID()
			var cryptoDBID [16]byte
			copy(cryptoDBID[:], dbID[:])
			key := make([]byte, 32)
			for i := range key {
				key[i] = byte(i + 1)
			}
			registry, err := spedsqlcrypto.OpenRegistry(dir+"/keys", spedsqlcrypto.Static(key), cryptoDBID)
			if err != nil {
				b.Fatal(err)
			}
			defer registry.Close()
			encFS, err := spedsqlcrypto.NewEncryptedFS(spedsqlcrypto.FSOptions{Base: vfs.Default, Registry: registry, DBID: cryptoDBID})
			if err != nil {
				b.Fatal(err)
			}
			s, err := Open(dir+"/data", node, dbID, Options{FS: encFS, Limits: codec.DefaultLimits()})
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
					writeBatch := s.db.NewBatch()
					_, _, err := s.mergeSnapshotChunk(writeBatch, raw, nil)
					if err == nil {
						err = s.commitBatch(writeBatch, pebble.Sync)
					}
					_ = writeBatch.Close()
					if err != nil {
						b.Fatal(fmt.Errorf("batch merge: %w", err))
					}
				} else if _, _, err := s.ingestSnapshotChunkToSST(context.Background(), raw, nil); err != nil {
					b.Fatal(fmt.Errorf("sstable ingest: %w", err))
				}
			}
		})
	}
}
