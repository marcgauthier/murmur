package state

import (
	"fmt"
	"testing"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

// Measures join/read/projection cost at fixed causal cardinality. Signature
// verification and durability are deliberately outside this kernel benchmark.
func BenchmarkMergePolicy(b *testing.B) {
	for _, p := range []schema.MergePolicy{schema.LWW, schema.PN_COUNTER, schema.OR_SET, schema.MAX, schema.MIN} {
		sizes := []int{1}
		if p == schema.PN_COUNTER || p == schema.OR_SET {
			sizes = []int{1, 100, 1000}
		}
		for _, size := range sizes {
			b.Run(fmt.Sprintf("%s/%d", p, size), func(b *testing.B) {
				s, err := openSignedFixture(b.TempDir(), ids.NewNodeID(), ids.NewDBID(), Options{})
				if err != nil {
					b.Fatal(err)
				}
				defer s.Close()
				typ := schema.ColReal
				if p == schema.OR_SET || p == schema.PN_COUNTER || p == schema.LWW {
					typ = schema.ColText
				}
				tables := []schema.TableSchema{{ID: 1, Name: "items", PK: 1, Columns: []schema.ColumnSchema{{ID: 1, Name: "id", Type: schema.ColBlob}, {ID: 2, Name: "value", Type: typ, MergePolicy: p}}}}
				manifest, err := schema.NewGenesis(tables, 1, s.NodeID(), 1)
				if err != nil {
					b.Fatal(err)
				}
				if err = s.StoreSchemaRevision(manifest); err != nil {
					b.Fatal(err)
				}
				row := ids.NewRowID()
				m := codec.Mutation{TableID: 1, RowID: row, ColumnID: 2, Policy: p, Value: codec.Real(1)}
				seed := s.mem.newBatch()
				for i := 0; i < size; i++ {
					var key, data []byte
					if p == schema.PN_COUNTER {
						key = policyCounterKey(s.DBID(), ids.NewNodeID(), false)
						data = []byte{1}
					} else if p == schema.OR_SET {
						key = SetTag(s.DBID(), s.NodeID(), ids.NewTxID(), 0)
						data, _ = codec.SetString(fmt.Sprint(i)).Encode()
					} else {
						continue
					}
					if err = seed.Set(crdtKey(1, row, 2, key), codec.EncodeCellState(nil, codec.CellState{Version: crdt.Version{HLC: 1, NodeID: s.NodeID()}, Value: codec.Blob(data)})); err != nil {
						b.Fatal(err)
					}
					m.Records = []codec.CRDTRecord{{Key: key, Data: data}}
					m.Value = codec.Null()
				}
				if err = s.commitBatch(seed, false); err != nil {
					b.Fatal(err)
				}
				seed.Close()
				if p == schema.LWW {
					m.Value = codec.Text("value")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					stage := make(map[string]*remoteGroupCell)
					var order []string
					batch := s.mem.newBatch()
					if err = s.mergeRemoteGroupBatch(batch, &codec.MutationBatch{HLC: uint64(i + 2), OriginNode: s.NodeID(), Mutations: []codec.Mutation{m}}, stage, &order); err != nil {
						b.Fatal(err)
					}
					batch.Close()
				}
			})
		}
	}
}
