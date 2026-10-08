package murmur

import (
	"bytes"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/schema"
)

func mutationsHaveMergePolicies(mutations []codec.Mutation) bool {
	for _, mutation := range mutations {
		if mutation.Policy != schema.LWW {
			return true
		}
	}
	return false
}

// Queued merge projections can be observed while the durable writer finalizes
// and signs their batches. Keep an immutable payload copy for those readers.
func copyMergeMutations(src []codec.Mutation) []codec.Mutation {
	var out []codec.Mutation
	for _, mutation := range src {
		if mutation.Policy == schema.LWW && !(mutation.Value.Type == codec.TypeBlob && bytes.Equal(mutation.Value.B, []byte{0})) {
			continue
		}
		mutation.Value.B = append([]byte(nil), mutation.Value.B...)
		records := make([]codec.CRDTRecord, len(mutation.Records))
		for i, record := range mutation.Records {
			records[i] = codec.CRDTRecord{Key: append([]byte(nil), record.Key...), Data: append([]byte(nil), record.Data...)}
		}
		mutation.Records = records
		out = append(out, mutation)
	}
	return out
}

type pendingMergeBatch struct {
	version   crdt.Version
	mutations []codec.Mutation
}
