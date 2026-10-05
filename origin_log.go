package murmur

import (
	"context"
	"fmt"

	"github.com/marcgauthier/murmur/codec"
)

// ScanReplicationLog visits complete, signed transactions without the bridge
// exporter's policy projection. It supports origin-proof inspection and
// preserves the original mutation bytes, identity, digest and signature.
func (db *DB) ScanReplicationLog(ctx context.Context, origin NodeID, fromSeq uint64, maxBatches, maxBytes int, visit func(*codec.MutationBatch) error) (uint64, error) {
	if visit == nil {
		return fromSeq, fmt.Errorf("murmur: nil replication log visitor")
	}
	if err := db.requireRead(); err != nil {
		return fromSeq, err
	}
	if err := ctx.Err(); err != nil {
		return fromSeq, err
	}
	return db.store.LogScan(origin, fromSeq, maxBatches, maxBytes, func(b *codec.MutationBatch) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return visit(b)
	})
}
