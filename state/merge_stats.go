package state

import (
	"context"
	"encoding/binary"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/schema"
)

// MergePolicyStats combines a durable metadata read cut with process counters.
// ActorComponents counts p/n components separately, scoped to each cell/branch.
// MetadataBytes counts logical encoded keys and values, not physical disk usage.
type MergePolicyStats struct {
	MetadataRecords uint64
	MetadataBytes   uint64
	ActorComponents uint64
	SetAdditions    uint64
	SetRemovals     uint64
	MergeAttempts   uint64
	MergeNanos      uint64
	RejectedBatches uint64
}

// MergePolicyStats scans retained metadata on demand; it is not a cheap poll.
func (s *Store) MergePolicyStats(ctx context.Context) (MergePolicyStats, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	stats := MergePolicyStats{MergeAttempts: s.policyMergeAttempts.Load(), MergeNanos: s.policyMergeNanos.Load(), RejectedBatches: s.policyRejected.Load()}
	err := s.snapshot(func(snap *snapshot) error {
		it, err := prefixIter(snap, []byte{prefixCRDT})
		if err != nil {
			return err
		}
		defer it.Close()
		for it.First(); it.Valid(); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key := it.Key()
			stats.MetadataRecords++
			stats.MetadataBytes += uint64(len(key) + len(it.Value()))
			if len(key) <= 25 {
				continue
			}
			table, col := binary.BigEndian.Uint32(key[1:5]), binary.BigEndian.Uint32(key[21:25])
			_, record, _ := codec.SplitEpochRecord(key[25:])
			if len(record) == 0 {
				continue
			}
			switch s.columnPolicy(table, col) {
			case schema.PN_COUNTER:
				stats.ActorComponents++
			case schema.OR_SET:
				if record[0] == 'a' {
					stats.SetAdditions++
				} else {
					stats.SetRemovals++
				}
			}
		}
		return it.Error()
	})
	return stats, err
}
