package state

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

const (
	// maxLocalGroupTransactions bounds one CommitLocalGroup call. The DB
	// layer caps groups below this; the store rejects anything larger as
	// a defensive backstop.
	maxLocalGroupTransactions = 512
	// maxLocalGroupBytes bounds the total encoded size of one
	// CommitLocalGroup call, matching the remote-group cap.
	maxLocalGroupBytes = 64 << 20
)

// LocalGroupMember reports one member's outcome within a CommitLocalGroup.
// Members aligns 1:1 with the input batches, including duplicates: a
// duplicate TxID neither assigns a sequence nor bumps the generation.
type LocalGroupMember struct {
	// Applied reports whether the member was durably recorded (false for
	// duplicates of an already-receipted TxID).
	Applied bool
	// Sequence is the assigned local sequence (zero when not applied).
	Sequence uint64
	// Generation is the state generation after this member's sub-commit.
	Generation uint64
}

// LocalGroupResult summarizes a committed local group.
type LocalGroupResult struct {
	MergeResult
	// GenerationBefore is the state generation before the group committed.
	GenerationBefore uint64
	// Members aligns 1:1 with the input batches.
	Members []LocalGroupMember
}

// CommitLocalGroup durably records and merges an ordered group of local
// transactions in one Pebble commit. Each transaction keeps its own log row,
// receipt, and assigned origin sequence, and bumps the generation once,
// exactly as if the members had committed sequentially in input order —
// concurrent writers therefore share one fsync while keeping synchronous
// durability each.
//
// Ordering contract: input order must match SQL-commit order with strictly
// increasing HLCs, so the staged LWW merge resolves intra-group conflicts
// the same way the materializer already did (later SQL commit wins).
// A gap-free caller-visible sequence follows: applied members receive
// contiguous sequences in input order.
//
// A duplicate TxID (already receipted, or repeated within the group) is
// acknowledged without reapplying, matching CommitLocal idempotency.
func (s *Store) CommitLocalGroup(_ context.Context, batches []*codec.MutationBatch) (LocalGroupResult, error) {
	if len(batches) == 0 {
		return LocalGroupResult{}, fmt.Errorf("state: empty local group")
	}
	if len(batches) > maxLocalGroupTransactions {
		return LocalGroupResult{}, fmt.Errorf("state: local group has %d transactions; maximum is %d", len(batches), maxLocalGroupTransactions)
	}
	if len(batches) == 1 {
		return s.commitLocalSingle(batches[0])
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	var groupBytes int64
	for _, batch := range batches {
		if batch.OriginNode != s.nodeID {
			return LocalGroupResult{}, fmt.Errorf("state: CommitLocalGroup with foreign origin")
		}
		if len(batch.Mutations) == 0 {
			return LocalGroupResult{}, fmt.Errorf("state: empty batch in local group")
		}
		if err := checkBatchLimits(batch, s.limits); err != nil {
			return LocalGroupResult{}, err
		}
		groupBytes += int64(codec.EncodedBatchSize(batch))
		if groupBytes > maxLocalGroupBytes {
			return LocalGroupResult{}, fmt.Errorf("state: local group exceeds %d bytes: %w", maxLocalGroupBytes, ErrTooBig)
		}
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	result := LocalGroupResult{Members: make([]LocalGroupMember, len(batches))}
	staged := make(map[string]*remoteGroupCell)
	order := make([]string, 0)
	seenTx := make(map[ids.TxID]struct{}, len(batches))
	seq, err := s.readU64Direct(sysLocalSeq)
	if err != nil {
		return LocalGroupResult{}, err
	}
	gen, err := s.readU64Direct(sysGeneration)
	if err != nil {
		return LocalGroupResult{}, err
	}
	result.GenerationBefore = gen
	maxHLC, err := s.readU64Direct(sysHLC)
	if err != nil {
		return LocalGroupResult{}, err
	}
	dirty := false
	for i, batch := range batches {
		if _, seen := seenTx[batch.TxID]; seen {
			result.Members[i] = LocalGroupMember{Generation: gen}
			continue
		}
		seenTx[batch.TxID] = struct{}{}
		if _, err := s.getDirect(ReceiptKey(batch.TxID)); err == nil {
			result.Members[i] = LocalGroupMember{Generation: gen}
			continue
		} else if !isNotFound(err) {
			return LocalGroupResult{}, err
		}
		seq++
		batch.Sequence = seq
		batch.ProtocolVersion = 5
		if err := s.finalizeLocalPolicies(batch, staged, &order); err != nil {
			return LocalGroupResult{}, err
		}
		if err := checkBatchLimits(batch, s.limits); err != nil {
			return LocalGroupResult{}, err
		}
		if err := codec.SignOrigin(batch, s.dbID, s.openOpt.OriginSigning.PrivateKey); err != nil {
			return LocalGroupResult{}, err
		}
		if err := s.mergeRemoteGroupBatch(b, batch, staged, &order); err != nil {
			return LocalGroupResult{}, err
		}
		if err := b.Set(LogKey(s.nodeID, seq), codec.EncodeBatch(nil, batch), nil); err != nil {
			return LocalGroupResult{}, err
		}
		var receipt [24]byte
		copy(receipt[:16], s.nodeID[:])
		binary.BigEndian.PutUint64(receipt[16:], seq)
		if err := b.Set(ReceiptKey(batch.TxID), receipt[:], nil); err != nil {
			return LocalGroupResult{}, err
		}
		maxHLC = maxU64(maxHLC, batch.HLC)
		gen++
		result.Members[i] = LocalGroupMember{Applied: true, Sequence: seq, Generation: gen}
		result.Applied = true
		dirty = true
	}
	if !dirty {
		result.Generation = gen
		return result, nil
	}
	var writeErr error
	result.Winners, writeErr = writeStagedCells(b, staged, order)
	if writeErr != nil {
		return LocalGroupResult{}, writeErr
	}

	if err := b.Set(RecvKey(s.nodeID), encodeU64(seq), nil); err != nil {
		return LocalGroupResult{}, err
	}
	if err := b.Set(SysKey(sysLocalSeq), encodeU64(seq), nil); err != nil {
		return LocalGroupResult{}, err
	}
	if err := b.Set(SysKey(sysHLC), encodeU64(maxU64(maxHLC, s.clock.Max())), nil); err != nil {
		return LocalGroupResult{}, err
	}
	if err := b.Set(SysKey(sysGeneration), encodeU64(gen), nil); err != nil {
		return LocalGroupResult{}, err
	}
	if err := s.commitBatch(b, s.writeOpts); err != nil {
		return LocalGroupResult{}, err
	}
	result.Generation = gen
	return result, nil
}

// commitLocalSingle commits a one-member group through CommitLocal so solo
// commits keep byte-identical behavior with the non-group path.
func (s *Store) commitLocalSingle(batch *codec.MutationBatch) (LocalGroupResult, error) {
	res, err := s.CommitLocal(context.Background(), batch)
	if err != nil {
		return LocalGroupResult{}, err
	}
	before := res.Generation
	if res.Applied {
		before--
	}
	return LocalGroupResult{
		MergeResult:      res,
		GenerationBefore: before,
		Members:          []LocalGroupMember{{Applied: res.Applied, Sequence: batch.Sequence, Generation: res.Generation}},
	}, nil
}
