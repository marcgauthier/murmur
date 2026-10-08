package state

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/schema"
)

// CommitLocalRecords persists a node-local typed transaction without adding a
// replication-log entry. Local rows use a separate key namespace and never
// appear in exported snapshots or replicated cell scans. The mutation subset
// currently supports LWW cells and row tombstones; CRDT fields remain
// available on replicated tables.
func (s *Store) CommitLocalRecords(ctx context.Context, batch *codec.MutationBatch) (MergeResult, error) {
	if batch == nil || batch.OriginNode != s.nodeID || batch.TxID.IsZero() || len(batch.Mutations) == 0 {
		return MergeResult{}, fmt.Errorf("state: local record batch requires origin, transaction ID, and mutations")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return MergeResult{}, err
	}
	if err := validateLocalRecordMutations(batch.Mutations, s.limits); err != nil {
		return MergeResult{}, err
	}
	if err := checkBatchLimits(batch, s.limits); err != nil {
		return MergeResult{}, err
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var res MergeResult
	if _, err := s.getDirect(ReceiptKey(batch.TxID)); err == nil {
		gen, err := s.readU64Direct(sysGeneration)
		if err != nil {
			return MergeResult{}, err
		}
		res.Generation = gen
		return res, nil
	} else if !isNotFound(err) {
		return MergeResult{}, err
	}
	if _, err := s.getDirect(LocalReceiptKey(batch.TxID)); err == nil {
		gen, err := s.readU64Direct(sysGeneration)
		if err != nil {
			return MergeResult{}, err
		}
		res.Generation = gen
		return res, nil
	} else if !isNotFound(err) {
		return MergeResult{}, err
	}
	b := s.mem.newBatch()
	defer b.Close()
	if err := s.mergeLocalRecordMutations(ctx, b, batch.Mutations, batch.Version()); err != nil {
		return MergeResult{}, err
	}
	gen, err := s.readU64Direct(sysGeneration)
	if err != nil {
		return MergeResult{}, err
	}
	gen++
	if err := b.Set(SysKey(sysGeneration), encodeU64(gen)); err != nil {
		return MergeResult{}, err
	}
	if err := b.Set(SysKey(sysHLC), encodeU64(maxU64(batch.HLC, s.clock.Max()))); err != nil {
		return MergeResult{}, err
	}
	if err := b.Set(LocalReceiptKey(batch.TxID), []byte{1}); err != nil {
		return MergeResult{}, err
	}
	if err := s.commitBatch(b, s.syncCommits); err != nil {
		return MergeResult{}, err
	}
	res.Applied = true
	res.Generation = gen
	return res, nil
}

// CommitLocalWithRecords atomically commits replicated mutations and
// node-local typed records in one Spool transaction. The local records share
// the replicated commit's transaction ID and version but are excluded from
// its signed replication batch and log payload.
func (s *Store) CommitLocalWithRecords(ctx context.Context, batch *codec.MutationBatch, local []codec.Mutation) (MergeResult, error) {
	if batch == nil || batch.OriginNode != s.nodeID || batch.TxID.IsZero() || len(batch.Mutations) == 0 || len(local) == 0 {
		return MergeResult{}, fmt.Errorf("state: mixed commit requires replicated and local mutations")
	}
	if err := validateLocalRecordMutations(local, s.limits); err != nil {
		return MergeResult{}, err
	}
	if err := checkBatchLimits(batch, s.limits); err != nil {
		return MergeResult{}, err
	}
	localSidecar := *batch
	localSidecar.Mutations = local
	if err := checkBatchLimits(&localSidecar, s.limits); err != nil {
		return MergeResult{}, err
	}
	if s.limits.MaxMutations > 0 && len(batch.Mutations)+len(local) > s.limits.MaxMutations {
		return MergeResult{}, fmt.Errorf("state: mixed transaction mutation count exceeds limit: %w", ErrTooBig)
	}
	if s.limits.MaxTransactionBytes > 0 && int64(codec.EncodedBatchSize(batch))+int64(codec.EncodedBatchSize(&localSidecar)) > s.limits.MaxTransactionBytes {
		return MergeResult{}, fmt.Errorf("state: mixed transaction encoded size exceeds limit: %w", ErrTooBig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return MergeResult{}, err
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	if err := ctx.Err(); err != nil {
		return MergeResult{}, err
	}
	if batch.OriginNode != s.nodeID {
		return MergeResult{}, fmt.Errorf("state: CommitLocal with foreign origin")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.getDirect(ReceiptKey(batch.TxID)); err == nil {
		gen, err := s.readU64Direct(sysGeneration)
		if err != nil {
			return MergeResult{}, err
		}
		return MergeResult{Generation: gen}, nil
	} else if !isNotFound(err) {
		return MergeResult{}, err
	}
	seq, err := s.readU64Direct(sysLocalSeq)
	if err != nil {
		return MergeResult{}, err
	}
	seq++
	batch.Sequence = seq
	batch.ProtocolVersion = 6
	if err := s.finalizeLocalPolicies(batch, make(map[string]*remoteGroupCell), new([]string)); err != nil {
		return MergeResult{}, err
	}
	if err := checkBatchLimits(batch, s.limits); err != nil {
		return MergeResult{}, err
	}
	if err := codec.SignOrigin(batch, s.dbID, s.openOpt.OriginSigning.PrivateKey); err != nil {
		return MergeResult{}, err
	}
	ver := batch.Version()
	b := s.mem.newBatch()
	defer b.Close()
	winners, err := s.mergeIntoBatch(b, batch, ver)
	if err != nil {
		return MergeResult{}, err
	}
	if err := s.mergeLocalRecordMutations(ctx, b, local, ver); err != nil {
		return MergeResult{}, err
	}
	if err := b.Set(LogKey(s.nodeID, seq), codec.EncodeBatch(nil, batch)); err != nil {
		return MergeResult{}, err
	}
	if err := b.Set(RecvKey(s.nodeID), encodeU64(seq)); err != nil {
		return MergeResult{}, err
	}
	if err := b.Set(SysKey(sysLocalSeq), encodeU64(seq)); err != nil {
		return MergeResult{}, err
	}
	if err := b.Set(SysKey(sysHLC), encodeU64(maxU64(batch.HLC, s.clock.Max()))); err != nil {
		return MergeResult{}, err
	}
	var receipt [24]byte
	copy(receipt[:16], s.nodeID[:])
	binary.BigEndian.PutUint64(receipt[16:], seq)
	if err := b.Set(ReceiptKey(batch.TxID), receipt[:]); err != nil {
		return MergeResult{}, err
	}
	if err := b.Set(LocalReceiptKey(batch.TxID), []byte{1}); err != nil {
		return MergeResult{}, err
	}
	gen, err := s.readU64Direct(sysGeneration)
	if err != nil {
		return MergeResult{}, err
	}
	gen++
	if err := b.Set(SysKey(sysGeneration), encodeU64(gen)); err != nil {
		return MergeResult{}, err
	}
	if err := s.commitBatch(b, s.syncCommits); err != nil {
		return MergeResult{}, err
	}
	return MergeResult{Winners: winners, Applied: true, Generation: gen}, nil
}

func validateLocalRecordMutations(mutations []codec.Mutation, limits codec.Limits) error {
	if len(mutations) == 0 {
		return fmt.Errorf("state: empty local record mutations")
	}
	for i := range mutations {
		m := &mutations[i]
		if m.Policy != schema.LWW || len(m.Records) != 0 || m.Flags&^(codec.FlagTombstone) != 0 {
			return fmt.Errorf("state: local record mutation %d must be LWW: %w", i, schema.ErrUnsupportedSchema)
		}
		if m.IsTombstone() && m.ColumnID != codec.ColumnTombstone || !m.IsTombstone() && m.ColumnID == codec.ColumnTombstone {
			return fmt.Errorf("state: malformed local record tombstone")
		}
		if limits.MaxValueBytes > 0 {
			size := 0
			if m.Value.Type == codec.TypeBlob {
				size = len(m.Value.B)
			}
			if m.Value.Type == codec.TypeText {
				size = len(m.Value.S)
			}
			if size > limits.MaxValueBytes {
				return fmt.Errorf("state: local record value exceeds limit: %w", ErrTooBig)
			}
		}
	}
	return nil
}

func (s *Store) mergeLocalRecordMutations(ctx context.Context, b *batch, mutations []codec.Mutation, ver crdt.Version) error {
	for i := range mutations {
		if err := ctx.Err(); err != nil {
			return err
		}
		m := &mutations[i]
		key := LocalCellKey(m.TableID, m.RowID, m.ColumnID)
		storedRaw, _, err := b.Get(key)
		present := err == nil
		if err != nil && !isNotFound(err) {
			return err
		}
		if m.IsTombstone() {
			var stored crdt.Version
			if present {
				stored, err = codec.DecodeTombstone(storedRaw)
				if err != nil {
					return fmt.Errorf("state: corrupt local row tombstone: %w", err)
				}
			}
			if crdt.MergeCell(ver, stored, present) == crdt.MergeTake {
				if err := b.Set(key, codec.EncodeTombstone(nil, ver)); err != nil {
					return err
				}
			}
			continue
		}
		var stored codec.CellState
		if present {
			stored, err = codec.DecodeCellState(storedRaw, s.limits)
			if err != nil {
				return fmt.Errorf("state: corrupt local cell: %w", err)
			}
		}
		if crdt.MergeCell(ver, stored.Version, present) == crdt.MergeTake {
			if err := b.Set(key, codec.EncodeCellState(nil, codec.CellState{Version: ver, Value: m.Value})); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetLocalRow reads the current cells and optional tombstone for one local row.
func (s *Store) GetLocalRow(table uint32, row ids.RowID) (map[uint32]codec.CellState, crdt.Version, bool, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	cells := make(map[uint32]codec.CellState)
	var tomb crdt.Version
	var tombPresent bool
	err := s.snapshot(func(snap *snapshot) error {
		prefix := LocalCellTablePrefix(table)
		prefix = append(prefix, row[:]...)
		it, err := prefixIter(snap, prefix)
		if err != nil {
			return err
		}
		defer it.Close()
		for it.SeekGE(prefix); it.Valid(); it.Next() {
			key := append([]byte(nil), it.Key()...)
			_, _, col, ok := ParseLocalCellKey(key)
			if !ok || col == codec.ColumnTombstone {
				continue
			}
			st, err := codec.DecodeCellState(append([]byte(nil), it.Value()...), s.limits)
			if err != nil {
				return fmt.Errorf("state: corrupt local cell: %w", err)
			}
			cells[col] = st
		}
		if err := it.Error(); err != nil {
			return err
		}
		raw, err := snapGet(snap, LocalCellKey(table, row, codec.ColumnTombstone))
		if err != nil {
			if isNotFound(err) {
				return nil
			}
			return err
		}
		tomb, err = codec.DecodeTombstone(raw)
		if err != nil {
			return fmt.Errorf("state: corrupt local row tombstone: %w", err)
		}
		tombPresent = true
		return nil
	})
	if err != nil {
		return nil, crdt.Version{}, false, err
	}
	return cells, tomb, tombPresent, nil
}

// IterateLocalTable iterates local rows in row-ID order, including tombstoned
// rows. Local data is never included by IterateTable or snapshot export.
func (s *Store) IterateLocalTable(table uint32, fn func(*Row) error) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.snapshot(func(snap *snapshot) error {
		prefix := LocalCellTablePrefix(table)
		it, err := prefixIter(snap, prefix)
		if err != nil {
			return err
		}
		defer it.Close()
		var current *Row
		flush := func() error {
			if current == nil {
				return nil
			}
			row := current
			current = nil
			if raw, err := snapGet(snap, LocalCellKey(table, row.ID, codec.ColumnTombstone)); err == nil {
				row.Tomb.Version, err = codec.DecodeTombstone(raw)
				if err != nil {
					return fmt.Errorf("state: corrupt local row tombstone: %w", err)
				}
				row.Tomb.Present = true
			} else if !isNotFound(err) {
				return err
			}
			return fn(row)
		}
		for it.SeekGE(prefix); it.Valid(); it.Next() {
			_, rowID, col, ok := ParseLocalCellKey(append([]byte(nil), it.Key()...))
			if !ok {
				continue
			}
			if current == nil || current.ID != rowID {
				if err := flush(); err != nil {
					return err
				}
				current = &Row{Table: table, ID: rowID, Cells: make(map[uint32]codec.CellState)}
			}
			if col == codec.ColumnTombstone {
				continue
			}
			st, err := codec.DecodeCellState(append([]byte(nil), it.Value()...), s.limits)
			if err != nil {
				return fmt.Errorf("state: corrupt local cell: %w", err)
			}
			current.Cells[col] = st
			if crdt.CompareVersion(st.Version, current.Newest) > 0 {
				current.Newest = st.Version
			}
		}
		if err := it.Error(); err != nil {
			return err
		}
		return flush()
	})
}
