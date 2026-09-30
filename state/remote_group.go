package state

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
)

type remoteGroupCell struct {
	key     []byte
	version crdt.Version
	value   codec.Value
	present bool
	changed bool
	row     ids.RowID
	table   uint32
	column  uint32
	tomb    bool
}

// CommitRemoteGroup durably records and merges an ordered group of remote
// transactions in one Pebble commit. Each transaction keeps its own log row,
// receipt, origin sequence, and generation increment. A gap aborts the whole
// group, so no caller can acknowledge a partially applied group.
func (s *Store) CommitRemoteGroup(ctx context.Context, batches []*codec.MutationBatch) (MergeResult, error) {
	if len(batches) == 0 {
		return MergeResult{}, fmt.Errorf("state: empty remote group")
	}
	if len(batches) > 64 {
		return MergeResult{}, fmt.Errorf("state: remote group has %d transactions; maximum is 64", len(batches))
	}
	if len(batches) == 1 {
		return s.CommitRemote(ctx, batches[0])
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	var groupBytes int64
	for _, batch := range batches {
		if len(batch.Mutations) == 0 {
			return MergeResult{}, fmt.Errorf("state: empty batch in remote group")
		}
		if err := checkBatchLimits(batch, s.limits); err != nil {
			return MergeResult{}, err
		}
		groupBytes += int64(len(codec.EncodeBatch(nil, batch)))
		if groupBytes > 64<<20 {
			return MergeResult{}, fmt.Errorf("state: remote group exceeds 64 MiB")
		}
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	result := MergeResult{}
	dirty := false
	watermarks := make(map[ids.NodeID]uint64)
	watermarkRead := make(map[ids.NodeID]bool)
	staged := make(map[string]*remoteGroupCell)
	order := make([]string, 0)
	receipts := make(map[ids.TxID]struct{}, len(batches))
	gen, err := s.readU64Direct(sysGeneration)
	if err != nil {
		return MergeResult{}, err
	}
	maxHLC, err := s.readU64Direct(sysHLC)
	if err != nil {
		return MergeResult{}, err
	}
	for _, batch := range batches {
		if _, seen := receipts[batch.TxID]; seen {
			continue
		}
		receipts[batch.TxID] = struct{}{}
		if _, err := s.getDirect(ReceiptKey(batch.TxID)); err == nil {
			continue
		} else if !isNotFound(err) {
			return MergeResult{}, err
		}
		wm := watermarks[batch.OriginNode]
		if !watermarkRead[batch.OriginNode] {
			wm, err = s.recvWatermarkDirect(batch.OriginNode)
			if err != nil {
				return MergeResult{}, err
			}
			watermarkRead[batch.OriginNode] = true
		}
		if batch.Sequence <= wm {
			if err := putRemoteReceipt(b, batch); err != nil {
				return MergeResult{}, err
			}
			dirty = true
			continue
		}
		if batch.Sequence != wm+1 {
			return MergeResult{}, fmt.Errorf("%w: origin %s want %d got %d", ErrGap, batch.OriginNode, wm+1, batch.Sequence)
		}
		if err := s.mergeRemoteGroupBatch(b, batch, staged, &order); err != nil {
			return MergeResult{}, err
		}
		if err := b.Set(LogKey(batch.OriginNode, batch.Sequence), codec.EncodeBatch(nil, batch), nil); err != nil {
			return MergeResult{}, err
		}
		if err := putRemoteReceipt(b, batch); err != nil {
			return MergeResult{}, err
		}
		wm = batch.Sequence
		watermarks[batch.OriginNode] = wm
		if err := b.Set(RecvKey(batch.OriginNode), encodeU64(wm), nil); err != nil {
			return MergeResult{}, err
		}
		maxHLC = maxU64(maxHLC, batch.HLC)
		gen++
		result.Applied = true
		dirty = true
		s.clock.Observe(batch.HLC)
	}
	if !dirty {
		result.Generation = gen
		return result, nil
	}
	for _, key := range order {
		cell := staged[key]
		if !cell.changed {
			continue
		}
		if cell.tomb {
			if err := b.Set(cell.key, codec.EncodeTombstone(nil, cell.version), nil); err != nil {
				return MergeResult{}, err
			}
			result.Winners = append(result.Winners, WinningChange{TableID: cell.table, RowID: cell.row, Tombstone: true, Version: cell.version})
		} else {
			value := codec.CellState{Version: cell.version, Value: cell.value}
			if err := b.Set(cell.key, codec.EncodeCellState(nil, value), nil); err != nil {
				return MergeResult{}, err
			}
			result.Winners = append(result.Winners, WinningChange{TableID: cell.table, RowID: cell.row, ColumnID: cell.column, Value: cell.value, Version: cell.version})
		}
	}
	if err := b.Set(SysKey(sysHLC), encodeU64(maxHLC), nil); err != nil {
		return MergeResult{}, err
	}
	if result.Applied {
		if err := b.Set(SysKey(sysGeneration), encodeU64(gen), nil); err != nil {
			return MergeResult{}, err
		}
	}
	if err := s.commitBatch(b, s.writeOpts); err != nil {
		return MergeResult{}, err
	}
	result.Generation = gen
	return result, nil
}

func putRemoteReceipt(b *pebble.Batch, batch *codec.MutationBatch) error {
	var receipt [24]byte
	copy(receipt[:16], batch.OriginNode[:])
	binary.BigEndian.PutUint64(receipt[16:], batch.Sequence)
	return b.Set(ReceiptKey(batch.TxID), receipt[:], nil)
}

func (s *Store) mergeRemoteGroupBatch(b *pebble.Batch, batch *codec.MutationBatch, staged map[string]*remoteGroupCell, order *[]string) error {
	ver := batch.Version()
	seen := make(map[string]struct{}, len(batch.Mutations))
	for i := range batch.Mutations {
		mutation := &batch.Mutations[i]
		var key []byte
		var mapKey string
		var storedVersion crdt.Version
		var storedValue codec.Value
		var present bool
		if mutation.IsTombstone() {
			key = TombKey(mutation.TableID, mutation.RowID)
			mapKey = string(key)
			if cell := staged[mapKey]; cell != nil {
				storedVersion, present = cell.version, cell.present
			} else {
				var err error
				storedVersion, present, err = s.getTombDirect(mutation.TableID, mutation.RowID)
				if err != nil {
					return err
				}
			}
		} else {
			key = CellKey(mutation.TableID, mutation.RowID, mutation.ColumnID)
			mapKey = string(key)
			if cell := staged[mapKey]; cell != nil {
				storedVersion, storedValue, present = cell.version, cell.value, cell.present
			} else {
				cell, found, err := s.getCellDirect(mutation.TableID, mutation.RowID, mutation.ColumnID)
				if err != nil {
					return err
				}
				storedVersion, storedValue, present = cell.Version, cell.Value, found
			}
		}
		if _, ok := seen[mapKey]; ok {
			continue // preserve first mutation per key within a transaction
		}
		seen[mapKey] = struct{}{}
		cell := staged[mapKey]
		if cell == nil {
			cell = &remoteGroupCell{key: key, version: storedVersion, value: storedValue, present: present, table: mutation.TableID, row: mutation.RowID, column: mutation.ColumnID, tomb: mutation.IsTombstone()}
			staged[mapKey] = cell
			*order = append(*order, mapKey)
		}
		if crdt.MergeCell(ver, storedVersion, present) == crdt.MergeTake {
			cell.version = ver
			cell.present = true
			cell.changed = true
			if !mutation.IsTombstone() {
				cell.value = mutation.Value
			}
		}
	}
	return nil
}
