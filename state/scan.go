package state

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/crdt"
	"github.com/nomadsql/replicateddb/ids"
)

// Row is the assembled current state of one replicated row.
type Row struct {
	Table uint32
	ID    ids.RowID
	// Cells maps column ID to its winning state.
	Cells map[uint32]codec.CellState
	// Newest is the newest cell version.
	Newest crdt.Version
	// Tomb, when Present, is the row tombstone version.
	Tomb crdt.TombstoneState
}

// Visible reports whether the row is query-visible (LWW delete semantics).
func (r *Row) Visible() bool {
	return crdt.Visible(len(r.Cells) > 0, r.Newest, r.Tomb)
}

// prefixIter opens a snapshot iterator bounded to prefix.
func prefixIter(snap *pebble.Snapshot, prefix []byte) (*pebble.Iterator, error) {
	return snap.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
}

// IterateTable assembles every row of one table in row-UUID order, calling
// fn for each (including tombstoned rows; check Visible). The callback must
// not retain the Row.
func (s *Store) IterateTable(tableID uint32, fn func(*Row) error) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.snapshot(func(snap *pebble.Snapshot) error {
		prefix := CellTablePrefix(tableID)
		it, err := prefixIter(snap, prefix)
		if err != nil {
			return err
		}
		defer it.Close()

		var cur *Row
		flush := func() error {
			if cur == nil {
				return nil
			}
			r := cur
			cur = nil
			tomb, present, err := getTombSnap(snap, tableID, r.ID)
			if err != nil {
				return err
			}
			if present {
				r.Tomb = crdt.TombstoneState{Present: true, Version: tomb}
			}
			return fn(r)
		}
		for it.SeekGE(prefix); it.Valid(); it.Next() {
			_, row, col, ok := ParseCellKey(append([]byte(nil), it.Key()...))
			if !ok {
				continue
			}
			if cur == nil || cur.ID != row {
				if err := flush(); err != nil {
					return err
				}
				cur = &Row{Table: tableID, ID: row, Cells: make(map[uint32]codec.CellState)}
			}
			st, err := codec.DecodeCellState(append([]byte(nil), it.Value()...), s.limits)
			if err != nil {
				return fmt.Errorf("state: corrupt cell: %w", err)
			}
			cur.Cells[col] = st
			if crdt.CompareVersion(st.Version, cur.Newest) > 0 {
				cur.Newest = st.Version
			}
		}
		if err := it.Error(); err != nil {
			return err
		}
		return flush()
	})
}

// GetRow reads one row's stored cells (for apply-path re-materialization).
func (s *Store) GetRow(table uint32, row ids.RowID) (map[uint32]codec.CellState, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	out := make(map[uint32]codec.CellState)
	err := s.snapshot(func(snap *pebble.Snapshot) error {
		prefix := CellRowPrefix(table, row)
		it, err := prefixIter(snap, prefix)
		if err != nil {
			return err
		}
		defer it.Close()
		for it.SeekGE(prefix); it.Valid(); it.Next() {
			_, _, col, ok := ParseCellKey(append([]byte(nil), it.Key()...))
			if !ok {
				continue
			}
			st, err := codec.DecodeCellState(append([]byte(nil), it.Value()...), s.limits)
			if err != nil {
				return fmt.Errorf("state: corrupt cell: %w", err)
			}
			out[col] = st
		}
		return it.Error()
	})
	return out, err
}

// IterateCells streams every stored cell and tombstone (for snapshots).
// ColumnID == codec.ColumnTombstone marks tombstones.
func (s *Store) IterateCells(fn func(codec.SnapshotCell) error) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.iterateCellsCore(fn)
}

func (s *Store) iterateCellsCore(fn func(codec.SnapshotCell) error) error {
	return s.snapshot(func(snap *pebble.Snapshot) error {
		// Cells.
		cprefix := []byte{prefixCell}
		it, err := prefixIter(snap, cprefix)
		if err != nil {
			return err
		}
		for it.SeekGE(cprefix); it.Valid(); it.Next() {
			table, row, col, ok := ParseCellKey(append([]byte(nil), it.Key()...))
			if !ok {
				continue
			}
			st, err := codec.DecodeCellState(append([]byte(nil), it.Value()...), s.limits)
			if err != nil {
				it.Close()
				return fmt.Errorf("state: corrupt cell: %w", err)
			}
			if err := fn(codec.SnapshotCell{
				TableID: table, RowID: row, ColumnID: col,
				Version: st.Version, Value: st.Value,
			}); err != nil {
				it.Close()
				return err
			}
		}
		if err := it.Error(); err != nil {
			it.Close()
			return err
		}
		it.Close()
		// Tombstones.
		tprefix := []byte{prefixTomb}
		tit, err := prefixIter(snap, tprefix)
		if err != nil {
			return err
		}
		defer tit.Close()
		for tit.SeekGE(tprefix); tit.Valid(); tit.Next() {
			table, row, ok := ParseTombKey(append([]byte(nil), tit.Key()...))
			if !ok {
				continue
			}
			v, err := codec.DecodeTombstone(append([]byte(nil), tit.Value()...))
			if err != nil {
				return fmt.Errorf("state: corrupt tombstone: %w", err)
			}
			if err := fn(codec.SnapshotCell{
				TableID: table, RowID: row, ColumnID: codec.ColumnTombstone,
				Version: v,
			}); err != nil {
				return err
			}
		}
		return tit.Error()
	})
}

// LogScan reads one origin's log starting at fromSeq (inclusive), invoking
// fn per batch until maxBatches or maxBytes is reached. It returns the last
// sequence passed to fn (fromSeq-1 when nothing was read).
//
// If fromSeq was already garbage collected, ErrLogGone is returned and the
// peer must snapshot-resync.
func (s *Store) LogScan(origin ids.NodeID, fromSeq uint64, maxBatches int, maxBytes int,
	fn func(*codec.MutationBatch) error) (uint64, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	last := fromSeq - 1
	err := s.snapshot(func(snap *pebble.Snapshot) error {
		prefix := LogOriginPrefix(origin)
		// Watermarks advance only on contiguous commits, so every sequence
		// at or below the watermark existed; a missing one was collected.
		if fromSeq >= 1 {
			wm, err := recvWatermarkSnap(snap, origin)
			if err != nil {
				return err
			}
			if fromSeq <= wm {
				if _, err := snapGet(snap, LogKey(origin, fromSeq)); err != nil {
					if isNotFound(err) {
						return fmt.Errorf("%w: origin %s from %d (watermark %d)",
							ErrLogGone, origin, fromSeq, wm)
					}
					return err
				}
			}
		}
		it, err := prefixIter(snap, prefix)
		if err != nil {
			return err
		}
		defer it.Close()
		// Detect GC truncation: the first retained sequence.
		if it.SeekGE(prefix); it.Valid() {
			_, first, ok := ParseLogKey(append([]byte(nil), it.Key()...))
			if ok && fromSeq < first {
				return fmt.Errorf("%w: origin %s from %d, first retained %d",
					ErrLogGone, origin, fromSeq, first)
			}
		}
		sent := 0
		nbytes := 0
		for it.SeekGE(LogKey(origin, fromSeq)); it.Valid(); it.Next() {
			_, seq, ok := ParseLogKey(append([]byte(nil), it.Key()...))
			if !ok {
				continue
			}
			if sent >= maxBatches {
				break
			}
			raw := append([]byte(nil), it.Value()...)
			if nbytes+len(raw) > maxBytes && sent > 0 {
				break
			}
			batch, _, err := codec.DecodeBatch(raw, s.limits)
			if err != nil {
				return fmt.Errorf("state: corrupt log entry %s/%d: %w", origin, seq, err)
			}
			if err := fn(batch); err != nil {
				return err
			}
			last = seq
			sent++
			nbytes += len(raw)
		}
		return it.Error()
	})
	return last, err
}

// FirstRetainedSeq returns the lowest retained log sequence for origin,
// or watermark+1 when the log is empty.
func (s *Store) FirstRetainedSeq(origin ids.NodeID) (uint64, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	var first uint64
	err := s.snapshot(func(snap *pebble.Snapshot) error {
		prefix := LogOriginPrefix(origin)
		it, err := prefixIter(snap, prefix)
		if err != nil {
			return err
		}
		defer it.Close()
		if it.SeekGE(prefix); it.Valid() {
			_, seq, ok := ParseLogKey(append([]byte(nil), it.Key()...))
			if ok {
				first = seq
				return nil
			}
		}
		if err := it.Error(); err != nil {
			return err
		}
		wm, err := recvWatermarkSnap(snap, origin)
		if err != nil {
			return err
		}
		first = wm + 1
		return nil
	})
	return first, err
}

// KnownOrigins lists origins with a receive watermark or retained log.
func (s *Store) KnownOrigins() ([]ids.NodeID, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	wms, err := s.receiveWatermarksCore()
	if err != nil {
		return nil, err
	}
	seen := make(map[ids.NodeID]bool, len(wms))
	var out []ids.NodeID
	for _, w := range wms {
		if !seen[w.Origin] {
			seen[w.Origin] = true
			out = append(out, w.Origin)
		}
	}
	return out, nil
}

// CollectLog deletes origin-log entries with sequence <= throughSeq whose
// HLC wall time is older than keepNewerThanMillis, except it always retains
// the newest minRetain entries.
func (s *Store) CollectLog(origin ids.NodeID, throughSeq uint64, keepNewerThanMillis int64, minRetain uint64) (int, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	// Find the newest sequence to enforce minRetain.
	wm, err := s.recvWatermarkDirect(origin)
	if err != nil {
		return 0, err
	}
	floor := throughSeq
	if wm > minRetain && floor > wm-minRetain {
		floor = wm - minRetain
	}
	if floor == 0 {
		return 0, nil
	}
	var keys [][]byte
	err = s.snapshot(func(snap *pebble.Snapshot) error {
		prefix := LogOriginPrefix(origin)
		it, err := prefixIter(snap, prefix)
		if err != nil {
			return err
		}
		defer it.Close()
		for it.SeekGE(prefix); it.Valid(); it.Next() {
			_, seq, ok := ParseLogKey(append([]byte(nil), it.Key()...))
			if !ok || seq > floor {
				break
			}
			raw := append([]byte(nil), it.Value()...)
			batch, _, err := codec.DecodeBatch(raw, s.limits)
			if err != nil {
				// Corrupt entries are not silently dropped by GC.
				return fmt.Errorf("state: corrupt log entry during GC %s/%d: %w", origin, seq, err)
			}
			if crdt.WallMillis(batch.HLC) > keepNewerThanMillis {
				continue
			}
			keys = append(keys, LogKey(origin, seq))
			// Bound one GC round to keep write batches small.
			if len(keys) >= 4096 {
				break
			}
		}
		return it.Error()
	})
	if err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	for _, k := range keys {
		if err := b.Delete(k, nil); err != nil {
			return 0, err
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return 0, err
	}
	return len(keys), nil
}

// CollectReceipts deletes TxID receipts whose origin sequence is at or below
// the given per-origin floors. Re-application after receipt loss is
// value-idempotent (same content, newer version), so this is safe.
func (s *Store) CollectReceipts(floors map[ids.NodeID]uint64) (int, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	var keys [][]byte
	err := s.snapshot(func(snap *pebble.Snapshot) error {
		prefix := []byte{prefixReceipt}
		it, err := prefixIter(snap, prefix)
		if err != nil {
			return err
		}
		defer it.Close()
		for it.SeekGE(prefix); it.Valid(); it.Next() {
			raw := append([]byte(nil), it.Value()...)
			if len(raw) != 24 {
				continue
			}
			var origin ids.NodeID
			copy(origin[:], raw[:16])
			seq := binary.BigEndian.Uint64(raw[16:])
			if fl, ok := floors[origin]; ok && seq <= fl {
				keys = append(keys, append([]byte(nil), it.Key()...))
				if len(keys) >= 4096 {
					break
				}
			}
		}
		return it.Error()
	})
	if err != nil {
		return 0, err
	}
	if len(keys) == 0 {
		return 0, nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	for _, k := range keys {
		// Pebble deletes are idempotent; missing keys are fine.
		if err := b.Delete(k, nil); err != nil {
			return 0, err
		}
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return 0, err
	}
	return len(keys), nil
}
