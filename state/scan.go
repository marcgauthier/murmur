package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble/v2"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
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

// OriginProgress is the bounded synchronization view for one origin.
// Applied is contiguous durable state; FirstRetained/LastRetained describe
// the contiguous retained transaction-log suffix, with FirstRetained ==
// Applied+1 and LastRetained == 0 when no history remains.
type OriginProgress struct {
	Origin        ids.NodeID
	Applied       uint64
	Observed      uint64
	FirstRetained uint64
	LastRetained  uint64
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
		return s.iterateTableSnapshot(context.Background(), snap, tableID, fn, nil)
	})
}

func (s *Store) iterateTableSnapshot(ctx context.Context, snap *pebble.Snapshot, tableID uint32, fn func(*Row) error, onCell func()) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
		if err := ctx.Err(); err != nil {
			return err
		}
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
		if onCell != nil {
			onCell()
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
		return s.iterateCellsSnap(snap, fn)
	})
}

func (s *Store) iterateCellsSnap(snap *pebble.Snapshot, fn func(codec.SnapshotCell) error) error {
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
		wm, err := recvWatermarkSnap(snap, origin)
		if err != nil {
			return err
		}
		first, err = firstRetainedSeqSnap(snap, origin, wm)
		return err
	})
	return first, err
}

func firstRetainedSeqSnap(snap *pebble.Snapshot, origin ids.NodeID, watermark uint64) (uint64, error) {
	prefix := LogOriginPrefix(origin)
	it, err := prefixIter(snap, prefix)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	if it.SeekGE(prefix); it.Valid() {
		_, seq, ok := ParseLogKey(append([]byte(nil), it.Key()...))
		if ok {
			return seq, nil
		}
	}
	if err := it.Error(); err != nil {
		return 0, err
	}
	return watermark + 1, nil
}

// ReceiveProgressPage returns at most limit sorted origin records strictly
// after afterOrigin. One Pebble snapshot keeps applied and retained-history
// bounds consistent within the page.
func (s *Store) ReceiveProgressPage(afterOrigin ids.NodeID, limit int) ([]OriginProgress, bool, error) {
	if limit <= 0 || limit > 1024 {
		return nil, false, fmt.Errorf("state: progress page limit %d outside 1..1024", limit)
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	var out []OriginProgress
	more := false
	err := s.snapshot(func(snap *pebble.Snapshot) error {
		progress := make(map[ids.NodeID]OriginProgress)
		recv, err := prefixIter(snap, []byte{prefixRecv})
		if err != nil {
			return err
		}
		for valid := recv.First(); valid; valid = recv.Next() {
			k := append([]byte(nil), recv.Key()...)
			if len(k) != 17 {
				continue
			}
			var origin ids.NodeID
			copy(origin[:], k[1:])
			applied, ok := decodeU64(append([]byte(nil), recv.Value()...))
			if !ok {
				_ = recv.Close()
				return fmt.Errorf("state: corrupt watermark")
			}
			progress[origin] = OriginProgress{Origin: origin, Applied: applied, Observed: applied}
		}
		if err := recv.Error(); err != nil {
			_ = recv.Close()
			return err
		}
		_ = recv.Close()
		staged, err := prefixIter(snap, []byte{prefixTxnStage})
		if err != nil {
			return err
		}
		for valid := staged.First(); valid; valid = staged.Next() {
			key := append([]byte(nil), staged.Key()...)
			if len(key) != 21 || binary.BigEndian.Uint32(key[17:21]) != ^uint32(0) {
				continue
			}
			meta, received, e := decodeStagedMeta(append([]byte(nil), staged.Value()...))
			if e != nil {
				_ = staged.Close()
				return e
			}
			if received == 0 {
				continue
			}
			item := progress[meta.Origin]
			item.Origin = meta.Origin
			if item.Observed < meta.Sequence {
				item.Observed = meta.Sequence
			}
			progress[meta.Origin] = item
		}
		if err := staged.Error(); err != nil {
			_ = staged.Close()
			return err
		}
		_ = staged.Close()
		origins := make([]ids.NodeID, 0, len(progress))
		for origin := range progress {
			if afterOrigin.IsZero() || bytes.Compare(origin[:], afterOrigin[:]) > 0 {
				origins = append(origins, origin)
			}
		}
		sort.Slice(origins, func(i, j int) bool { return bytes.Compare(origins[i][:], origins[j][:]) < 0 })
		if len(origins) > limit {
			more = true
			origins = origins[:limit]
		}
		for _, origin := range origins {
			item := progress[origin]
			first, e := firstRetainedSeqSnap(snap, origin, item.Applied)
			if e != nil {
				return e
			}
			item.FirstRetained = first
			if first <= item.Applied {
				item.LastRetained = item.Applied
			}
			out = append(out, item)
		}
		return nil
	})
	return out, more, err
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
// the newest minRetain entries. Active unexpired retention leases and bridge
// export resume points bound throughSeq.
// CollectUnitCap bounds one CollectLog/CollectReceipts call so a single
// collection keeps its write batch small. Callers that must drain fully
// loop until a call returns fewer than the cap.
const CollectUnitCap = 4096

func (s *Store) CollectLog(origin ids.NodeID, throughSeq uint64, keepNewerThanMillis int64, minRetain uint64) (int, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if rFloor, ok := s.RetentionFloor(origin); ok {
		if throughSeq > rFloor {
			throughSeq = rFloor
		}
	}
	if bFloor, ok := s.BridgeExportFloor(origin); ok {
		if throughSeq > bFloor {
			throughSeq = bFloor
		}
	}
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
			if len(keys) >= CollectUnitCap {
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
	if err := s.commitBatch(b, s.writeOpts); err != nil {
		return 0, err
	}
	return len(keys), nil
}

// CollectReceipts deletes TxID receipts whose origin sequence is at or below
// the given per-origin floors. Re-application after receipt loss is
// value-idempotent (same content, newer version), so this is safe. Active
// unexpired retention leases and bridge export resume points bound the per-origin floors.
func (s *Store) CollectReceipts(floors map[ids.NodeID]uint64) (int, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	effectiveFloors := make(map[ids.NodeID]uint64, len(floors))
	for k, v := range floors {
		effectiveFloors[k] = v
		if rFloor, ok := s.RetentionFloor(k); ok {
			if effectiveFloors[k] > rFloor {
				effectiveFloors[k] = rFloor
			}
		}
		if bFloor, ok := s.BridgeExportFloor(k); ok {
			if effectiveFloors[k] > bFloor {
				effectiveFloors[k] = bFloor
			}
		}
	}
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
			if fl, ok := effectiveFloors[origin]; ok && seq <= fl {
				keys = append(keys, append([]byte(nil), it.Key()...))
				if len(keys) >= CollectUnitCap {
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
	if err := s.commitBatch(b, s.writeOpts); err != nil {
		return 0, err
	}
	return len(keys), nil
}
