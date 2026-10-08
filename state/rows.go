package state

import (
	"context"
	"fmt"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
)

// RowRef identifies one authoritative row to read from a single state snapshot.
type RowRef struct {
	Table uint32
	ID    ids.RowID
}

const MaxRowsPerRead = 4096

// MaxRowsPerStream bounds the reference list retained by snapshot streaming
// callers while allowing remote commits larger than the legacy bulk-read cap.
// The callback receives rows one at a time from a single point-in-time view.
const MaxRowsPerStream = 100_000

// GetRows returns complete current row states from one point-in-time snapshot.
// Missing rows are represented by empty Rows, preserving input order. The row
// limit bounds iterator and decoding work in apply paths.
func (s *Store) GetRows(refs []RowRef) ([]*Row, error) {
	if len(refs) > MaxRowsPerRead {
		return nil, fmt.Errorf("state: row read has %d rows; maximum is %d", len(refs), MaxRowsPerRead)
	}
	rows := make([]*Row, len(refs))
	err := s.ForEachRowSnapshot(context.Background(), refs, func(i int, row *Row) error {
		rows[i] = row
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// ForEachRowSnapshot visits complete current rows from one point-in-time
// snapshot, preserving reference order and bounding row materialization to a
// single callback. The callback must not retain or mutate the row. Returning an
// error stops the scan and releases the snapshot and store read lock.
func (s *Store) ForEachRowSnapshot(ctx context.Context, refs []RowRef, visit func(int, *Row) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if visit == nil {
		return fmt.Errorf("state: nil row snapshot visitor")
	}
	if len(refs) > MaxRowsPerStream {
		return fmt.Errorf("state: row stream has %d rows; maximum is %d", len(refs), MaxRowsPerStream)
	}
	seen := make(map[RowRef]struct{}, len(refs))
	for _, ref := range refs {
		if _, ok := seen[ref]; ok {
			return fmt.Errorf("state: duplicate row reference in read")
		}
		seen[ref] = struct{}{}
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	return s.snapshot(func(snap *snapshot) error {
		for i, ref := range refs {
			if err := ctx.Err(); err != nil {
				return err
			}
			row := &Row{Table: ref.Table, ID: ref.ID, Cells: make(map[uint32]codec.CellState)}
			prefix := CellRowPrefix(ref.Table, ref.ID)
			it, err := prefixIter(snap, prefix)
			if err != nil {
				return err
			}
			for it.SeekGE(prefix); it.Valid(); it.Next() {
				key := append([]byte(nil), it.Key()...)
				_, _, col, ok := ParseCellKey(key)
				if !ok {
					continue
				}
				st, err := codec.DecodeCellState(append([]byte(nil), it.Value()...), s.limits)
				if err != nil {
					_ = it.Close()
					return fmt.Errorf("state: corrupt cell: %w", err)
				}
				row.Cells[col] = st
				if crdt.CompareVersion(st.Version, row.Newest) > 0 {
					row.Newest = st.Version
				}
			}
			if err := it.Error(); err != nil {
				_ = it.Close()
				return err
			}
			if err := it.Close(); err != nil {
				return err
			}
			tomb, present, err := getTombSnap(snap, ref.Table, ref.ID)
			if err != nil {
				return err
			}
			if present {
				row.Tomb = crdt.TombstoneState{Present: true, Version: tomb}
			}
			if err := visit(i, row); err != nil {
				return err
			}
		}
		return nil
	})
}
