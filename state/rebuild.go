package state

import (
	"context"
	"fmt"
	"sync"

	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crdt"
	"github.com/marcgauthier/murmur/ids"
)

// RebuildSnapshot pins logical keys for materialization and progress reporting.
// Use it sequentially and Close it before closing its Store.
type RebuildSnapshot struct {
	s           *Store
	snap        *snapshot
	ctx         context.Context
	onProcessed func(uint64)
	once        sync.Once
	closeErr    error
}

// NewRebuildSnapshot holds a store read lease until Close. onProcessed receives
// deltas of successfully decoded cells, including cells in invisible rows.
func (s *Store) NewRebuildSnapshot(ctx context.Context, onProcessed func(uint64)) (*RebuildSnapshot, error) {
	s.gate.RLock()
	if err := ctx.Err(); err != nil {
		s.gate.RUnlock()
		return nil, err
	}
	if err := s.failedErr(); err != nil {
		s.gate.RUnlock()
		return nil, err
	}
	return &RebuildSnapshot{s: s, snap: s.mem.newSnapshot(), ctx: ctx, onProcessed: onProcessed}, nil
}

// Close releases the pinned snapshot and store read lease. It is idempotent.
func (r *RebuildSnapshot) Close() error {
	r.once.Do(func() { r.closeErr = r.snap.Close(); r.s.gate.RUnlock() })
	return r.closeErr
}

// IterateTable decodes cells once and reports progress in bounded batches.
func (r *RebuildSnapshot) IterateTable(table uint32, fn func(*Row) error) error {
	var pending uint64
	flush := func() {
		if pending != 0 && r.onProcessed != nil {
			r.onProcessed(pending)
			pending = 0
		}
	}
	defer flush()
	var onCell func()
	if r.onProcessed != nil {
		onCell = func() {
			pending++
			if pending == 1024 {
				flush()
			}
		}
	}
	return r.s.iterateTableSnapshot(r.ctx, r.snap, table, fn, onCell)
}

// GetRow reads winning cells from the pinned snapshot.
func (r *RebuildSnapshot) GetRow(table uint32, row ids.RowID) (map[uint32]codec.CellState, error) {
	if err := r.ctx.Err(); err != nil {
		return nil, err
	}
	prefix := CellRowPrefix(table, row)
	it, err := prefixIter(r.snap, prefix)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := make(map[uint32]codec.CellState)
	for it.SeekGE(prefix); it.Valid(); it.Next() {
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
		_, _, col, ok := ParseCellKey(it.Key())
		if !ok {
			continue
		}
		st, err := codec.DecodeCellState(append([]byte(nil), it.Value()...), r.s.limits)
		if err != nil {
			return nil, fmt.Errorf("state: corrupt cell: %w", err)
		}
		out[col] = st
	}
	return out, it.Error()
}

// GetTombstone reads a row tombstone from the pinned snapshot.
func (r *RebuildSnapshot) GetTombstone(table uint32, row ids.RowID) (crdt.Version, bool, error) {
	if err := r.ctx.Err(); err != nil {
		return crdt.Version{}, false, err
	}
	return getTombSnap(r.snap, table, row)
}
