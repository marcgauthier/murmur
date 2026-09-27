package state

import (
	"context"

	"github.com/cockroachdb/pebble/v2"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/crdt"
	"github.com/nomadsql/replicateddb/ids"
)

// ExportSnapshot streams the logical current state in chunks of at most
// chunkCells cells. The manifest carries this node's receive watermarks so
// the receiver can request newer mutations afterwards.
func (s *Store) ExportSnapshot(chunkCells int, fn func(manifest *codec.SnapshotManifest, chunk []codec.SnapshotCell, last bool) error) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	wms, err := s.receiveWatermarksCore()
	if err != nil {
		return err
	}
	manifest := &codec.SnapshotManifest{
		SnapshotID: ids.NewTxID(),
		DBID:       s.dbID,
		CreatedHLC: s.clock.Max(),
		Watermarks: wms,
	}
	epoch, hash, err := s.schemaEpochCore()
	if err != nil {
		return err
	}
	manifest.SchemaEpoch = epoch
	manifest.SchemaHash = hash

	var chunk []codec.SnapshotCell
	flush := func(last bool) error {
		if len(chunk) == 0 && !last {
			return nil
		}
		out := chunk
		chunk = nil
		return fn(manifest, out, last)
	}
	if err := s.iterateCellsCore(func(c codec.SnapshotCell) error {
		chunk = append(chunk, c)
		if len(chunk) >= chunkCells {
			return flush(false)
		}
		return nil
	}); err != nil {
		return err
	}
	return flush(true)
}

// ImportSnapshotChunk LWW-merges one snapshot chunk and advances receive
// watermarks towards the manifest. Snapshot data is versioned, so merging
// (rather than replacing) converges correctly and never loses newer local
// writes. Returns winners to materialize.
func (s *Store) ImportSnapshotChunk(_ context.Context, manifest *codec.SnapshotManifest, chunk []codec.SnapshotCell) (MergeResult, error) {
	var res MergeResult
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	// Intra-chunk duplicates keep the first write (old read-your-writes).
	staged := make(map[string]struct{}, len(chunk))
	for i := range chunk {
		c := &chunk[i]
		if c.ColumnID == codec.ColumnTombstone {
			key := string(TombKey(c.TableID, c.RowID))
			if _, ok := staged[key]; ok {
				continue
			}
			staged[key] = struct{}{}
			stored, present, err := s.getTombDirect(c.TableID, c.RowID)
			if err != nil {
				return MergeResult{}, err
			}
			if crdt.MergeCell(c.Version, stored, present) == crdt.MergeTake {
				if err := b.Set(TombKey(c.TableID, c.RowID), codec.EncodeTombstone(nil, c.Version), nil); err != nil {
					return MergeResult{}, err
				}
				res.Winners = append(res.Winners, WinningChange{
					TableID: c.TableID, RowID: c.RowID,
					Tombstone: true, Version: c.Version,
				})
			}
			continue
		}
		key := string(CellKey(c.TableID, c.RowID, c.ColumnID))
		if _, ok := staged[key]; ok {
			continue
		}
		staged[key] = struct{}{}
		stored, present, err := s.getCellDirect(c.TableID, c.RowID, c.ColumnID)
		if err != nil {
			return MergeResult{}, err
		}
		if crdt.MergeCell(c.Version, stored.Version, present) == crdt.MergeTake {
			st := codec.CellState{Version: c.Version, Value: c.Value}
			if err := b.Set(CellKey(c.TableID, c.RowID, c.ColumnID), codec.EncodeCellState(nil, st), nil); err != nil {
				return MergeResult{}, err
			}
			res.Winners = append(res.Winners, WinningChange{
				TableID: c.TableID, RowID: c.RowID, ColumnID: c.ColumnID,
				Value: c.Value, Version: c.Version,
			})
		}
	}
	// Advance watermarks to at least the manifest's (max wins).
	for _, w := range manifest.Watermarks {
		cur, err := s.recvWatermarkDirect(w.Origin)
		if err != nil {
			return MergeResult{}, err
		}
		if w.Sequence > cur {
			if err := b.Set(RecvKey(w.Origin), encodeU64(w.Sequence), nil); err != nil {
				return MergeResult{}, err
			}
		}
	}
	s.clock.Observe(manifest.CreatedHLC)
	if err := b.Set(SysKey(sysHLC), encodeU64(maxU64(manifest.CreatedHLC, s.clock.Max())), nil); err != nil {
		return MergeResult{}, err
	}
	gen, err := s.readU64Direct(sysGeneration)
	if err != nil {
		return MergeResult{}, err
	}
	gen++
	if err := b.Set(SysKey(sysGeneration), encodeU64(gen), nil); err != nil {
		return MergeResult{}, err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return MergeResult{}, err
	}
	res.Applied = true
	res.Generation = gen
	return res, nil
}

// SnapshotMetadata records the last applied snapshot id (debugging/audit).
func (s *Store) SnapshotMetadata(name string, val []byte) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b := s.db.NewBatch()
	defer b.Close()
	if err := b.Set(SnapshotKey(name), val, nil); err != nil {
		return err
	}
	return b.Commit(pebble.Sync)
}
