package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/marcgauthier/spedsql/codec"
	"github.com/marcgauthier/spedsql/crdt"
	"github.com/marcgauthier/spedsql/ids"
)

// DefaultSnapshotAtomicMergeBytes caps the encoded snapshot size merged in
// one atomic Pebble batch (8 MiB keeps the transient batch far below the
// memtable/RAM budget). Larger validated snapshots merge chunk by chunk
// with durable resume progress and publish watermarks/generation atomically
// last. Selectable via Options.SnapshotAtomicMergeBytes.
const DefaultSnapshotAtomicMergeBytes = 8 << 20

// ExportSnapshot streams the logical current state in chunks of at most
// chunkCells cells. The manifest carries this node's receive watermarks so
// the receiver can request newer mutations afterwards.
func (s *Store) ExportSnapshot(chunkCells int, fn func(manifest *codec.SnapshotManifest, chunk []codec.SnapshotCell, last bool) error) error {
	return s.ExportSnapshotContext(context.Background(), chunkCells, fn)
}

// ExportSnapshotContext exports from one read cut, stopping if the transfer
// lease expires so Pebble's source snapshot is released promptly. It acquires
// a bounded source log-retention lease covering the snapshot's watermarks so
// concurrent local writes and log GC preserve tail repair history.
func (s *Store) ExportSnapshotContext(ctx context.Context, chunkCells int, fn func(manifest *codec.SnapshotManifest, chunk []codec.SnapshotCell, last bool) error) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	if chunkCells < 1 {
		chunkCells = 1
	}
	return s.snapshot(func(snap *pebble.Snapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		manifest, err := s.snapshotManifest(snap)
		if err != nil {
			return err
		}
		leaseExpires := time.Now().Add(10 * time.Minute)
		if dl, ok := ctx.Deadline(); ok {
			leaseExpires = dl
		}
		wms := make(map[ids.NodeID]uint64, len(manifest.Watermarks))
		for _, w := range manifest.Watermarks {
			wms[w.Origin] = w.Sequence
		}
		releaseLease, err := s.AcquireRetentionLease("snapshot-export-"+manifest.SnapshotID.String(), leaseExpires, wms)
		if err != nil {
			return err
		}
		defer releaseLease()
		// Determine bounded chunk metadata before sending the manifest.
		var count, encoded uint64
		var chunk []codec.SnapshotCell
		visit := func(c codec.SnapshotCell) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			chunk = append(chunk, c)
			if len(chunk) == chunkCells {
				raw := codec.EncodeSnapshotCells(nil, chunk)
				encoded += uint64(len(raw))
				count++
				chunk = nil
			}
			return nil
		}
		if err := s.iterateCellsSnap(snap, visit); err != nil {
			return err
		}
		if len(chunk) > 0 || count == 0 {
			raw := codec.EncodeSnapshotCells(nil, chunk)
			encoded += uint64(len(raw))
			count++
		}
		manifest.ChunkCount, manifest.EncodedBytes = count, encoded
		// Recompute using final count/size in metadata.
		digest := sha256.New()
		base := *manifest
		base.ContentHash = [32]byte{}
		digest.Write(codec.EncodeManifest(nil, &base))
		chunk = nil
		nChunks := uint64(0)
		err = s.iterateCellsSnap(snap, func(c codec.SnapshotCell) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			chunk = append(chunk, c)
			if len(chunk) == chunkCells {
				raw := codec.EncodeSnapshotCells(nil, chunk)
				var n [8]byte
				binary.BigEndian.PutUint64(n[:], uint64(len(raw)))
				digest.Write(n[:])
				digest.Write(raw)
				nChunks++
				chunk = nil
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(chunk) > 0 || nChunks == 0 {
			raw := codec.EncodeSnapshotCells(nil, chunk)
			var n [8]byte
			binary.BigEndian.PutUint64(n[:], uint64(len(raw)))
			digest.Write(n[:])
			digest.Write(raw)
			nChunks++
		}
		copy(manifest.ContentHash[:], digest.Sum(nil))
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk = nil
		index := uint64(0)
		err = s.iterateCellsSnap(snap, func(c codec.SnapshotCell) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			chunk = append(chunk, c)
			if len(chunk) == chunkCells {
				out := chunk
				chunk = nil
				last := index+1 == manifest.ChunkCount
				if err := fn(manifest, out, last); err != nil {
					return err
				}
				index++
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(chunk) > 0 || index == 0 {
			return fn(manifest, chunk, true)
		}
		return nil
	})
}

func (s *Store) snapshotManifest(snap *pebble.Snapshot) (*codec.SnapshotManifest, error) {
	m := &codec.SnapshotManifest{FormatVersion: 1, SnapshotID: ids.NewTxID(), DBID: s.dbID}
	var err error
	if m.CreatedHLC, err = readU64Snap(snap, sysHLC); err != nil {
		return nil, err
	}
	if m.StateGeneration, err = readU64Snap(snap, sysGeneration); err != nil {
		return nil, err
	}
	raw, err := snapGet(snap, SysKey(sysSchemaEpoch))
	if err != nil && !isNotFound(err) {
		return nil, err
	}
	if len(raw) == 8 {
		m.SchemaEpoch = binary.BigEndian.Uint64(raw)
	}
	raw, err = snapGet(snap, SysKey(sysSchemaHash))
	if err != nil && !isNotFound(err) {
		return nil, err
	}
	if len(raw) == 32 {
		copy(m.SchemaHash[:], raw)
	}
	prefix := []byte{prefixRecv}
	it, err := snap.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.SeekGE(prefix); it.Valid(); it.Next() {
		if len(it.Key()) != 17 {
			continue
		}
		seq, ok := decodeU64(append([]byte(nil), it.Value()...))
		if !ok {
			return nil, fmt.Errorf("state: corrupt watermark")
		}
		var origin ids.NodeID
		copy(origin[:], it.Key()[1:])
		m.Watermarks = append(m.Watermarks, codec.OriginWatermark{Origin: origin, Sequence: seq})
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return m, nil
}

// mergeProgress is the durable resume point of a chunked snapshot merge:
// nextChunk indexes the next chunk to merge and prevKey carries the last
// merged key so the canonical-order check continues across batches,
// crashes, and restarts.
type mergeProgress struct {
	nextChunk uint64
	prevKey   []byte
}

func encodeMergeProgress(nextChunk uint64, prevKey []byte) []byte {
	out := make([]byte, 8+4+len(prevKey))
	binary.BigEndian.PutUint64(out, nextChunk)
	binary.BigEndian.PutUint32(out[8:], uint32(len(prevKey)))
	copy(out[12:], prevKey)
	return out
}

func decodeMergeProgress(raw []byte) (mergeProgress, error) {
	var p mergeProgress
	if len(raw) < 12 {
		return p, fmt.Errorf("state: corrupt snapshot merge progress")
	}
	p.nextChunk = binary.BigEndian.Uint64(raw)
	n := binary.BigEndian.Uint32(raw[8:])
	if uint64(len(raw))-12 != uint64(n) {
		return p, fmt.Errorf("state: corrupt snapshot merge progress")
	}
	p.prevKey = append([]byte(nil), raw[12:]...)
	return p, nil
}

// mergeSnapshotChunk decodes one staged chunk and merges its cells and
// tombstones into b by LWW version, continuing the canonical-order check
// from prevKey (nil at the start). It returns the last merged key and
// whether any winner was staged. Both the atomic and the chunked merge
// paths share it, so large snapshots merge with identical semantics.
func (s *Store) mergeSnapshotChunk(b *pebble.Batch, raw []byte, prevKey []byte) (lastKey []byte, changed bool, err error) {
	return s.mergeSnapshotChunkWithSet(func(key, value []byte) error { return b.Set(key, value, nil) }, raw, prevKey)
}

func (s *Store) mergeSnapshotChunkWithSet(set func(key, value []byte) error, raw []byte, prevKey []byte) (lastKey []byte, changed bool, err error) {
	cells, rest, err := codec.DecodeSnapshotCells(raw, s.limits, 1<<24)
	if err != nil || len(rest) != 0 {
		return nil, false, fmt.Errorf("state: invalid staged snapshot chunk: %w", err)
	}
	if !bytes.Equal(codec.EncodeSnapshotCells(nil, cells), raw) {
		return nil, false, fmt.Errorf("state: non-canonical staged snapshot chunk")
	}
	lastKey = prevKey
	for i := range cells {
		c := &cells[i]
		if c.ColumnID == codec.ColumnTombstone {
			key := TombKey(c.TableID, c.RowID)
			if lastKey != nil && bytes.Compare(lastKey, key) >= 0 {
				return nil, false, fmt.Errorf("state: snapshot cells are not in canonical order")
			}
			lastKey = key
			stored, present, err := s.getTombDirect(c.TableID, c.RowID)
			if err != nil {
				return nil, false, err
			}
			if crdt.MergeCell(c.Version, stored, present) == crdt.MergeTake {
				changed = true
				if err := set(TombKey(c.TableID, c.RowID), codec.EncodeTombstone(nil, c.Version)); err != nil {
					return nil, false, err
				}
			}
			continue
		}
		key := CellKey(c.TableID, c.RowID, c.ColumnID)
		if lastKey != nil && bytes.Compare(lastKey, key) >= 0 {
			return nil, false, fmt.Errorf("state: snapshot cells are not in canonical order")
		}
		lastKey = key
		stored, present, err := s.getCellDirect(c.TableID, c.RowID, c.ColumnID)
		if err != nil {
			return nil, false, err
		}
		if crdt.MergeCell(c.Version, stored.Version, present) == crdt.MergeTake {
			changed = true
			st := codec.CellState{Version: c.Version, Value: c.Value}
			if err := set(CellKey(c.TableID, c.RowID, c.ColumnID), codec.EncodeCellState(nil, st)); err != nil {
				return nil, false, err
			}
		}
	}
	return lastKey, changed, nil
}

// atomicMergeBytes returns the encoded-size threshold at or below which a
// snapshot merges in one atomic batch.
func (s *Store) atomicMergeBytes() uint64 {
	if s.openOpt.SnapshotAtomicMergeBytes != 0 {
		return s.openOpt.SnapshotAtomicMergeBytes
	}
	return DefaultSnapshotAtomicMergeBytes
}

// ImportSnapshotChunk durably stages a chunk. It publishes all cells and
// coverage watermarks together only after the complete digest validates.
//
// Snapshots at or below the atomic threshold merge in one synced batch.
// Larger snapshots merge chunk by chunk with durable resume progress and
// publish watermarks, HLC, and generation in one final atomic batch, so a
// crash exposes merged state with unadvanced watermarks (resumable, never
// partially published) or the complete merged state.
func (s *Store) ImportSnapshotChunk(ctx context.Context, manifest *codec.SnapshotManifest, index uint64, chunk []codec.SnapshotCell, last bool, maxBytes uint64) (MergeResult, bool, error) {
	var res MergeResult
	s.gate.RLock()
	defer s.gate.RUnlock()
	if manifest.FormatVersion != 1 || manifest.SnapshotID.IsZero() || manifest.DBID != s.dbID || manifest.ChunkCount == 0 || manifest.ChunkCount > 65_536 || index >= manifest.ChunkCount || last != (index+1 == manifest.ChunkCount) {
		return res, false, fmt.Errorf("state: invalid snapshot chunk position")
	}
	for i, w := range manifest.Watermarks {
		if w.Origin.IsZero() || (i > 0 && bytes.Compare(manifest.Watermarks[i-1].Origin[:], w.Origin[:]) >= 0) {
			return res, false, fmt.Errorf("state: invalid snapshot watermark ordering")
		}
	}
	if maxBytes == 0 {
		maxBytes = 512 << 20
	}
	if manifest.EncodedBytes > maxBytes {
		return res, false, fmt.Errorf("state: snapshot exceeds staging limit")
	}
	chunkRaw := codec.EncodeSnapshotCells(nil, chunk)
	stagePrefix := "recv/" + string(manifest.SnapshotID[:]) + "/"
	manifestKey := SnapshotKey(stagePrefix + "manifest")
	activeKey := SnapshotKey("recv/active")
	progressKey := SnapshotKey(stagePrefix + "merge/progress")
	chunkKey := SnapshotKey(fmt.Sprintf("%schunk/%020d", stagePrefix, index))
	// Snapshots above the atomic threshold merge chunk by chunk; the
	// final chunk must be staged too so a crash mid-merge can resume
	// from durable state instead of losing the in-memory tail.
	chunked := manifest.EncodedBytes > s.atomicMergeBytes()
	if err := s.stageSnapshotChunk(manifest, index, chunkRaw, chunked, manifestKey, activeKey, chunkKey); err != nil {
		return res, false, err
	}
	// Load the bounded staged transfer in index order. This also makes
	// reconnect retransmission idempotent: already durable chunks are reused.
	// Reads run without the writer lock; a concurrent preemption only swaps
	// the staging namespace, which the merge steps re-validate.
	chunks := make([][]byte, manifest.ChunkCount)
	var encoded uint64
	for i := uint64(0); i < manifest.ChunkCount; i++ {
		k := SnapshotKey(fmt.Sprintf("%schunk/%020d", stagePrefix, i))
		raw, e := s.getDirect(k)
		if i == index && !chunked && (last || index+1 == manifest.ChunkCount) {
			raw, e = chunkRaw, nil
		}
		if e != nil {
			if isNotFound(e) {
				return res, false, nil
			}
			return res, false, e
		}
		encoded += uint64(len(raw))
		chunks[i] = raw
	}
	if encoded != manifest.EncodedBytes {
		return res, false, fmt.Errorf("state: snapshot encoded size mismatch")
	}
	h := sha256.New()
	mcopy := *manifest
	mcopy.ContentHash = [32]byte{}
	h.Write(codec.EncodeManifest(nil, &mcopy))
	for _, raw := range chunks {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(raw)))
		h.Write(n[:])
		h.Write(raw)
	}
	if string(h.Sum(nil)) != string(manifest.ContentHash[:]) {
		return res, false, fmt.Errorf("state: snapshot content digest mismatch")
	}
	if !chunked {
		return s.publishSnapshotAtomic(manifest, chunks, stagePrefix, manifestKey, activeKey)
	}
	return s.mergeSnapshotChunked(ctx, manifest, stagePrefix, manifestKey, activeKey, progressKey)
}

// stageSnapshotChunk admits the transfer (preempting any older staged
// transfer) and durably stages the chunk. The final chunk is staged too
// for chunked merges so crash-resume never loses the in-memory tail.
func (s *Store) stageSnapshotChunk(manifest *codec.SnapshotManifest, index uint64, chunkRaw []byte, chunked bool, manifestKey, activeKey, chunkKey []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	storedManifest, err := s.getDirect(manifestKey)
	if err == nil {
		if string(storedManifest) != string(codec.EncodeManifest(nil, manifest)) {
			return fmt.Errorf("state: snapshot manifest changed during transfer")
		}
	} else if isNotFound(err) {
		active, activeErr := s.getDirect(activeKey)
		if activeErr != nil && !isNotFound(activeErr) {
			return activeErr
		}
		if !bytes.Equal(active, manifest.SnapshotID[:]) {
			b := s.db.NewBatch()
			defer b.Close()
			if err := s.snapshot(func(snap *pebble.Snapshot) error {
				prefix := []byte{prefixSnapshot}
				it, err := snap.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
				if err != nil {
					return err
				}
				defer it.Close()
				for it.SeekGE(prefix); it.Valid(); it.Next() {
					key := append([]byte(nil), it.Key()...)
					if bytes.HasPrefix(key, SnapshotKey("recv/")) {
						if err := b.Delete(key, nil); err != nil {
							return err
						}
					}
				}
				return it.Error()
			}); err != nil {
				return err
			}
			if err := b.Set(activeKey, manifest.SnapshotID[:], nil); err != nil {
				return err
			}
			if err := b.Set(manifestKey, codec.EncodeManifest(nil, manifest), nil); err != nil {
				return err
			}
			if err := s.commitBatch(b, pebble.Sync); err != nil {
				return err
			}
		}
	} else {
		return err
	}
	prior, err := s.getDirect(chunkKey)
	if err == nil && string(prior) != string(chunkRaw) {
		return fmt.Errorf("state: conflicting duplicate snapshot chunk")
	}
	if err != nil && !isNotFound(err) {
		return err
	}
	if isNotFound(err) && (chunked || index+1 != manifest.ChunkCount) {
		if err := s.dbSet(chunkKey, chunkRaw, pebble.Sync); err != nil {
			return err
		}
	}
	return nil
}

// publishSnapshotAtomic merges a validated small snapshot and publishes
// cells, watermarks, HLC, and generation in one synced batch: the durable
// publication boundary. A crash exposes either the complete old state or
// the complete merged state, never a partial set of chunks or watermarks.
func (s *Store) publishSnapshotAtomic(manifest *codec.SnapshotManifest, chunks [][]byte, stagePrefix string, manifestKey, activeKey []byte) (MergeResult, bool, error) {
	var res MergeResult
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.checkSnapshotActive(manifest, manifestKey, activeKey); err != nil {
		return res, false, err
	}
	b := s.db.NewBatch()
	defer b.Close()
	changed := false
	var previousKey []byte
	for _, raw := range chunks {
		lastKey, c, err := s.mergeSnapshotChunk(b, raw, previousKey)
		if err != nil {
			return res, false, err
		}
		previousKey = lastKey
		changed = changed || c
	}
	return s.commitSnapshotPublication(b, manifest, changed, stagePrefix, manifestKey, activeKey, nil)
}

// mergeSnapshotChunked merges a validated large snapshot chunk by chunk,
// one synced batch per chunk with durable resume progress, then publishes
// watermarks, HLC, and generation in one final atomic batch. The writer
// lock is released between chunks so local commits and remote apply
// proceed during a long merge; LWW version comparison keeps interleaved
// writes correct. A crash resumes from the durable progress marker, and
// watermarks publish only after every chunk merged.
func (s *Store) mergeSnapshotChunked(ctx context.Context, manifest *codec.SnapshotManifest, stagePrefix string, manifestKey, activeKey, progressKey []byte) (MergeResult, bool, error) {
	var res MergeResult
	mergedChanged := false
	for {
		s.writeMu.Lock()
		if err := s.checkSnapshotActive(manifest, manifestKey, activeKey); err != nil {
			s.writeMu.Unlock()
			return res, false, err
		}
		prog, err := s.readMergeProgress(progressKey)
		if err != nil {
			s.writeMu.Unlock()
			return res, false, err
		}
		if prog.nextChunk >= manifest.ChunkCount {
			s.writeMu.Unlock()
			break
		}
		raw, err := s.getDirect(SnapshotKey(fmt.Sprintf("%schunk/%020d", stagePrefix, prog.nextChunk)))
		if err != nil {
			s.writeMu.Unlock()
			if isNotFound(err) {
				return res, false, nil
			}
			return res, false, err
		}
		lastKey, changed, merr := s.ingestSnapshotChunkToSST(ctx, raw, prog.prevKey)
		if merr == nil && s.snapshotIngestFault != nil {
			merr = s.snapshotIngestFault()
		}
		b := s.db.NewBatch()
		if merr == nil {
			merr = b.Set(progressKey, encodeMergeProgress(prog.nextChunk+1, lastKey), nil)
		}
		if merr == nil {
			merr = s.commitBatch(b, pebble.Sync)
		}
		_ = b.Close()
		if merr != nil {
			s.writeMu.Unlock()
			return res, false, merr
		}
		mergedChanged = mergedChanged || changed
		next := prog.nextChunk + 1
		fault := s.snapshotMergeFault
		s.writeMu.Unlock()
		// Fault injection (tests only) fires after the merge batch is
		// durable, proving crash-resume from every boundary including
		// all-merged-but-unpublished.
		if fault != nil {
			if err := fault(next); err != nil {
				return res, false, err
			}
		}
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.checkSnapshotActive(manifest, manifestKey, activeKey); err != nil {
		return res, false, err
	}
	b := s.db.NewBatch()
	defer b.Close()
	return s.commitSnapshotPublication(b, manifest, mergedChanged, stagePrefix, manifestKey, activeKey, progressKey)
}

// checkSnapshotActive verifies the transfer still owns the receive
// namespace: a newer transfer preempts staging, and its partial merge must
// never publish watermarks.
func (s *Store) checkSnapshotActive(manifest *codec.SnapshotManifest, manifestKey, activeKey []byte) error {
	stored, err := s.getDirect(manifestKey)
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("state: snapshot transfer superseded")
		}
		return err
	}
	if string(stored) != string(codec.EncodeManifest(nil, manifest)) {
		return fmt.Errorf("state: snapshot manifest changed during transfer")
	}
	active, err := s.getDirect(activeKey)
	if err != nil {
		if isNotFound(err) {
			return fmt.Errorf("state: snapshot transfer superseded")
		}
		return err
	}
	if !bytes.Equal(active, manifest.SnapshotID[:]) {
		return fmt.Errorf("state: snapshot transfer superseded")
	}
	return nil
}

// readMergeProgress loads the durable chunked-merge resume point; a
// missing marker means the merge has not started.
func (s *Store) readMergeProgress(progressKey []byte) (mergeProgress, error) {
	var p mergeProgress
	raw, err := s.getDirect(progressKey)
	if err != nil {
		if isNotFound(err) {
			return p, nil
		}
		return p, err
	}
	return decodeMergeProgress(raw)
}

// commitSnapshotPublication advances watermarks to at least the manifest's
// (max wins), merges HLC, bumps the storage generation when anything
// changed, and deletes the transfer staging, all in one synced batch: the
// crash-safe publication boundary. progressKey is nil for atomic merges.
func (s *Store) commitSnapshotPublication(b *pebble.Batch, manifest *codec.SnapshotManifest, changed bool, stagePrefix string, manifestKey, activeKey, progressKey []byte) (MergeResult, bool, error) {
	var res MergeResult
	for _, w := range manifest.Watermarks {
		cur, err := s.recvWatermarkDirect(w.Origin)
		if err != nil {
			return res, false, err
		}
		if w.Sequence > cur {
			changed = true
			if err := b.Set(RecvKey(w.Origin), encodeU64(w.Sequence), nil); err != nil {
				return res, false, err
			}
		}
	}
	storedHLC, err := s.readU64Direct(sysHLC)
	if err != nil {
		return res, false, err
	}
	if manifest.CreatedHLC > storedHLC {
		s.clock.Observe(manifest.CreatedHLC)
	}
	mergedHLC := maxU64(storedHLC, maxU64(manifest.CreatedHLC, s.clock.Max()))
	if mergedHLC > storedHLC {
		changed = true
		if err := b.Set(SysKey(sysHLC), encodeU64(mergedHLC), nil); err != nil {
			return res, false, err
		}
	}
	gen, err := s.readU64Direct(sysGeneration)
	if err != nil {
		return res, false, err
	}
	if changed {
		gen++
		if err := b.Set(SysKey(sysGeneration), encodeU64(gen), nil); err != nil {
			return res, false, err
		}
	}
	for i := uint64(0); i < manifest.ChunkCount; i++ {
		if err := b.Delete(SnapshotKey(fmt.Sprintf("%schunk/%020d", stagePrefix, i)), nil); err != nil {
			return res, false, err
		}
	}
	if err := b.Delete(manifestKey, nil); err != nil {
		return res, false, err
	}
	if err := b.Delete(activeKey, nil); err != nil {
		return res, false, err
	}
	if progressKey != nil {
		if err := b.Delete(progressKey, nil); err != nil {
			return res, false, err
		}
	}
	if err := s.commitBatch(b, pebble.Sync); err != nil {
		return res, false, err
	}
	res.Applied = changed
	res.Generation = gen
	return res, true, nil
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
	return s.commitBatch(b, pebble.Sync)
}
