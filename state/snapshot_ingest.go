package state

// commitSnapshotChunk merges one validated snapshot chunk's winning cells
// through a synchronous Spool commit. The caller serializes this operation
// with the state writer lock and persists merge progress after the commit;
// a crash between the chunk commit and the progress commit is safe because
// replay is LWW-idempotent.
func (s *Store) commitSnapshotChunk(raw, prevKey []byte) (lastKey []byte, changed bool, err error) {
	b := s.mem.newBatch()
	defer b.Close()
	lastKey, changed, err = s.mergeSnapshotChunkWithSet(func(key, value []byte) error { return b.Set(key, value) }, raw, prevKey)
	if err != nil {
		return nil, false, err
	}
	if !changed {
		return lastKey, false, nil
	}
	// Synchronous: chunk data must be durable before the caller commits
	// merge progress, or a crash could keep progress for lost data.
	if err := s.commitBatch(b, true); err != nil {
		return nil, false, err
	}
	return lastKey, true, nil
}
