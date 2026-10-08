package state

import (
	"fmt"

	"github.com/marcgauthier/murmur/ids"
)

// CompleteBridgeImport publishes completion receipts and contiguous stream
// progress in one atomic batch. The caller must have applied all bundle effects.
// Existing authenticated receipts are preserved and progress never decreases.
func (s *Store) CompleteBridgeImport(stream string, applied uint64, receipts []ids.TxID) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.failedErr(); err != nil {
		return err
	}
	b := s.mem.newBatch()
	defer b.Close()
	var receipt [24]byte
	copy(receipt[:16], s.nodeID[:])
	for _, txID := range receipts {
		if txID.IsZero() {
			continue
		}
		if _, err := s.getDirect(ReceiptKey(txID)); err == nil {
			continue
		} else if !isNotFound(err) {
			return err
		}
		if err := b.Set(ReceiptKey(txID), receipt[:]); err != nil {
			return err
		}
	}
	current, err := s.getDirect(BridgeProgressKey(stream))
	if err == nil {
		seq, ok := decodeU64(current)
		if !ok {
			return fmt.Errorf("state: invalid bridge stream progress encoding for %q", stream)
		}
		if seq > applied {
			applied = seq
		}
	} else if !isNotFound(err) {
		return err
	}
	if err := b.Set(BridgeProgressKey(stream), encodeU64(applied)); err != nil {
		return err
	}
	return s.commitBatch(b, s.syncCommits)
}
