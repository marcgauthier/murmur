package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/ids"
)

const MaxStagedTransactionBytes int64 = 256 << 20
const MaxStagedTransactions = 4096

var ErrStagingOverloaded = errors.New("state: staged transaction budget exhausted")

type StagedTransactionProgress struct {
	TxID       ids.TxID
	Origin     ids.NodeID
	Sequence   uint64
	ChunkCount uint32
	Received   []bool
}

// StagedTransactionsPage lists bounded, sorted durable transfer availability.
func (s *Store) StagedTransactionsPage(after ids.TxID, limit int) ([]StagedTransactionProgress, bool, error) {
	if limit <= 0 || limit > 128 {
		return nil, false, fmt.Errorf("state: staged progress page limit outside 1..128")
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	prefix := []byte{prefixTxnStage}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: []byte{prefixTxnStage + 1}})
	if err != nil {
		return nil, false, err
	}
	defer iter.Close()
	var out []StagedTransactionProgress
	more := false
	for valid := iter.First(); valid; valid = iter.Next() {
		key := iter.Key()
		if len(key) != 21 || binary.BigEndian.Uint32(key[17:21]) != ^uint32(0) {
			continue
		}
		var tx ids.TxID
		copy(tx[:], key[1:17])
		if !after.IsZero() && bytes.Compare(tx[:], after[:]) <= 0 {
			continue
		}
		meta, n, e := decodeStagedMeta(append([]byte(nil), iter.Value()...))
		if e != nil {
			return nil, false, e
		}
		if meta.TxID != tx {
			return nil, false, fmt.Errorf("state: staged metadata key/identity mismatch")
		}
		if n == 0 {
			continue
		}
		if len(out) == limit {
			more = true
			break
		}
		bits, e := s.stagedBitmapDirect(tx, meta.Count)
		if e != nil {
			return nil, false, e
		}
		out = append(out, StagedTransactionProgress{TxID: tx, Origin: meta.Origin, Sequence: meta.Sequence, ChunkCount: meta.Count, Received: bits})
	}
	return out, more, iter.Error()
}

func (s *Store) stagedBitmapDirect(tx ids.TxID, count uint32) ([]bool, error) {
	bits := make([]bool, count)
	prefix := TransactionStagePrefix(tx)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		key := it.Key()
		if len(key) != 21 {
			continue
		}
		i := binary.BigEndian.Uint32(key[17:21])
		if i == ^uint32(0) {
			continue
		}
		if i >= count {
			return nil, fmt.Errorf("state: staged chunk index exceeds manifest")
		}
		bits[i] = true
	}
	return bits, it.Error()
}

// ReadStagedTransactionChunks returns only requested fragments that are present.
func (s *Store) ReadStagedTransactionChunks(tx ids.TxID, indexes []uint32) (map[uint32][]byte, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	out := make(map[uint32][]byte, len(indexes))
	for _, i := range indexes {
		raw, err := s.getDirect(transactionStageKey(tx, i))
		if isNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		chunk, err := codec.DecodeTransactionChunk(raw, s.limits.MaxTransactionBytes)
		if err != nil {
			return nil, err
		}
		if chunk.TxID != tx || chunk.Index != i {
			return nil, fmt.Errorf("state: staged chunk key/identity mismatch")
		}
		out[i] = raw
	}
	return out, nil
}

// StageTransactionChunk durably records one independently validated chunk.
// Pebble's configured filesystem encrypts these records at rest. The returned
// batch is non-nil only after every chunk and the complete digest validate.
func (s *Store) StageTransactionChunk(_ context.Context, raw []byte, maxBytes int64) (batch *codec.MutationBatch, present []bool, err error) {
	c, err := codec.DecodeTransactionChunk(raw, maxBytes)
	if err != nil {
		return nil, nil, err
	}
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	metaKey := transactionStageMetaKey(c.TxID)
	metaRaw, metaErr := s.getDirect(metaKey)
	var received uint32
	if metaErr == nil {
		meta, count, e := decodeStagedMeta(metaRaw)
		if e != nil {
			return nil, nil, e
		}
		if !sameStagedTransaction(c, meta) {
			return nil, nil, fmt.Errorf("state: conflicting staged transaction metadata")
		}
		received = count
	} else if !isNotFound(metaErr) {
		return nil, nil, metaErr
	}
	chunkKey := transactionStageKey(c.TxID, c.Index)
	oldRaw, oldErr := s.getDirect(chunkKey)
	if oldErr == nil {
		old, e := codec.DecodeTransactionChunk(oldRaw, maxBytes)
		if e != nil {
			return nil, nil, fmt.Errorf("state: corrupt staged transaction chunk: %w", e)
		}
		if !sameStagedTransaction(c, old) || string(old.Data) != string(c.Data) {
			return nil, nil, fmt.Errorf("state: conflicting duplicate transaction chunk %d", c.Index)
		}
	} else if isNotFound(oldErr) {
		iter, e := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte{prefixTxnStage}, UpperBound: []byte{prefixTxnStage + 1}})
		if e != nil {
			return nil, nil, e
		}
		var staged int64
		transfers := 0
		for valid := iter.First(); valid; valid = iter.Next() {
			staged += int64(len(iter.Value()))
			key := iter.Key()
			if len(key) == 21 && binary.BigEndian.Uint32(key[17:21]) == ^uint32(0) {
				transfers++
			}
		}
		e = iter.Error()
		_ = iter.Close()
		if e != nil {
			return nil, nil, e
		}
		if staged+int64(len(raw)) > s.stagedTransactionByteLimit {
			return nil, nil, fmt.Errorf("%w: staged byte budget exceeded", ErrStagingOverloaded)
		}
		if isNotFound(metaErr) && transfers >= s.stagedTransactionCountLimit {
			return nil, nil, fmt.Errorf("%w: staged transaction count budget exceeded", ErrStagingOverloaded)
		}
		b := s.db.NewBatch()
		defer b.Close()
		if err := b.Set(chunkKey, raw, nil); err != nil {
			return nil, nil, err
		}
		if metaErr != nil {
			received = 0
		}
		if err := b.Set(metaKey, encodeStagedMeta(c, received+1), nil); err != nil {
			return nil, nil, err
		}
		if s.transactionStageFault != nil {
			if err := s.transactionStageFault(); err != nil {
				return nil, nil, err
			}
		}
		if err := s.commitBatch(b, s.writeOpts); err != nil {
			return nil, nil, err
		}
		received++
	} else {
		return nil, nil, oldErr
	}
	if received < c.Count {
		return nil, nil, nil
	}
	chunks := make([]*codec.TransactionChunk, c.Count)
	for i := uint32(0); i < c.Count; i++ {
		stored, e := s.getDirect(transactionStageKey(c.TxID, i))
		if e != nil {
			return nil, nil, fmt.Errorf("state: staged chunk count complete but index %d missing: %w", i, e)
		}
		chunks[i], e = codec.DecodeTransactionChunk(stored, maxBytes)
		if e != nil {
			return nil, nil, fmt.Errorf("state: invalid staged chunk %d: %w", i, e)
		}
		if !sameStagedTransaction(c, chunks[i]) {
			return nil, nil, fmt.Errorf("state: staged chunk %d metadata mismatch", i)
		}
	}
	batch, err = codec.AssembleTransactionChunks(chunks, s.limits)
	if err != nil {
		return nil, nil, err
	}
	return batch, nil, nil
}

// StagedTransactionChunks returns the durable availability bitmap for a TxID.
func (s *Store) StagedTransactionChunks(tx ids.TxID) ([]bool, error) {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	prefix := TransactionStagePrefix(tx)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var present []bool
	for valid := iter.First(); valid; valid = iter.Next() {
		key := iter.Key()
		if len(key) != 21 || binary.BigEndian.Uint32(key[17:21]) == ^uint32(0) {
			continue
		}
		_, decodeErr := codec.DecodeTransactionChunk(iter.Value(), s.limits.MaxTransactionBytes)
		if decodeErr != nil {
			return nil, fmt.Errorf("state: corrupt staged transaction chunk: %w", decodeErr)
		}
		i := int(binary.BigEndian.Uint32(key[17:21]))
		if i >= 65_536 {
			return nil, fmt.Errorf("state: staged chunk index out of bounds")
		}
		if i >= len(present) {
			grown := make([]bool, i+1)
			copy(grown, present)
			present = grown
		}
		present[i] = true
	}
	if raw, e := s.getDirect(transactionStageMetaKey(tx)); e == nil {
		meta, _, e := decodeStagedMeta(raw)
		if e != nil {
			return nil, e
		}
		if meta.Count > uint32(len(present)) {
			present = append(present, make([]bool, int(meta.Count)-len(present))...)
		}
	} else if !isNotFound(e) {
		return nil, e
	}
	return present, iter.Error()
}

// ClearStagedTransaction removes persisted fragments after the transaction is
// durably applied. A crash before this cleanup is safe: replay is deduplicated.
func (s *Store) ClearStagedTransaction(tx ids.TxID) error {
	s.gate.RLock()
	defer s.gate.RUnlock()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	prefix := TransactionStagePrefix(tx)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return err
	}
	defer iter.Close()
	b := s.db.NewBatch()
	defer b.Close()
	found := false
	for valid := iter.First(); valid; valid = iter.Next() {
		found = true
		if err := b.Delete(append([]byte(nil), iter.Key()...), nil); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if !found {
		return nil
	}
	return s.commitBatch(b, s.writeOpts)
}

func sameStagedTransaction(a, b *codec.TransactionChunk) bool {
	return a.Version == b.Version && a.Origin == b.Origin && a.Sequence == b.Sequence && a.TxID == b.TxID && a.SchemaEpoch == b.SchemaEpoch && a.SchemaHash == b.SchemaHash && a.TotalLength == b.TotalLength && a.Digest == b.Digest && a.Count == b.Count
}

func encodeStagedMeta(c *codec.TransactionChunk, received uint32) []byte {
	out := make([]byte, 0, 130)
	out = binary.BigEndian.AppendUint16(out, c.Version)
	out = append(out, c.Origin[:]...)
	out = binary.BigEndian.AppendUint64(out, c.Sequence)
	out = append(out, c.TxID[:]...)
	out = binary.BigEndian.AppendUint64(out, c.SchemaEpoch)
	out = append(out, c.SchemaHash[:]...)
	out = binary.BigEndian.AppendUint64(out, c.TotalLength)
	out = append(out, c.Digest[:]...)
	out = binary.BigEndian.AppendUint32(out, c.Count)
	return binary.BigEndian.AppendUint32(out, received)
}

func decodeStagedMeta(raw []byte) (*codec.TransactionChunk, uint32, error) {
	if len(raw) != 130 {
		return nil, 0, fmt.Errorf("state: corrupt staged transaction metadata")
	}
	c := &codec.TransactionChunk{}
	o := 0
	c.Version = binary.BigEndian.Uint16(raw[o : o+2])
	o += 2
	copy(c.Origin[:], raw[o:o+16])
	o += 16
	c.Sequence = binary.BigEndian.Uint64(raw[o : o+8])
	o += 8
	copy(c.TxID[:], raw[o:o+16])
	o += 16
	c.SchemaEpoch = binary.BigEndian.Uint64(raw[o : o+8])
	o += 8
	copy(c.SchemaHash[:], raw[o:o+32])
	o += 32
	c.TotalLength = binary.BigEndian.Uint64(raw[o : o+8])
	o += 8
	copy(c.Digest[:], raw[o:o+32])
	o += 32
	c.Count = binary.BigEndian.Uint32(raw[o : o+4])
	o += 4
	received := binary.BigEndian.Uint32(raw[o : o+4])
	if c.Count == 0 || received > c.Count {
		return nil, 0, fmt.Errorf("state: invalid staged transaction metadata")
	}
	return c, received, nil
}

func prefixUpperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}
