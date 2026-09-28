package replication

import (
	"errors"
	"fmt"
	"time"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/overload"
	"github.com/nomadsql/replicateddb/state"
)

func (m *Manager) onTransactionChunk(p *peerState, raw []byte) error {
	p.mu.Lock()
	supported := p.caps&CapTransactionChunks != 0
	p.mu.Unlock()
	if !supported {
		return fmt.Errorf("replication: transaction chunks were not negotiated")
	}
	chunk, err := codec.DecodeTransactionChunk(raw, m.cfg.MaxTransactionBytes)
	if err != nil {
		return err
	}
	if err := m.waitTransfer(p, len(raw)); err != nil {
		if errors.Is(err, overload.ErrOverloaded) {
			m.queueRetryError(p, ErrOverloadedCode, retryHint(chunk.Origin, chunk.Sequence))
			return nil
		}
		return err
	}
	if wm, e := m.cfg.Store.ReceiveWatermark(chunk.Origin); e != nil {
		return e
	} else if wm >= chunk.Sequence {
		return m.cfg.Store.ClearStagedTransaction(chunk.TxID)
	}
	batch, _, err := m.cfg.Store.StageTransactionChunk(m.ctx, raw, m.cfg.MaxTransactionBytes)
	if err != nil {
		if errors.Is(err, state.ErrStagingOverloaded) {
			m.queueRetryError(p, ErrOverloadedCode, retryHint(chunk.Origin, chunk.Sequence))
			return nil
		}
		return err
	}
	if batch == nil {
		if wm, e := m.cfg.Store.ReceiveWatermark(chunk.Origin); e != nil {
			return e
		} else if wm >= chunk.Sequence {
			return m.cfg.Store.ClearStagedTransaction(chunk.TxID)
		}
		m.mu.Lock()
		if m.chunkRepairAt == nil {
			m.chunkRepairAt = make(map[ids.TxID]time.Time)
		}
		now := time.Now()
		lastRequest := m.chunkRepairAt[chunk.TxID]
		wantRepair := lastRequest.IsZero() || now.Sub(lastRequest) > 10*time.Second
		if wantRepair {
			m.chunkRepairAt[chunk.TxID] = now
		}
		m.mu.Unlock()
		if !wantRepair {
			return nil
		}
		present, err := m.cfg.Store.StagedTransactionChunks(chunk.TxID)
		if err != nil {
			return err
		}
		missing := make([]uint32, 0, len(present))
		for i, ok := range present {
			if !ok {
				missing = append(missing, uint32(i))
			}
		}
		if len(missing) == 0 {
			return nil
		}
		request := ChunkNeed{Origin: chunk.Origin, Sequence: chunk.Sequence, TxID: chunk.TxID, Missing: missing}
		m.mu.Lock()
		if m.chunkRepairNeed == nil {
			m.chunkRepairNeed = make(map[ids.TxID]ChunkNeed)
		}
		if len(m.chunkRepairNeed) < 4096 || m.chunkRepairNeed[chunk.TxID].TxID == chunk.TxID {
			m.chunkRepairNeed[chunk.TxID] = request
		}
		m.mu.Unlock()
		m.requestChunkSources(p, request, missing)
		return nil
	}
	if err := m.validateBatch(batch); err != nil {
		return err
	}
	if !m.batchSchemaKnown(batch) {
		if m.canSyncSchema() {
			m.queueSchemaRequest(p, SchemaRequest{WantCurrent: true})
		}
		return nil
	}
	if err := m.applyWithRetry(batch); err != nil {
		if state.IsGap(err) {
			wm, _ := m.cfg.Store.ReceiveWatermark(batch.OriginNode)
			m.queueCtrl(p, MsgNeed, 0, EncodeNeed(nil, Need{Origin: batch.OriginNode, FromSeq: wm + 1}))
			return nil
		}
		return err
	}
	if err := m.cfg.Store.ClearStagedTransaction(batch.TxID); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.chunkRepairAt, batch.TxID)
	delete(m.chunkRepairNeed, batch.TxID)
	m.mu.Unlock()
	m.st.batchesReceived.Add(1)
	m.st.mutationsReceived.Add(uint64(len(batch.Mutations)))
	if m.plumtree != nil {
		_ = m.plumtreeReceive(p.id, batch)
	}
	return nil
}

func (m *Manager) serveChunkNeed(p *peerState, ps *peerSession, raw []byte) error {
	return m.serveChunkNeedCapped(p, ps, raw, 16)
}

func (m *Manager) serveChunkNeedCapped(p *peerState, ps *peerSession, raw []byte, maxChunks int) error {
	p.mu.Lock()
	supported := p.caps&CapTransactionChunks != 0
	p.mu.Unlock()
	if !supported {
		return fmt.Errorf("replication: transaction chunks were not negotiated")
	}
	n, err := DecodeChunkNeed(raw)
	if err != nil {
		return err
	}
	remainingRequest := n
	if maxChunks > 0 && len(n.Missing) > maxChunks {
		remainingRequest.Missing = append([]uint32(nil), n.Missing[maxChunks:]...)
		n.Missing = n.Missing[:maxChunks]
	} else {
		remainingRequest.Missing = nil
	}
	staged, err := m.cfg.Store.ReadStagedTransactionChunks(n.TxID, n.Missing)
	if err != nil {
		return err
	}
	sent := make(map[uint32]bool, len(staged))
	for index, raw := range staged {
		if err := m.waitTransfer(p, len(raw)); err != nil {
			return err
		}
		if err := ps.sendDataType(MsgTransactionChunk, 0, raw); err != nil {
			return err
		}
		sent[index] = true
	}
	remaining := make([]uint32, 0, len(n.Missing))
	for _, index := range n.Missing {
		if !sent[index] {
			remaining = append(remaining, index)
		}
	}
	requested := make(map[uint32]bool, len(remaining))
	for _, index := range remaining {
		requested[index] = true
	}
	if len(remaining) > 0 {
		var batch *codec.MutationBatch
		_, scanErr := m.cfg.Store.LogScan(n.Origin, n.Sequence, 1, int(m.cfg.MaxTransactionBytes), func(b *codec.MutationBatch) error { batch = b; return nil })
		if scanErr == nil && batch != nil && batch.Sequence == n.Sequence && batch.TxID == n.TxID {
			err = codec.VisitTransactionChunks(batch, m.cfg.MaxTransactionBytes, func(index uint32, frame []byte) error {
				if !requested[index] {
					return nil
				}
				if err := m.waitTransfer(p, len(frame)); err != nil {
					return err
				}
				if err := ps.sendDataType(MsgTransactionChunk, 0, frame); err != nil {
					return err
				}
				sent[index] = true
				return nil
			})
			if err != nil {
				return err
			}
		}
	}
	for _, index := range n.Missing {
		if sent[index] {
			continue
		}
	}
	for _, index := range n.Missing {
		if !sent[index] {
			return ps.send(MsgError, 0, EncodeError(nil, ErrRangeUnavailable, n.TxID.String()))
		}
	}
	if len(remainingRequest.Missing) > 0 {
		m.queueCtrl(p, MsgChunkNeed, 0, EncodeChunkNeed(nil, remainingRequest))
	}
	return nil
}

// chunkMessageFor converts a durable batch only when it exceeds the bounded
// regular data-frame size; ordinary batches retain the compact batch format.
func (m *Manager) sendBatchOrChunks(p *peerState, ps *peerSession, batch *codec.MutationBatch) error {
	if codec.EncodedBatchSize(batch)+4 <= MaxFrameBytes {
		raw := EncodeBatches(nil, []*codec.MutationBatch{batch})
		if err := m.waitTransfer(p, len(raw)); err != nil {
			return err
		}
		return ps.send(MsgBatches, 0, raw)
	}
	p.mu.Lock()
	supported := p.caps&CapTransactionChunks != 0
	p.mu.Unlock()
	if !supported {
		return fmt.Errorf("replication: peer does not support oversized transactions")
	}
	return codec.VisitTransactionChunks(batch, m.cfg.MaxTransactionBytes, func(_ uint32, chunk []byte) error {
		if err := m.waitTransfer(p, len(chunk)); err != nil {
			return err
		}
		return ps.sendDataType(MsgTransactionChunk, 0, chunk)
	})
}
