package replication

import (
	"fmt"
	"time"

	"github.com/marcgauthier/murmur/ids"
)

func (m *Manager) onChunkAvailabilityRequest(p *peerState, payload []byte) error {
	after, err := DecodeChunkAvailabilityRequest(payload)
	if err != nil {
		return err
	}
	p.mu.Lock()
	enabled := p.caps&CapTransactionChunks != 0
	p.mu.Unlock()
	if !enabled {
		return fmt.Errorf("replication: chunk availability was not negotiated")
	}
	items, more, err := m.cfg.Store.StagedTransactionsPage(after, ChunkAvailabilityPageEntries)
	if err != nil {
		return err
	}
	page := ChunkAvailabilityPage{More: more, Items: make([]ChunkAvailability, 0, len(items))}
	for _, item := range items {
		page.Items = append(page.Items, ChunkAvailability{TxID: item.TxID, Origin: item.Origin, Sequence: item.Sequence, ChunkCount: item.ChunkCount, Received: item.Received})
	}
	m.queueCtrl(p, MsgChunkAvailabilityPage, 0, EncodeChunkAvailabilityPage(nil, page))
	return nil
}

func (m *Manager) onChunkAvailabilityPage(p *peerState, payload []byte) error {
	page, err := DecodeChunkAvailabilityPage(payload)
	if err != nil {
		return err
	}
	p.mu.Lock()
	if p.caps&CapTransactionChunks == 0 {
		p.mu.Unlock()
		return fmt.Errorf("replication: unsolicited chunk availability page")
	}
	for _, item := range page.Items {
		if len(p.chunkAvailability) < 4096 || p.chunkAvailability[item.TxID].TxID == item.TxID {
			p.chunkAvailability[item.TxID] = item
		}
	}
	p.mu.Unlock()
	for _, item := range page.Items {
		local, err := m.cfg.Store.ReceiveWatermark(item.Origin)
		if err != nil {
			return err
		}
		if item.Sequence <= local {
			continue
		}
		present, err := m.cfg.Store.StagedTransactionChunks(item.TxID)
		if err != nil {
			return err
		}
		if len(present) < int(item.ChunkCount) {
			present = append(present, make([]bool, int(item.ChunkCount)-len(present))...)
		}
		missing := make([]uint32, 0, item.ChunkCount)
		for i, has := range present {
			if !has {
				missing = append(missing, uint32(i))
			}
		}
		if len(missing) == 0 {
			continue
		}
		m.mu.Lock()
		if m.chunkRepairNeed == nil {
			m.chunkRepairNeed = make(map[ids.TxID]ChunkNeed)
		}
		last := m.chunkRepairAt[item.TxID]
		send := last.IsZero() || time.Since(last) > 10*time.Second
		if send {
			m.chunkRepairAt[item.TxID] = time.Now()
			need := ChunkNeed{Origin: item.Origin, Sequence: item.Sequence, TxID: item.TxID, Missing: missing}
			m.chunkRepairNeed[item.TxID] = need
		}
		m.mu.Unlock()
		if send {
			m.requestChunkSources(p, ChunkNeed{Origin: item.Origin, Sequence: item.Sequence, TxID: item.TxID, Missing: missing}, missing)
		}
	}
	if page.More && len(page.Items) > 0 {
		last := page.Items[len(page.Items)-1].TxID
		m.queueCtrl(p, MsgChunkAvailabilityRequest, 0, EncodeChunkAvailabilityRequest(nil, last))
	}
	return nil
}

func (m *Manager) requestChunkSources(primary *peerState, chunk ChunkNeed, missing []uint32) int {
	needFor := func(av ChunkAvailability) []uint32 {
		var out []uint32
		for _, i := range missing {
			if int(i) < len(av.Received) && av.Received[i] {
				out = append(out, i)
			}
		}
		return out
	}
	sent := 0
	primary.mu.Lock()
	primarySession := primary.session != nil && !primary.chunkUnavailable[chunk.TxID]
	primary.mu.Unlock()
	if primarySession {
		m.queueCtrl(primary, MsgChunkNeed, 0, EncodeChunkNeed(nil, ChunkNeed{Origin: chunk.Origin, Sequence: chunk.Sequence, TxID: chunk.TxID, Missing: missing}))
		sent++
	}
	m.mu.Lock()
	for _, candidate := range m.peers {
		if candidate == primary {
			continue
		}
		candidate.mu.Lock()
		if !candidate.agreed || candidate.session == nil || candidate.caps&CapTransactionChunks == 0 || candidate.chunkUnavailable[chunk.TxID] {
			candidate.mu.Unlock()
			continue
		}
		item, advertised := candidate.chunkAvailability[chunk.TxID]
		var request []uint32
		if advertised && item.Origin == chunk.Origin && item.Sequence == chunk.Sequence {
			request = needFor(item)
		}
		if len(request) == 0 && candidate.retainedFrom[chunk.Origin] != 0 && candidate.retainedFrom[chunk.Origin] <= chunk.Sequence && candidate.retainedThrough[chunk.Origin] >= chunk.Sequence {
			request = missing
		}
		if len(request) == 0 && candidate.have[chunk.Origin] >= chunk.Sequence {
			request = missing
		}
		candidate.mu.Unlock()
		if len(request) > 0 {
			m.queueCtrl(candidate, MsgChunkNeed, 0, EncodeChunkNeed(nil, ChunkNeed{Origin: chunk.Origin, Sequence: chunk.Sequence, TxID: chunk.TxID, Missing: request}))
			sent++
			if sent >= 3 {
				break
			}
		}
	}
	m.mu.Unlock()
	return sent
}
