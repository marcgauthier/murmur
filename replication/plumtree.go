package replication

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"time"

	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/ids"
)

const plumtreeGraftDelay = 75 * time.Millisecond

func plumtreeIdentity(b *codec.MutationBatch) ([32]byte, []byte) {
	raw := EncodeBatches(nil, []*codec.MutationBatch{b})
	return sha256.Sum256(raw), raw
}

func (m *Manager) plumtreePeerSets(exclude ids.NodeID) (ordered, protected []ids.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	type candidate struct {
		id       ids.NodeID
		selected bool
	}
	var candidates []candidate
	for id, p := range m.peers {
		if id == exclude {
			continue
		}
		p.mu.Lock()
		ok := p.plumtree && p.agreed && p.session != nil && !p.session.isClosed()
		selected := p.selected
		p.mu.Unlock()
		if ok {
			candidates = append(candidates, candidate{id: id, selected: selected})
		}
	}
	byID := append([]candidate(nil), candidates...)
	sort.Slice(byID, func(i, j int) bool { return byID[i].id.Compare(byID[j].id) < 0 })
	protectedSet := make(map[ids.NodeID]struct{}, 2)
	if len(byID) > 1 {
		at := sort.Search(len(byID), func(i int) bool { return byID[i].id.Compare(m.cfg.Local) >= 0 })
		pred := (at - 1 + len(byID)) % len(byID)
		succ := at % len(byID)
		protectedSet[byID[pred].id] = struct{}{}
		protectedSet[byID[succ].id] = struct{}{}
	} else if len(byID) == 1 {
		protectedSet[byID[0].id] = struct{}{}
	}
	for id := range protectedSet {
		protected = append(protected, id)
	}
	sort.Slice(protected, func(i, j int) bool { return protected[i].Compare(protected[j]) < 0 })
	sort.Slice(candidates, func(i, j int) bool {
		_, iProtected := protectedSet[candidates[i].id]
		_, jProtected := protectedSet[candidates[j].id]
		if iProtected != jProtected {
			return iProtected
		}
		if candidates[i].selected != candidates[j].selected {
			return candidates[i].selected
		}
		return candidates[i].id.Compare(candidates[j].id) < 0
	})
	for _, c := range candidates {
		if c.id != exclude {
			ordered = append(ordered, c.id)
		}
	}
	return ordered, protected
}

func (m *Manager) queuePlumtree(id ids.NodeID, typ uint16, payload []byte, due time.Time) {
	if len(payload) > MaxFrameBytes {
		return // ordinary range/snapshot repair remains available for large transactions
	}
	m.mu.Lock()
	p := m.peers[id]
	m.mu.Unlock()
	if p == nil {
		return
	}
	p.mu.Lock()
	active := p.plumtree && p.agreed && p.session != nil
	p.mu.Unlock()
	if !active {
		return
	}
	f := ctrlFrame{typ: typ, payload: append([]byte(nil), payload...), due: due}
	select {
	case p.plumtreeCh <- f:
		p.pokeWake()
	default:
		// Eager queue drops are repaired by ordinary Ack/Need anti-entropy.
	}
}

func (m *Manager) plumtreeStart(exclude ids.NodeID, b *codec.MutationBatch) {
	if m.plumtree == nil {
		return
	}
	id, raw := plumtreeIdentity(b)
	if m.plumtree.Has(id) {
		return
	}
	peers, protected := m.plumtreePeerSets(exclude)
	if len(peers) == 0 {
		return
	}
	m.plumtree.SetPeers(peers, protected)
	plan, err := m.plumtree.Start(id, raw, peers)
	if err != nil {
		return
	}
	data := EncodePlumtreeData(nil, id, raw)
	hint := EncodePlumtreeHint(nil, PlumtreeHint{ID: id, Origin: b.OriginNode, Seq: b.Sequence})
	for _, peer := range plan.Eager {
		m.queuePlumtree(peer, MsgPlumtreeData, data, time.Time{})
	}
	for _, peer := range plan.Lazy {
		m.queuePlumtree(peer, MsgPlumtreeIHave, hint, time.Time{})
	}
}

func (m *Manager) plumtreeReceive(from ids.NodeID, b *codec.MutationBatch) error {
	if m.plumtree == nil {
		return nil
	}
	id, raw := plumtreeIdentity(b)
	peers, protected := m.plumtreePeerSets(ids.NodeID{})
	m.plumtree.SetPeers(peers, protected)
	plan, err := m.plumtree.Receive(id, raw, from)
	if err != nil {
		return err
	}
	hint := EncodePlumtreeHint(nil, PlumtreeHint{ID: id, Origin: b.OriginNode, Seq: b.Sequence})
	if plan.Prune {
		m.queuePlumtree(from, MsgPlumtreePrune, hint, time.Time{})
	}
	data := EncodePlumtreeData(nil, id, raw)
	for _, peer := range plan.Eager {
		m.queuePlumtree(peer, MsgPlumtreeData, data, time.Time{})
	}
	for _, peer := range plan.Lazy {
		if peer != from {
			m.queuePlumtree(peer, MsgPlumtreeIHave, hint, time.Time{})
		}
	}
	return nil
}

func (m *Manager) onPlumtreeData(p *peerState, payload []byte) error {
	if m.plumtree == nil || !peerUsesPlumtree(p) {
		return fmt.Errorf("replication: plumtree data was not negotiated")
	}
	id, batchPayload, err := DecodePlumtreeData(payload)
	if err != nil {
		return err
	}
	batches, err := DecodeBatches(batchPayload, m.cfg.Limits)
	if err != nil {
		return err
	}
	if len(batches) != 1 {
		return fmt.Errorf("replication: plumtree frame must contain one batch")
	}
	actual, _ := plumtreeIdentity(batches[0])
	if actual != id {
		return fmt.Errorf("replication: plumtree message identity mismatch")
	}
	return m.onBatches(p, nil, batchPayload)
}

func (m *Manager) onPlumtreeIHave(p *peerState, payload []byte) error {
	if m.plumtree == nil || !peerUsesPlumtree(p) {
		return fmt.Errorf("replication: plumtree mode was not negotiated")
	}
	h, err := DecodePlumtreeHint(payload)
	if err != nil {
		return err
	}
	if !m.plumtree.Has(h.ID) {
		m.queuePlumtree(p.id, MsgPlumtreeGraft, payload, time.Now().Add(plumtreeGraftDelay))
	}
	return nil
}

func (m *Manager) onPlumtreePrune(p *peerState, payload []byte) error {
	if m.plumtree == nil || !peerUsesPlumtree(p) {
		return fmt.Errorf("replication: plumtree mode was not negotiated")
	}
	if _, err := DecodePlumtreeHint(payload); err != nil {
		return err
	}
	m.plumtree.Prune(p.id)
	return nil
}

func (m *Manager) onPlumtreeGraft(p *peerState, _ *peerSession, payload []byte) error {
	if m.plumtree == nil || !peerUsesPlumtree(p) {
		return fmt.Errorf("replication: plumtree mode was not negotiated")
	}
	h, err := DecodePlumtreeHint(payload)
	if err != nil {
		return err
	}
	plan := m.plumtree.Graft(h.ID, p.id)
	if len(plan.Payload) > 0 {
		m.queuePlumtree(p.id, MsgPlumtreeData, EncodePlumtreeData(nil, h.ID, plan.Payload), time.Time{})
		return nil
	}
	// The cache expired or was evicted. Fall back to the durable origin log.
	m.queueCtrl(p, MsgNeed, 0, EncodeNeed(nil, Need{Origin: h.Origin, FromSeq: h.Seq}))
	return nil
}

func peerUsesPlumtree(p *peerState) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.plumtree
}

func (m *Manager) sendQueuedPlumtree(p *peerState, ps *peerSession) error {
	for i := 0; i < 16; i++ {
		select {
		case frame := <-p.plumtreeCh:
			if !frame.due.IsZero() && time.Now().Before(frame.due) {
				select {
				case p.plumtreeCh <- frame:
				default:
				}
				return nil
			}
			if frame.typ == MsgPlumtreeGraft && m.plumtree != nil {
				h, err := DecodePlumtreeHint(frame.payload)
				if err != nil || m.plumtree.Has(h.ID) {
					continue // eager data arrived before the IHAVE timer expired
				}
			}
			if err := ps.send(frame.typ, frame.flags, frame.payload); err != nil {
				return err
			}
		default:
			return nil
		}
	}
	return nil
}
