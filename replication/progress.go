package replication

import (
	"fmt"

	"github.com/marcgauthier/murmur/ids"
)

func (m *Manager) requestProgress(p *peerState, after ids.NodeID) {
	p.mu.Lock()
	ok := p.progressPages && p.session != nil
	p.mu.Unlock()
	if ok {
		m.queueCtrl(p, MsgProgressRequest, 0, EncodeProgressRequest(nil, after))
	}
}

func (m *Manager) onProgressRequest(p *peerState, payload []byte) error {
	after, err := DecodeProgressRequest(payload)
	if err != nil {
		return err
	}
	p.mu.Lock()
	enabled := p.progressPages
	p.mu.Unlock()
	if !enabled {
		return fmt.Errorf("replication: progress pages were not negotiated")
	}
	items, more, err := m.cfg.Store.ReceiveProgressPage(after, ProgressPageEntries)
	if err != nil {
		return err
	}
	page := ProgressPage{More: more, Items: make([]ProgressItem, 0, len(items))}
	for _, item := range items {
		page.Items = append(page.Items, ProgressItem{Origin: item.Origin, Applied: item.Applied, Observed: item.Observed, RetainedFrom: item.FirstRetained, RetainedThrough: item.LastRetained})
	}
	m.queueCtrl(p, MsgProgressPage, 0, EncodeProgressPage(nil, page))
	return nil
}

func (m *Manager) onProgressPage(p *peerState, payload []byte) error {
	page, err := DecodeProgressPage(payload)
	if err != nil {
		return err
	}
	p.mu.Lock()
	if !p.progressPages {
		p.mu.Unlock()
		return fmt.Errorf("replication: unsolicited progress page")
	}
	for _, item := range page.Items {
		if item.Applied > p.have[item.Origin] {
			p.have[item.Origin] = item.Applied
		}
		p.observed[item.Origin] = item.Observed
		p.retainedFrom[item.Origin] = item.RetainedFrom
		p.retainedThrough[item.Origin] = item.RetainedThrough
	}
	p.mu.Unlock()

	for _, item := range page.Items {
		local, e := m.cfg.Store.ReceiveWatermark(item.Origin)
		if e != nil {
			return e
		}
		if item.Applied <= local {
			continue
		}
		from := local + 1
		var source *peerState
		if item.RetainedFrom != 0 && item.RetainedFrom <= from && item.RetainedThrough >= from {
			source = p
		}
		if source == nil {
			m.mu.Lock()
			for _, candidate := range m.peers {
				candidate.mu.Lock()
				eligible := candidate != p && candidate.progressPages && candidate.agreed && candidate.have[item.Origin] >= from && candidate.retainedFrom[item.Origin] != 0 && candidate.retainedFrom[item.Origin] <= from && candidate.retainedThrough[item.Origin] >= from && candidate.session != nil
				candidate.mu.Unlock()
				if eligible {
					source = candidate
					break
				}
			}
			m.mu.Unlock()
		}
		if source != nil {
			m.onNeed(source, nil, EncodeNeed(nil, Need{Origin: item.Origin, FromSeq: from}))
			continue
		}
		// No peer advertises the needed retained suffix. Ask the most advanced
		// connected source for a snapshot, which can reconstruct the gap.
		m.mu.Lock()
		best := p
		bestWM := item.Applied
		for _, candidate := range m.peers {
			candidate.mu.Lock()
			wm := candidate.have[item.Origin]
			eligible := candidate.agreed && candidate.session != nil
			candidate.mu.Unlock()
			if eligible && wm > bestWM {
				best, bestWM = candidate, wm
			}
		}
		m.mu.Unlock()
		m.markSnapshotRequested(best)
		m.queueCtrl(best, MsgSnapshotRequest, 0, nil)
	}
	if page.More && len(page.Items) != 0 {
		m.requestProgress(p, page.Items[len(page.Items)-1].Origin)
	}
	return nil
}
