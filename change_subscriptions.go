package murmur

import (
	"context"
	"sync"
)

// ChangeSubscription receives lightweight commit notifications without
// materializing a query snapshot. Use typed RecordSubscription when row-level
// values or diffs are needed.
type ChangeSubscription struct {
	manager *subscriptionManager
	id      uint64
	events  chan uint64
	mu      sync.Mutex
	stops   []func() bool
	closed  bool
	once    sync.Once
}

// SubscribeChanges observes local commits, remote applies, and materializer
// rebuilds. Notifications are coalesced; consumers should reread current data.
func (db *DB) SubscribeChanges(ctx context.Context) (*ChangeSubscription, error) {
	if db == nil {
		return nil, ErrNotReady
	}
	if err := db.requireRead(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m := db.subMgr
	if m == nil {
		return nil, ErrNotReady
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	if len(m.recordListeners) >= m.cfg.MaxSubscribers {
		m.mu.Unlock()
		return nil, ErrMaxSubscribersReached
	}
	m.nextSubID++
	sub := &ChangeSubscription{manager: m, id: m.nextSubID, events: make(chan uint64, m.cfg.EventBufferSize)}
	m.recordListeners[sub.id] = sub.events
	m.mu.Unlock()
	sub.addStop(context.AfterFunc(ctx, func() { _ = sub.Close() }))
	if ctx != m.ctx {
		sub.addStop(context.AfterFunc(m.ctx, func() { _ = sub.Close() }))
	}
	return sub, nil
}

func (s *ChangeSubscription) addStop(stop func() bool) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		stop()
		return
	}
	s.stops = append(s.stops, stop)
	s.mu.Unlock()
}

// Events returns commit sequence notifications. Intermediate values may be
// coalesced during backpressure; the latest value always signals a reread.
func (s *ChangeSubscription) Events() <-chan uint64 {
	if s == nil {
		return nil
	}
	return s.events
}

// Close removes the listener and closes its event channel.
func (s *ChangeSubscription) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		s.mu.Lock()
		stops := append([]func() bool(nil), s.stops...)
		s.stops = nil
		s.closed = true
		s.mu.Unlock()
		for _, stop := range stops {
			stop()
		}
		m := s.manager
		if m == nil {
			return
		}
		m.mu.Lock()
		if _, ok := m.recordListeners[s.id]; ok {
			delete(m.recordListeners, s.id)
			close(s.events)
		}
		m.mu.Unlock()
	})
	return nil
}
