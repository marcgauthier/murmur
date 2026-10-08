package murmur

import (
	"context"
	"sync"

	"github.com/marcgauthier/murmur/ids"
)

// SubscriptionEventType identifies a typed record subscription event.
type SubscriptionEventType string

const (
	// EventInitial carries the initial typed query snapshot.
	EventInitial SubscriptionEventType = "initial"
	// EventUpdate carries the latest typed query snapshot and its row diff.
	EventUpdate SubscriptionEventType = "update"
	// EventReset means the subscriber must take a new snapshot.
	EventReset SubscriptionEventType = "reset"
)

// subscriptionManager owns process-local observer cursors and wakes typed
// record subscriptions after local writes, remote applies, or materializer
// rebuilds. Query evaluation belongs to RecordSubscription, not this manager.
type subscriptionManager struct {
	cfg    SubscriptionConfig
	ctx    context.Context
	cancel context.CancelFunc
	epoch  ids.TxID

	mu              sync.RWMutex
	nextSubID       uint64
	recordListeners map[uint64]chan uint64
	cursor          uint64
	closed          bool

	historyMu sync.RWMutex
	history   []uint64
	minCursor uint64

	wg sync.WaitGroup
}

func newSubscriptionManager(parent context.Context, cfg SubscriptionConfig) *subscriptionManager {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &subscriptionManager{
		cfg:             cfg,
		ctx:             ctx,
		cancel:          cancel,
		epoch:           ids.NewTxID(),
		recordListeners: make(map[uint64]chan uint64),
		cursor:          1,
		minCursor:       1,
		history:         make([]uint64, 0, cfg.MaxRetainedEvents),
	}
}

func (m *subscriptionManager) notifyChange(rebuild bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.cursor++
	cur := m.cursor

	// Keep retained cursors in commit-notification order. A rebuild starts a
	// new observer history because cursors from the replaced materializer are
	// invalid even if their sequence would otherwise remain in the window.
	m.historyMu.Lock()
	if rebuild {
		m.history = m.history[:0]
		m.minCursor = cur
	}
	m.history = append(m.history, cur)
	if len(m.history) > m.cfg.MaxRetainedEvents {
		overflow := len(m.history) - m.cfg.MaxRetainedEvents
		m.history = m.history[overflow:]
		m.minCursor = m.history[0]
	}
	m.historyMu.Unlock()

	for _, listener := range m.recordListeners {
		select {
		case listener <- cur:
		default:
			select {
			case <-listener:
			default:
			}
			select {
			case listener <- cur:
			default:
			}
		}
	}
}

func (m *subscriptionManager) close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	m.cancel()
	m.mu.Unlock()
	m.wg.Wait()
}
