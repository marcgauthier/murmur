package murmur

import (
	"bytes"
	"context"
	"reflect"
	"sync"
)

// SubscriptionEventType distinguishes the kind of event emitted on a subscription.
type SubscriptionEventType string

const (
	// EventInitial is delivered as the first event, containing the full initial query result.
	EventInitial SubscriptionEventType = "initial"
	// EventUpdate is delivered when committed state changes update or affect the query result.
	EventUpdate SubscriptionEventType = "update"
	// EventReset indicates the subscription has fallen behind or the materializer was rebuilt,
	// requiring the client to resnapshot / re-subscribe.
	EventReset SubscriptionEventType = "reset"
)

// SubscriptionRow represents a single row in a query result.
type SubscriptionRow struct {
	Values []any
}

// SubscriptionEvent is emitted to subscribers when query results change.
type SubscriptionEvent struct {
	Type    SubscriptionEventType
	Cursor  uint64
	Columns []string
	Rows    []SubscriptionRow
	Err     error
}

// SubscriptionOptions configures an individual query subscription.
type SubscriptionOptions struct {
	// ResumeFromCursor, if non-zero, attempts to resume from this cursor using the retained history buffer.
	// If the cursor is older than the retained history or in the future, Subscribe returns ErrSubscriptionExpired.
	ResumeFromCursor uint64
	// BufferSize overrides the subscriber channel buffer (capped at Config.Subscription.EventBufferSize).
	BufferSize int
	// EmitUnchanged, if true, emits an EventUpdate on every committed state change even if the query result is identical.
	EmitUnchanged bool
}

// Subscription represents an active reactive query subscription.
type Subscription struct {
	db       *DB
	mgr      *subscriptionManager
	id       uint64
	query    string
	args     []any
	opts     SubscriptionOptions
	columns  []string
	lastRows []SubscriptionRow
	cursor   uint64

	events chan SubscriptionEvent
	done   chan struct{}

	mu     sync.Mutex
	closed bool
	err    error
}

// Events returns the receive-only channel of subscription events.
func (s *Subscription) Events() <-chan SubscriptionEvent {
	return s.events
}

// Cursor returns the current/latest cursor observed by this subscription.
func (s *Subscription) Cursor() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor
}

// Query returns the subscribed query string.
func (s *Subscription) Query() string {
	return s.query
}

// Columns returns the column names of the query result.
func (s *Subscription) Columns() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.columns
}

// Close terminates the subscription and releases resources.
func (s *Subscription) Close() error {
	return s.mgr.unsubscribe(s.id)
}

func (s *Subscription) reset(err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.err = err
	close(s.done)
	cur := s.cursor
	s.mu.Unlock()

	s.mgr.mu.Lock()
	delete(s.mgr.subscribers, s.id)
	s.mgr.mu.Unlock()

	// Deliver EventReset before closing events channel
	select {
	case s.events <- SubscriptionEvent{Type: EventReset, Cursor: cur, Err: err}:
	default:
		// Drain one item if buffer full so reset is reliably placed
		select {
		case <-s.events:
		default:
		}
		select {
		case s.events <- SubscriptionEvent{Type: EventReset, Cursor: cur, Err: err}:
		default:
		}
	}
	close(s.events)
}

type changeNotification struct {
	cursor  uint64
	rebuild bool
}

type subscriptionManager struct {
	db  *DB
	cfg SubscriptionConfig

	mu          sync.RWMutex
	nextSubID   uint64
	subscribers map[uint64]*Subscription
	cursor      uint64

	historyMu sync.RWMutex
	history   []uint64
	minCursor uint64

	notifyCh chan changeNotification
	stopCh   chan struct{}
	wg       sync.WaitGroup
	closed   bool
}

func newSubscriptionManager(db *DB, cfg SubscriptionConfig) *subscriptionManager {
	return &subscriptionManager{
		db:          db,
		cfg:         cfg,
		subscribers: make(map[uint64]*Subscription),
		cursor:      1,
		minCursor:   1,
		history:     make([]uint64, 0, cfg.MaxRetainedEvents),
		notifyCh:    make(chan changeNotification, 128),
		stopCh:      make(chan struct{}),
	}
}

func (m *subscriptionManager) start() {
	m.wg.Add(1)
	go m.run()
}

func (m *subscriptionManager) run() {
	defer m.wg.Done()
	for {
		select {
		case <-m.stopCh:
			return
		case notif := <-m.notifyCh:
			m.dispatch(notif)
		}
	}
}

func (m *subscriptionManager) notifyChange(rebuild bool) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.cursor++
	cur := m.cursor
	m.mu.Unlock()

	m.historyMu.Lock()
	m.history = append(m.history, cur)
	if len(m.history) > m.cfg.MaxRetainedEvents {
		overflow := len(m.history) - m.cfg.MaxRetainedEvents
		m.history = m.history[overflow:]
		m.minCursor = m.history[0]
	}
	m.historyMu.Unlock()

	select {
	case m.notifyCh <- changeNotification{cursor: cur, rebuild: rebuild}:
	default:
	}
}

func (m *subscriptionManager) dispatch(notif changeNotification) {
	m.mu.RLock()
	subs := make([]*Subscription, 0, len(m.subscribers))
	for _, s := range m.subscribers {
		subs = append(subs, s)
	}
	m.mu.RUnlock()

	if notif.rebuild {
		// Schema migration or materializer rebuild: reset all subscribers
		for _, s := range subs {
			s.reset(ErrSubscriptionReset)
		}
		return
	}

	for _, s := range subs {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			continue
		}
		query := s.query
		args := s.args
		lastRows := s.lastRows
		opts := s.opts
		s.mu.Unlock()

		cols, newRows, err := runSubscriptionQuery(context.Background(), m.db, query, args)
		if err != nil {
			// Engine error (e.g. materializer dirty)
			continue
		}

		changed := !rowsEqual(lastRows, newRows)
		if !changed && !opts.EmitUnchanged {
			continue
		}

		event := SubscriptionEvent{
			Type:    EventUpdate,
			Cursor:  notif.cursor,
			Columns: cols,
			Rows:    newRows,
		}

		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			continue
		}
		select {
		case s.events <- event:
			s.lastRows = newRows
			s.columns = cols
			s.cursor = notif.cursor
			s.mu.Unlock()
		default:
			// Channel full - slow consumer
			s.mu.Unlock()
			s.reset(ErrSubscriptionReset)
		}
	}
}

func (m *subscriptionManager) subscribe(ctx context.Context, query string, opts SubscriptionOptions, args ...any) (*Subscription, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	if len(m.subscribers) >= m.cfg.MaxSubscribers {
		m.mu.Unlock()
		return nil, ErrMaxSubscribersReached
	}
	curCursor := m.cursor
	m.nextSubID++
	subID := m.nextSubID
	m.mu.Unlock()

	cols, rows, err := runSubscriptionQuery(ctx, m.db, query, args)
	if err != nil {
		return nil, err
	}

	if opts.ResumeFromCursor > 0 {
		m.historyMu.RLock()
		minCursor := m.minCursor
		m.historyMu.RUnlock()

		if opts.ResumeFromCursor > curCursor || opts.ResumeFromCursor < minCursor {
			return nil, ErrSubscriptionExpired
		}
	}

	bufSize := m.cfg.EventBufferSize
	if opts.BufferSize > 0 && opts.BufferSize <= m.cfg.EventBufferSize {
		bufSize = opts.BufferSize
	}

	events := make(chan SubscriptionEvent, bufSize)
	done := make(chan struct{})

	sub := &Subscription{
		db:       m.db,
		mgr:      m,
		id:       subID,
		query:    query,
		args:     args,
		opts:     opts,
		columns:  cols,
		lastRows: rows,
		cursor:   curCursor,
		events:   events,
		done:     done,
	}

	if opts.ResumeFromCursor == 0 {
		// Initial query result event
		events <- SubscriptionEvent{
			Type:    EventInitial,
			Cursor:  curCursor,
			Columns: cols,
			Rows:    rows,
		}
	} else if opts.ResumeFromCursor < curCursor {
		// Resuming from an earlier retained cursor: emit catchup update
		events <- SubscriptionEvent{
			Type:    EventUpdate,
			Cursor:  curCursor,
			Columns: cols,
			Rows:    rows,
		}
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosed
	}
	m.subscribers[subID] = sub
	m.mu.Unlock()

	if ctx != nil && ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				_ = sub.Close()
			case <-sub.done:
			}
		}()
	}

	return sub, nil
}

func (m *subscriptionManager) unsubscribe(id uint64) error {
	m.mu.Lock()
	sub, exists := m.subscribers[id]
	if exists {
		delete(m.subscribers, id)
	}
	m.mu.Unlock()

	if !exists {
		return nil
	}

	sub.mu.Lock()
	if sub.closed {
		sub.mu.Unlock()
		return nil
	}
	sub.closed = true
	sub.err = ErrSubscriptionClosed
	close(sub.done)
	close(sub.events)
	sub.mu.Unlock()
	return nil
}

func (m *subscriptionManager) close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	close(m.stopCh)
	subs := make([]*Subscription, 0, len(m.subscribers))
	for _, s := range m.subscribers {
		subs = append(subs, s)
	}
	m.subscribers = make(map[uint64]*Subscription)
	m.mu.Unlock()

	m.wg.Wait()

	for _, s := range subs {
		s.mu.Lock()
		if !s.closed {
			s.closed = true
			s.err = ErrClosed
			close(s.done)
			close(s.events)
		}
		s.mu.Unlock()
	}
}

func runSubscriptionQuery(ctx context.Context, db *DB, query string, args []any) ([]string, []SubscriptionRow, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	cols := rows.Columns()

	var result []SubscriptionRow
	for rows.Next() {
		colValues := make([]any, len(cols))
		colPointers := make([]any, len(cols))
		for i := range colValues {
			colPointers[i] = &colValues[i]
		}
		if err := rows.Scan(colPointers...); err != nil {
			return nil, nil, err
		}
		rowVals := make([]any, len(cols))
		for i, v := range colValues {
			if b, ok := v.([]byte); ok {
				cp := make([]byte, len(b))
				copy(cp, b)
				rowVals[i] = cp
			} else {
				rowVals[i] = v
			}
		}
		result = append(result, SubscriptionRow{Values: rowVals})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return cols, result, nil
}

func rowsEqual(a, b []SubscriptionRow) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i].Values) != len(b[i].Values) {
			return false
		}
		for j := range a[i].Values {
			if !valuesEqual(a[i].Values[j], b[i].Values[j]) {
				return false
			}
		}
	}
	return true
}

func valuesEqual(v1, v2 any) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}
	b1, ok1 := v1.([]byte)
	b2, ok2 := v2.([]byte)
	if ok1 && ok2 {
		return bytes.Equal(b1, b2)
	}
	return reflect.DeepEqual(v1, v2)
}
