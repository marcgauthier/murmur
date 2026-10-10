package murmur

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/q"
)

// This file implements item-level query subscriptions over q matchers. The
// worker mirrors the typed subscription lifecycle: an initial snapshot, then
// coalesced updates after durable local or remote changes, with the same
// observer cursors, reset semantics, and subscriber bounds.

// ItemSubscriptionOptions controls bounded delivery for an item subscription.
type ItemSubscriptionOptions struct {
	// BufferSize overrides the configured event channel capacity when positive.
	BufferSize int
	// EmitUnchanged requests an event when the query result is logically equal.
	EmitUnchanged bool
	// ResumeFrom requests the latest snapshot after this retained cursor. The
	// token is bound to the database and materializer generation; a stale,
	// future, or expired token returns ErrSubscriptionExpired.
	ResumeFrom ItemSubscriptionCursor
}

// ItemSubscriptionCursor is a process-local, generation-bound observer
// cursor. It can resume a subscription only while the same database open epoch
// and materializer generation remain active. It is not a durable replication
// position: closing and reopening the database invalidates the token,
// regardless of the configured durability mode.
type ItemSubscriptionCursor struct {
	// Database is the durable database identity.
	Database ids.DBID
	// Epoch distinguishes observer cursors from separate database opens.
	Epoch ids.TxID
	// Generation changes when the typed RIME materializer is rebuilt.
	Generation uint64
	// Sequence is the local observer cursor within Epoch and Generation.
	Sequence uint64
}

func (c ItemSubscriptionCursor) isZero() bool {
	return c.Database.IsZero() && c.Epoch.IsZero() && c.Generation == 0 && c.Sequence == 0
}

// ItemChangeType identifies a row-level subscription diff.
type ItemChangeType string

const (
	ItemAdded   ItemChangeType = "added"
	ItemUpdated ItemChangeType = "updated"
	ItemRemoved ItemChangeType = "removed"
)

// ItemSubscriptionChange is one canonical primary-key diff. Row is nil for
// removals and a detached *T record for additions and updates.
type ItemSubscriptionChange struct {
	Type ItemChangeType
	Key  ids.RowID
	Row  any
}

// ItemSubscriptionEvent contains a detached item query snapshot. Rows holds
// detached *T records for the subscribed model.
type ItemSubscriptionEvent struct {
	Type         SubscriptionEventType
	Cursor       uint64
	ResumeCursor ItemSubscriptionCursor
	Rows         []any
	Changes      []ItemSubscriptionChange
	Err          error
}

// ItemSubscription tracks an item query until Close or context cancellation.
// Query updates are coalesced by the database observer cursor.
type ItemSubscription struct {
	db      *DB
	model   any
	filters []q.Matcher
	id      uint64
	opts    ItemSubscriptionOptions
	events  chan ItemSubscriptionEvent
	done    chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	manager func() bool
	wake    <-chan uint64

	mu       sync.Mutex
	cursor   uint64
	resume   ItemSubscriptionCursor
	closed   bool
	lastRows []any
}

// Subscribe creates an initial snapshot and then emits updates after durable
// local or remote changes. Filters combine conjunctively; the model may be a
// T value or *T pointer.
func (db *DB) Subscribe(ctx context.Context, model any, opts ItemSubscriptionOptions, filters ...q.Matcher) (*ItemSubscription, error) {
	if db == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	binding, err := db.resolveItemBinding(model)
	if err != nil {
		return nil, err
	}
	for _, matcher := range filters {
		if err := binding.validateMatcher(matcher); err != nil {
			return nil, err
		}
		if hasItemParam(matcher) {
			return nil, fmt.Errorf("murmur: subscriptions do not accept query parameters")
		}
	}
	if err := db.requireRead(); err != nil {
		return nil, err
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
	if opts.BufferSize < 0 || opts.BufferSize > m.cfg.EventBufferSize {
		m.mu.Unlock()
		return nil, errors.New("murmur: item subscription buffer size is outside configured bounds")
	}
	if len(m.recordListeners) >= m.cfg.MaxSubscribers {
		m.mu.Unlock()
		return nil, ErrMaxSubscribersReached
	}
	m.nextSubID++
	id := m.nextSubID
	wake := make(chan uint64, 1)
	m.recordListeners[id] = wake
	m.wg.Add(1)
	cursor := m.cursor
	m.mu.Unlock()

	bufSize := opts.BufferSize
	if bufSize == 0 {
		bufSize = m.cfg.EventBufferSize
	}
	subCtx, cancel := context.WithCancel(ctx)
	stopManager := context.AfterFunc(m.ctx, cancel)
	sub := &ItemSubscription{
		db: db, model: model, id: id, opts: opts, events: make(chan ItemSubscriptionEvent, bufSize),
		done: make(chan struct{}), ctx: subCtx, cancel: cancel, manager: stopManager,
		filters: append([]q.Matcher(nil), filters...), wake: wake, cursor: cursor,
	}
	initial, stableCursor, _, err := sub.evaluateStable()
	if err != nil {
		m.mu.Lock()
		delete(m.recordListeners, id)
		m.mu.Unlock()
		m.wg.Done()
		stopManager()
		cancel()
		return nil, err
	}
	initialClone, err := cloneItemRows(db, binding, initial)
	if err != nil {
		m.mu.Lock()
		delete(m.recordListeners, id)
		m.mu.Unlock()
		m.wg.Done()
		stopManager()
		cancel()
		return nil, err
	}
	resume := opts.ResumeFrom
	if !resume.isZero() {
		m.historyMu.RLock()
		minimum := m.minCursor
		m.historyMu.RUnlock()
		if resume.Database != stableCursor.Database || resume.Epoch != stableCursor.Epoch ||
			resume.Generation != stableCursor.Generation || resume.Sequence < minimum || resume.Sequence > stableCursor.Sequence {
			m.mu.Lock()
			delete(m.recordListeners, id)
			m.mu.Unlock()
			m.wg.Done()
			stopManager()
			cancel()
			return nil, ErrSubscriptionExpired
		}
	}
	if resume.isZero() || resume.Sequence < stableCursor.Sequence {
		eventType := EventInitial
		if !resume.isZero() {
			eventType = EventUpdate
		}
		if err := sub.send(ItemSubscriptionEvent{Type: eventType, Cursor: stableCursor.Sequence, ResumeCursor: stableCursor, Rows: initial}); err != nil {
			m.mu.Lock()
			delete(m.recordListeners, id)
			m.mu.Unlock()
			m.wg.Done()
			stopManager()
			cancel()
			return nil, err
		}
	}
	sub.cursor = stableCursor.Sequence
	sub.resume = stableCursor
	sub.lastRows = initialClone
	go sub.run()
	return sub, nil
}

// hasItemParam reports whether a matcher tree contains a bind placeholder,
// which subscriptions cannot evaluate without arguments.
func hasItemParam(matcher q.Matcher) bool {
	matcher, err := derefItemMatcher(matcher)
	if err != nil {
		return false
	}
	switch m := matcher.(type) {
	case q.Comparison:
		_, ok := m.Value.(q.Placeholder)
		return ok
	case q.InMatcher:
		for _, value := range m.Values {
			if _, ok := value.(q.Placeholder); ok {
				return true
			}
		}
		return false
	case q.StringMatcher:
		return false
	case q.AndMatcher:
		for _, child := range m.Matchers {
			if hasItemParam(child) {
				return true
			}
		}
		return false
	case q.OrMatcher:
		for _, child := range m.Matchers {
			if hasItemParam(child) {
				return true
			}
		}
		return false
	case q.NotMatcher:
		return m.Matcher != nil && hasItemParam(m.Matcher)
	default:
		return false
	}
}

// Events returns the receive-only stream of initial and updated snapshots.
func (s *ItemSubscription) Events() <-chan ItemSubscriptionEvent {
	if s == nil {
		return nil
	}
	return s.events
}

// Cursor returns the latest observer cursor evaluated by this subscription.
func (s *ItemSubscription) Cursor() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cursor
}

// ResumeCursor returns the latest cursor for reconnecting during the current
// database open epoch. Callers may serialize it, but it is not valid after the
// database closes or the materializer is rebuilt. It does not identify a
// durable replication position.
func (s *ItemSubscription) ResumeCursor() ItemSubscriptionCursor {
	if s == nil {
		return ItemSubscriptionCursor{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resume
}

// Close cancels the subscription and waits for its worker to release resources.
func (s *ItemSubscription) Close() error {
	if s == nil {
		return nil
	}
	s.cancel()
	<-s.done
	return nil
}

func (s *ItemSubscription) run() {
	m := s.db.subMgr
	defer m.wg.Done()
	defer close(s.done)
	defer close(s.events)
	defer s.manager()
	defer s.cancel()
	defer func() {
		m.mu.Lock()
		delete(m.recordListeners, s.id)
		m.mu.Unlock()
	}()

	last := s.lastRows
	for {
		select {
		case <-s.ctx.Done():
			if m.ctx.Err() != nil {
				s.sendReset(ErrClosed)
			}
			return
		case notifiedCursor := <-s.wake:
			s.mu.Lock()
			closedOrOld := s.closed || notifiedCursor <= s.cursor
			s.mu.Unlock()
			if closedOrOld {
				continue
			}
			rows, cursor, binding, err := s.evaluateStable()
			if err != nil {
				if s.ctx.Err() == nil {
					s.sendReset(err)
				}
				return
			}
			s.mu.Lock()
			staleGeneration := cursor.Database != s.resume.Database || cursor.Epoch != s.resume.Epoch || cursor.Generation != s.resume.Generation
			s.mu.Unlock()
			if staleGeneration {
				s.sendReset(ErrSubscriptionReset)
				return
			}
			changes, err := diffItemRows(s.db, binding, last, rows)
			if err != nil {
				s.sendReset(err)
				return
			}
			changed := len(changes) > 0
			s.mu.Lock()
			s.cursor = cursor.Sequence
			s.resume = cursor
			s.mu.Unlock()
			if !changed && !s.opts.EmitUnchanged {
				continue
			}
			privateRows, err := cloneItemRows(s.db, binding, rows)
			if err != nil {
				s.sendReset(err)
				return
			}
			if err := s.send(ItemSubscriptionEvent{Type: EventUpdate, Cursor: cursor.Sequence, ResumeCursor: cursor, Rows: rows, Changes: changes}); err != nil {
				s.sendReset(err)
				return
			}
			last = privateRows
		}
	}
}

func (s *ItemSubscription) evaluateStable() ([]any, ItemSubscriptionCursor, *itemBinding, error) {
	m := s.db.subMgr
	if err := s.ctx.Err(); err != nil {
		return nil, ItemSubscriptionCursor{}, nil, err
	}
	binding, err := s.db.resolveItemBinding(s.model)
	if err != nil {
		return nil, ItemSubscriptionCursor{}, nil, err
	}
	// Commit publication and its observer cursor advance share applyMu. Pin a
	// RIME snapshot while holding it, then evaluate without blocking writers.
	s.db.applyMu.Lock()
	readTx, err := s.db.readTxContext(s.ctx)
	if err != nil {
		s.db.applyMu.Unlock()
		return nil, ItemSubscriptionCursor{}, nil, err
	}
	m.mu.RLock()
	cursor := ItemSubscriptionCursor{
		Database: s.db.DBID(), Epoch: m.epoch, Generation: readTx.gen, Sequence: m.cursor,
	}
	m.mu.RUnlock()
	s.db.applyMu.Unlock()
	defer readTx.Close()
	rows, err := binding.ops.find(s.db, &itemQuerySpec{binding: binding, matchers: s.filters, limit: -1, read: readTx, ctx: s.ctx})
	if err != nil {
		return nil, ItemSubscriptionCursor{}, nil, err
	}
	return rows, cursor, binding, nil
}

func diffItemRows(db *DB, binding *itemBinding, oldRows, newRows []any) ([]ItemSubscriptionChange, error) {
	oldByKey := make(map[ids.RowID]any, len(oldRows))
	newByKey := make(map[ids.RowID]any, len(newRows))
	for _, row := range oldRows {
		key, err := binding.rowID(row)
		if err != nil {
			return nil, err
		}
		oldByKey[key] = row
	}
	for _, row := range newRows {
		key, err := binding.rowID(row)
		if err != nil {
			return nil, err
		}
		newByKey[key] = row
	}
	changes := make([]ItemSubscriptionChange, 0)
	for key, row := range newByKey {
		previous, exists := oldByKey[key]
		if !exists {
			changes = append(changes, ItemSubscriptionChange{Type: ItemAdded, Key: key, Row: row})
			continue
		}
		equal, err := binding.ops.rowEqual(db, binding.table, previous, row)
		if err != nil {
			return nil, err
		}
		if !equal {
			changes = append(changes, ItemSubscriptionChange{Type: ItemUpdated, Key: key, Row: row})
		}
	}
	for key := range oldByKey {
		if _, exists := newByKey[key]; !exists {
			changes = append(changes, ItemSubscriptionChange{Type: ItemRemoved, Key: key})
		}
	}
	sort.Slice(changes, func(i, j int) bool { return bytes.Compare(changes[i].Key[:], changes[j].Key[:]) < 0 })
	return changes, nil
}

func cloneItemRows(db *DB, binding *itemBinding, rows []any) ([]any, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([]any, len(rows))
	for i, row := range rows {
		cloned, err := binding.ops.rowClone(db, binding.table, row)
		if err != nil {
			return nil, err
		}
		out[i] = cloned
	}
	return out, nil
}

func (s *ItemSubscription) send(event ItemSubscriptionEvent) error {
	select {
	case s.events <- event:
		return nil
	default:
		return ErrSubscriptionReset
	}
}

func (s *ItemSubscription) sendReset(err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	cursor := s.cursor
	resume := s.resume
	s.mu.Unlock()
	select {
	case s.events <- ItemSubscriptionEvent{Type: EventReset, Cursor: cursor, ResumeCursor: resume, Err: err}:
	default:
		select {
		case <-s.events:
		default:
		}
		select {
		case s.events <- ItemSubscriptionEvent{Type: EventReset, Cursor: cursor, ResumeCursor: resume, Err: err}:
		default:
		}
	}
}
