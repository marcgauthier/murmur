package murmur

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/rimeadapter"
	"github.com/marcgauthier/murmur/rime"
)

// recordSubscriptionOptions controls bounded delivery for a typed query
// subscription.
type recordSubscriptionOptions struct {
	// BufferSize overrides the configured event channel capacity when positive.
	BufferSize int
	// EmitUnchanged requests an event when the query result is logically equal.
	EmitUnchanged bool
	// ResumeFrom requests the latest snapshot after this retained cursor. The
	// token is bound to the database and materializer generation; a stale,
	// future, or expired token returns ErrSubscriptionExpired.
	ResumeFrom recordSubscriptionCursor
}

// recordSubscriptionCursor is a process-local, generation-bound observer
// cursor. It can resume a subscription only while the same database open epoch
// and materializer generation remain active. It is not a durable replication
// position: closing and reopening the database invalidates the token, regardless
// of the configured durability mode.
type recordSubscriptionCursor struct {
	// Database is the durable database identity.
	Database ids.DBID
	// Epoch distinguishes observer cursors from separate database opens.
	Epoch ids.TxID
	// Generation changes when the typed RIME materializer is rebuilt.
	Generation uint64
	// Sequence is the local observer cursor within Epoch and Generation.
	Sequence uint64
}

func (c recordSubscriptionCursor) isZero() bool {
	return c.Database.IsZero() && c.Epoch.IsZero() && c.Generation == 0 && c.Sequence == 0
}

// recordChangeType identifies a row-level typed subscription diff.
type recordChangeType string

const (
	RecordAdded   recordChangeType = "added"
	RecordUpdated recordChangeType = "updated"
	RecordRemoved recordChangeType = "removed"
)

// recordSubscriptionChange is one canonical primary-key diff. Row is nil for
// removals and detached from the materializer for additions and updates.
type recordSubscriptionChange[T any] struct {
	Type recordChangeType
	Key  ids.RowID
	Row  *T
}

// recordSubscriptionEvent contains a detached typed query snapshot.
type recordSubscriptionEvent[T any] struct {
	Type         SubscriptionEventType
	Cursor       uint64
	ResumeCursor recordSubscriptionCursor
	Rows         []*T
	Changes      []recordSubscriptionChange[T]
	Err          error
}

// recordSubscription tracks a typed table query until Close or context
// cancellation. Query updates are coalesced by the database observer cursor.
type recordSubscription[T any] struct {
	db          *DB
	table       *recordTable[T]
	exprs       []rime.Expr[T]
	id          uint64
	opts        recordSubscriptionOptions
	events      chan recordSubscriptionEvent[T]
	done        chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
	stopManager func() bool
	wake        <-chan uint64

	mu       sync.Mutex
	cursor   uint64
	resume   recordSubscriptionCursor
	closed   bool
	lastRows []*T
}

// Subscribe creates an initial snapshot and then emits updates after durable
// local or remote changes. The expression list is reapplied to the latest
// materializer generation for every evaluation.
func (t *recordTable[T]) Subscribe(ctx context.Context, opts recordSubscriptionOptions, exprs ...rime.Expr[T]) (*recordSubscription[T], error) {
	if t == nil || t.db == nil {
		return nil, rime.ErrBadView
	}
	if ctx == nil {
		ctx = context.Background()
	}
	db := t.db
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
		return nil, errors.New("murmur: typed subscription buffer size is outside configured bounds")
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
	sub := &recordSubscription[T]{
		db: db, id: id, opts: opts, events: make(chan recordSubscriptionEvent[T], bufSize),
		done: make(chan struct{}), ctx: subCtx, cancel: cancel, stopManager: stopManager,
		table: t, exprs: append([]rime.Expr[T](nil), exprs...), wake: wake, cursor: cursor,
	}
	initial, stableCursor, adapterTable, err := sub.evaluateStable()
	if err != nil {
		m.mu.Lock()
		delete(m.recordListeners, id)
		m.mu.Unlock()
		m.wg.Done()
		stopManager()
		cancel()
		return nil, err
	}
	initialClone, err := cloneTypedRows(initial, adapterTable.Clone)
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
		if err := sub.send(recordSubscriptionEvent[T]{Type: eventType, Cursor: stableCursor.Sequence, ResumeCursor: stableCursor, Rows: initial}); err != nil {
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

// Events returns the receive-only stream of initial and updated snapshots.
func (s *recordSubscription[T]) Events() <-chan recordSubscriptionEvent[T] {
	if s == nil {
		return nil
	}
	return s.events
}

// Cursor returns the latest observer cursor evaluated by this subscription.
func (s *recordSubscription[T]) Cursor() uint64 {
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
func (s *recordSubscription[T]) ResumeCursor() recordSubscriptionCursor {
	if s == nil {
		return recordSubscriptionCursor{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resume
}

// Close cancels the subscription and waits for its worker to release resources.
func (s *recordSubscription[T]) Close() error {
	if s == nil {
		return nil
	}
	s.cancel()
	<-s.done
	return nil
}

func (s *recordSubscription[T]) run() {
	m := s.db.subMgr
	defer m.wg.Done()
	defer close(s.done)
	defer close(s.events)
	defer s.stopManager()
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
			rows, cursor, inner, err := s.evaluateStable()
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
			changes, err := diffTypedRows(last, rows, inner)
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
			privateRows, err := cloneTypedRows(rows, inner.Clone)
			if err != nil {
				s.sendReset(err)
				return
			}
			if err := s.send(recordSubscriptionEvent[T]{Type: EventUpdate, Cursor: cursor.Sequence, ResumeCursor: cursor, Rows: rows, Changes: changes}); err != nil {
				s.sendReset(err)
				return
			}
			last = privateRows
		}
	}
}

func (s *recordSubscription[T]) evaluateStable() ([]*T, recordSubscriptionCursor, *rimeadapter.Table[T], error) {
	m := s.db.subMgr
	if err := s.ctx.Err(); err != nil {
		return nil, recordSubscriptionCursor{}, nil, err
	}
	// Commit publication and its observer cursor advance share applyMu. Pin a
	// RIME snapshot while holding it, then evaluate without blocking writers.
	s.db.applyMu.Lock()
	readTx, err := s.db.readTxContext(s.ctx)
	if err != nil {
		s.db.applyMu.Unlock()
		return nil, recordSubscriptionCursor{}, nil, err
	}
	m.mu.RLock()
	cursor := recordSubscriptionCursor{
		Database: s.db.DBID(), Epoch: m.epoch, Generation: readTx.gen, Sequence: m.cursor,
	}
	m.mu.RUnlock()
	s.db.applyMu.Unlock()
	defer readTx.Close()
	query, err := s.table.WhereReadTx(readTx, s.exprs...)
	if err != nil {
		return nil, recordSubscriptionCursor{}, nil, err
	}
	inner, ok := readTx.tables[s.table.name].(*rimeadapter.Table[T])
	if !ok || inner == nil {
		return nil, recordSubscriptionCursor{}, nil, ErrUnsupportedSchema
	}
	rows, err := query.WithContext(s.ctx).Find()
	if err != nil {
		return nil, recordSubscriptionCursor{}, nil, err
	}
	return rows, cursor, inner, nil
}

func diffTypedRows[T any](oldRows, newRows []*T, table *rimeadapter.Table[T]) ([]recordSubscriptionChange[T], error) {
	oldByKey := make(map[ids.RowID]*T, len(oldRows))
	newByKey := make(map[ids.RowID]*T, len(newRows))
	for _, row := range oldRows {
		key, err := table.PrimaryKey(row)
		if err != nil {
			return nil, err
		}
		oldByKey[key] = row
	}
	for _, row := range newRows {
		key, err := table.PrimaryKey(row)
		if err != nil {
			return nil, err
		}
		newByKey[key] = row
	}
	changes := make([]recordSubscriptionChange[T], 0)
	for key, row := range newByKey {
		previous, exists := oldByKey[key]
		if !exists {
			changes = append(changes, recordSubscriptionChange[T]{Type: RecordAdded, Key: key, Row: row})
			continue
		}
		equal, err := table.Equal(previous, row)
		if err != nil {
			return nil, err
		}
		if !equal {
			changes = append(changes, recordSubscriptionChange[T]{Type: RecordUpdated, Key: key, Row: row})
		}
	}
	for key := range oldByKey {
		if _, exists := newByKey[key]; !exists {
			changes = append(changes, recordSubscriptionChange[T]{Type: RecordRemoved, Key: key})
		}
	}
	sort.Slice(changes, func(i, j int) bool { return bytes.Compare(changes[i].Key[:], changes[j].Key[:]) < 0 })
	return changes, nil
}

func cloneTypedRows[T any](rows []*T, clone func(*T) (*T, error)) ([]*T, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([]*T, len(rows))
	for i, row := range rows {
		if clone != nil {
			cloned, err := clone(row)
			if err != nil {
				return nil, err
			}
			out[i] = cloned
		} else {
			out[i] = rime.CloneRecord(row)
		}
	}
	return out, nil
}

func (s *recordSubscription[T]) send(event recordSubscriptionEvent[T]) error {
	select {
	case s.events <- event:
		return nil
	default:
		return ErrSubscriptionReset
	}
}

func (s *recordSubscription[T]) sendReset(err error) {
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
	case s.events <- recordSubscriptionEvent[T]{Type: EventReset, Cursor: cursor, ResumeCursor: resume, Err: err}:
	default:
		select {
		case <-s.events:
		default:
		}
		select {
		case s.events <- recordSubscriptionEvent[T]{Type: EventReset, Cursor: cursor, ResumeCursor: resume, Err: err}:
		default:
		}
	}
}
