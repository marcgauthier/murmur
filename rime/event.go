package rime

import "sync"

// Asynchronous events are separate from transactional hooks: hooks execute as
// part of the operation/commit path, while events dispatch after a successful
// commit on a bounded worker. Backpressure is explicit and configurable:
//
//   - The queue holds at most queueSize pending change batches (DB option
//     WithEventQueueSize, default 1024).
//   - When full, WithEventDropOldest(true) drops the oldest batch; the
//     default blocks the committer until room is available.
//
// RIME never spawns an unbounded goroutine per event: one worker per table
// serves all of that table's subscribers.
type eventBus[T any] struct {
	mu      sync.Mutex
	emitMu  sync.Mutex // serializes producers with channel closure; worker never takes it
	ch      chan Change[T]
	dropOld bool
	started bool
	closed  bool
	wg      sync.WaitGroup

	inserted  []func(*T)
	updated   []func(old, newv *T)
	deleted   []func(*T)
	committed []func(Change[T])
}

func (t *Table[T]) ensureBus() *eventBus[T] {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.bus == nil {
		t.bus = &eventBus[T]{ch: make(chan Change[T], t.db.cfg.eventQueue), dropOld: t.db.cfg.eventDropOld, closed: t.db.closed.Load()}
	}
	t.hasAfter.Store(true)
	return t.bus
}

func (b *eventBus[T]) ensureWorker() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started || b.closed {
		return
	}
	if len(b.inserted)+len(b.updated)+len(b.deleted)+len(b.committed) == 0 {
		return
	}
	b.started = true
	b.wg.Add(1)
	go b.loop()
}

func (b *eventBus[T]) loop() {
	defer b.wg.Done()
	for ch := range b.ch {
		b.mu.Lock()
		ins := append([]func(*T){}, b.inserted...)
		upd := append([]func(old, newv *T){}, b.updated...)
		del := append([]func(*T){}, b.deleted...)
		com := append([]func(Change[T]){}, b.committed...)
		b.mu.Unlock()
		switch ch.Operation {
		case OpInsert:
			for _, fn := range ins {
				fn(ch.New)
			}
		case OpUpdate:
			for _, fn := range upd {
				fn(ch.Old, ch.New)
			}
		case OpDelete:
			for _, fn := range del {
				fn(ch.Old)
			}
		}
		for _, fn := range com {
			fn(ch)
		}
	}
}

func (b *eventBus[T]) emit(ch Change[T]) {
	b.emitMu.Lock()
	defer b.emitMu.Unlock()
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.mu.Unlock()
	if b.dropOldest() {
		select {
		case b.ch <- ch:
		default:
			select {
			case <-b.ch:
			default:
			}
			select {
			case b.ch <- ch:
			default:
			}
		}
		return
	}
	b.ch <- ch // blocks the committer: explicit backpressure
}

func (b *eventBus[T]) dropOldest() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropOld
}

func (t *Table[T]) closeEvents() {
	t.mu.Lock()
	bus := t.bus
	t.mu.Unlock()
	if bus == nil {
		return
	}
	bus.emitMu.Lock()
	bus.mu.Lock()
	if !bus.closed {
		bus.closed = true
		close(bus.ch)
	}
	bus.mu.Unlock()
	bus.emitMu.Unlock()
	bus.wg.Wait()
}

// OnInserted subscribes to async post-commit insert events.
func (t *Table[T]) OnInserted(fn func(*T)) *Table[T] {
	b := t.ensureBus()
	if t.subscribeClosed(b) {
		return t
	}
	b.mu.Lock()
	b.inserted = append(b.inserted, fn)
	b.mu.Unlock()
	b.ensureWorker()
	return t
}

// subscribeClosed marks the bus closed when the database is already closed,
// so post-close subscriptions neither buffer events nor start workers.
func (t *Table[T]) subscribeClosed(b *eventBus[T]) bool {
	if !t.db.closed.Load() {
		return false
	}
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	return true
}

// OnUpdated subscribes to async post-commit update events.
func (t *Table[T]) OnUpdated(fn func(old, newv *T)) *Table[T] {
	b := t.ensureBus()
	if t.subscribeClosed(b) {
		return t
	}
	b.mu.Lock()
	b.updated = append(b.updated, fn)
	b.mu.Unlock()
	b.ensureWorker()
	return t
}

// OnDeleted subscribes to async post-commit delete events.
func (t *Table[T]) OnDeleted(fn func(*T)) *Table[T] {
	b := t.ensureBus()
	if t.subscribeClosed(b) {
		return t
	}
	b.mu.Lock()
	b.deleted = append(b.deleted, fn)
	b.mu.Unlock()
	b.ensureWorker()
	return t
}

// OnCommitted subscribes to async post-commit events for every change.
func (t *Table[T]) OnCommitted(fn func(Change[T])) *Table[T] {
	b := t.ensureBus()
	if t.subscribeClosed(b) {
		return t
	}
	b.mu.Lock()
	b.committed = append(b.committed, fn)
	b.mu.Unlock()
	b.ensureWorker()
	return t
}
