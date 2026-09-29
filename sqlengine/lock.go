package sqlengine

import (
	"context"
	"fmt"
	"sync"
)

// engineLock provides a context-aware, cancelable reader-writer lock.
// It serializes writers and permits concurrent readers, while ensuring that:
// 1. Pending writers take precedence to avoid writer starvation.
// 2. Both RLock and Lock acquisitions respect context cancellation and deadlines.
// 3. Canceling a waiting Lock/RLock unblocks cleanly without leaking lock state.
// 4. Closing the lock wakes all pending waiters immediately with an error.
type engineLock struct {
	mu          sync.Mutex
	closed      bool
	readers     int
	writer      bool
	writeWait   int
	readWaiters  []chan struct{}
	writeWaiters []chan struct{}
}

func newEngineLock() *engineLock {
	return &engineLock{}
}

// RLock acquires read access respecting ctx.
func (l *engineLock) RLock(ctx context.Context) error {
	l.mu.Lock()
	for {
		if l.closed {
			l.mu.Unlock()
			return fmt.Errorf("sqlengine: closed")
		}
		if err := ctx.Err(); err != nil {
			l.mu.Unlock()
			return err
		}
		// Allow read if no active writer and no queued writers
		if !l.writer && l.writeWait == 0 {
			l.readers++
			l.mu.Unlock()
			return nil
		}
		waitCh := make(chan struct{}, 1)
		l.readWaiters = append(l.readWaiters, waitCh)
		l.mu.Unlock()

		select {
		case <-waitCh:
			l.mu.Lock()
		case <-ctx.Done():
			l.mu.Lock()
			for i, ch := range l.readWaiters {
				if ch == waitCh {
					l.readWaiters = append(l.readWaiters[:i], l.readWaiters[i+1:]...)
					break
				}
			}
			l.mu.Unlock()
			return ctx.Err()
		}
	}
}

// RUnlock releases read access and wakes waiting writers or readers.
func (l *engineLock) RUnlock() {
	l.mu.Lock()
	l.readers--
	if l.readers < 0 {
		l.mu.Unlock()
		panic("sqlengine: negative readers")
	}
	if l.readers == 0 && len(l.writeWaiters) > 0 {
		ch := l.writeWaiters[0]
		l.writeWaiters = l.writeWaiters[1:]
		select {
		case ch <- struct{}{}:
		default:
		}
	} else if l.readers == 0 && len(l.readWaiters) > 0 && l.writeWait == 0 {
		for _, ch := range l.readWaiters {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
		l.readWaiters = nil
	}
	l.mu.Unlock()
}

// Lock acquires write access respecting ctx.
func (l *engineLock) Lock(ctx context.Context) error {
	l.mu.Lock()
	for {
		if l.closed {
			l.mu.Unlock()
			return fmt.Errorf("sqlengine: closed")
		}
		if err := ctx.Err(); err != nil {
			l.mu.Unlock()
			return err
		}
		if !l.writer && l.readers == 0 {
			l.writer = true
			l.mu.Unlock()
			return nil
		}
		l.writeWait++
		waitCh := make(chan struct{}, 1)
		l.writeWaiters = append(l.writeWaiters, waitCh)
		l.mu.Unlock()

		select {
		case <-waitCh:
			l.mu.Lock()
			l.writeWait--
		case <-ctx.Done():
			l.mu.Lock()
			l.writeWait--
			for i, ch := range l.writeWaiters {
				if ch == waitCh {
					l.writeWaiters = append(l.writeWaiters[:i], l.writeWaiters[i+1:]...)
					break
				}
			}
			if l.writeWait == 0 && l.readers == 0 && len(l.readWaiters) > 0 {
				for _, ch := range l.readWaiters {
					select {
					case ch <- struct{}{}:
					default:
					}
				}
				l.readWaiters = nil
			}
			l.mu.Unlock()
			return ctx.Err()
		}
	}
}

// Unlock releases write access and wakes waiting writers or readers.
func (l *engineLock) Unlock() {
	l.mu.Lock()
	if !l.writer {
		l.mu.Unlock()
		panic("sqlengine: unlock of unlocked write lock")
	}
	l.writer = false
	if len(l.writeWaiters) > 0 {
		ch := l.writeWaiters[0]
		l.writeWaiters = l.writeWaiters[1:]
		select {
		case ch <- struct{}{}:
		default:
		}
	} else if len(l.readWaiters) > 0 {
		for _, ch := range l.readWaiters {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
		l.readWaiters = nil
	}
	l.mu.Unlock()
}

// Close closes the lock and wakes all pending waiters.
func (l *engineLock) Close() {
	l.mu.Lock()
	l.closed = true
	for _, ch := range l.writeWaiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	l.writeWaiters = nil
	for _, ch := range l.readWaiters {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	l.readWaiters = nil
	l.mu.Unlock()
}
