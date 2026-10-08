// Copyright 2026. Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package state

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// ErrStorageFailed is returned by every Store operation once Spool reports
// a fatal storage error (disk full, unrecoverable I/O error, failed
// authoritative publication, or an internal invariant violation). The
// failure is sticky: the node has failed closed and only a process
// restart (after the operator resolves the underlying problem, e.g.
// freeing disk space) can clear it. Restart replays the last durable
// prefix; writes that returned this error were never durable and stay
// absent.
//
// Callers can distinguish a full disk with errors.Is(err, syscall.ENOSPC):
// the terminal failure cause is preserved.
var ErrStorageFailed = errors.New("state: storage failed; node failed closed, restart required")

// IsStorageFailure reports whether err is (or wraps) the sticky fail-closed
// storage error.
func IsStorageFailure(err error) bool { return errors.Is(err, ErrStorageFailed) }

// fatalCapture records the first fatal storage failure. Spool surfaces
// unrecoverable storage errors through StorageError (polled after every
// fallible operation) and the OnStorageError callback (background
// failures). A library must not exit the host process: instead the
// capture trips a sticky fail-closed gate and every Store operation
// thereafter returns ErrStorageFailed.
type fatalCapture struct {
	mu      sync.Mutex
	tripped atomic.Bool
	msg     string
	cause   error
}

// noteFatal trips the gate. The first message wins; later ones are dropped.
// Nil-safe so callbacks on a partially built Store cannot panic.
func (c *fatalCapture) noteFatal(msg string) {
	if c == nil {
		return
	}
	if c.tripped.Swap(true) {
		return
	}
	c.mu.Lock()
	c.msg = msg
	c.mu.Unlock()
}

// noteTerminal trips the gate with a typed cause. The first message and
// cause win; later ones are dropped.
func (c *fatalCapture) noteTerminal(msg string, cause error) {
	if c == nil {
		return
	}
	if c.tripped.Swap(true) {
		return
	}
	c.mu.Lock()
	c.msg = msg
	c.cause = cause
	c.mu.Unlock()
}

// err returns the sticky failure, or nil while the store is healthy.
func (c *fatalCapture) err() error {
	if c == nil || !c.tripped.Load() {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cause != nil {
		return fmt.Errorf("%w: %s: %w", ErrStorageFailed, c.msg, c.cause)
	}
	return fmt.Errorf("%w: %s", ErrStorageFailed, c.msg)
}
