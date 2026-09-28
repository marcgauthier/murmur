// Copyright 2026. Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package state

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// ErrStorageFailed is returned by every Store operation once Pebble reports
// a fatal storage error (disk full, unrecoverable I/O error, MANIFEST
// failure, background flush/compaction failure, or an internal invariant
// violation). The failure is sticky: the node has failed closed and only a
// process restart (after the operator resolves the underlying problem, e.g.
// freeing disk space) can clear it. Restart replays the last durable WAL
// prefix; writes that returned this error were never durable and stay
// absent.
//
// Callers can distinguish a full disk with errors.Is(err, syscall.ENOSPC):
// the first data-path I/O error is preserved as the failure cause.
var ErrStorageFailed = errors.New("state: storage failed; node failed closed, restart required")

// IsStorageFailure reports whether err is (or wraps) the sticky fail-closed
// storage error.
func IsStorageFailure(err error) bool { return errors.Is(err, ErrStorageFailed) }

// fatalCapture records the first fatal storage failure. Pebble surfaces
// unrecoverable storage errors through Logger.Fatalf (foreground commit
// errors, MANIFEST failures) and EventListener.BackgroundError (background
// flush/compaction errors). Pebble's contract treats Fatalf as terminal
// (its default logger exits the process), but a library must not exit the
// host: instead the capture trips a sticky fail-closed gate and every Store
// operation thereafter returns ErrStorageFailed.
//
// A failing vfs layer below Pebble (see watchFS) additionally records the
// first data-path I/O error as the typed cause, so operators can tell a
// full disk (ENOSPC) from other failures.
type fatalCapture struct {
	mu      sync.Mutex
	tripped atomic.Bool
	msg     string
	cause   error
}

// noteFatal trips the gate. The first message wins; later ones are dropped.
// Nil-safe so loggers and listeners on a partially built Store cannot panic.
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

// noteIOErr records the first data-path I/O error as the failure cause.
// It does not trip the gate by itself: only Pebble's own fatal signal
// (Fatalf/BackgroundError) is authoritative, since Pebble may see errors
// the filesystem layer cannot classify.
func (c *fatalCapture) noteIOErr(err error) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cause == nil {
		c.cause = err
	}
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

// watchFS wraps the filesystem handed to Pebble and records the first
// data-path write error (Write/WriteAt/Sync/SyncData/SyncTo) as the typed
// failure cause. Read-path and metadata errors are never recorded: Pebble
// legitimately encounters those during normal operation (missing files on
// first boot, dropped obsolete files), and a stale cause would mislabel a
// later real failure.
type watchFS struct {
	vfs.FS
	fatal *fatalCapture
}

var _ vfs.FS = (*watchFS)(nil)

func (fs *watchFS) Unwrap() vfs.FS { return fs.FS }

func (fs *watchFS) wrap(f vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	return &watchFile{File: f, fatal: fs.fatal}, nil
}

func (fs *watchFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	return fs.wrap(fs.FS.Create(name, category))
}

func (fs *watchFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return fs.wrap(fs.FS.Open(name, opts...))
}

func (fs *watchFS) OpenReadWrite(
	name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption,
) (vfs.File, error) {
	return fs.wrap(fs.FS.OpenReadWrite(name, category, opts...))
}

func (fs *watchFS) ReuseForWrite(
	oldname, newname string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	return fs.wrap(fs.FS.ReuseForWrite(oldname, newname, category))
}

type watchFile struct {
	vfs.File
	fatal *fatalCapture
}

func (f *watchFile) Write(p []byte) (int, error) {
	n, err := f.File.Write(p)
	if err != nil {
		f.fatal.noteIOErr(err)
	}
	return n, err
}

func (f *watchFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	if err != nil {
		f.fatal.noteIOErr(err)
	}
	return n, err
}

func (f *watchFile) Sync() error {
	if err := f.File.Sync(); err != nil {
		f.fatal.noteIOErr(err)
		return err
	}
	return nil
}

func (f *watchFile) SyncData() error {
	if err := f.File.SyncData(); err != nil {
		f.fatal.noteIOErr(err)
		return err
	}
	return nil
}

func (f *watchFile) SyncTo(length int64) (bool, error) {
	done, err := f.File.SyncTo(length)
	if err != nil {
		f.fatal.noteIOErr(err)
	}
	return done, err
}

// pebbleFatalListener reports Pebble background errors (flush/compaction
// failures) into the fail-closed capture. Pebble retries failed
// flushes/compactions internally, but while storage is failing the node
// must not keep acknowledging new writes: tripping the gate converts a
// silent stall into an explicit, immediate failure.
func pebbleFatalListener(fatal *fatalCapture) *pebble.EventListener {
	return &pebble.EventListener{
		BackgroundError: func(err error) {
			fatal.noteFatal(fmt.Sprintf("pebble: background error: %v", err))
		},
	}
}
