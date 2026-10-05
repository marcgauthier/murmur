package state

import (
	"errors"
	"io"
	"syscall"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

func TestFatalCaptureStickyFirstWins(t *testing.T) {
	var c fatalCapture
	if err := c.err(); err != nil {
		t.Fatalf("healthy err = %v, want nil", err)
	}
	c.noteFatal("first")
	c.noteFatal("second")
	err := c.err()
	if !IsStorageFailure(err) {
		t.Fatalf("err = %v, want storage failure", err)
	}
	if got, want := err.Error(), "first"; !contains(got, want) {
		t.Fatalf("err = %q, want it to mention %q", got, want)
	}
	if got := err.Error(); contains(got, "second") {
		t.Fatalf("err = %q, want first message to win", got)
	}
}

func TestFatalCaptureCauseWrapsENOSPC(t *testing.T) {
	var c fatalCapture
	c.noteIOErr(syscall.ENOSPC)
	if err := c.err(); err != nil {
		t.Fatalf("cause-only err = %v, want nil (cause must not trip the gate)", err)
	}
	c.noteFatal("pebble: fatal commit error")
	err := c.err()
	if !IsStorageFailure(err) {
		t.Fatalf("err = %v, want storage failure", err)
	}
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("err = %v, want ENOSPC cause", err)
	}
}

func TestFatalCaptureNilSafe(t *testing.T) {
	var c *fatalCapture
	c.noteFatal("x")
	c.noteIOErr(io.ErrUnexpectedEOF)
	if err := c.err(); err != nil {
		t.Fatalf("nil capture err = %v, want nil", err)
	}
}

// errFS fails all data-path writes with ENOSPC while letting metadata ops
// through, proving watchFS records write errors (and only write errors).
type errFS struct {
	vfs.FS
	writeErr error
}

func (fs *errFS) wrap(f vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	return &errFile{File: f, err: fs.writeErr}, nil
}

func (fs *errFS) Create(name string, cat vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(name, cat)
	return fs.wrap(f, err)
}

func (fs *errFS) openSignedFixture(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	return fs.wrap(f, err)
}

func (fs *errFS) OpenReadWrite(
	name string, cat vfs.DiskWriteCategory, opts ...vfs.OpenOption,
) (vfs.File, error) {
	f, err := fs.FS.OpenReadWrite(name, cat, opts...)
	return fs.wrap(f, err)
}

func (fs *errFS) ReuseForWrite(
	oldname, newname string, cat vfs.DiskWriteCategory,
) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname, cat)
	return fs.wrap(f, err)
}

func (fs *errFS) Unwrap() vfs.FS { return fs.FS }

type errFile struct {
	vfs.File
	err error
}

func (f *errFile) Write(p []byte) (int, error)            { return 0, f.err }
func (f *errFile) WriteAt(p []byte, _ int64) (int, error) { return 0, f.err }
func (f *errFile) Sync() error                            { return f.err }
func (f *errFile) SyncData() error                        { return f.err }
func (f *errFile) SyncTo(_ int64) (bool, error)           { return false, f.err }

func TestWatchFSRecordsFirstWriteError(t *testing.T) {
	var c fatalCapture
	w := &watchFS{FS: &errFS{FS: vfs.NewMem(), writeErr: syscall.ENOSPC}, fatal: &c}
	f, err := w.Create("x", vfs.WriteCategoryUnspecified)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("data")); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("Write err = %v, want ENOSPC", err)
	}
	// The cause is recorded but the gate stays open until Pebble's own
	// fatal signal arrives.
	if err := c.err(); err != nil {
		t.Fatalf("gate err = %v, want nil before fatal", err)
	}
	c.noteFatal("boom")
	if err := c.err(); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("gate err = %v, want ENOSPC cause", err)
	}
	// Reads and stats flow through untouched.
	if _, err := f.Stat(); err != nil {
		t.Fatalf("Stat err = %v, want nil", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
