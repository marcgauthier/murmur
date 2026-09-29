package bridge

import (
	"sync/atomic"
	"syscall"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// failFS wraps a vfs.FS and fails data writes with ENOSPC once armed,
// simulating a full disk for deterministic storage-failure tests. Reads
// always pass through.
type failFS struct {
	vfs.FS
	armed atomic.Bool
}

func (f *failFS) arm()    { f.armed.Store(true) }
func (f *failFS) disarm() { f.armed.Store(false) }

func (f *failFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.Create(name, category)
	if err != nil {
		return nil, err
	}
	return &failFile{File: file, fs: f}, nil
}

func (f *failFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return &failFile{File: file, fs: f}, nil
}

func (f *failFS) OpenReadWrite(name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.FS.OpenReadWrite(name, category, opts...)
	if err != nil {
		return nil, err
	}
	return &failFile{File: file, fs: f}, nil
}

func (f *failFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.ReuseForWrite(oldname, newname, category)
	if err != nil {
		return nil, err
	}
	return &failFile{File: file, fs: f}, nil
}

type failFile struct {
	vfs.File
	fs *failFS
}

func (f *failFile) Write(p []byte) (int, error) {
	if f.fs.armed.Load() {
		return 0, syscall.ENOSPC
	}
	return f.File.Write(p)
}

func (f *failFile) WriteAt(p []byte, off int64) (int, error) {
	if f.fs.armed.Load() {
		return 0, syscall.ENOSPC
	}
	return f.File.WriteAt(p, off)
}

func (f *failFile) Sync() error {
	if f.fs.armed.Load() {
		return syscall.ENOSPC
	}
	return f.File.Sync()
}

func (f *failFile) SyncData() error {
	if f.fs.armed.Load() {
		return syscall.ENOSPC
	}
	return f.File.SyncData()
}

func (f *failFile) SyncTo(length int64) (bool, error) {
	if f.fs.armed.Load() {
		return false, syscall.ENOSPC
	}
	return f.File.SyncTo(length)
}
