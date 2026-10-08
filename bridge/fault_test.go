package bridge

import (
	"sync/atomic"
	"syscall"

	"github.com/marcgauthier/murmur/spool"
)

// failStorage injects ENOSPC errors on write/sync once armed,
// simulating a full disk for deterministic storage-failure tests.
type failStorage struct {
	armed atomic.Bool
}

func (f *failStorage) arm()    { f.armed.Store(true) }
func (f *failStorage) disarm() { f.armed.Store(false) }

func (f *failStorage) hooks() *spool.FaultHooks {
	return &spool.FaultHooks{
		Append: func() error {
			if f.armed.Load() {
				return syscall.ENOSPC
			}
			return nil
		},
		SegmentSync: func() error {
			if f.armed.Load() {
				return syscall.ENOSPC
			}
			return nil
		},
	}
}
