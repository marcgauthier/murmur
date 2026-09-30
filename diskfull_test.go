package replicateddb

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/marcgauthier/murmur/state"
)

// failFS wraps a vfs.FS and fails all file data writes once armed,
// simulating a full disk. Reads keep working.
type failFS struct {
	vfs.FS
	armed    atomic.Bool
	writes   atomic.Int64
	writeAts atomic.Int64
	syncs    atomic.Int64
}

func (fs *failFS) wrap(f vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	return &failFile{File: f, fs: fs}, nil
}

func (fs *failFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	return fs.wrap(fs.FS.Create(name, category))
}

func (fs *failFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return fs.wrap(fs.FS.Open(name, opts...))
}

func (fs *failFS) OpenReadWrite(name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	return fs.wrap(fs.FS.OpenReadWrite(name, category, opts...))
}

func (fs *failFS) Unwrap() vfs.FS { return fs.FS }

func (fs *failFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	return fs.wrap(fs.FS.ReuseForWrite(oldname, newname, category))
}

type failFile struct {
	vfs.File
	fs *failFS
}

func (f *failFile) Write(p []byte) (int, error) {
	f.fs.writes.Add(1)
	if f.fs.armed.Load() {
		return 0, syscall.ENOSPC
	}
	return f.File.Write(p)
}

func (f *failFile) WriteAt(p []byte, off int64) (int, error) {
	f.fs.writeAts.Add(1)
	if f.fs.armed.Load() {
		return 0, syscall.ENOSPC
	}
	return f.File.WriteAt(p, off)
}

func (f *failFile) Sync() error {
	f.fs.syncs.Add(1)
	if f.fs.armed.Load() {
		return syscall.ENOSPC
	}
	return f.File.Sync()
}

func (f *failFile) SyncTo(length int64) (bool, error) {
	if f.fs.armed.Load() {
		return false, syscall.ENOSPC
	}
	return f.File.SyncTo(length)
}

func (f *failFile) SyncData() error {
	if f.fs.armed.Load() {
		return syscall.ENOSPC
	}
	return f.File.SyncData()
}

// TestDiskFullFailsClosed is the disk-full acceptance test: a write under
// ENOSPC fails, the node fails closed (reads and writes rejected), and a
// restart on a healthy filesystem recovers the last durable state with the
// failed write absent.
func TestDiskFullFailsClosed(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	fsys := &failFS{FS: vfs.Default}

	cfg := testConfig(path)
	cfg.Pebble.BaseFS = fsys
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	id1 := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id1[:], "ann"); err != nil {
		t.Fatal(err)
	}

	// Fill the disk: the next durable write fails.
	t.Logf("before arm: writes=%d writeAts=%d syncs=%d", fsys.writes.Load(), fsys.writeAts.Load(), fsys.syncs.Load())
	fsys.armed.Store(true)
	id2 := NewRowID()
	_, werr := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id2[:], "bob")
	t.Logf("after write: writes=%d writeAts=%d syncs=%d err=%v", fsys.writes.Load(), fsys.writeAts.Load(), fsys.syncs.Load(), werr)
	if werr == nil {
		t.Fatal("write on a full disk succeeded")
	}
	if !state.IsStorageFailure(werr) {
		t.Fatalf("disk-full write err = %v, want storage failure", werr)
	}
	if !errors.Is(werr, syscall.ENOSPC) {
		t.Fatalf("disk-full write err = %v, want ENOSPC cause", werr)
	}
	t.Logf("disk-full write error: %v", werr)

	if st := db.Status().State; st != StateFailed {
		t.Fatalf("state = %s, want failed", st)
	}
	// Failed nodes reject reads and writes until restart.
	cid := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, cid[:], "cid"); !state.IsStorageFailure(err) {
		t.Fatalf("write on failed node err = %v, want storage failure", err)
	}
	if _, err := db.QueryContext(ctx, `SELECT id FROM contacts`); !state.IsStorageFailure(err) {
		t.Fatalf("read on failed node err = %v, want storage failure", err)
	}

	// A failed node still closes (releasing LOCK, goroutines, listeners),
	// even with the disk still full; the close may honestly report the
	// final flush failure.
	if err := db.Close(); err != nil && !errors.Is(err, syscall.ENOSPC) {
		t.Fatal(err)
	}

	// Restart on a healthy filesystem (space freed): Pebble-authoritative
	// state serves again, with only the pre-failure write present.
	fsys.armed.Store(false)
	db2, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if st := db2.Status().State; st != StateReady {
		t.Fatalf("reopened state = %s, want ready", st)
	}
	rows := queryAll(t, db2, `SELECT name FROM contacts ORDER BY name`)
	if len(rows) != 1 || rows[0][0] != "ann" {
		t.Fatalf("reopened rows = %v, want [ann] (failed write absent)", rows)
	}
}

// TestLegacyAckPeersAdmitted proves pre-upgrade durable acknowledgement
// progress (no admission record) is granted one explicit retention window
// instead of losing its GC obligation on restart.
func TestLegacyAckPeersAdmitted(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, testConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	id := NewRowID()
	if _, err := db.ExecContext(ctx, `INSERT INTO contacts (id, name) VALUES (?, ?)`, id[:], "ann"); err != nil {
		t.Fatal(err)
	}
	legacy := NewNodeID()
	if err := db.store.SetPeerAck(legacy, db.cfg.NodeID, 1); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixMilli()
	db.admitLegacyAckPeers(now)
	rec, err := db.store.GetMember(legacy)
	if err != nil {
		t.Fatal(err)
	}
	wantDeadline := now + db.cfg.Replication.MaxOfflineLogRetention.Milliseconds()
	if rec.Status != state.MemberActive || rec.FirstAdmittedAt != now || rec.RetentionDeadline != wantDeadline {
		t.Fatalf("legacy member = %+v, want active admitted at %d deadline %d", rec, now, wantDeadline)
	}
}
