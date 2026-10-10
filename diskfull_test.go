package murmur

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/spool"
	"github.com/marcgauthier/murmur/state"
)

// TestDiskFullFailsClosed is the disk-full acceptance test: a write under
// ENOSPC fails, the node fails closed (reads and writes rejected), and a
// restart on a healthy filesystem recovers the last durable state with the
// failed write absent.
func TestDiskFullFailsClosed(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	var armed atomic.Bool
	faults := &spool.FaultHooks{
		Append: func() error {
			if armed.Load() {
				return syscall.ENOSPC
			}
			return nil
		},
		SegmentSync: func() error {
			if armed.Load() {
				return syscall.ENOSPC
			}
			return nil
		},
	}

	cfg := testConfig(path)
	cfg.Spool.Faults = faults
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	insert := func(rec *facadeRecord) error {
		tx, err := db.BeginTx(ctx)
		if err != nil {
			return err
		}
		if err := table.Insert(tx, rec); err != nil {
			return err
		}
		return tx.Commit()
	}

	if err := insert(&facadeRecord{ID: NewRowID(), Name: "ann"}); err != nil {
		t.Fatal(err)
	}

	// Fill the disk: the next durable write fails.
	armed.Store(true)
	werr := insert(&facadeRecord{ID: NewRowID(), Name: "bob"})
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
	if err := insert(&facadeRecord{ID: NewRowID(), Name: "cid"}); !state.IsStorageFailure(err) {
		t.Fatalf("write on failed node err = %v, want storage failure", err)
	}
	if _, err := table.Where().Count(); !state.IsStorageFailure(err) {
		t.Fatalf("read on failed node err = %v, want storage failure", err)
	}

	// A failed node still closes (releasing LOCK, goroutines, listeners),
	// even with the disk still full; the close may honestly report the
	if err := db.Close(); err != nil && !errors.Is(err, syscall.ENOSPC) && !errors.Is(err, spool.ErrStorageFailed) && !state.IsStorageFailure(err) {
		t.Fatal(err)
	}

	// Restart on a healthy filesystem (space freed): state
	// serves again, with only the pre-failure write present.
	armed.Store(false)
	db2, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if st := db2.Status().State; st != StateReady {
		t.Fatalf("reopened state = %s, want ready", st)
	}
	table2, err := tableOf[facadeRecord](db2, "records")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := table2.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Name != "ann" {
		t.Fatalf("reopened rows = %v, want [ann] (failed write absent)", rows)
	}
}

// TestLegacyAckPeersAdmitted proves pre-upgrade durable acknowledgement
// progress (no admission record) is granted one explicit retention window
// instead of losing its GC obligation on restart.
func TestLegacyAckPeersAdmitted(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig(t.TempDir())
	cfg.Schema.Tables = nil
	cfg.Tables = []TableDefinition{recordDefinition(t)}
	db, err := openSignedFixture(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	table, err := tableOf[facadeRecord](db, "records")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(tx, &facadeRecord{ID: NewRowID(), Name: "ann"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
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
