package rime_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

type Device struct {
	ID       string `rime:"primary"`
	Hostname string `rime:"unique,prefix"`
	Site     string `rime:"index"`
	Status   int    `rime:"index,ordered"`
	Latency  int    `rime:"ordered"`
}

type ownedRecord struct {
	ID     string `rime:"primary"`
	Labels map[string][]byte
	Items  []*string
}

func TestDefaultRecordOwnership(t *testing.T) {
	db := rime.New()
	defer db.Close()
	tab, err := rime.Register[ownedRecord](db)
	if err != nil {
		t.Fatal(err)
	}
	label := []byte("original")
	item := "before"
	in := &ownedRecord{ID: "owned", Labels: map[string][]byte{"a": label}, Items: []*string{&item}}
	if err := tab.Insert(in); err != nil {
		t.Fatal(err)
	}
	label[0] = 'X'
	item = "caller changed"
	got, err := tab.Get("owned")
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Labels["a"]) != "original" || *got.Items[0] != "before" {
		t.Fatalf("stored record shares caller-owned data: %+v", got)
	}
	if err := tab.Update("owned", func(r *ownedRecord) error {
		r.Labels["a"][0] = 'Y'
		*r.Items[0] = "updated"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if string(got.Labels["a"]) != "original" || *got.Items[0] != "before" {
		t.Fatalf("update changed an earlier published version: %+v", got)
	}
}

func TestOptionalPresenceQueriesAndNotNull(t *testing.T) {
	db := rime.New()
	defer db.Close()
	type record struct {
		ID       string             `rime:"primary"`
		Required rime.Optional[int] `rime:"notnull"`
		Maybe    rime.Optional[int]
	}
	tab, err := rime.Register[record](db)
	if err != nil {
		t.Fatal(err)
	}
	if err := tab.Insert(&record{ID: "missing-required"}); !errors.Is(err, rime.ErrNotNull) {
		t.Fatalf("absent NOT NULL optional accepted: %v", err)
	}
	if err := tab.Insert(&record{ID: "zero", Required: rime.Some(0)}); err != nil {
		t.Fatal(err)
	}
	if err := tab.Insert(&record{ID: "present-zero", Required: rime.Some(1), Maybe: rime.Some(0)}); err != nil {
		t.Fatal(err)
	}
	maybe := rime.F[record, rime.Optional[int]](tab, "Maybe")
	nullCount, err := tab.Where(maybe.IsNull()).Count()
	if err != nil || nullCount != 1 {
		t.Fatalf("IS NULL count=%d err=%v", nullCount, err)
	}
	presentCount, err := tab.Where(maybe.IsNotNull()).Count()
	if err != nil || presentCount != 1 {
		t.Fatalf("IS NOT NULL count=%d err=%v", presentCount, err)
	}
}

func TestPreparedChangesCoalesceAndDetach(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "existing", Site: "old"})
	snapshot := db.ReadTx()
	base := snapshot.Snapshot()
	snapshot.Close()

	var captured []rime.PreparedChange
	err := db.WriteTx(func(tx *rime.Tx) error {
		bound := dev.In(tx)
		if err := bound.Update("existing", func(d *Device) error { d.Site = "middle"; return nil }); err != nil {
			return err
		}
		if err := bound.Update("existing", func(d *Device) error { d.Site = "final"; return nil }); err != nil {
			return err
		}
		if err := bound.Insert(&Device{ID: "transient"}); err != nil {
			return err
		}
		if err := bound.Delete("transient"); err != nil {
			return err
		}
		var err error
		captured, err = tx.PreparedChanges()
		if err != nil {
			return err
		}
		if len(captured) != 1 {
			return fmt.Errorf("prepared changes = %d, want 1", len(captured))
		}
		captured[0].New.(*Device).Site = "detached mutation"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	change := captured[0]
	if change.Table != "Device" || change.Key != "existing" || change.Operation != rime.OpUpdate || change.Base != base {
		t.Fatalf("prepared identity/base mismatch: %+v", change)
	}
	if change.Old.(*Device).Site != "old" || change.New.(*Device).Site != "detached mutation" {
		t.Fatalf("prepared values mismatch: old=%+v new=%+v", change.Old, change.New)
	}
	got, err := dev.Get("existing")
	if err != nil {
		t.Fatal(err)
	}
	if got.Site != "final" {
		t.Fatalf("detached prepared value changed commit: %+v", got)
	}
}

func TestExplicitWriteTransactionLifecycle(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	tx, err := db.BeginTx(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := dev.In(tx).Insert(&Device{ID: "committed"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(tx.Commit(), rime.ErrTxClosed) {
		t.Fatal("second commit should report closed transaction")
	}
	if _, err := dev.Get("committed"); err != nil {
		t.Fatalf("committed row missing: %v", err)
	}

	tx, err = db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := dev.In(tx).Insert(&Device{ID: "rolled-back"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := dev.Get("rolled-back"); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("rollback left a row behind: %v", err)
	}
	if err := tx.Rollback(); !errors.Is(err, rime.ErrTxClosed) {
		t.Fatal("rollback after close should report closed transaction")
	}
}

// TestRollbackHygiene proves explicit rollback leaves no residue: staged
// inserts stay absent, staged updates leave the committed values untouched,
// and every rolled-back identity (primary key and unique hostname) is
// immediately reusable. Each round stages two inserts plus one update,
// verifies read-your-write, rolls back, then requires exact absence and
// unmodified seeds; every fifth round re-inserts a rolled-back identity to
// prove its claims were released. Commit-after-rollback must fail closed.
func TestRollbackHygiene(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()

	const seeds = 10
	const rounds = 30
	seedOf := func(k int) string { return fmt.Sprintf("rb-seed-%02d", k) }
	for k := 0; k < seeds; k++ {
		rec := &Device{ID: seedOf(k), Hostname: fmt.Sprintf("rb-h-%02d", k), Site: "OTT", Status: k}
		if err := dev.Insert(rec); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	reused := 0
	for round := 0; round < rounds; round++ {
		upd := round % seeds
		newA := fmt.Sprintf("rb-new-%02d-a", round)
		newB := fmt.Sprintf("rb-new-%02d-b", round)
		tx, err := db.BeginTx(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		b := dev.In(tx)
		if err := b.Insert(&Device{ID: newA, Hostname: newA, Status: 9999}); err != nil {
			t.Fatal(err)
		}
		if err := b.Insert(&Device{ID: newB, Hostname: newB, Status: 9999}); err != nil {
			t.Fatal(err)
		}
		if err := b.Update(seedOf(upd), func(d *Device) error {
			d.Status = 9999
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if got, err := b.Get(newA); err != nil || got.Status != 9999 {
			t.Fatalf("round %d: own staged %s = %+v %v", round, newA, got, err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("round %d rollback: %v", round, err)
		}
		if err := tx.Commit(); !errors.Is(err, rime.ErrTxClosed) {
			t.Fatalf("round %d: commit after rollback = %v, want ErrTxClosed", round, err)
		}
		for _, id := range []string{newA, newB} {
			if _, err := dev.Get(id); !errors.Is(err, rime.ErrNotFound) {
				t.Fatalf("round %d: rolled-back %s visible: %v", round, id, err)
			}
		}
		got, err := dev.Get(seedOf(upd))
		if err != nil || got.Status != upd {
			t.Fatalf("round %d: seed %s = %+v %v, want status %d", round, seedOf(upd), got, err, upd)
		}
		if round%5 == 4 {
			// Reuse probe: the rolled-back identity must be fully free.
			if err := dev.Insert(&Device{ID: newA, Hostname: newA, Status: round}); err != nil {
				t.Fatalf("round %d: reuse %s: %v (claim leaked)", round, newA, err)
			}
			reused++
			if err := dev.Delete(newA); err != nil {
				t.Fatalf("round %d: cleanup %s: %v", round, newA, err)
			}
		}
	}
	if reused != rounds/5 {
		t.Fatalf("reuse probes=%d want %d", reused, rounds/5)
	}
	rows, err := dev.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != seeds {
		t.Fatalf("final rows=%d want %d", len(rows), seeds)
	}
	for k := 0; k < seeds; k++ {
		got, err := dev.Get(seedOf(k))
		if err != nil || got.Status != k {
			t.Fatalf("final %s = %+v %v want status %d", seedOf(k), got, err, k)
		}
	}
	t.Logf("rollback rounds=%d reused=%d", rounds, reused)
}

func TestPreparedCommitTokenOwnsPublicationOrder(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := dev.In(tx).Insert(&Device{ID: "managed", Hostname: "managed", Site: "durable"}); err != nil {
		t.Fatal(err)
	}
	token, err := tx.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	if token.CommitID() == 0 || len(token.Changes()) != 1 {
		t.Fatalf("bad prepared token: id=%d changes=%d", token.CommitID(), len(token.Changes()))
	}
	firstView := token.Changes()
	firstView[0].New.(*Device).Site = "caller mutation"
	if token.Changes()[0].New.(*Device).Site != "durable" {
		t.Fatal("token change view was mutable")
	}
	if err := dev.In(tx).Update("managed", func(d *Device) error { d.Site = "late"; return nil }); !errors.Is(err, rime.ErrTxPrepared) {
		t.Fatalf("write through prepared transaction: %v", err)
	}
	if _, err := dev.Get("managed"); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("prepared row visible early: %v", err)
	}
	if err := token.Publish(); err != nil {
		t.Fatal(err)
	}
	if got, err := dev.Get("managed"); err != nil || got.Site != "durable" {
		t.Fatalf("published row: got=%+v err=%v", got, err)
	}

	tx, err = db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := dev.In(tx).Insert(&Device{ID: "aborted", Hostname: "aborted"}); err != nil {
		t.Fatal(err)
	}
	token, err = tx.PrepareCommit()
	if err != nil {
		t.Fatal(err)
	}
	if err := token.Abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := dev.Get("aborted"); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("aborted prepared row visible: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, rime.ErrTxClosed) {
		t.Fatalf("abort did not close tx: %v", err)
	}
}

// TestManagedTokenEdgesConcurrent pins the prepared-token terminal contract
// and its waiter fan-out. Phase A is deterministic: double-publish and
// publish-after-abort fail with ErrTxClosed, abort stays idempotent-nil
// after either terminal, and Commit-after-publish reports ErrTxClosed.
// Phase B holds one token while N standalone writers block on the
// coordinator and readers proceed, then releases via publish or abort:
// every waiter must commit exactly once with nothing lost or duplicated.
func TestManagedTokenEdgesConcurrent(t *testing.T) {
	t.Run("terminal edges", func(t *testing.T) {
		db, dev := openDevices(t)
		defer db.Close()
		mkToken := func(id string) (*rime.Tx, *rime.PreparedTx) {
			t.Helper()
			tx, err := db.BeginTx(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := dev.In(tx).Insert(&Device{ID: id, Hostname: id}); err != nil {
				t.Fatal(err)
			}
			token, err := tx.PrepareCommit()
			if err != nil {
				t.Fatal(err)
			}
			return tx, token
		}
		// Publish path: second publish and late commit fail closed.
		tx, tok := mkToken("pub")
		if err := tok.Publish(); err != nil {
			t.Fatalf("publish: %v", err)
		}
		if err := tok.Publish(); !errors.Is(err, rime.ErrTxClosed) {
			t.Fatalf("double publish = %v, want ErrTxClosed", err)
		}
		if err := tok.Abort(); err != nil {
			t.Fatalf("abort after publish = %v, want nil (idempotent)", err)
		}
		if err := tx.Commit(); !errors.Is(err, rime.ErrTxClosed) {
			t.Fatalf("commit after publish = %v, want ErrTxClosed", err)
		}
		if _, err := dev.Get("pub"); err != nil {
			t.Fatalf("published row missing: %v", err)
		}
		// Abort path: publish after abort fails, abort stays nil.
		_, tok = mkToken("ab")
		if err := tok.Abort(); err != nil {
			t.Fatalf("abort: %v", err)
		}
		if err := tok.Abort(); err != nil {
			t.Fatalf("double abort = %v, want nil", err)
		}
		if err := tok.Publish(); !errors.Is(err, rime.ErrTxClosed) {
			t.Fatalf("publish after abort = %v, want ErrTxClosed", err)
		}
		if _, err := dev.Get("ab"); !errors.Is(err, rime.ErrNotFound) {
			t.Fatalf("aborted row visible: %v", err)
		}
	})

	for _, release := range []string{"publish", "abort"} {
		t.Run("waiters/"+release, func(t *testing.T) {
			db, dev := openDevices(t)
			defer db.Close()
			tx, err := db.BeginTx(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if err := dev.In(tx).Insert(&Device{ID: "managed", Hostname: "managed"}); err != nil {
				t.Fatal(err)
			}
			token, err := tx.PrepareCommit()
			if err != nil {
				t.Fatal(err)
			}
			const waiters = 4
			done := make(chan error, waiters)
			for w := 0; w < waiters; w++ {
				go func(w int) {
					id := fmt.Sprintf("w-%d", w)
					done <- dev.Upsert(&Device{ID: id, Hostname: id, Status: w})
				}(w)
			}
			var reads atomic.Int64
			stopReaders := make(chan struct{})
			var rwg sync.WaitGroup
			for r := 0; r < 2; r++ {
				rwg.Add(1)
				go func() {
					defer rwg.Done()
					for {
						select {
						case <-stopReaders:
							return
						default:
						}
						if _, err := dev.Get("w-0"); err != nil && !errors.Is(err, rime.ErrNotFound) {
							t.Errorf("reader: %v", err)
							return
						}
						reads.Add(1)
					}
				}()
			}
			// All writers must still be waiting on the coordinator.
			select {
			case err := <-done:
				t.Fatalf("standalone commit slipped past held token: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			if reads.Load() == 0 {
				t.Fatal("no reads proceeded during held token")
			}
			if release == "publish" {
				if err := token.Publish(); err != nil {
					t.Fatal(err)
				}
			} else if err := token.Abort(); err != nil {
				t.Fatal(err)
			}
			close(stopReaders)
			rwg.Wait()
			timeout := time.After(5 * time.Second)
			for w := 0; w < waiters; w++ {
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("waiter %d after %s: %v", w, release, err)
					}
				case <-timeout:
					t.Fatalf("waiter %d stuck after %s", w, release)
				}
			}
			for w := 0; w < waiters; w++ {
				got, err := dev.Get(fmt.Sprintf("w-%d", w))
				if err != nil || got.Status != w {
					t.Fatalf("w-%d = %+v %v, want status %d", w, got, err, w)
				}
			}
			_, err = dev.Get("managed")
			if release == "publish" && err != nil {
				t.Fatalf("published row missing: %v", err)
			}
			if release == "abort" && !errors.Is(err, rime.ErrNotFound) {
				t.Fatalf("aborted row visible: %v", err)
			}
		})
	}
}

func TestManagedPrepareRequiresBeginTx(t *testing.T) {
	db := rime.New()
	defer db.Close()
	err := db.WriteTx(func(tx *rime.Tx) error {
		_, err := tx.PrepareCommit()
		return err
	})
	if !errors.Is(err, rime.ErrManagedCommitRequired) {
		t.Fatalf("PrepareCommit in callback tx: %v", err)
	}
}

func openDevices(t *testing.T, opts ...rime.Option) (*rime.DB, *rime.Table[Device]) {
	t.Helper()
	db := rime.New(opts...)
	dev, err := rime.Register[Device](db, rime.WithCompound[Device]("site_status", "Site", "Status"))
	if err != nil {
		t.Fatal(err)
	}
	return db, dev
}

func mustSave(t *testing.T, dev *rime.Table[Device], d Device) {
	t.Helper()
	if err := dev.Upsert(&d); err != nil {
		t.Fatal(err)
	}
}

func seedDevices(t *testing.T, dev *rime.Table[Device], n int) {
	t.Helper()
	recs := make([]*Device, n)
	for i := 0; i < n; i++ {
		recs[i] = &Device{
			ID:       fmt.Sprintf("d-%04d", i),
			Hostname: fmt.Sprintf("host-%04d", i),
			Site:     []string{"OTT", "MTL", "WPG"}[i%3],
			Status:   i % 4,
			Latency:  i % 200,
		}
	}
	if err := dev.UpsertMany(recs); err != nil {
		t.Fatal(err)
	}
}

func TestCRUD(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	site := rime.F[Device, string](dev, "Site")

	mustSave(t, dev, Device{ID: "a", Hostname: "h-a", Site: "OTT", Status: 1, Latency: 10})

	tx := db.ReadTx()
	d, err := dev.In(tx).Get("a")
	tx.Close()
	if err != nil {
		t.Fatal(err)
	}
	if d.Hostname != "h-a" || d.Site != "OTT" {
		t.Fatalf("bad record: %+v", d)
	}

	if err := dev.Update("a", func(d *Device) error { d.Status = 2; return nil }); err != nil {
		t.Fatal(err)
	}
	tx2 := db.ReadTx()
	got, _ := dev.In(tx2).Get("a")
	tx2.Close()
	if got.Status != 2 {
		t.Fatalf("update not visible: %+v", got)
	}

	// Query by indexed field.
	rows, err := dev.Where(site.Eq("OTT")).Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}

	if err := dev.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := dev.Get("a"); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestInsertDuplicate(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "a", Hostname: "h-a"})
	if err := dev.Insert(&Device{ID: "a", Hostname: "h-a2"}); !errors.Is(err, rime.ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
}

func TestUniqueConstraint(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "a", Hostname: "same"})
	if err := dev.Upsert(&Device{ID: "b", Hostname: "same"}); !errors.Is(err, rime.ErrUnique) {
		t.Fatalf("want ErrUnique, got %v", err)
	}
	// Same-key overwrite with same unique value is fine.
	if err := dev.Upsert(&Device{ID: "a", Hostname: "same", Site: "X"}); err != nil {
		t.Fatal(err)
	}
}

func TestCopyOnWriteImmutability(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "a", Hostname: "h-a", Status: 1})

	tx := db.ReadTx()
	old, err := dev.In(tx).Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if err := dev.Update("a", func(d *Device) error { d.Status = 99; return nil }); err != nil {
		t.Fatal(err)
	}
	if old.Status != 1 {
		t.Fatalf("snapshot pointer mutated: %+v", old)
	}
	cur, _ := dev.In(tx).Get("a") // same snapshot: still old
	if cur.Status != 1 {
		t.Fatalf("snapshot isolation broken: %+v", cur)
	}
	tx.Close()
	fresh, _ := dev.Get("a")
	if fresh.Status != 99 {
		t.Fatalf("update lost: %+v", fresh)
	}
}

func TestMVCCSnapshots(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 10)

	snap := db.ReadTx()
	defer snap.Close()
	n0, err := dev.Where().In(snap).Find()
	if err != nil {
		t.Fatal(err)
	}
	mustSave(t, dev, Device{ID: "new", Hostname: "new", Site: "OTT"})
	n1, _ := dev.Where().In(snap).Find()
	if len(n1) != len(n0) {
		t.Fatalf("snapshot changed underfoot: %d -> %d", len(n0), len(n1))
	}
	n2, _ := dev.Where().Find()
	if len(n2) != len(n0)+1 {
		t.Fatalf("new write invisible: %d", len(n2))
	}
	// Delete visibility: tombstone visible at new snapshots only.
	if err := dev.Delete("new"); err != nil {
		t.Fatal(err)
	}
	stillThere, _ := dev.Where().In(snap).Find()
	if len(stillThere) != len(n0) {
		t.Fatalf("old snapshot lost pre-delete view")
	}
}

func TestWriteConflict(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "a", Hostname: "h-a", Status: 1})

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = db.WriteTx(func(tx *rime.Tx) error {
				return dev.In(tx).Update("a", func(d *Device) error {
					entered <- struct{}{}
					<-release // both mutations overlap on the same base version
					d.Status = 10 + i
					return nil
				})
			})
		}(i)
	}
	<-entered
	<-entered
	close(release)
	wg.Wait()
	conflicts := 0
	for _, e := range errs {
		if errors.Is(e, rime.ErrConflict) {
			conflicts++
		} else if e != nil {
			t.Fatal(e)
		}
	}
	if conflicts != 1 {
		t.Fatalf("want exactly 1 conflict, got %d (%v)", conflicts, errs)
	}
}

func TestMultiOpTransactionAtomic(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	err := db.WriteTx(func(tx *rime.Tx) error {
		if err := dev.In(tx).Upsert(&Device{ID: "a", Hostname: "h-a"}); err != nil {
			return err
		}
		if err := dev.In(tx).Upsert(&Device{ID: "b", Hostname: "h-b"}); err != nil {
			return err
		}
		return errors.New("boom")
	})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("want boom, got %v", err)
	}
	if _, err := dev.Get("a"); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("rollback failed: %v", err)
	}
	// Read-your-writes inside one tx.
	err = db.WriteTx(func(tx *rime.Tx) error {
		if err := dev.In(tx).Upsert(&Device{ID: "c", Hostname: "h-c", Status: 1}); err != nil {
			return err
		}
		got, err := dev.In(tx).Get("c")
		if err != nil {
			return err
		}
		if got.Status != 1 {
			return fmt.Errorf("read-your-writes broken")
		}
		return dev.In(tx).Update("c", func(d *Device) error { d.Status = 2; return nil })
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := dev.Get("c")
	if got.Status != 2 {
		t.Fatalf("stacked writes lost: %+v", got)
	}
}

func TestGC(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "a", Hostname: "h-a", Status: 1})
	for i := 0; i < 10; i++ {
		if err := dev.Update("a", func(d *Device) error { d.Status++; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	before := db.Stats()
	if before.Versions < 11 {
		t.Fatalf("want >= 11 versions, got %d", before.Versions)
	}
	res := db.GC()
	if res.Reclaimed == 0 {
		t.Fatalf("GC reclaimed nothing: %+v", res)
	}
	after := db.Stats()
	if after.Versions >= before.Versions {
		t.Fatalf("versions not pruned: %d -> %d", before.Versions, after.Versions)
	}
	got, err := dev.Get("a")
	if err != nil || got.Status != 11 {
		t.Fatalf("GC corrupted head: %+v %v", got, err)
	}
	// Deleted rows vanish physically after GC with no pinning snapshot.
	if err := dev.Delete("a"); err != nil {
		t.Fatal(err)
	}
	db.GC()
	st := dev.TableStats()
	if st.Tombstones != 0 {
		t.Fatalf("tombstone not reclaimed: %+v", st)
	}
}

func TestGCPinnedSnapshot(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "a", Hostname: "h-a", Status: 1})
	pin := db.ReadTx()
	defer pin.Close()
	for i := 0; i < 5; i++ {
		if err := dev.Update("a", func(d *Device) error { d.Status++; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	db.GC()
	got, err := dev.In(pin).Get("a")
	if err != nil || got.Status != 1 {
		t.Fatalf("pinned snapshot lost history: %+v %v", got, err)
	}
}

// TestGCTombstoneBaseTolerance pins the GC/OCC boundary deterministically: a
// transaction staged on a tombstone must still commit after GC collects that
// tombstone (absent-after-tombstone is still deleted), while any intervening
// writer commit must still conflict. Case 1 stages an upsert on a tombstone,
// runs GC with the transaction held open, and requires commit success with
// the exact staged value. Case 2 adds an intervening live commit before GC
// and requires ErrConflict with the intervenor's value kept. Case 3 stages
// on a live base, deletes it from another transaction, runs GC, and requires
// ErrConflict: the tolerance applies to tombstone bases only.
func TestGCTombstoneBaseTolerance(t *testing.T) {
	setup := func(t *testing.T) (*rime.DB, *rime.Table[Device]) {
		t.Helper()
		db, dev := openDevices(t)
		t.Cleanup(func() { db.Close() })
		if err := dev.Insert(&Device{ID: "k", Hostname: "h-k", Status: 1}); err != nil {
			t.Fatal(err)
		}
		if err := dev.Delete("k"); err != nil {
			t.Fatal(err)
		}
		return db, dev
	}

	t.Run("collected tombstone base commits", func(t *testing.T) {
		db, dev := setup(t)
		tx, err := db.BeginTx(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		rec := &Device{ID: "k", Hostname: "h-k2", Status: 2}
		if err := dev.In(tx).Upsert(rec); err != nil {
			t.Fatal(err)
		}
		if res := db.GC(); res.Reclaimed == 0 {
			t.Fatalf("GC reclaimed nothing (tolerance path not exercised): %+v", res)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit over collected tombstone base: %v, want nil", err)
		}
		got, err := dev.Get("k")
		if err != nil || got.Status != 2 || got.Hostname != "h-k2" {
			t.Fatalf("k = %+v %v, want staged upsert", got, err)
		}
	})

	t.Run("intervening live commit still conflicts", func(t *testing.T) {
		db, dev := setup(t)
		tx, err := db.BeginTx(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := dev.In(tx).Upsert(&Device{ID: "k", Hostname: "h-staged", Status: 2}); err != nil {
			t.Fatal(err)
		}
		if err := dev.Upsert(&Device{ID: "k", Hostname: "h-live", Status: 3}); err != nil {
			t.Fatal(err)
		}
		db.GC()
		if err := tx.Commit(); !errors.Is(err, rime.ErrConflict) {
			t.Fatalf("commit = %v, want ErrConflict", err)
		}
		got, err := dev.Get("k")
		if err != nil || got.Status != 3 || got.Hostname != "h-live" {
			t.Fatalf("k = %+v %v, want intervenor's value", got, err)
		}
	})

	t.Run("live base deleted before commit still conflicts", func(t *testing.T) {
		db, dev := openDevices(t)
		defer db.Close()
		if err := dev.Insert(&Device{ID: "k", Hostname: "h-k", Status: 1}); err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := dev.In(tx).Update("k", func(d *Device) error {
			d.Status = 2
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := dev.Delete("k"); err != nil {
			t.Fatal(err)
		}
		db.GC()
		if err := tx.Commit(); !errors.Is(err, rime.ErrConflict) {
			t.Fatalf("commit = %v, want ErrConflict", err)
		}
		if _, err := dev.Get("k"); !errors.Is(err, rime.ErrNotFound) {
			t.Fatalf("Get k = %v, want ErrNotFound", err)
		}
	})
}

// TestGCConcurrentPinning proves MVCC GC never reclaims versions still
// observable by a pinned snapshot under concurrent churn: stability readers
// snapshot at random points and must observe byte-identical fingerprints
// across re-reads while a writer churns generations and GC runs
// continuously. A generation-0 pin must still read exact initial values at
// the end, and after all pins close the chains collapse to one head each.
func TestGCConcurrentPinning(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	const keys = 32
	for i := 0; i < keys; i++ {
		mustSave(t, dev, Device{ID: fmt.Sprintf("g%d", i), Hostname: fmt.Sprintf("h-g%d", i), Site: "s"})
	}
	gen0 := db.ReadTx()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}

	// Writer: churn whole-table generations until stopped.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for gen := 1; ; gen++ {
			select {
			case <-stop:
				return
			default:
			}
			for i := 0; i < keys; i++ {
				id := fmt.Sprintf("g%d", i)
				if err := dev.Update(id, func(d *Device) error {
					d.Status, d.Latency = gen, gen
					return nil
				}); err != nil {
					fail("update gen %d: %v", gen, err)
					return
				}
			}
		}
	}()

	// GC loop: prune continuously while snapshots are pinned.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				db.GC()
			}
		}
	}()

	// Stability readers: snapshot, fingerprint, and re-verify.
	const readers = 4
	const rounds = 3
	const verifies = 20
	var rwg sync.WaitGroup
	for r := 0; r < readers; r++ {
		rwg.Add(1)
		go func(r int) {
			defer rwg.Done()
			time.Sleep(time.Duration(r) * 25 * time.Millisecond)
			for s := 0; s < rounds; s++ {
				pin := db.ReadTx()
				wantS := make([]int, keys)
				wantL := make([]int, keys)
				bound := dev.In(pin)
				for i := 0; i < keys; i++ {
					got, err := bound.Get(fmt.Sprintf("g%d", i))
					if err != nil {
						fail("reader %d round %d: get: %v", r, s, err)
						pin.Close()
						return
					}
					wantS[i], wantL[i] = got.Status, got.Latency
				}
				for v := 0; v < verifies; v++ {
					time.Sleep(5 * time.Millisecond)
					for i := 0; i < keys; i++ {
						got, err := bound.Get(fmt.Sprintf("g%d", i))
						if err != nil {
							fail("reader %d round %d verify %d: get: %v", r, s, v, err)
							pin.Close()
							return
						}
						if got.Status != wantS[i] || got.Latency != wantL[i] {
							fail("reader %d round %d verify %d key g%d: got (%d,%d), want (%d,%d)",
								r, s, v, i, got.Status, got.Latency, wantS[i], wantL[i])
							pin.Close()
							return
						}
					}
				}
				pin.Close()
			}
		}(r)
	}
	rwg.Wait()
	close(stop)
	wg.Wait()

	for {
		select {
		case msg := <-errCh:
			t.Fatalf("concurrent GC failure: %s", msg)
		default:
			goto drained
		}
	}
drained:

	// Generation-0 pin still reads exact initial values.
	for i := 0; i < keys; i++ {
		got, err := dev.In(gen0).Get(fmt.Sprintf("g%d", i))
		if err != nil || got.Status != 0 || got.Latency != 0 {
			t.Fatalf("gen0 pin lost history at g%d: %+v %v", i, got, err)
		}
	}
	// Retention policy (gcOldest): newest version <= oldest snapshot plus
	// ALL newer versions are kept, so churn accumulates while gen0 is pinned.
	if st := dev.TableStats(); st.Versions <= keys {
		t.Fatalf("expected accumulated history while pinned, versions = %d", st.Versions)
	}
	gen0.Close()

	// With all pins closed, chains collapse to one head per key and the
	// retained history is reclaimed.
	res := db.GC()
	if res.Reclaimed == 0 {
		t.Fatalf("final GC reclaimed nothing: %+v", res)
	}
	if st := db.Stats(); st.GCReclaimed == 0 {
		t.Fatalf("cumulative GCReclaimed = 0: %+v", st)
	}
	after := dev.TableStats()
	if after.Versions != keys {
		t.Fatalf("versions after final GC = %d, want %d", after.Versions, keys)
	}
	if after.Tombstones != 0 {
		t.Fatalf("tombstones = %d, want 0", after.Tombstones)
	}
	if after.Records != keys {
		t.Fatalf("records = %d, want %d", after.Records, keys)
	}
	// Heads hold churned values and stay internally consistent.
	for i := 0; i < keys; i++ {
		got, err := dev.Get(fmt.Sprintf("g%d", i))
		if err != nil || got.Status < 1 || got.Latency != got.Status {
			t.Fatalf("head g%d inconsistent: %+v %v", i, got, err)
		}
	}
}
