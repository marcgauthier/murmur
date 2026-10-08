package rime

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type qualificationRow struct {
	ID    string `rime:"primary"`
	Value int    `rime:"ordered"`
}

// Pause between row publications to expose a partially published commit without
// relying on the scheduler to hit the narrow commit window.
func TestQualificationCommitPublication(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[qualificationRow](db)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.UpsertMany([]*qualificationRow{{ID: "a", Value: 50}, {ID: "b", Value: 50}}); err != nil {
		t.Fatal(err)
	}
	before := db.latest()
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.WriteTx(func(tx *Tx) error {
			if err := table.In(tx).Update("a", func(r *qualificationRow) error { r.Value = 40; return nil }); err != nil {
				return err
			}
			if err := table.In(tx).Update("b", func(r *qualificationRow) error { r.Value = 60; return nil }); err != nil {
				return err
			}
			p := tx.pending[1]
			p.apply = func(id TxID) func() {
				close(entered)
				<-release
				eff := p.table.applyPending(id, p)
				return func() { p.table.fireAfterEffect(eff) }
			}
			return nil
		})
	}()
	<-entered
	observed := db.latest()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if observed != before {
		t.Fatalf("commit became visible before publication completed: %d -> %d", before, observed)
	}
	tx := db.ReadTx()
	defer tx.Close()
	a, err := table.In(tx).Get("a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := table.In(tx).Get("b")
	if err != nil {
		t.Fatal(err)
	}
	if a.Value+b.Value != 100 {
		t.Fatalf("partial commit: %+v %+v", a, b)
	}
}

func TestQualificationExpiredSnapshot(t *testing.T) {
	db := New(WithMaxTxAge(time.Hour))
	defer db.Close()
	table, err := Register[qualificationRow](db)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Upsert(&qualificationRow{ID: "a", Value: 1}); err != nil {
		t.Fatal(err)
	}
	tx := db.ReadTx()
	defer tx.Close()
	tx.start = time.Now().Add(-2 * time.Hour)
	db.activeMu.Lock()
	r := db.active[tx]
	r.start = tx.start
	db.active[tx] = r
	db.activeMu.Unlock()
	if err := table.Upsert(&qualificationRow{ID: "a", Value: 2}); err != nil {
		t.Fatal(err)
	}
	db.GC()
	if _, err := table.In(tx).Get("a"); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("expired snapshot must fail explicitly: %v", err)
	}
}

func TestQualificationCancelBeforeCommit(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[qualificationRow](db)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	err = db.WriteTxContext(ctx, func(tx *Tx) error {
		if err := table.In(tx).Upsert(&qualificationRow{ID: "a", Value: 1}); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled transaction committed: %v", err)
	}
	if _, err := table.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("aborted row visible: %v", err)
	}
}

func TestQualificationCancellationDuringScan(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[qualificationRow](db)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 256; i++ {
		if err := table.Upsert(&qualificationRow{ID: fmt.Sprint(i), Value: i}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	predicate := OF[qualificationRow, int](table, "Value").Ne(-1).(*expr[qualificationRow])
	match := predicate.fn
	predicate.fn = func(r *qualificationRow) bool { cancel(); return match(r) }
	if _, err := table.Where(predicate).WithContext(ctx).Find(); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-scan cancellation: %v", err)
	}
	if db.Stats().ActiveTxns != 0 {
		t.Fatal("canceled automatic query leaked snapshot")
	}
}

// GC must exclude registration of new snapshots until all tables are pruned.
type qualificationGCTable struct {
	innerTable
	entered, release chan struct{}
}

func (t *qualificationGCTable) gcOldest(ctx context.Context, oldest TxID) int {
	close(t.entered)
	<-t.release
	return t.innerTable.gcOldest(ctx, oldest)
}
func TestQualificationSnapshotRegistrationDuringGC(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[qualificationRow](db)
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Upsert(&qualificationRow{ID: "a", Value: 1}); err != nil {
		t.Fatal(err)
	}
	for i := 2; i <= 3; i++ {
		if err := table.Upsert(&qualificationRow{ID: "a", Value: i}); err != nil {
			t.Fatal(err)
		}
	}
	wrapper := &qualificationGCTable{innerTable: table, entered: make(chan struct{}), release: make(chan struct{})}
	db.tables[table.Name()] = wrapper
	done := make(chan struct{})
	go func() { db.GC(); close(done) }()
	<-wrapper.entered
	snapshots := make(chan *Tx, 1)
	go func() { snapshots <- db.ReadTx() }()
	close(wrapper.release)
	<-done
	tx := <-snapshots
	defer tx.Close()
	db.tables[table.Name()] = table
	if err := table.Upsert(&qualificationRow{ID: "a", Value: 4}); err != nil {
		t.Fatal(err)
	}
	db.GC()
	row, err := table.In(tx).Get("a")
	if err != nil || row.Value != 3 {
		t.Fatalf("new snapshot not protected from GC: %+v %v", row, err)
	}
}

func TestQualificationHookPanicCleanup(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[qualificationRow](db)
	if err != nil {
		t.Fatal(err)
	}
	panicHook := true
	table.BeforeCommit(func(*Tx) error {
		if panicHook {
			panic("hook panic")
		}
		return nil
	})
	func() {
		defer func() {
			if recover() == nil {
				t.Error("hook did not panic")
			}
		}()
		_ = table.Upsert(&qualificationRow{ID: "a", Value: 1})
	}()
	if len(db.OpenTransactions()) != 0 {
		t.Fatal("panic leaked transaction")
	}
	panicHook = false
	if err := table.Upsert(&qualificationRow{ID: "b", Value: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := table.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("panic partially published: %v", err)
	}
}

func TestQualificationCacheBound(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[qualificationRow](db)
	if err != nil {
		t.Fatal(err)
	}
	value := OF[qualificationRow, int](table, "Value")
	for i := 0; i < planCacheMax; i++ {
		if _, err := table.Where(value.Eq(i)).Find(); err != nil {
			t.Fatal(err)
		}
	}
	if err := table.Upsert(&qualificationRow{ID: "a", Value: 1}); err != nil {
		t.Fatal(err)
	}
	// Replacing the oldest stale fingerprint must not remove its FIFO entry.
	if _, err := table.Where(value.Eq(0)).Find(); err != nil {
		t.Fatal(err)
	}
	for i := planCacheMax; i < planCacheMax*2; i++ {
		if _, err := table.Where(value.Eq(i)).Find(); err != nil {
			t.Fatal(err)
		}
	}
	table.pc.mu.Lock()
	defer table.pc.mu.Unlock()
	if len(table.pc.items) > planCacheMax || len(table.pc.order) != len(table.pc.items) {
		t.Fatalf("unbounded cache: items=%d order=%d", len(table.pc.items), len(table.pc.order))
	}
}

func TestQualificationLargeBatch(t *testing.T) {
	db := New()
	defer db.Close()
	table, err := Register[qualificationRow](db)
	if err != nil {
		t.Fatal(err)
	}
	const count = 4096
	rows := make([]*qualificationRow, count)
	for i := range rows {
		rows[i] = &qualificationRow{ID: fmt.Sprint(i), Value: i}
	}
	err = db.WriteTx(func(tx *Tx) error {
		if err := table.In(tx).UpsertMany(rows); err != nil {
			return err
		}
		for i := range rows {
			got, err := table.In(tx).Get(rows[i].ID)
			if err != nil || *got != *rows[i] {
				return fmt.Errorf("staged batch row %d: %+v %v", i, got, err)
			}
		}
		return table.In(tx).Update(rows[0].ID, func(r *qualificationRow) error { r.Value = count; return nil })
	})
	if err != nil {
		t.Fatal(err)
	}
	all, err := table.Where().Find()
	if err != nil || len(all) != count {
		t.Fatalf("batch publication: %d %v", len(all), err)
	}
	first, err := table.Get(rows[0].ID)
	if err != nil || first.Value != count {
		t.Fatalf("stacked batch write: %+v %v", first, err)
	}
}

func TestQualificationSubscribeAfterClose(t *testing.T) {
	db := New()
	table, err := Register[qualificationRow](db)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	table.OnInserted(func(*qualificationRow) { t.Error("closed database delivered an event") })
	table.bus.mu.Lock()
	defer table.bus.mu.Unlock()
	if !table.bus.closed || table.bus.started {
		t.Fatal("subscription started a worker after database closure")
	}
}
