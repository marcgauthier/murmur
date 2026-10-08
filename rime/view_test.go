package rime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type viewRecord struct {
	ID    int    `rime:"primary"`
	Value int    `rime:"ordered"`
	Group string `rime:"index"`
}

func viewTable(t *testing.T, db *DB, name string) *Table[viewRecord] {
	t.Helper()
	table, err := Register[viewRecord](db, WithTableName[viewRecord](name))
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func viewRows(rows []*viewRecord) []viewRecord {
	out := make([]viewRecord, len(rows))
	for i, r := range rows {
		out[i] = *r
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func TestViewsFilterAndOrderedMaintenance(t *testing.T) {
	db := New()
	defer db.Close()
	table := viewTable(t, db, "records")
	value := OF[viewRecord, int](table, "Value")
	q := table.Where(value.Ge(2))
	filtered, err := NewView("filtered", q)
	if err != nil {
		t.Fatal(err)
	}
	pagedQ := q.OrderByDesc(value).Offset(1).Limit(2)
	paged, err := NewView("paged", pagedQ)
	if err != nil {
		t.Fatal(err)
	}
	// Builders are copied, even if an application mutates its original handle.
	q.limit = 0
	operations := []func() error{
		func() error {
			return table.UpsertMany([]*viewRecord{{ID: 1, Value: 2}, {ID: 2, Value: 4}, {ID: 3, Value: 6}, {ID: 4, Value: 8}})
		},
		func() error { return table.Update(2, func(r *viewRecord) error { r.Value = 10; return nil }) },
		func() error { return table.Update(1, func(r *viewRecord) error { r.Value = 0; return nil }) },
		func() error { return table.Delete(3) },
		func() error {
			return db.WriteTx(func(tx *Tx) error {
				if err := table.In(tx).Upsert(&viewRecord{ID: 1, Value: 12}); err != nil {
					return err
				}
				if err := table.In(tx).Delete(1); err != nil {
					return err
				}
				return table.In(tx).Upsert(&viewRecord{ID: 1, Value: 14})
			})
		},
		func() error { return table.DeleteMany([]any{1, 2, 4}) },
	}
	for i, op := range operations {
		if err := op(); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		a, err := filtered.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		fresh, err := table.Where(value.Ge(2)).Find()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(viewRows(a.Rows), viewRows(fresh)) || a.Commit != db.latest() {
			t.Fatalf("filter step %d: %+v vs %+v", i, a, fresh)
		}
		b, err := paged.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		fresh, err = pagedQ.Find()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(b.Rows, fresh) || b.Commit != db.latest() {
			t.Fatalf("paged step %d: %+v vs %+v", i, b, fresh)
		}
	}
	before, _ := filtered.Snapshot()
	other := viewTable(t, db, "other")
	if err := other.Insert(&viewRecord{ID: 1}); err != nil {
		t.Fatal(err)
	}
	after, _ := filtered.Snapshot()
	if before.Commit != after.Commit {
		t.Fatal("unrelated commit refreshed view")
	}
	if err := table.Insert(&viewRecord{ID: 1, Value: 8}); err != nil {
		t.Fatal(err)
	}
	held, _ := filtered.Snapshot()
	if err := table.Update(1, func(r *viewRecord) error { r.Value = 9; return nil }); err != nil {
		t.Fatal(err)
	}
	if held.Rows[0].Value != 8 {
		t.Fatal("old snapshot mutated")
	}
	held.Rows[0] = nil
	latest, _ := filtered.Snapshot()
	if latest.Rows[0] == nil {
		t.Fatal("result slice shared")
	}
}

func TestComputedViewPreviewQueries(t *testing.T) {
	db := New()
	defer db.Close()
	left, right := viewTable(t, db, "left"), viewTable(t, db, "right")
	type summary struct {
		Pairs     int
		Matched   int
		Max       int
		Sum       float64
		Each      int
		Point     int
		Groups    int
		Projected int
	}
	group := SF[viewRecord](left, "Group")
	value := OF[viewRecord, int](left, "Value")
	build := func(tx *Tx) ([]summary, error) {
		a, err := InnerJoin(left.In(tx), right.In(tx), func(r *viewRecord) int { return r.ID }, func(r *viewRecord) int { return r.ID })
		if err != nil {
			return nil, err
		}
		b, err := LeftJoinOn(left.In(tx), F[viewRecord, int](left, "ID"), right.In(tx), F[viewRecord, int](right, "ID"))
		if err != nil {
			return nil, err
		}
		c, err := InnerJoinOn(left.In(tx), F[viewRecord, int](left, "ID"), right.In(tx), F[viewRecord, int](right, "ID"))
		if err != nil {
			return nil, err
		}
		if len(a) != len(c) {
			return nil, errors.New("join paths disagree")
		}
		s := summary{Pairs: len(b), Matched: len(a)}
		agg, err := left.Where().In(tx).Aggregate(MaxOf(value), SumOf(value))
		if err != nil {
			return nil, err
		}
		if agg[0] != nil {
			s.Max = agg[0].(int)
		}
		s.Sum = agg[1].(float64)
		err = left.Where().In(tx).Each(func(r *viewRecord) error { s.Each += r.Value; return nil })
		if err != nil {
			return nil, err
		}
		r, found, err := left.In(tx).Lookup(1)
		if err != nil {
			return nil, err
		}
		if found {
			s.Point = r.Value
		}
		groups, err := left.Where().In(tx).GroupBy(group).Aggregate(Count[viewRecord]())
		if err != nil {
			return nil, err
		}
		s.Groups = len(groups)
		projected, err := Project(left.Where().In(tx), func(r *viewRecord) int { return r.Value })
		if err != nil {
			return nil, err
		}
		for _, n := range projected {
			s.Projected += n
		}
		return []summary{s}, nil
	}
	v, err := NewComputedView(db, "summary", []ViewSource{left, right}, build)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.WriteTx(func(tx *Tx) error {
		if err := left.In(tx).Upsert(&viewRecord{ID: 1, Value: 7, Group: "a"}); err != nil {
			return err
		}
		if err := left.In(tx).Upsert(&viewRecord{ID: 2, Value: 9, Group: "b"}); err != nil {
			return err
		}
		return right.In(tx).Upsert(&viewRecord{ID: 1})
	}); err != nil {
		t.Fatal(err)
	}
	got, err := v.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	want := summary{Pairs: 2, Matched: 1, Max: 9, Sum: 16, Each: 16, Point: 7, Groups: 2, Projected: 16}
	if !reflect.DeepEqual(got.Rows, []summary{want}) {
		t.Fatalf("got %+v want %+v", got, want)
	}
	if err := db.WriteTx(func(tx *Tx) error {
		if err := left.In(tx).Delete(2); err != nil {
			return err
		}
		return right.In(tx).Delete(1)
	}); err != nil {
		t.Fatal(err)
	}
	tx := db.ReadTx()
	defer tx.Close()
	fresh, err := build(tx)
	if err != nil {
		t.Fatal(err)
	}
	got, err = v.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Rows, fresh) {
		t.Fatalf("got %+v fresh %+v", got.Rows, fresh)
	}
}

func TestViewUsesLatestStateAndPublishesAtomically(t *testing.T) {
	db := New()
	defer db.Close()
	table := viewTable(t, db, "records")
	if err := table.UpsertMany([]*viewRecord{{ID: 1, Value: 1}, {ID: 2, Value: 2}}); err != nil {
		t.Fatal(err)
	}
	var block atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	v, err := NewComputedView(db, "all", []ViewSource{table}, func(tx *Tx) ([]*viewRecord, error) {
		if block.Load() {
			close(entered)
			<-release
		}
		return table.Where().In(tx).Find()
	})
	if err != nil {
		t.Fatal(err)
	}
	staged, proceed := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.WriteTx(func(tx *Tx) error {
			if err := table.In(tx).Update(1, func(r *viewRecord) error { r.Value = 10; return nil }); err != nil {
				return err
			}
			close(staged)
			<-proceed
			return nil
		})
	}()
	<-staged
	if err := table.Update(2, func(r *viewRecord) error { r.Value = 20; return nil }); err != nil {
		t.Fatal(err)
	}
	before, _ := v.Snapshot()
	block.Store(true)
	close(proceed)
	<-entered
	during, _ := v.Snapshot()
	row, readErr := table.Get(1)
	clock := db.latest()
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if readErr != nil || row.Value != 1 || clock != before.Commit || !reflect.DeepEqual(viewRows(before.Rows), viewRows(during.Rows)) {
		t.Fatal("unpublished state exposed")
	}
	after, err := v.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(viewRows(after.Rows), []viewRecord{{ID: 1, Value: 10}, {ID: 2, Value: 20}}) {
		t.Fatalf("lost intervening commit: %+v", after)
	}
}

func TestViewFailuresRecoveryAndLifecycle(t *testing.T) {
	db := New()
	table := viewTable(t, db, "records")
	other := viewTable(t, db, "other")
	var mode atomic.Int32
	cause := errors.New("build failed")
	build := func(tx *Tx) ([]int, error) {
		switch mode.Load() {
		case 1:
			return nil, cause
		case 2:
			panic("boom")
		case 3:
			_, _ = other.In(tx).Get(1) // Ignoring dependency errors must still fail.
		case 4:
			_ = table.In(tx).Insert(&viewRecord{ID: 999})
		case 5:
			tx.Close()
		}
		rows, err := table.Where().In(tx).Find()
		out := make([]int, len(rows))
		for i, r := range rows {
			out[i] = r.Value
		}
		sort.Ints(out)
		return out, err
	}
	v, err := NewComputedView(db, "computed", []ViewSource{table}, build)
	if err != nil {
		t.Fatal(err)
	}
	good, err := NewView("good", table.Where())
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []int32{1, 2, 3, 4, 5} {
		mode.Store(m)
		if err := table.Upsert(&viewRecord{ID: 1, Value: int(m)}); err != nil {
			t.Fatalf("failure rejected write: %v", err)
		}
		s, err := v.Snapshot()
		var ve *ViewError
		if err == nil || !errors.As(err, &ve) || len(s.Rows) != 0 || ve.Commit != db.latest() {
			t.Fatalf("mode %d: %+v %v", m, s, err)
		}
		if m == 1 && !errors.Is(err, cause) {
			t.Fatal(err)
		}
		if m == 3 && !errors.Is(err, ErrViewDependency) {
			t.Fatal(err)
		}
		if m == 4 && !errors.Is(err, ErrTxReadOnly) {
			t.Fatal(err)
		}
		if m == 5 && !errors.Is(err, ErrTxClosed) {
			t.Fatal(err)
		}
		g, err := good.Snapshot()
		if err != nil || len(g.Rows) != 1 || g.Rows[0].Value != int(m) {
			t.Fatalf("healthy view affected: %+v %v", g, err)
		}
		mode.Store(0)
		if m%2 == 0 {
			err = v.Refresh(context.Background())
		} else {
			err = table.Update(1, func(r *viewRecord) error { r.Value = 0; return nil })
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := v.Snapshot(); err != nil {
			t.Fatalf("recovery: %v", err)
		}
	}
	if _, err := table.Get(999); !errors.Is(err, ErrNotFound) {
		t.Fatal("read-only callback wrote data")
	}
	held, _ := v.Snapshot()
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Snapshot(); !errors.Is(err, ErrViewClosed) {
		t.Fatal(err)
	}
	if err := v.Refresh(context.Background()); !errors.Is(err, ErrViewClosed) {
		t.Fatal(err)
	}
	mode.Store(1)
	if _, err := NewComputedView(db, "computed", []ViewSource{table}, build); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	mode.Store(0)
	v, err = NewComputedView(db, "computed", []ViewSource{table}, build)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewComputedView(db, "computed", []ViewSource{table}, build); !errors.Is(err, ErrViewExists) {
		t.Fatal(err)
	}
	db.Close()
	if _, err := v.Snapshot(); !errors.Is(err, ErrDBClosed) {
		t.Fatal(err)
	}
	if len(held.Rows) != 1 {
		t.Fatal("closed snapshot lost results")
	}
	if len(db.views) != 0 || len(db.viewsBySource) != 0 || v.build != nil {
		t.Fatal("view references retained")
	}
}

func TestViewLimitsDefinitionsAndCancellation(t *testing.T) {
	db := New(WithMaxResults(1))
	defer db.Close()
	table := viewTable(t, db, "records")
	v, err := NewView("limited", table.Where())
	if err != nil {
		t.Fatal(err)
	}
	if err := table.UpsertMany([]*viewRecord{{ID: 1}, {ID: 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Snapshot(); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal(err)
	}
	if err := table.Delete(2); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Snapshot(); err != nil {
		t.Fatal(err)
	}
	tx := db.ReadTx()
	defer tx.Close()
	if _, err := NewView("bound", table.Where().In(tx)); !errors.Is(err, ErrBadView) {
		t.Fatal(err)
	}
	if _, err := NewView[viewRecord]("nil", nil); !errors.Is(err, ErrBadView) {
		t.Fatal(err)
	}
	if _, err := NewView(" ", table.Where()); !errors.Is(err, ErrBadView) {
		t.Fatal(err)
	}
	var nilTable *Table[viewRecord]
	if _, err := NewComputedView(db, "nilsource", []ViewSource{nilTable}, func(*Tx) ([]int, error) { return nil, nil }); !errors.Is(err, ErrBadView) {
		t.Fatal(err)
	}
	db2 := New()
	defer db2.Close()
	foreign := viewTable(t, db2, "foreign")
	if _, err := NewComputedView(db, "foreign", []ViewSource{foreign}, func(*Tx) ([]int, error) { return nil, nil }); !errors.Is(err, ErrTxDatabase) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := v.Refresh(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := v.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	var cancelWrite context.CancelFunc
	cv, err := NewComputedView(db, "cancel", []ViewSource{table}, func(tx *Tx) ([]int, error) {
		if cancelWrite != nil {
			cancelWrite()
		}
		rows, err := table.Where().In(tx).Find()
		out := make([]int, len(rows))
		return out, err
	})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := cv.Snapshot()
	ctx, cancelWrite = context.WithCancel(context.Background())
	if err := db.WriteTxContext(ctx, func(tx *Tx) error {
		return table.In(tx).Update(1, func(r *viewRecord) error { r.Value = 100; return nil })
	}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	cancelWrite = nil
	after, err := cv.Snapshot()
	if err != nil || after.Commit != before.Commit {
		t.Fatalf("aborted refresh published: %+v %v", after, err)
	}
	row, _ := table.Get(1)
	if row.Value != 0 {
		t.Fatal("canceled source published")
	}
}

func TestViewRegistrationAndCloseRace(t *testing.T) {
	db := New()
	defer db.Close()
	table := viewTable(t, db, "records")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	created := make(chan *View[int], 1)
	errs := make(chan error, 1)
	go func() {
		v, err := NewComputedView(db, "racing", []ViewSource{table}, func(tx *Tx) ([]int, error) {
			once.Do(func() { close(entered); <-release })
			n, err := table.Where().In(tx).Count()
			return []int{n}, err
		})
		created <- v
		errs <- err
	}()
	<-entered
	wrote := make(chan error, 1)
	go func() { wrote <- table.Insert(&viewRecord{ID: 1}) }()
	close(release)
	v := <-created
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	if err := <-wrote; err != nil {
		t.Fatal(err)
	}
	s, err := v.Snapshot()
	if err != nil || !reflect.DeepEqual(s.Rows, []int{1}) {
		t.Fatalf("missed registration commit: %+v %v", s, err)
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			_ = v.Refresh(context.Background())
			_, _ = v.Snapshot()
			db.GC()
		}
		close(done)
	}()
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("lifecycle deadlock")
	}
}

func BenchmarkViewRead(b *testing.B) {
	for _, n := range []int{100, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			db := New()
			defer db.Close()
			table, _ := Register[viewRecord](db)
			for i := 0; i < n; i++ {
				_ = table.Insert(&viewRecord{ID: i, Value: i})
			}
			q := table.Where(OF[viewRecord, int](table, "Value").Ge(n / 2))
			v, err := NewView("filter", q)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("cached", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := v.Snapshot(); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("query", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := q.Find(); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkViewMaintenance(b *testing.B) {
	for _, kind := range []string{"none", "incremental", "computed"} {
		b.Run(kind, func(b *testing.B) {
			db := New()
			defer db.Close()
			table, _ := Register[viewRecord](db)
			for i := 0; i < 1000; i++ {
				_ = table.Insert(&viewRecord{ID: i, Value: i})
			}
			q := table.Where(OF[viewRecord, int](table, "Value").Ge(500))
			if kind == "incremental" {
				if _, err := NewView("filter", q); err != nil {
					b.Fatal(err)
				}
			}
			if kind == "computed" {
				if _, err := NewComputedView(db, "filter", []ViewSource{table}, func(tx *Tx) ([]*viewRecord, error) { return q.In(tx).Find() }); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := table.Upsert(&viewRecord{ID: i % 1000, Value: i % 1000}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestViewRejectedWritesAndHookGrowth(t *testing.T) {
	db := New()
	defer db.Close()
	left, right := viewTable(t, db, "left"), viewTable(t, db, "right")
	left.BeforeCommit(func(tx *Tx) error { return right.In(tx).Upsert(&viewRecord{ID: 2, Value: 20}) })
	var builds atomic.Int64
	v, err := NewComputedView(db, "both", []ViewSource{left, right}, func(tx *Tx) ([]int, error) {
		builds.Add(1)
		a, err := left.Where().In(tx).Find()
		if err != nil {
			return nil, err
		}
		b, err := right.Where().In(tx).Find()
		if err != nil {
			return nil, err
		}
		sum := 0
		for _, r := range a {
			sum += r.Value
		}
		for _, r := range b {
			sum += r.Value
		}
		return []int{sum}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := left.Insert(&viewRecord{ID: 1, Value: 10}); err != nil {
		t.Fatal(err)
	}
	s, err := v.Snapshot()
	if err != nil || !reflect.DeepEqual(s.Rows, []int{30}) || builds.Load() != 2 {
		t.Fatalf("hook growth: %+v %v builds=%d", s, err, builds.Load())
	}
	stop := errors.New("abort")
	if err := db.WriteTx(func(tx *Tx) error {
		if err := left.In(tx).Update(1, func(r *viewRecord) error { r.Value = 999; return nil }); err != nil {
			return err
		}
		return stop
	}); !errors.Is(err, stop) {
		t.Fatal(err)
	}
	after, _ := v.Snapshot()
	if after.Commit != s.Commit || builds.Load() != 2 {
		t.Fatal("rolled-back write refreshed view")
	}
	staged, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- db.WriteTx(func(tx *Tx) error {
			if err := left.In(tx).Update(1, func(r *viewRecord) error { r.Value = 999; return nil }); err != nil {
				return err
			}
			close(staged)
			<-release
			return nil
		})
	}()
	<-staged
	if err := left.Update(1, func(r *viewRecord) error { r.Value = 30; return nil }); err != nil {
		t.Fatal(err)
	}
	beforeConflict, _ := v.Snapshot()
	count := builds.Load()
	close(release)
	if err := <-done; !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	after, _ = v.Snapshot()
	if !reflect.DeepEqual(after, beforeConflict) || builds.Load() != count {
		t.Fatal("conflicting write refreshed view")
	}
}

func TestViewScanAndComputedResultLimits(t *testing.T) {
	db := New(WithMaxScan(1))
	defer db.Close()
	table := viewTable(t, db, "limited")
	v, err := NewView("filtered", table.Where())
	if err != nil {
		t.Fatal(err)
	}
	if err := table.UpsertMany([]*viewRecord{{ID: 1}, {ID: 2}}); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Snapshot(); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal(err)
	}
	if err := table.Delete(2); err != nil {
		t.Fatal(err)
	}
	// The commit preview still scans the pre-delete rows. Rebuild after the
	// source delete commits to fit the one-record scan budget.
	if err := v.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := v.Snapshot(); err != nil {
		t.Fatal(err)
	}
	db2 := New(WithMaxResults(1))
	defer db2.Close()
	left, right := viewTable(t, db2, "left"), viewTable(t, db2, "right")
	if err := left.UpsertMany([]*viewRecord{{ID: 1}, {ID: 2}}); err != nil {
		t.Fatal(err)
	}
	join, err := NewComputedView(db2, "join", []ViewSource{left, right}, func(tx *Tx) ([]JoinRow[viewRecord, viewRecord], error) {
		return InnerJoin(left.In(tx), right.In(tx), func(r *viewRecord) int { return r.ID }, func(r *viewRecord) int { return r.ID })
	})
	if err != nil {
		t.Fatal(err)
	}
	// Two input records, but only one output: the result limit must not cap inputs.
	if err := right.Insert(&viewRecord{ID: 1}); err != nil {
		t.Fatal(err)
	}
	s, err := join.Snapshot()
	if err != nil || len(s.Rows) != 1 {
		t.Fatalf("join input capped: %+v %v", s, err)
	}
	if err := right.Insert(&viewRecord{ID: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := join.Snapshot(); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal(err)
	}
	if err := right.Delete(2); err != nil {
		t.Fatal(err)
	}
	if _, err := join.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewComputedView(db2, "oversize", []ViewSource{left}, func(*Tx) ([]int, error) { return []int{1, 2}, nil }); !errors.Is(err, ErrLimitExceeded) {
		t.Fatal(err)
	}
}
