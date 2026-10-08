package rime_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/marcgauthier/murmur/rime"
)

func requireRows(t *testing.T, got []*Device, want map[string]Device) {
	t.Helper()
	seen := make(map[string]bool)
	for _, row := range got {
		expected, ok := want[row.ID]
		if !ok || expected != *row || seen[row.ID] {
			t.Fatalf("unexpected/duplicate row: %+v expected=%+v", row, expected)
		}
		seen[row.ID] = true
	}
	if len(seen) != len(want) {
		t.Fatalf("row count: got %d want %d", len(seen), len(want))
	}
}

func TestQualificationSnapshots(t *testing.T) {
	db, tab := openDevices(t)
	defer db.Close()
	snapshots := []*rime.Tx{}
	models := []map[string]Device{}
	current := map[string]Device{}
	defer func() {
		for _, tx := range snapshots {
			if tx != nil {
				tx.Close()
			}
		}
	}()
	site := rime.SF[Device](tab, "Site")
	latency := rime.OF[Device, int](tab, "Latency")
	for step := 0; step < 12; step++ {
		key := fmt.Sprintf("k%d", step%3)
		if step%4 == 3 {
			if _, ok := current[key]; ok {
				if err := tab.Delete(key); err != nil {
					t.Fatal(err)
				}
				delete(current, key)
			}
		} else {
			row := Device{ID: key, Hostname: "h" + key, Site: fmt.Sprintf("s%d", step%2), Status: step, Latency: 100 - step}
			mustSave(t, tab, row)
			current[key] = row
		}
		snapshots = append(snapshots, db.ReadTx())
		copyModel := map[string]Device{}
		for key, row := range current {
			copyModel[key] = row
		}
		models = append(models, copyModel)
		db.GC()
		for i, tx := range snapshots {
			if tx == nil {
				continue
			}
			rows, err := tab.Where().In(tx).Find()
			if err != nil {
				t.Fatal(err)
			}
			requireRows(t, rows, models[i])
			for _, value := range []string{"s0", "s1"} {
				want := map[string]Device{}
				for k, row := range models[i] {
					if row.Site == value {
						want[k] = row
					}
				}
				rows, err := tab.Where(site.Eq(value)).In(tx).Find()
				if err != nil {
					t.Fatal(err)
				}
				requireRows(t, rows, want)
			}
			extremes, err := tab.Where().In(tx).Aggregate(rime.MinOf(latency))
			if err != nil {
				t.Fatal(err)
			}
			var min any
			for _, row := range models[i] {
				if min == nil || row.Latency < min.(int) {
					min = row.Latency
				}
			}
			if !reflect.DeepEqual(extremes, []any{min}) {
				t.Fatalf("snapshot %d minimum: %v want %v", i, extremes, min)
			}
		}
		if step > 3 {
			snapshots[step-3].Close()
			snapshots[step-3] = nil
		}
	}
	old := snapshots[0].Snapshot()
	for _, tx := range snapshots {
		if tx != nil {
			tx.Close()
		}
	}
	snapshots = nil
	db.GC()
	historical := db.ReadAt(old)
	defer historical.Close()
	if _, err := tab.Where().In(historical).Find(); !errors.Is(err, rime.ErrSnapshotUnavailable) {
		t.Fatalf("reclaimed snapshot silently accepted: %v", err)
	}
	if st := tab.TableStats(); st.Versions != st.Records {
		t.Fatalf("unreleased versions: %+v", st)
	}
}

func TestQualificationQueryEquivalence(t *testing.T) {
	db, tab := openDevices(t)
	defer db.Close()
	site := rime.SF[Device](tab, "Site")
	host := rime.SF[Device](tab, "Hostname")
	status := rime.OF[Device, int](tab, "Status")
	latency := rime.OF[Device, int](tab, "Latency")
	id := rime.SF[Device](tab, "ID")
	type caseDef struct {
		name    string
		expr    rime.Expr[Device]
		matches func(Device) bool
	}
	cases := []caseDef{
		{"eq", site.Eq("OTT"), func(d Device) bool { return d.Site == "OTT" }},
		{"ne", site.Ne("OTT"), func(d Device) bool { return d.Site != "OTT" }},
		{"in", site.In("OTT", "WPG"), func(d Device) bool { return d.Site == "OTT" || d.Site == "WPG" }},
		{"notin", site.NotIn("OTT", "WPG"), func(d Device) bool { return d.Site == "MTL" }},
		{"range", latency.Between(3, 11), func(d Device) bool { return d.Latency >= 3 && d.Latency <= 11 }},
		{"gt", latency.Gt(11), func(d Device) bool { return d.Latency > 11 }},
		{"ge", latency.Ge(11), func(d Device) bool { return d.Latency >= 11 }},
		{"lt", latency.Lt(3), func(d Device) bool { return d.Latency < 3 }},
		{"le", latency.Le(3), func(d Device) bool { return d.Latency <= 3 }},
		{"prefix", host.StartsWith("h1"), func(d Device) bool { return strings.HasPrefix(d.Hostname, "h1") }},
		{"contains", host.Contains("1"), func(d Device) bool { return strings.Contains(d.Hostname, "1") }},
		{"suffix", host.EndsWith("1"), func(d Device) bool { return strings.HasSuffix(d.Hostname, "1") }},
		{"like", host.Like("h1%"), func(d Device) bool { return strings.HasPrefix(d.Hostname, "h1") }},
		{"compound", rime.And(site.Eq("OTT"), status.Eq(1)), func(d Device) bool { return d.Site == "OTT" && d.Status == 1 }},
		{"or", rime.Or(site.Eq("OTT"), status.Eq(1)), func(d Device) bool { return d.Site == "OTT" || d.Status == 1 }},
		{"not", rime.Not(site.Eq("OTT")), func(d Device) bool { return d.Site != "OTT" }},
		{"contradiction", rime.And(site.Eq("OTT"), site.Eq("MTL")), func(Device) bool { return false }},
	}
	model := map[string]Device{}
	for i := 0; i < 24; i++ {
		d := Device{ID: fmt.Sprintf("k%02d", i), Hostname: fmt.Sprintf("h%d", i), Site: []string{"OTT", "MTL", "WPG"}[i%3], Status: i % 4, Latency: i % 17}
		mustSave(t, tab, d)
		model[d.ID] = d
	}
	pinned := db.ReadTx()
	defer pinned.Close()
	compiled := tab.Compile(site.Eq(rime.Param[string]()))
	check := func(tx *rime.Tx, want map[string]Device) {
		for _, c := range cases {
			expected := map[string]Device{}
			for k, d := range want {
				if c.matches(d) {
					expected[k] = d
				}
			}
			for repeat := 0; repeat < 2; repeat++ {
				rows, err := tab.Where(c.expr).In(tx).Find()
				if err != nil {
					t.Fatalf("%s: %v", c.name, err)
				}
				requireRows(t, rows, expected)
				n, err := tab.Where(c.expr).In(tx).Count()
				if err != nil || n != len(expected) {
					t.Fatalf("%s count %d want %d: %v", c.name, n, len(expected), err)
				}
			}
		}
		for _, value := range []string{"OTT", "MTL", "OTT", "WPG", "missing"} {
			rows, err := compiled.In(tx).Find(value)
			if err != nil {
				t.Fatal(err)
			}
			expected := map[string]Device{}
			for k, d := range want {
				if d.Site == value {
					expected[k] = d
				}
			}
			requireRows(t, rows, expected)
		}
		ordered := make([]Device, 0, len(want))
		for _, d := range want {
			ordered = append(ordered, d)
		}
		sort.Slice(ordered, func(i, j int) bool {
			if ordered[i].Latency != ordered[j].Latency {
				return ordered[i].Latency < ordered[j].Latency
			}
			return ordered[i].ID < ordered[j].ID
		})
		rows, err := tab.Where().OrderByAsc(latency).OrderByAsc(id).Offset(3).Limit(7).In(tx).Find()
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 7 {
			t.Fatalf("pagination count: %d", len(rows))
		}
		for i, row := range rows {
			if *row != ordered[i+3] {
				t.Fatalf("pagination %d: %+v want %+v", i, row, ordered[i+3])
			}
		}
		empty, err := tab.Where().OrderByAsc(latency).Limit(0).In(tx).Find()
		if err != nil || len(empty) != 0 {
			t.Fatalf("limit zero: %v %v", empty, err)
		}
	}
	check(nil, model)
	fresh := map[string]Device{}
	for k, d := range model {
		d.Site = "MTL"
		d.Status = 1
		d.Latency += 7
		d.Hostname = "new-" + d.ID
		mustSave(t, tab, d)
		fresh[k] = d
	}
	db.GC()
	check(pinned, model)
	check(nil, fresh)
}

func TestQualificationCommitFailures(t *testing.T) {
	for _, failure := range []string{"unique", "check", "hook", "conflict"} {
		t.Run(failure, func(t *testing.T) {
			db, tab := openDevices(t)
			defer db.Close()
			other, err := rime.Register[Device](db, rime.WithTableName[Device]("other"))
			if err != nil {
				t.Fatal(err)
			}
			mustSave(t, tab, Device{ID: "existing", Hostname: "taken"})
			var events atomic.Int64
			tab.AfterSave(func(rime.Change[Device]) { events.Add(1) })
			other.AfterSave(func(rime.Change[Device]) { events.Add(1) })
			before := tab.TableStats()
			beforeOther := other.TableStats()
			beforeCommit := db.Stats().LatestCommit
			reject := false
			tab.AddCheck(func(d *Device) error {
				if reject && d.ID == "new" {
					return errors.New("rejected")
				}
				return nil
			})
			if failure == "hook" {
				tab.BeforeCommit(func(*rime.Tx) error { return rime.ErrHookRejected })
			}
			err = db.WriteTx(func(tx *rime.Tx) error {
				if err := other.In(tx).Upsert(&Device{ID: "other", Hostname: "other"}); err != nil {
					return err
				}
				if err := tab.In(tx).Upsert(&Device{ID: "new", Hostname: "free"}); err != nil {
					return err
				}
				switch failure {
				case "unique":
					return tab.In(tx).Upsert(&Device{ID: "bad", Hostname: "taken"})
				case "check":
					reject = true
				case "conflict":
					return tab.Upsert(&Device{ID: "new", Hostname: "winner"})
				}
				return nil
			})
			wantErr := map[string]error{"unique": rime.ErrUnique, "check": rime.ErrCheck, "hook": rime.ErrHookRejected, "conflict": rime.ErrConflict}[failure]
			if !errors.Is(err, wantErr) {
				t.Fatalf("error %v want %v", err, wantErr)
			}
			if _, err := other.Get("other"); !errors.Is(err, rime.ErrNotFound) {
				t.Fatalf("partial commit: %v", err)
			}
			afterOther := other.TableStats()
			if afterOther.Records != beforeOther.Records || afterOther.Versions != beforeOther.Versions || afterOther.IndexEntries != beforeOther.IndexEntries {
				t.Fatalf("aborted table changed: %+v", afterOther)
			}
			expectedEvents := int64(0)
			if failure == "conflict" {
				expectedEvents = 1
				row, err := tab.Get("new")
				if err != nil || row.Hostname != "winner" {
					t.Fatalf("winner lost: %+v %v", row, err)
				}
			} else {
				if _, err := tab.Get("new"); !errors.Is(err, rime.ErrNotFound) {
					t.Fatalf("aborted record visible: %v", err)
				}
				after := tab.TableStats()
				if after.Records != before.Records || after.Versions != before.Versions || after.IndexEntries != before.IndexEntries {
					t.Fatalf("abort changed stats: %+v", after)
				}
				if db.Stats().LatestCommit != beforeCommit {
					t.Fatal("abort advanced committed clock")
				}
			}
			if events.Load() != expectedEvents {
				t.Fatalf("aborted events: %d want %d", events.Load(), expectedEvents)
			}
		})
	}
}

func TestQualificationUniqueReuse(t *testing.T) {
	db, tab := openDevices(t)
	defer db.Close()
	mustSave(t, tab, Device{ID: "a", Hostname: "one"})
	mustSave(t, tab, Device{ID: "b", Hostname: "two"})
	err := db.WriteTx(func(tx *rime.Tx) error {
		if err := tab.In(tx).Update("a", func(d *Device) error { d.Hostname = "two"; return nil }); err != nil {
			return err
		}
		return tab.In(tx).Update("b", func(d *Device) error { d.Hostname = "one"; return nil })
	})
	if err != nil {
		t.Fatalf("atomic unique swap: %v", err)
	}
	err = db.WriteTx(func(tx *rime.Tx) error {
		if err := tab.In(tx).Upsert(&Device{ID: "c", Hostname: "temp"}); err != nil {
			return err
		}
		if err := tab.In(tx).Upsert(&Device{ID: "c", Hostname: "final"}); err != nil {
			return err
		}
		return tab.In(tx).Upsert(&Device{ID: "d", Hostname: "temp"})
	})
	if err != nil {
		t.Fatalf("intermediate unique claim retained: %v", err)
	}
	host := rime.SF[Device](tab, "Hostname")
	for value, key := range map[string]string{"one": "b", "two": "a", "final": "c", "temp": "d"} {
		rows, err := tab.Where(host.Eq(value)).Find()
		if err != nil || len(rows) != 1 || rows[0].ID != key {
			t.Fatalf("unique %s: %+v %v", value, rows, err)
		}
	}
}

func TestQualificationGeneratedIDs(t *testing.T) {
	type generated struct {
		ID    rime.UUID `rime:"primary,uuid5"`
		Value int
	}
	db := rime.New()
	defer db.Close()
	tab, err := rime.Register[generated](db)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]*generated, 64)
	for i := range rows {
		rows[i] = &generated{Value: i}
	}
	if err := tab.UpsertMany(rows); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); errs <- tab.Insert(&generated{Value: 64 + i}) }(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := tab.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 128 {
		t.Fatalf("generated IDs collided: %d rows", len(got))
	}
	seen := map[rime.UUID]bool{}
	values := map[int]bool{}
	for _, row := range got {
		if row.ID.IsZero() || seen[row.ID] || values[row.Value] {
			t.Fatalf("duplicate generated row: %+v", row)
		}
		seen[row.ID] = true
		values[row.Value] = true
	}
}

func TestQualificationLifecycle(t *testing.T) {
	db, tab := openDevices(t)
	mustSave(t, tab, Device{ID: "a", Hostname: "a"})
	db2, _ := openDevices(t)
	defer db2.Close()
	foreign := db2.ReadTx()
	defer foreign.Close()
	if _, err := tab.In(foreign).Get("a"); !errors.Is(err, rime.ErrTxDatabase) {
		t.Fatalf("foreign read: %v", err)
	}
	if _, err := tab.Where().In(foreign).Find(); !errors.Is(err, rime.ErrTxDatabase) {
		t.Fatalf("foreign query: %v", err)
	}
	err := db2.WriteTx(func(tx *rime.Tx) error { return tab.In(tx).Upsert(&Device{ID: "x", Hostname: "x"}) })
	if !errors.Is(err, rime.ErrTxDatabase) {
		t.Fatalf("foreign write: %v", err)
	}
	read := db.ReadTx()
	if err := tab.In(read).Upsert(&Device{ID: "x", Hostname: "x"}); !errors.Is(err, rime.ErrTxReadOnly) {
		t.Fatalf("read-only: %v", err)
	}
	closed := db.ReadTx()
	closed.Close()
	closed.Close()
	if _, err := tab.In(closed).Get("a"); !errors.Is(err, rime.ErrTxClosed) {
		t.Fatalf("closed transaction: %v", err)
	}
	db.Close()
	db.Close()
	if _, err := tab.In(read).Get("a"); err != nil {
		t.Fatalf("existing snapshot after close: %v", err)
	}
	read.Close()
	if _, err := tab.Get("a"); !errors.Is(err, rime.ErrDBClosed) {
		t.Fatalf("automatic read after close: %v", err)
	}
	if err := tab.Upsert(&Device{ID: "x", Hostname: "x"}); !errors.Is(err, rime.ErrDBClosed) {
		t.Fatalf("closed write: %v", err)
	}
	if db.Stats().ActiveTxns != 0 {
		t.Fatal("transaction leak")
	}
}

func TestQualificationCancellationAndLimits(t *testing.T) {
	db, tab := openDevices(t)
	defer db.Close()
	seedDevices(t, tab, 128)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	latency := rime.OF[Device, int](tab, "Latency")
	site := rime.SF[Device](tab, "Site")
	checks := map[string]func() error{
		"scan":    func() error { _, err := tab.Where().WithContext(ctx).Find(); return err },
		"indexed": func() error { _, err := tab.Where(site.Eq("OTT")).WithContext(ctx).Find(); return err },
		"count":   func() error { _, err := tab.Where().WithContext(ctx).Count(); return err },
		"empty":   func() error { _, err := tab.Where(site.Eq("missing")).WithContext(ctx).Find(); return err },
		"minimum": func() error { _, err := tab.Where().WithContext(ctx).Aggregate(rime.MinOf(latency)); return err },
		"group": func() error {
			_, err := tab.Where().GroupBy(site).WithContext(ctx).Aggregate(rime.Count[Device]())
			return err
		},
		"join": func() error {
			_, err := rime.InnerJoinOn(tab.WithContext(ctx).In(nil), site, tab.WithContext(ctx).In(nil), site)
			return err
		},
		"projection": func() error {
			_, err := rime.ProjectContext(ctx, tab.Where(), func(d *Device) string { return d.ID })
			return err
		},
		"bulk": func() error { return tab.WithContext(ctx).UpsertMany([]*Device{{ID: "new", Hostname: "new"}}) },
	}
	for name, fn := range checks {
		t.Run(name, func(t *testing.T) {
			if err := fn(); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled operation: %v", err)
			}
		})
	}
	if result := db.GCContext(ctx); result.Reclaimed != 0 {
		t.Fatalf("canceled GC: %+v", result)
	}
	mid, cancelMid := context.WithCancel(context.Background())
	_, err := rime.ProjectContext(mid, tab.Where(), func(d *Device) string { cancelMid(); return d.ID })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("projection cancellation during mapping: %v", err)
	}
	bulk, cancelBulk := context.WithCancel(context.Background())
	tab.BeforeInsert(func(d *Device) error {
		if d.ID == "bulk" {
			cancelBulk()
		}
		return nil
	})
	err = tab.WithContext(bulk).UpsertMany([]*Device{{ID: "bulk", Hostname: "bulk"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("bulk canceled before commit: %v", err)
	}
	if _, err := tab.Get("bulk"); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("canceled bulk published: %v", err)
	}
	for _, limit := range []int{1, 3} {
		t.Run(fmt.Sprintf("limits%d", limit), func(t *testing.T) {
			limited, table := openDevices(t, rime.WithMaxResults(limit), rime.WithMaxScan(limit), rime.WithMaxMutations(limit))
			defer limited.Close()
			for i := 0; i < limit; i++ {
				mustSave(t, table, Device{ID: fmt.Sprint(i), Hostname: fmt.Sprint(i)})
			}
			if rows, err := table.Where().Find(); err != nil || len(rows) != limit {
				t.Fatalf("exact boundary: %d %v", len(rows), err)
			}
			mustSave(t, table, Device{ID: "extra", Hostname: "extra"})
			if _, err := table.Where().Find(); !errors.Is(err, rime.ErrLimitExceeded) {
				t.Fatalf("scan over boundary: %v", err)
			}
			records := make([]*Device, limit+1)
			for i := range records {
				records[i] = &Device{ID: fmt.Sprintf("new%d", i), Hostname: fmt.Sprintf("new%d", i)}
			}
			if err := table.UpsertMany(records); !errors.Is(err, rime.ErrLimitExceeded) {
				t.Fatalf("bulk over boundary: %v", err)
			}
			for _, d := range records {
				if _, err := table.Get(d.ID); !errors.Is(err, rime.ErrNotFound) {
					t.Fatalf("partial limited bulk: %v", err)
				}
			}
		})
	}
}

type qualificationDeep struct {
	ID     string `rime:"primary"`
	Tags   []string
	Values map[string]int
	Nested *int
}

func (d *qualificationDeep) Clone() *qualificationDeep {
	out := *d
	out.Tags = append([]string(nil), d.Tags...)
	out.Values = map[string]int{}
	for k, v := range d.Values {
		out.Values[k] = v
	}
	if d.Nested != nil {
		n := *d.Nested
		out.Nested = &n
	}
	return &out
}
func TestQualificationDeepImmutability(t *testing.T) {
	db := rime.New()
	defer db.Close()
	tab, err := rime.Register[qualificationDeep](db)
	if err != nil {
		t.Fatal(err)
	}
	n := 1
	input := &qualificationDeep{ID: "a", Tags: []string{"old"}, Values: map[string]int{"x": 1}, Nested: &n}
	if err := tab.Upsert(input); err != nil {
		t.Fatal(err)
	}
	tx := db.ReadTx()
	defer tx.Close()
	old, err := tab.In(tx).Get("a")
	if err != nil {
		t.Fatal(err)
	}
	input.Tags[0] = "caller"
	input.Values["x"] = 99
	*input.Nested = 99
	mutate := func(d *qualificationDeep) error { d.Tags[0] = "new"; d.Values["x"] = 2; *d.Nested = 2; return nil }
	if err := tab.Update("a", mutate); err != nil {
		t.Fatal(err)
	}
	abort := errors.New("abort")
	err = db.WriteTx(func(tx *rime.Tx) error {
		if err := tab.In(tx).Update("a", func(d *qualificationDeep) error { d.Tags[0] = "aborted"; d.Values["x"] = 3; *d.Nested = 3; return nil }); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	db.GC()
	again, err := tab.In(tx).Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if old != again || old.Tags[0] != "old" || old.Values["x"] != 1 || *old.Nested != 1 {
		t.Fatalf("old pointer mutated: %+v", old)
	}
	fresh, err := tab.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Tags[0] != "new" || fresh.Values["x"] != 2 || *fresh.Nested != 2 {
		t.Fatalf("abort/input changed published head: %+v", fresh)
	}
}

func TestQualificationJoinAndGroup(t *testing.T) {
	db, tab := openDevices(t)
	defer db.Close()
	other, err := rime.Register[Device](db, rime.WithTableName[Device]("right"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		mustSave(t, tab, Device{ID: fmt.Sprint(i), Hostname: fmt.Sprint(i), Site: fmt.Sprint(i % 3)})
		if i < 4 {
			mustSave(t, other, Device{ID: fmt.Sprint(i), Hostname: fmt.Sprint(i), Site: fmt.Sprint(i % 2)})
		}
	}
	leftSite := rime.SF[Device](tab, "Site")
	rightSite := rime.SF[Device](other, "Site")
	snap := db.ReadTx()
	defer snap.Close()
	check := func(tx *rime.Tx) {
		left, err := tab.Where().In(tx).Find()
		if err != nil {
			t.Fatal(err)
		}
		right, err := other.Where().In(tx).Find()
		if err != nil {
			t.Fatal(err)
		}
		expected := map[string]bool{}
		expectedLeft := map[string]bool{}
		for _, l := range left {
			hit := false
			for _, r := range right {
				if l.Site == r.Site {
					key := l.ID + ":" + r.ID
					expected[key] = true
					expectedLeft[key] = true
					hit = true
				}
			}
			if !hit {
				expectedLeft[l.ID+":null"] = true
			}
		}
		indexed, err := rime.InnerJoinOn(tab.In(tx), leftSite, other.In(tx), rightSite)
		if err != nil {
			t.Fatal(err)
		}
		hashed, err := rime.InnerJoin(tab.In(tx), other.In(tx), func(d *Device) string { return d.Site }, func(d *Device) string { return d.Site })
		if err != nil {
			t.Fatal(err)
		}
		outer, err := rime.LeftJoinOn(tab.In(tx), leftSite, other.In(tx), rightSite)
		if err != nil {
			t.Fatal(err)
		}
		for name, rows := range map[string][]rime.JoinRow[Device, Device]{"indexed": indexed, "hash": hashed, "left": outer} {
			want := expected
			if name == "left" {
				want = expectedLeft
			}
			seen := map[string]bool{}
			for _, pair := range rows {
				key := pair.Left.ID + ":null"
				if pair.Right != nil {
					key = pair.Left.ID + ":" + pair.Right.ID
				}
				if !want[key] || seen[key] {
					t.Fatalf("%s unexpected pair %s", name, key)
				}
				seen[key] = true
			}
			if len(seen) != len(want) {
				t.Fatalf("%s pairs %d want %d", name, len(seen), len(want))
			}
		}
	}
	check(nil)
	if _, err := other.Where().Update(func(d *Device) error { d.Site = "2"; return nil }); err != nil {
		t.Fatal(err)
	}
	db.GC()
	check(snap)
	check(nil)
	// A filter can run user code while commits change the probe index.
	var once sync.Once
	joined, err := rime.InnerJoinOn(tab.In(nil), leftSite, other.In(nil), rightSite, func(_, _ *Device) bool {
		once.Do(func() {
			if _, err := other.Where().Update(func(d *Device) error { d.Site = "99"; return nil }); err != nil {
				t.Error(err)
			}
		})
		return true
	})
	if err != nil || len(joined) != 8 {
		t.Fatalf("join lost snapshot during probe invalidation: %d %v", len(joined), err)
	}
	// These tuples collided when grouping used delimiter concatenation.
	type grouped struct {
		ID   string `rime:"primary"`
		A, B string
	}
	groups, err := rime.Register[grouped](db)
	if err != nil {
		t.Fatal(err)
	}
	if err := groups.UpsertMany([]*grouped{{ID: "a", A: "x\x1fstring\x00y", B: "z"}, {ID: "b", A: "x", B: "y\x1fstring\x00z"}}); err != nil {
		t.Fatal(err)
	}
	rows, err := groups.Where().GroupBy(rime.SF[grouped](groups, "A"), rime.SF[grouped](groups, "B")).Aggregate(rime.Count[grouped]())
	if err != nil || len(rows) != 2 {
		t.Fatalf("group keys collided: %+v %v", rows, err)
	}
}
