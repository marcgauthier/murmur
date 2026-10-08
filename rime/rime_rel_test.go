package rime_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

type Site struct {
	ID     string `rime:"primary"`
	Region string `rime:"index"`
}

func TestJoins(t *testing.T) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	sites, err := rime.Register[Site](db, rime.WithTableName[Site]("sites"))
	if err != nil {
		t.Fatal(err)
	}
	mustSave(t, dev, Device{ID: "d1", Hostname: "h1", Site: "s1", Latency: 5})
	mustSave(t, dev, Device{ID: "d2", Hostname: "h2", Site: "s2", Latency: 50})
	mustSave(t, dev, Device{ID: "d9", Hostname: "h9", Site: "sx", Latency: 1})
	if err := sites.Upsert(&Site{ID: "s1", Region: "East"}); err != nil {
		t.Fatal(err)
	}
	if err := sites.Upsert(&Site{ID: "s2", Region: "West"}); err != nil {
		t.Fatal(err)
	}
	dsite := rime.SF[Device](dev, "Site")
	sid := rime.SF[Site](sites, "ID")

	inner, err := rime.InnerJoinOn(dev.In(nil), dsite, sites.In(nil), sid)
	if err != nil {
		t.Fatal(err)
	}
	if len(inner) != 2 {
		t.Fatalf("inner join: %d", len(inner))
	}
	// Filtered join: East region only.
	sregion := rime.SF[Site](sites, "Region")
	_ = sregion
	east, err := rime.InnerJoinOn(dev.In(nil), dsite, sites.In(nil), sid,
		func(d *Device, s *Site) bool { return s.Region == "East" })
	if err != nil {
		t.Fatal(err)
	}
	if len(east) != 1 || east[0].Left.ID != "d1" {
		t.Fatalf("filtered join: %+v", east)
	}
	// Left join keeps the orphan.
	left, err := rime.LeftJoinOn(dev.In(nil), dsite, sites.In(nil), sid)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 3 {
		t.Fatalf("left join: %d", len(left))
	}
	orphans := 0
	for _, r := range left {
		if r.Right == nil {
			orphans++
		}
	}
	if orphans != 1 {
		t.Fatalf("orphans: %d", orphans)
	}
	// Self join.
	did := rime.SF[Device](dev, "ID")
	self, err := rime.InnerJoinOn(dev.In(nil), did, dev.In(nil), did)
	if err != nil {
		t.Fatal(err)
	}
	if len(self) != 3 {
		t.Fatalf("self join: %d", len(self))
	}
}

func TestAggregates(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 30)
	site := rime.SF[Device](dev, "Site")
	lat := rime.OF[Device, int](dev, "Latency")

	n, err := dev.Where(site.Eq("OTT")).Count()
	if err != nil || n != 10 {
		t.Fatalf("count: %d %v", n, err)
	}
	vals, err := dev.Where().Aggregate(rime.Count[Device](), rime.SumOf(lat), rime.AvgOf(lat))
	if err != nil {
		t.Fatal(err)
	}
	if vals[0].(int) != 30 {
		t.Fatalf("count agg: %v", vals[0])
	}
	// Latencies 0..29 twice? seed uses i%200 over 30 rows: 0..29.
	if vals[1].(float64) != 435 {
		t.Fatalf("sum: %v", vals[1])
	}
	if vals[2].(float64) != 14.5 {
		t.Fatalf("avg: %v", vals[2])
	}
	mm, err := dev.Where().Aggregate(rime.MinOf(lat), rime.MaxOf(lat))
	if err != nil {
		t.Fatal(err)
	}
	if mm[0].(int) != 0 || mm[1].(int) != 29 {
		t.Fatalf("min/max: %v", mm)
	}
	// Group by site.
	groups, err := dev.Where().GroupBy(site).Aggregate(rime.Count[Device](), rime.AvgOf(lat))
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 3 {
		t.Fatalf("groups: %d", len(groups))
	}
	for _, g := range groups {
		if g.Values[0].(int) != 10 {
			t.Fatalf("group count: %+v", g)
		}
	}
}

func TestProjection(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 5)
	type Status struct {
		Hostname string
		Status   int
	}
	id := rime.F[Device, string](dev, "ID")
	rows, err := rime.Project(dev.Where().OrderByAsc(id).Limit(5), func(d *Device) Status {
		return Status{Hostname: d.Hostname, Status: d.Status}
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 || rows[0].Hostname != "host-0000" {
		t.Fatalf("projection: %+v", rows)
	}
}

func TestHooks(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	var mu sync.Mutex
	var log []string
	appendLog := func(s string) {
		mu.Lock()
		log = append(log, s)
		mu.Unlock()
	}
	dev.BeforeInsert(func(d *Device) error {
		if d.ID == "bad" {
			return errors.New("rejected")
		}
		appendLog("before-insert")
		return nil
	})
	dev.AfterInsert(func(d *Device) { appendLog("after-insert:" + d.ID) })
	dev.BeforeUpdate(func(o, n *Device) error { appendLog("before-update"); return nil })
	dev.AfterUpdate(func(o, n *Device) { appendLog("after-update") })
	dev.BeforeDelete(func(d *Device) error { appendLog("before-delete"); return nil })
	dev.AfterDelete(func(d *Device) { appendLog("after-delete") })
	dev.AfterSave(func(c rime.Change[Device]) { appendLog("save:" + c.Operation.String()) })
	var commits []rime.TxID
	dev.AfterCommit(func(id rime.TxID) { commits = append(commits, id) })

	if err := dev.Insert(&Device{ID: "bad", Hostname: "x"}); err == nil {
		t.Fatal("want hook rejection")
	}
	mustSave(t, dev, Device{ID: "a", Hostname: "h-a"})
	if err := dev.Update("a", func(d *Device) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := dev.Delete("a"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"before-insert", "after-insert:a", "save:insert",
		"before-update", "after-update", "save:update",
		"before-delete", "after-delete", "save:delete",
	}
	if len(log) != len(want) {
		t.Fatalf("hook log:\n%v", log)
	}
	for i := range want {
		if log[i] != want[i] {
			t.Fatalf("hook order:\n%v", log)
		}
	}
	if len(commits) != 3 {
		t.Fatalf("aftercommit: %v", commits)
	}
}

func TestEvents(t *testing.T) {
	db := rime.New(rime.WithEventQueueSize(64))
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var got []string
	var wg sync.WaitGroup
	wg.Add(3)
	dev.OnInserted(func(d *Device) {
		mu.Lock()
		got = append(got, "ins:"+d.ID)
		mu.Unlock()
		wg.Done()
	})
	dev.OnUpdated(func(o, n *Device) {
		mu.Lock()
		got = append(got, "upd")
		mu.Unlock()
		wg.Done()
	})
	dev.OnDeleted(func(d *Device) {
		mu.Lock()
		got = append(got, "del")
		mu.Unlock()
		wg.Done()
	})
	mustSave(t, dev, Device{ID: "a", Hostname: "h-a"})
	if err := dev.Update("a", func(d *Device) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := dev.Delete("a"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("events not delivered")
	}
}

func TestConstraints(t *testing.T) {
	db := rime.New(rime.WithForeignKeys(true))
	defer db.Close()
	type Owner struct {
		ID   string                `rime:"primary"`
		Name rime.Optional[string] `rime:"notnull"`
	}
	type Pet struct {
		ID    string `rime:"primary"`
		Owner string `rime:"fk:owners.ID"`
	}
	owners, err := rime.Register[Owner](db, rime.WithTableName[Owner]("owners"))
	if err != nil {
		t.Fatal(err)
	}
	pets, err := rime.Register[Pet](db, rime.WithTableName[Pet]("pets"))
	if err != nil {
		t.Fatal(err)
	}
	owners.AddCheck(func(o *Owner) error {
		if len(o.Name.Value) < 2 {
			return errors.New("name too short")
		}
		return nil
	})
	if err := owners.Upsert(&Owner{ID: "o1"}); !errors.Is(err, rime.ErrNotNull) {
		t.Fatalf("notnull: %v", err)
	}
	if err := owners.Upsert(&Owner{ID: "o1", Name: rime.Some("x")}); !errors.Is(err, rime.ErrCheck) {
		t.Fatalf("check: %v", err)
	}
	if err := pets.Upsert(&Pet{ID: "p1", Owner: "ghost"}); !errors.Is(err, rime.ErrForeignKey) {
		t.Fatalf("fk: %v", err)
	}
	if err := owners.Upsert(&Owner{ID: "o1", Name: rime.Some("ann")}); err != nil {
		t.Fatal(err)
	}
	if err := pets.Upsert(&Pet{ID: "p1", Owner: "o1"}); err != nil {
		t.Fatal(err)
	}
	pets.SetForeignKeys(false)
	if err := pets.Upsert(&Pet{ID: "p2", Owner: "ghost"}); err != nil {
		t.Fatalf("disabled fk should pass: %v", err)
	}
}

func TestDefaultsAndUUID(t *testing.T) {
	db := rime.New()
	defer db.Close()
	type Sess struct {
		ID     rime.UUID           `rime:"primary,uuid5"`
		User   string              `rime:"index"`
		Active rime.Optional[bool] `rime:"default=true"`
	}
	sess, err := rime.Register[Sess](db)
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.Upsert(&Sess{User: "u1"}); err != nil {
		t.Fatal(err)
	}
	rows, err := sess.Where().Find()
	if err != nil || len(rows) != 1 {
		t.Fatal(err, rows)
	}
	if rows[0].ID.IsZero() {
		t.Fatal("uuid5 not generated")
	}
	if !rows[0].Active.Present || !rows[0].Active.Value {
		t.Fatal("default not applied")
	}
	if err := sess.Upsert(&Sess{User: "u2", Active: rime.Some(false)}); err != nil {
		t.Fatal(err)
	}
	second, err := sess.Where(rime.StringFieldOf[Sess](sess, "User").Eq("u2")).First()
	if err != nil || !second.Active.Present || second.Active.Value {
		t.Fatalf("explicit false replaced by default: %+v %v", second, err)
	}
	ns := rime.TableNamespace("x")
	a := rime.NewUUIDv5(ns, "hello")
	b := rime.NewUUIDv5(ns, "hello")
	if a != b {
		t.Fatal("uuid5 not deterministic")
	}
	if _, err := rime.ParseUUID(a.String()); err != nil {
		t.Fatal(err)
	}
}

type deepRec struct {
	ID   string `rime:"primary"`
	Tags []string
}

func (d *deepRec) Clone() *deepRec {
	out := *d
	out.Tags = append([]string(nil), d.Tags...)
	return &out
}

func TestDeepCopyCloner(t *testing.T) {
	db := rime.New()
	defer db.Close()
	tab, err := rime.Register[deepRec](db)
	if err != nil {
		t.Fatal(err)
	}
	if err := tab.Upsert(&deepRec{ID: "a", Tags: []string{"x"}}); err != nil {
		t.Fatal(err)
	}
	if err := tab.Update("a", func(d *deepRec) error {
		d.Tags[0] = "MUT"
		d.Tags = append(d.Tags, "y")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	tx := db.ReadTx()
	defer tx.Close()
	_ = tx
	// Head must reflect the mutation...
	head, _ := tab.Get("a")
	if len(head.Tags) != 2 || head.Tags[0] != "MUT" {
		t.Fatalf("head wrong: %+v", head.Tags)
	}
}

func TestStatsAndLimits(t *testing.T) {
	db := rime.New(rime.WithMaxResults(5), rime.WithMaxMutations(3))
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		mustSave(t, dev, Device{ID: string(rune('a' + i)), Hostname: string(rune('A' + i))})
	}
	st := db.Stats()
	if st.Tables != 1 || st.Records != 10 || st.ActiveTxns != 0 {
		t.Fatalf("stats: %+v", st)
	}
	if _, err := dev.Where().Find(); !errors.Is(err, rime.ErrLimitExceeded) {
		t.Fatalf("maxresults: %v", err)
	}
	err = db.WriteTx(func(tx *rime.Tx) error {
		for i := 0; i < 5; i++ {
			if err := dev.In(tx).Upsert(&Device{ID: string(rune('a'+i)) + "x", Hostname: string(rune('a'+i)) + "hx"}); err != nil {
				return err
			}
		}
		return nil
	})
	if !errors.Is(err, rime.ErrLimitExceeded) {
		t.Fatalf("maxmutations: %v", err)
	}
}

func TestConcurrentMixed(t *testing.T) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	seedDevices(t, dev, 200)
	var ops atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				id := string(rune('a'+g)) + "-" + string(rune('0'+i%10))
				if err := dev.Upsert(&Device{ID: id, Hostname: id + "-h", Site: "OTT", Status: i % 3}); err != nil {
					t.Error(err)
					return
				}
				if _, err := dev.Get(id); err != nil {
					t.Error(err)
					return
				}
				if _, err := dev.Where(rime.SF[Device](dev, "Site").Eq("OTT")).Limit(10).Find(); err != nil {
					t.Error(err)
					return
				}
				ops.Add(1)
			}
		}(g)
	}
	wg.Wait()
	if ops.Load() != 1600 {
		t.Fatalf("ops: %d", ops.Load())
	}
	if res := db.GC(); res.Reclaimed < 0 {
		t.Fatal("gc negative")
	}
}

// TestAggregateFindEquivalence differentials Aggregate against manual
// computation over the same query's Find rows across filter, plan, limit,
// offset, and ordering variants.
func TestAggregateFindEquivalence(t *testing.T) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	site := rime.SF[Device](dev, "Site")
	status := rime.OF[Device, int](dev, "Status")
	lat := rime.OF[Device, int](dev, "Latency")
	sites := []string{"OTT", "MTL", "WPG"}
	for i := 0; i < 60; i++ {
		mustSave(t, dev, Device{
			ID:       string(rune('a'+i/26)) + string(rune('a'+i%26)) + "x",
			Hostname: "h" + string(rune('a'+i)),
			Site:     sites[i%3],
			Status:   i % 4,
			Latency:  (i*37 + 11) % 101,
		})
	}
	filters := map[string][]rime.Expr[Device]{
		"none":    nil,
		"hash":    {site.Eq("OTT")},
		"ordered": {lat.Gt(50)},
		"multi":   {rime.And(status.Eq(1), lat.Le(60))},
		"nomatch": {site.Eq("ZZZ")},
	}
	type mod struct {
		name  string
		apply func(q *rime.Query[Device]) *rime.Query[Device]
		// exact=false for unordered Limit/Offset: the winning subset is
		// map-order dependent, so only the count is deterministic.
		exact bool
	}
	mods := []mod{
		{"plain", func(q *rime.Query[Device]) *rime.Query[Device] { return q }, true},
		{"limit", func(q *rime.Query[Device]) *rime.Query[Device] { return q.Limit(5) }, false},
		{"offset", func(q *rime.Query[Device]) *rime.Query[Device] { return q.Offset(7) }, false},
		{"both", func(q *rime.Query[Device]) *rime.Query[Device] { return q.Offset(3).Limit(9) }, false},
		{"zero", func(q *rime.Query[Device]) *rime.Query[Device] { return q.Limit(0) }, true},
		{"order", func(q *rime.Query[Device]) *rime.Query[Device] { return q.OrderByAsc(lat) }, true},
		{"orderlim", func(q *rime.Query[Device]) *rime.Query[Device] {
			return q.OrderByDescending(lat).Limit(11)
		}, true},
	}
	aggs := []rime.Agg[Device]{rime.Count[Device](), rime.SumOf(lat), rime.AvgOf(lat), rime.MinOf(lat), rime.MaxOf(lat)}
	for fname, f := range filters {
		for _, m := range mods {
			q := dev.Where(f...)
			rows, err := m.apply(q).Find()
			if err != nil {
				t.Fatalf("%s/%s find: %v", fname, m.name, err)
			}
			// Independent oracle over the Find rows.
			var sum float64
			min, max := 0, 0
			for i, r := range rows {
				sum += float64(r.Latency)
				if i == 0 || r.Latency < min {
					min = r.Latency
				}
				if i == 0 || r.Latency > max {
					max = r.Latency
				}
			}
			avg := 0.0
			if len(rows) > 0 {
				avg = sum / float64(len(rows))
			}
			got, err := m.apply(dev.Where(f...)).Aggregate(aggs...)
			if err != nil {
				t.Fatalf("%s/%s aggregate: %v", fname, m.name, err)
			}
			if got[0] != len(rows) {
				t.Errorf("%s/%s count: got %v want %d", fname, m.name, got[0], len(rows))
			}
			if !m.exact {
				continue
			}
			if got[1] != sum {
				t.Errorf("%s/%s sum: got %v want %v", fname, m.name, got[1], sum)
			}
			if got[2] != avg {
				t.Errorf("%s/%s avg: got %v want %v", fname, m.name, got[2], avg)
			}
			wantMin, wantMax := any(nil), any(nil)
			if len(rows) > 0 {
				wantMin, wantMax = min, max
			}
			if got[3] != wantMin {
				t.Errorf("%s/%s min: got %v want %v", fname, m.name, got[3], wantMin)
			}
			if got[4] != wantMax {
				t.Errorf("%s/%s max: got %v want %v", fname, m.name, got[4], wantMax)
			}
		}
	}
}

// TestIndexedJoinRepeatKeys guards the indexed-join match cache: repeated and
// alternating keys must reuse match sets while pair filters still evaluate
// per row and outer joins still emit unmatched rows.
func TestIndexedJoinRepeatKeys(t *testing.T) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	sites, err := rime.Register[Site](db, rime.WithTableName[Site]("sites"))
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []Site{{ID: "s1", Region: "East"}, {ID: "s2", Region: "West"}} {
		if err := sites.Upsert(&s); err != nil {
			t.Fatal(err)
		}
	}
	// Alternating keys: s1,s2,s1,s2,s1 plus an unmatched key.
	keys := []string{"s1", "s2", "s1", "s2", "s1", "sx"}
	for i, k := range keys {
		mustSave(t, dev, Device{ID: "d" + string(rune('0'+i)), Hostname: "h" + string(rune('0'+i)), Site: k, Latency: i})
	}
	dsite := rime.SF[Device](dev, "Site")
	sid := rime.SF[Site](sites, "ID")
	// Filter accepts only even-latency pairs: same key, different rows,
	// different outcomes — the cache must not memoize filter results.
	even := func(d *Device, _ *Site) bool { return d.Latency%2 == 0 }
	rows, err := rime.InnerJoinOn(dev.In(nil), dsite, sites.In(nil), sid, even)
	if err != nil {
		t.Fatal(err)
	}
	// Even latencies: rows 0(s1),2(s1),4(s1); row 5(sx) unmatched.
	if len(rows) != 3 {
		t.Fatalf("inner rows=%d want 3", len(rows))
	}
	for _, r := range rows {
		if r.Right == nil || r.Left.Site != r.Right.ID {
			t.Fatalf("mismatched pair %+v", r)
		}
		if r.Left.Latency%2 != 0 {
			t.Fatalf("filter bypassed: %+v", r.Left)
		}
	}
	out, err := rime.LeftJoinOn(dev.In(nil), dsite, sites.In(nil), sid)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 6 {
		t.Fatalf("outer rows=%d want 6", len(out))
	}
	nilRight := 0
	for _, r := range out {
		if r.Right == nil {
			nilRight++
			if r.Left.Site != "sx" {
				t.Fatalf("unexpected null extended row %+v", r.Left)
			}
		}
	}
	if nilRight != 1 {
		t.Fatalf("null-extended=%d want 1", nilRight)
	}
}

type joinShapeL struct {
	ID string    `rime:"primary"`
	I  int       `rime:"index"`
	S  string    `rime:"index"`
	U  uint      `rime:"index"`
	F  float32   `rime:"index"`
	B  bool      `rime:"index"`
	N  joinNamed `rime:"index"`
}

type joinShapeR struct {
	ID string    `rime:"primary"`
	I  int       `rime:"index"`
	S  string    `rime:"unique"`
	U  uint      `rime:"index"`
	F  float32   `rime:"index"`
	B  bool      `rime:"index"`
	N  joinNamed `rime:"index"`
}

type joinNamed int

// TestIndexedJoinProbeShapes joins every index kind through the typed probe
// (unique + hash, single + fan-out buckets) and named keys through the
// scratch fallback, verifying exact pair sets.
func TestIndexedJoinProbeShapes(t *testing.T) {
	db := rime.New()
	defer db.Close()
	left, err := rime.Register[joinShapeL](db, rime.WithTableName[joinShapeL]("jl"))
	if err != nil {
		t.Fatal(err)
	}
	right, err := rime.Register[joinShapeR](db, rime.WithTableName[joinShapeR]("jr"))
	if err != nil {
		t.Fatal(err)
	}
	lrows := []joinShapeL{
		{ID: "l1", I: 7, S: "a", U: 1, F: 1.5, B: true, N: 3},
		{ID: "l2", I: 7, S: "b", U: 2, F: 2.5, B: false, N: 4},
		{ID: "l3", I: 8, S: "zzz", U: 99, F: 9.5, B: true, N: 99},
	}
	rrows := []joinShapeR{
		{ID: "r1", I: 7, S: "a", U: 1, F: 1.5, B: true, N: 3},
		{ID: "r2", I: 7, S: "b", U: 1, F: 1.5, B: true, N: 3},
		{ID: "r3", I: 8, S: "c", U: 2, F: 2.5, B: false, N: 4},
	}
	for i := range lrows {
		if err := left.Upsert(&lrows[i]); err != nil {
			t.Fatal(err)
		}
	}
	for i := range rrows {
		if err := right.Upsert(&rrows[i]); err != nil {
			t.Fatal(err)
		}
	}
	check := func(name string, pairs int, fn func() (int, error)) {
		t.Helper()
		n, err := fn()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if n != pairs {
			t.Fatalf("%s: pairs=%d want %d", name, n, pairs)
		}
	}
	// int hash fan-out: l1,l2 x r1,r2 (key 7) + l3 x r3 (key 8) = 5.
	li := rime.F[joinShapeL, int](left, "I")
	ri := rime.F[joinShapeR, int](right, "I")
	check("int", 5, func() (int, error) {
		rows, err := rime.InnerJoinOn(left.In(nil), li, right.In(nil), ri)
		if err != nil {
			return 0, err
		}
		for _, r := range rows {
			if r.Left.I != r.Right.I {
				t.Fatalf("int mismatch %+v", r)
			}
		}
		return len(rows), nil
	})
	// string unique 1:1 on "a","b"; l3 misses. (S is unique on right.)
	ls := rime.SF[joinShapeL](left, "S")
	rs := rime.SF[joinShapeR](right, "S")
	check("string", 2, func() (int, error) {
		rows, err := rime.InnerJoinOn(left.In(nil), ls, right.In(nil), rs)
		if err != nil {
			return 0, err
		}
		for _, r := range rows {
			if r.Left.S != r.Right.S {
				t.Fatalf("string mismatch %+v", r)
			}
		}
		return len(rows), nil
	})
	// uint: l1 x r1,r2 (1) + l2 x r3 (2) = 3.
	lu := rime.F[joinShapeL, uint](left, "U")
	ru := rime.F[joinShapeR, uint](right, "U")
	check("uint", 3, func() (int, error) {
		rows, err := rime.InnerJoinOn(left.In(nil), lu, right.In(nil), ru)
		if err != nil {
			return 0, err
		}
		for _, r := range rows {
			if r.Left.U != r.Right.U {
				t.Fatalf("uint mismatch %+v", r)
			}
		}
		return len(rows), nil
	})
	// float32: l1 x r1,r2 (1.5) + l2 x r3 (2.5) = 3.
	lf := rime.OF[joinShapeL, float32](left, "F")
	rf := rime.OF[joinShapeR, float32](right, "F")
	check("float", 3, func() (int, error) {
		rows, err := rime.InnerJoinOn(left.In(nil), lf, right.In(nil), rf)
		if err != nil {
			return 0, err
		}
		return len(rows), nil
	})
	// bool: true: l1,l3 x r1,r2 = 4; false: l2 x r3 = 1; total 5.
	lb := rime.BF[joinShapeL](left, "B")
	rb := rime.BF[joinShapeR](right, "B")
	check("bool", 5, func() (int, error) {
		rows, err := rime.InnerJoinOn(left.In(nil), lb, right.In(nil), rb)
		if err != nil {
			return 0, err
		}
		return len(rows), nil
	})
	// named int via scratch fallback: l1 x r1,r2 (3) + l2 x r3 (4) = 3.
	ln := rime.F[joinShapeL, joinNamed](left, "N")
	rn := rime.F[joinShapeR, joinNamed](right, "N")
	check("named", 3, func() (int, error) {
		rows, err := rime.InnerJoinOn(left.In(nil), ln, right.In(nil), rn)
		if err != nil {
			return 0, err
		}
		for _, r := range rows {
			if r.Left.N != r.Right.N {
				t.Fatalf("named mismatch %+v", r)
			}
		}
		return len(rows), nil
	})
}

// TestJoinCanceledContextDeterministic pins join cancellation determinism:
// with one CPU, the async AfterFunc propagation can never outrun the join
// body, so only a synchronous pre-check makes pre-canceled joins fail.
// (No test in this package uses t.Parallel, so juggling GOMAXPROCS is safe.)
func TestJoinCanceledContextDeterministic(t *testing.T) {
	prev := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(prev)
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		mustSave(t, dev, Device{ID: fmt.Sprintf("c%d", i), Hostname: fmt.Sprintf("ch%d", i), Site: []string{"OTT", "MTL"}[i%2]})
	}
	site := rime.SF[Device](dev, "Site")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 200; i++ {
		if _, err := rime.InnerJoinOn(dev.WithContext(ctx).In(nil), site, dev.WithContext(ctx).In(nil), site); !errors.Is(err, context.Canceled) {
			t.Fatalf("iter %d: pre-canceled join returned %v", i, err)
		}
		if _, err := rime.LeftJoinOn(dev.WithContext(ctx).In(nil), site, dev.WithContext(ctx).In(nil), site); !errors.Is(err, context.Canceled) {
			t.Fatalf("iter %d: pre-canceled left join returned %v", i, err)
		}
	}
}
