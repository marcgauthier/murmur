package rime_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

// TestModelEquivalence is the deterministic Phase 1 gate: a seeded workload
// over two tables checked against an in-test reference model after every
// transaction, inside the transaction (staged overlay) and after commit.
// It covers point reads, filtered/indexed/compound reads, exact OrderBy +
// Limit/Offset sequences, inner/left/filtered joins, aggregates, group-by,
// rollback, hook streams, and cancellation. Seeds are fixed, so failures
// reproduce exactly; fuzz targets explore beyond these streams.
func TestModelEquivalence(t *testing.T) {
	for _, seed := range []int64{1, 7, 42} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			runEquivalence(t, seed)
		})
	}
}

// FuzzEquivalence explores workloads beyond the fixed deterministic seeds by
// running the same reference-model differential with a fuzzer-chosen seed.
// Each input is one reproducible workload: the first eight bytes are the
// stream seed, and every operation choice derives from it.
func FuzzEquivalence(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{9, 9, 9, 9, 9, 9, 9, 9})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 8 {
			t.Skip("short input")
		}
		var seed int64
		for i := 0; i < 8; i++ {
			seed |= int64(data[i]) << (8 * i)
		}
		runEquivalenceN(t, seed, 16)
	})
}

type eqHook struct {
	op  rime.Operation
	key string
	old *Device
	new *Device
}

type eqWorld struct {
	t         *testing.T
	db        *rime.DB
	dev       *rime.Table[Device]
	sites     *rime.Table[Site]
	rng       *rand.Rand
	model     map[string]Device
	siteModel map[string]Site
	latCtr    int
	hooks     []eqHook
	beforeIns int
	beforeUpd int
	beforeDel int
	expBefIns int
	expBefUpd int
	expBefDel int
	devCommit int
	siteTouch int
	expDevC   int
	expSiteC  int
}

func newEqWorld(t *testing.T, seed int64) *eqWorld {
	t.Helper()
	db, dev := openDevices(t)
	sites, err := rime.Register[Site](db, rime.WithTableName[Site]("sites"))
	if err != nil {
		t.Fatal(err)
	}
	w := &eqWorld{
		t: t, db: db, dev: dev, sites: sites,
		rng:       rand.New(rand.NewSource(seed)),
		model:     map[string]Device{},
		siteModel: map[string]Site{},
	}
	dev.AfterSave(func(c rime.Change[Device]) {
		var old, newv *Device
		if c.Old != nil {
			cp := *c.Old
			old = &cp
		}
		if c.New != nil {
			cp := *c.New
			newv = &cp
		}
		key := ""
		if newv != nil {
			key = newv.ID
		} else {
			key = old.ID
		}
		w.hooks = append(w.hooks, eqHook{op: c.Operation, key: key, old: old, new: newv})
	})
	dev.BeforeInsert(func(*Device) error { w.beforeIns++; return nil })
	dev.BeforeUpdate(func(old, _ *Device) error { w.beforeUpd++; return nil })
	dev.BeforeDelete(func(*Device) error { w.beforeDel++; return nil })
	dev.BeforeCommit(func(*rime.Tx) error { w.devCommit++; return nil })
	sites.BeforeCommit(func(*rime.Tx) error { w.siteTouch++; return nil })
	t.Cleanup(func() { db.Close() })
	return w
}

func runEquivalence(t *testing.T, seed int64) {
	runEquivalenceN(t, seed, 60)
}

func runEquivalenceN(t *testing.T, seed int64, txns int) {
	w := newEqWorld(t, seed)
	abort := errors.New("equivalence rollback")
	for i := 0; i < txns; i++ {
		rollback := i%5 == 4
		staged, stagedSites, wantHooks := w.runTxn(abort, rollback, i)
		if rollback {
			w.verify(nil, w.model, w.siteModel, i, "post-rollback")
			continue
		}
		w.model, w.siteModel = staged, stagedSites
		if len(w.hooks) != len(wantHooks) {
			t.Fatalf("seed txn %d: %d after-hooks, want %d", i, len(w.hooks), len(wantHooks))
		}
		for j := range wantHooks {
			if !eqHookEqual(w.hooks[j], wantHooks[j]) {
				t.Fatalf("seed txn %d hook %d: %+v want %+v", i, j, w.hooks[j], wantHooks[j])
			}
		}
		w.hooks = w.hooks[:0]
		w.verify(nil, w.model, w.siteModel, i, "post-commit")
		if i%7 == 6 {
			w.db.GC()
			w.verify(nil, w.model, w.siteModel, i, "post-gc")
		}
	}
	if w.devCommit != w.expDevC {
		t.Fatalf("dev BeforeCommit fired %d times, want %d", w.devCommit, w.expDevC)
	}
	if w.beforeIns != w.expBefIns || w.beforeUpd != w.expBefUpd || w.beforeDel != w.expBefDel {
		t.Fatalf("before hooks got ins=%d upd=%d del=%d, want %d/%d/%d",
			w.beforeIns, w.beforeUpd, w.beforeDel, w.expBefIns, w.expBefUpd, w.expBefDel)
	}
	if w.siteTouch != w.expSiteC {
		t.Fatalf("site BeforeCommit fired %d times, want %d", w.siteTouch, w.expSiteC)
	}
	w.verifyContexts()
	w.verify(nil, w.model, w.siteModel, txns, "final")
}

func eqHookEqual(a, b eqHook) bool {
	if a.op != b.op || a.key != b.key {
		return false
	}
	if (a.old == nil) != (b.old == nil) || (a.new == nil) != (b.new == nil) {
		return false
	}
	if a.old != nil && *a.old != *b.old {
		return false
	}
	return a.new == nil || *a.new == *b.new
}

// runTxn executes one randomized write transaction, mirroring staged ops into
// a scratch model. It verifies the in-transaction overlay against the scratch
// model, then commits or rolls back. wantHooks holds the expected AfterSave
// stream for committed device ops in staged order.
func (w *eqWorld) runTxn(abort error, rollback bool, txn int) (map[string]Device, map[string]Site, []eqHook) {
	staged := cloneDevices(w.model)
	stagedSites := cloneSites(w.siteModel)
	var wantHooks []eqHook
	sim := cloneDevices(w.model) // sequential-install head simulation
	devTouched, siteTouched := false, false
	err := w.db.WriteTx(func(tx *rime.Tx) error {
		n := 1 + w.rng.Intn(4)
		for k := 0; k < n; k++ {
			if w.deviceOp(tx, staged, sim, &wantHooks) {
				devTouched = true
			}
		}
		if w.rng.Intn(3) == 0 && w.siteOp(tx, stagedSites) {
			siteTouched = true
		}
		w.verify(tx, staged, stagedSites, txn, "overlay")
		if rollback {
			return abort
		}
		return nil
	})
	if rollback {
		if !errors.Is(err, abort) {
			w.t.Fatalf("txn %d rollback: %v", txn, err)
		}
		// Rolled-back staged ops fire Before hooks but no After hooks.
		w.hooks = w.hooks[:0]
		return w.model, w.siteModel, nil
	}
	if err != nil {
		w.t.Fatalf("txn %d: %v", txn, err)
	}
	if devTouched {
		w.expDevC++
	}
	if siteTouched {
		w.expSiteC++
	}
	return staged, stagedSites, wantHooks
}

func (w *eqWorld) deviceOp(tx *rime.Tx, staged, sim map[string]Device, wantHooks *[]eqHook) bool {
	key := fmt.Sprintf("k%d", w.rng.Intn(12))
	bound := w.dev.In(tx)
	switch w.rng.Intn(10) {
	case 0, 1, 2, 3, 4, 5:
		w.latCtr++
		row := Device{
			ID:       key,
			Hostname: "h-" + key,
			Site:     fmt.Sprintf("s%d", w.rng.Intn(5)),
			Status:   w.rng.Intn(5),
			Latency:  w.latCtr,
		}
		_, existed := staged[key]
		if err := bound.Upsert(&row); err != nil {
			w.t.Fatalf("upsert %s: %v", key, err)
		}
		if existed {
			w.expBefUpd++
		} else {
			w.expBefIns++
		}
		staged[key] = row
		op := rime.OpInsert
		if _, ok := sim[key]; ok {
			op = rime.OpUpdate
		}
		*wantHooks = append(*wantHooks, eqHook{op: op, key: key, old: simPtr(sim, key), new: &row})
		sim[key] = row
		return true
	case 6, 7:
		err := bound.Update(key, func(d *Device) error {
			d.Status = (d.Status + 1) % 5
			w.latCtr++
			d.Latency = w.latCtr
			if w.rng.Intn(3) == 0 {
				d.Site = fmt.Sprintf("s%d", w.rng.Intn(5))
			}
			return nil
		})
		_, ok := staged[key]
		if !ok {
			if !errors.Is(err, rime.ErrNotFound) {
				w.t.Fatalf("missing update %s: %v", key, err)
			}
			return false
		}
		if err != nil {
			w.t.Fatalf("update %s: %v", key, err)
		}
		// The engine applied the closure above; re-read the staged row so
		// the model mirrors exactly what was stored.
		got, err := bound.Get(key)
		if err != nil {
			w.t.Fatalf("reread %s: %v", key, err)
		}
		staged[key] = *got
		w.expBefUpd++
		*wantHooks = append(*wantHooks, eqHook{op: rime.OpUpdate, key: key, old: simPtr(sim, key), new: got})
		sim[key] = *got
		return true
	default:
		err := bound.Delete(key)
		if _, ok := staged[key]; ok {
			if err != nil {
				w.t.Fatalf("delete %s: %v", key, err)
			}
			w.expBefDel++
			delete(staged, key)
			*wantHooks = append(*wantHooks, eqHook{op: rime.OpDelete, key: key, old: simPtr(sim, key)})
			delete(sim, key)
			return true
		}
		if !errors.Is(err, rime.ErrNotFound) {
			w.t.Fatalf("missing delete %s: %v", key, err)
		}
		return false
	}
}

func (w *eqWorld) siteOp(tx *rime.Tx, staged map[string]Site) bool {
	id := fmt.Sprintf("s%d", w.rng.Intn(4))
	bound := w.sites.In(tx)
	if w.rng.Intn(4) == 0 {
		err := bound.Delete(id)
		if _, ok := staged[id]; ok {
			if err != nil {
				w.t.Fatalf("site delete %s: %v", id, err)
			}
			delete(staged, id)
			return true
		}
		if !errors.Is(err, rime.ErrNotFound) {
			w.t.Fatalf("site missing delete %s: %v", id, err)
		}
		return false
	}
	row := Site{ID: id, Region: []string{"East", "West"}[w.rng.Intn(2)]}
	if err := bound.Upsert(&row); err != nil {
		w.t.Fatalf("site upsert %s: %v", id, err)
	}
	staged[id] = row
	return true
}

func simPtr(m map[string]Device, key string) *Device {
	if d, ok := m[key]; ok {
		cp := d
		return &cp
	}
	return nil
}

func cloneDevices(m map[string]Device) map[string]Device {
	out := make(map[string]Device, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func cloneSites(m map[string]Site) map[string]Site {
	out := make(map[string]Site, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// verify checks every read path against the reference models: point reads,
// full scans, indexed/compound/range/prefix filters, exact ordering with
// pagination, counts, aggregates, group-by, and joins.
func (w *eqWorld) verify(tx *rime.Tx, devModel map[string]Device, siteModel map[string]Site, txn int, phase string) {
	t := w.t
	where := fmt.Sprintf("txn %d %s", txn, phase)
	devB := w.dev.In(tx)
	siteB := w.sites.In(tx)
	siteF := rime.SF[Device](w.dev, "Site")
	statusF := rime.OF[Device, int](w.dev, "Status")
	latF := rime.OF[Device, int](w.dev, "Latency")
	hostF := rime.SF[Device](w.dev, "Hostname")

	// Point reads, present and missing.
	for i := 0; i < 12; i++ {
		key := fmt.Sprintf("k%d", i)
		got, err := devB.Get(key)
		want, ok := devModel[key]
		if !ok {
			if !errors.Is(err, rime.ErrNotFound) {
				t.Fatalf("%s: get %s = %+v, %v", where, key, got, err)
			}
			continue
		}
		if err != nil || *got != want {
			t.Fatalf("%s: get %s = %+v, %v want %+v", where, key, got, err, want)
		}
	}
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("s%d", i)
		got, err := siteB.Get(id)
		want, ok := siteModel[id]
		if !ok {
			if !errors.Is(err, rime.ErrNotFound) {
				t.Fatalf("%s: site get %s = %+v, %v", where, id, got, err)
			}
			continue
		}
		if err != nil || *got != want {
			t.Fatalf("%s: site get %s = %+v, %v want %+v", where, id, got, err, want)
		}
	}

	// Full scans.
	rows, err := w.dev.Where().In(tx).Find()
	if err != nil {
		t.Fatalf("%s: full scan: %v", where, err)
	}
	requireRows(t, rows, devModel)
	srows, err := w.sites.Where().In(tx).Find()
	if err != nil {
		t.Fatalf("%s: site scan: %v", where, err)
	}
	if len(srows) != len(siteModel) {
		t.Fatalf("%s: site scan %d rows, want %d", where, len(srows), len(siteModel))
	}
	for _, r := range srows {
		if want, ok := siteModel[r.ID]; !ok || *r != want {
			t.Fatalf("%s: site scan row %+v, want %+v", where, r, want)
		}
	}

	idsOf := func(rs []*Device) []string {
		out := make([]string, len(rs))
		for i, r := range rs {
			out[i] = r.ID
		}
		return out
	}
	setOf := func(rs []*Device) map[string]bool {
		out := make(map[string]bool, len(rs))
		for _, r := range rs {
			out[r.ID] = true
		}
		return out
	}
	// Filtered sets: hash index, ordered range, compound, prefix.
	cases := []struct {
		name string
		expr rime.Expr[Device]
		keep func(Device) bool
	}{
		{"site-eq", siteF.Eq("s1"), func(d Device) bool { return d.Site == "s1" }},
		{"status-range", statusF.Between(1, 3), func(d Device) bool { return d.Status >= 1 && d.Status <= 3 }},
		{"compound", rime.And(siteF.Eq("s2"), statusF.Eq(2)), func(d Device) bool { return d.Site == "s2" && d.Status == 2 }},
		{"lat-gt", latF.Gt(0), func(d Device) bool { return d.Latency > 0 }},
		{"host-prefix", hostF.StartsWith("h-k1"), func(d Device) bool {
			return len(d.Hostname) >= 4 && d.Hostname[:4] == "h-k1"
		}},
	}
	for _, c := range cases {
		got, err := w.dev.Where(c.expr).In(tx).Find()
		if err != nil {
			t.Fatalf("%s: %s: %v", where, c.name, err)
		}
		want := map[string]bool{}
		for id, d := range devModel {
			if c.keep(d) {
				want[id] = true
			}
		}
		if gs := setOf(got); !eqSet(gs, want) {
			t.Fatalf("%s: %s set %v want %v", where, c.name, gs, want)
		}
		n, err := w.dev.Where(c.expr).In(tx).Count()
		if err != nil || n != len(want) {
			t.Fatalf("%s: %s count %d (%v) want %d", where, c.name, n, err, len(want))
		}
	}

	// Exact ordering: latencies are unique by construction, so ties cannot
	// make two orderings valid.
	ordered := make([]Device, 0, len(devModel))
	for _, d := range devModel {
		ordered = append(ordered, d)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Latency < ordered[j].Latency })
	wantIDs := make([]string, len(ordered))
	for i, d := range ordered {
		wantIDs[i] = d.ID
	}
	gotRows, err := w.dev.Where().In(tx).OrderBy(latF).Find()
	if err != nil {
		t.Fatalf("%s: order asc: %v", where, err)
	}
	if got := idsOf(gotRows); !eqSeq(got, wantIDs) {
		t.Fatalf("%s: order asc %v want %v", where, got, wantIDs)
	}
	rev := append([]string(nil), wantIDs...)
	for i, j := 0, len(rev)-1; i < j; i, j = i+1, j-1 {
		rev[i], rev[j] = rev[j], rev[i]
	}
	if len(rev) > 5 {
		rev = rev[:5]
	}
	gotRows, err = w.dev.Where().In(tx).OrderByDesc(latF).Limit(5).Find()
	if err != nil {
		t.Fatalf("%s: order desc limit: %v", where, err)
	}
	if got := idsOf(gotRows); !eqSeq(got, rev) {
		t.Fatalf("%s: order desc limit %v want %v", where, got, rev)
	}
	page := wantIDs
	if len(page) > 3 {
		page = page[3:]
	} else {
		page = nil
	}
	if len(page) > 4 {
		page = page[:4]
	}
	gotRows, err = w.dev.Where().In(tx).OrderBy(latF).Offset(3).Limit(4).Find()
	if err != nil {
		t.Fatalf("%s: order page: %v", where, err)
	}
	if got := idsOf(gotRows); !eqSeq(got, page) {
		t.Fatalf("%s: order page %v want %v", where, got, page)
	}

	// Aggregates over a filtered set and over everything.
	for _, exprs := range [][]rime.Expr[Device]{{}, {siteF.Eq("s0")}} {
		var sum, n, min, max int
		first := true
		for _, d := range devModel {
			if len(exprs) > 0 && d.Site != "s0" {
				continue
			}
			sum += d.Latency
			n++
			if first || d.Latency < min {
				min = d.Latency
			}
			if first || d.Latency > max {
				max = d.Latency
			}
			first = false
		}
		vals, err := w.dev.Where(exprs...).In(tx).Aggregate(rime.Count[Device](), rime.SumOf(latF))
		if err != nil {
			t.Fatalf("%s: aggregate: %v", where, err)
		}
		if vals[0].(int) != n || vals[1].(float64) != float64(sum) {
			t.Fatalf("%s: count/sum %v want %d/%d", where, vals, n, sum)
		}
		if n == 0 {
			continue
		}
		vals, err = w.dev.Where(exprs...).In(tx).Aggregate(rime.AvgOf(latF), rime.MinOf(latF), rime.MaxOf(latF))
		if err != nil {
			t.Fatalf("%s: minmax: %v", where, err)
		}
		if vals[0].(float64) != float64(sum)/float64(n) || vals[1].(int) != min || vals[2].(int) != max {
			t.Fatalf("%s: avg/min/max %v want %v/%d/%d", where, vals, float64(sum)/float64(n), min, max)
		}
	}

	// Group-by site with count and latency sum per group.
	groups, err := w.dev.Where().In(tx).GroupBy(siteF).Aggregate(rime.Count[Device](), rime.SumOf(latF))
	if err != nil {
		t.Fatalf("%s: group by: %v", where, err)
	}
	wantGroups := map[string][2]float64{}
	for _, d := range devModel {
		g := wantGroups[d.Site]
		g[0]++
		g[1] += float64(d.Latency)
		wantGroups[d.Site] = g
	}
	if len(groups) != len(wantGroups) {
		t.Fatalf("%s: %d groups want %d", where, len(groups), len(wantGroups))
	}
	for _, g := range groups {
		key, ok := g.Keys[0].(string)
		if !ok {
			t.Fatalf("%s: group key %v not a string", where, g.Keys)
		}
		want, ok := wantGroups[key]
		if !ok {
			t.Fatalf("%s: unexpected group %q", where, key)
		}
		if float64(g.Values[0].(int)) != want[0] || g.Values[1].(float64) != want[1] {
			t.Fatalf("%s: group %q %v want %v", where, key, g.Values, want)
		}
		delete(wantGroups, key)
	}

	// Joins as pair sets; sites keyed by ID give at most one match.
	dsite := rime.SF[Device](w.dev, "Site")
	sid := rime.SF[Site](w.sites, "ID")
	inner, err := rime.InnerJoinOn(devB, dsite, siteB, sid)
	if err != nil {
		t.Fatalf("%s: inner join: %v", where, err)
	}
	wantInner := map[string]bool{}
	for id, d := range devModel {
		if _, ok := siteModel[d.Site]; ok {
			wantInner[id+"\x00"+d.Site] = true
		}
	}
	gotInner := map[string]bool{}
	for _, r := range inner {
		if r.Left.Site != r.Right.ID {
			t.Fatalf("%s: inner mismatch %+v %+v", where, r.Left, r.Right)
		}
		gotInner[r.Left.ID+"\x00"+r.Right.ID] = true
	}
	if !eqSet(gotInner, wantInner) {
		t.Fatalf("%s: inner set %v want %v", where, gotInner, wantInner)
	}
	left, err := rime.LeftJoinOn(devB, dsite, siteB, sid)
	if err != nil {
		t.Fatalf("%s: left join: %v", where, err)
	}
	if len(left) != len(devModel) {
		t.Fatalf("%s: left join %d rows want %d", where, len(left), len(devModel))
	}
	for _, r := range left {
		want, ok := siteModel[r.Left.Site]
		if !ok {
			if r.Right != nil {
				t.Fatalf("%s: orphan %s matched %+v", where, r.Left.ID, r.Right)
			}
			continue
		}
		if r.Right == nil || *r.Right != want {
			t.Fatalf("%s: left %s -> %+v want %+v", where, r.Left.ID, r.Right, want)
		}
	}
	east, err := rime.InnerJoinOn(devB, dsite, siteB, sid,
		func(d *Device, s *Site) bool { return s.Region == "East" })
	if err != nil {
		t.Fatalf("%s: filtered join: %v", where, err)
	}
	wantEast := map[string]bool{}
	for id, d := range devModel {
		if s, ok := siteModel[d.Site]; ok && s.Region == "East" {
			wantEast[id+"\x00"+d.Site] = true
		}
	}
	gotEast := map[string]bool{}
	for _, r := range east {
		gotEast[r.Left.ID+"\x00"+r.Right.ID] = true
	}
	if !eqSet(gotEast, wantEast) {
		t.Fatalf("%s: east set %v want %v", where, gotEast, wantEast)
	}
}

// verifyContexts proves cancellation is deterministic and never commits
// partial work: the store still matches the model afterwards.
func (w *eqWorld) verifyContexts() {
	t := w.t
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := w.dev.Where().In(nil).WithContext(ctx).Find(); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled find: %v", err)
	}
	if err := w.db.WriteTxContext(ctx, func(tx *rime.Tx) error {
		return w.dev.In(tx).Upsert(&Device{ID: "cancel-me", Hostname: "h-cancel-me"})
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled writetx: %v", err)
	}
	if _, err := rime.InnerJoinOn(
		w.dev.In(nil).WithContext(ctx), rime.SF[Device](w.dev, "Site"),
		w.sites.In(nil), rime.SF[Site](w.sites, "ID"),
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled join: %v", err)
	}
	if _, err := w.dev.Get("cancel-me"); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("canceled write leaked: %v", err)
	}
}

func eqSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func eqSeq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestExplainGolden freezes planner output for a fixed dataset so unintended
// plan changes (lost index seeks, flipped sort strategy, estimate drift)
// fail loudly instead of slipping into a release.
func TestExplainGolden(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 100)
	site := rime.SF[Device](dev, "Site")
	status := rime.OF[Device, int](dev, "Status")
	lat := rime.OF[Device, int](dev, "Latency")
	id := rime.F[Device, string](dev, "ID")
	plans := map[string]string{
		"compound":    dev.Where(rime.And(site.Eq("OTT"), status.Eq(1), lat.Lt(100))).Limit(100).Explain(),
		"range":       dev.Where(lat.Between(5, 10)).Explain(),
		"contradict":  dev.Where(rime.And(site.Eq("OTT"), site.Eq("MTL"))).Explain(),
		"hash":        dev.Where(site.Eq("OTT")).Explain(),
		"scan":        dev.Where().Explain(),
		"index-order": dev.Where().OrderBy(lat).Limit(5).Explain(),
		"sort-order":  dev.Where().OrderByAsc(id).Limit(3).Explain(),
		"status-eq":   dev.Where(status.Eq(2)).Explain(),
	}
	for name, golden := range explainGoldens {
		if got := plans[name]; got != golden+"\n" {
			t.Errorf("plan %s drifted:\n got:\n%s\nwant:\n%s", name, got, golden)
		}
	}
}

var explainGoldens = map[string]string{
	"compound": `TABLE Device
ROWS 100
INDEX SEEK site_status
    Site = "OTT"
    Status = 1
ESTIMATED CANDIDATES 8
FILTER
    Latency < 100
LIMIT 100`,
	"range": `TABLE Device
ROWS 100
ORDERED RANGE Latency
    Latency > 5
    Latency < 10
ESTIMATED CANDIDATES 6`,
	"contradict": `TABLE Device
ROWS 100
EMPTY (contradiction)
ESTIMATED CANDIDATES 0`,
	"hash": `TABLE Device
ROWS 100
INDEX SEEK Site
    Site = "OTT"
ESTIMATED CANDIDATES 34`,
	"scan": `TABLE Device
ROWS 100
FULL SCAN
ESTIMATED CANDIDATES 100`,
	"index-order": `TABLE Device
ROWS 100
ORDERED SCAN Latency
ESTIMATED CANDIDATES 100
ORDER BY Latency ASC (from index)
LIMIT 5`,
	"sort-order": `TABLE Device
ROWS 100
FULL SCAN
ESTIMATED CANDIDATES 100
ORDER BY ID ASC (sort)
LIMIT 3`,
	"status-eq": `TABLE Device
ROWS 100
INDEX SEEK Status
    Status = 2
ESTIMATED CANDIDATES 25`,
}

// TestConflictMatrix proves optimistic concurrency control: two transactions
// staging overlapping writes serialize so exactly one commits and the loser
// observes ErrConflict with the winner's state intact, while disjoint writes
// never conflict.
func TestConflictMatrix(t *testing.T) {
	setup := func(t *testing.T) (*rime.DB, *rime.Table[Device]) {
		t.Helper()
		db, dev := openDevices(t)
		t.Cleanup(func() { db.Close() })
		for i := 0; i < 4; i++ {
			d := Device{ID: fmt.Sprintf("k%d", i), Hostname: fmt.Sprintf("h-%d", i), Site: "s0"}
			if err := dev.Upsert(&d); err != nil {
				t.Fatal(err)
			}
		}
		return db, dev
	}
	cases := []struct {
		name  string
		first func(b rime.BoundTable[Device]) error
		other func(b rime.BoundTable[Device]) error
	}{
		{"update-update", func(b rime.BoundTable[Device]) error {
			return b.Update("k0", func(d *Device) error { d.Status = 1; return nil })
		}, func(b rime.BoundTable[Device]) error {
			return b.Update("k0", func(d *Device) error { d.Status = 2; return nil })
		}},
		{"update-delete", func(b rime.BoundTable[Device]) error {
			return b.Update("k1", func(d *Device) error { d.Status = 3; return nil })
		}, func(b rime.BoundTable[Device]) error {
			return b.Delete("k1")
		}},
		{"delete-delete", func(b rime.BoundTable[Device]) error {
			return b.Delete("k2")
		}, func(b rime.BoundTable[Device]) error {
			return b.Delete("k2")
		}},
		{"insert-insert", func(b rime.BoundTable[Device]) error {
			return b.Insert(&Device{ID: "kn", Hostname: "h-n"})
		}, func(b rime.BoundTable[Device]) error {
			return b.Insert(&Device{ID: "kn", Hostname: "h-n2"})
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, dev := setup(t)
			ctx := context.Background()
			t1, err := db.BeginTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t2, err := db.BeginTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.first(dev.In(t1)); err != nil {
				t.Fatalf("first stage: %v", err)
			}
			if err := c.other(dev.In(t2)); err != nil {
				t.Fatalf("other stage: %v", err)
			}
			if err := t1.Commit(); err != nil {
				t.Fatalf("first commit: %v", err)
			}
			if err := t2.Commit(); !errors.Is(err, rime.ErrConflict) {
				t.Fatalf("other commit: %v, want conflict", err)
			}
			t2.Rollback()
			// Winner's state is intact and complete.
			switch c.name {
			case "update-update":
				got, err := dev.Get("k0")
				if err != nil || got.Status != 1 {
					t.Fatalf("k0 = %+v, %v want status 1", got, err)
				}
			case "update-delete":
				got, err := dev.Get("k1")
				if err != nil || got.Status != 3 {
					t.Fatalf("k1 = %+v, %v want status 3", got, err)
				}
			case "delete-delete":
				if _, err := dev.Get("k2"); !errors.Is(err, rime.ErrNotFound) {
					t.Fatalf("k2 = %v want deleted", err)
				}
			case "insert-insert":
				got, err := dev.Get("kn")
				if err != nil || got.Hostname != "h-n" {
					t.Fatalf("kn = %+v, %v want first writer", got, err)
				}
			}
		})
	}
	t.Run("disjoint", func(t *testing.T) {
		db, dev := setup(t)
		ctx := context.Background()
		t1, err := db.BeginTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t2, err := db.BeginTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := dev.In(t1).Update("k0", func(d *Device) error { d.Status = 10; return nil }); err != nil {
			t.Fatal(err)
		}
		if err := dev.In(t2).Update("k1", func(d *Device) error { d.Status = 20; return nil }); err != nil {
			t.Fatal(err)
		}
		if err := t1.Commit(); err != nil {
			t.Fatalf("first: %v", err)
		}
		if err := t2.Commit(); err != nil {
			t.Fatalf("disjoint second commit conflicted: %v", err)
		}
		a, _ := dev.Get("k0")
		b, _ := dev.Get("k1")
		if a.Status != 10 || b.Status != 20 {
			t.Fatalf("disjoint states %+v %+v", a, b)
		}
	})
}

// TestStatsEquivalence proves record/version/tombstone statistics track the
// reference model exactly across commits and rollbacks. Every committed
// staged write appends exactly one retained version (no GC runs here), live
// heads equal the model size, and head tombstones equal keys whose latest
// committed state is deleted.
func TestStatsEquivalence(t *testing.T) {
	w := newEqWorld(t, 777)
	abort := errors.New("stats rollback")
	tombs := map[string]bool{}
	var totalOps int64
	check := func(txn int, phase string) {
		t.Helper()
		ts := w.dev.TableStats()
		if ts.Records != int64(len(w.model)) {
			t.Fatalf("txn %d %s: records %d want %d", txn, phase, ts.Records, len(w.model))
		}
		if ts.Tombstones != int64(len(tombs)) {
			t.Fatalf("txn %d %s: tombstones %d want %d", txn, phase, ts.Tombstones, len(tombs))
		}
		if ts.Versions != totalOps {
			t.Fatalf("txn %d %s: versions %d want %d", txn, phase, ts.Versions, totalOps)
		}
		ds := w.db.Stats()
		if ds.Tables != 2 || ds.Records != int64(len(w.model)+len(w.siteModel)) {
			t.Fatalf("txn %d %s: db tables=%d records=%d", txn, phase, ds.Tables, ds.Records)
		}
		if ds.ActiveTxns != 0 {
			t.Fatalf("txn %d %s: %d active txns outside transactions", txn, phase, ds.ActiveTxns)
		}
	}
	const txns = 40
	for i := 0; i < txns; i++ {
		rollback := i%7 == 6
		staged := cloneDevices(w.model)
		simTombs := make(map[string]bool, len(tombs))
		for k := range tombs {
			simTombs[k] = true
		}
		ops := 0
		err := w.db.WriteTx(func(tx *rime.Tx) error {
			bound := w.dev.In(tx)
			for k := 0; k < 1+w.rng.Intn(4); k++ {
				key := fmt.Sprintf("k%d", w.rng.Intn(10))
				switch w.rng.Intn(10) {
				case 0, 1, 2, 3, 4, 5:
					w.latCtr++
					row := Device{ID: key, Hostname: "h-" + key,
						Site: fmt.Sprintf("s%d", w.rng.Intn(4)), Status: w.rng.Intn(4), Latency: w.latCtr}
					if err := bound.Upsert(&row); err != nil {
						return err
					}
					staged[key] = row
					delete(simTombs, key)
					ops++
				case 6, 7:
					err := bound.Update(key, func(d *Device) error {
						d.Status = (d.Status + 1) % 4
						return nil
					})
					if _, ok := staged[key]; !ok {
						if !errors.Is(err, rime.ErrNotFound) {
							t.Fatalf("txn %d missing update %s: %v", i, key, err)
						}
						continue
					}
					if err != nil {
						return err
					}
					d := staged[key]
					d.Status = (d.Status + 1) % 4
					staged[key] = d
					ops++
				default:
					err := bound.Delete(key)
					if _, ok := staged[key]; ok {
						if err != nil {
							return err
						}
						delete(staged, key)
						simTombs[key] = true
						ops++
					} else if !errors.Is(err, rime.ErrNotFound) {
						t.Fatalf("txn %d missing delete %s: %v", i, key, err)
					}
				}
			}
			if rollback {
				return abort
			}
			return nil
		})
		if rollback {
			if !errors.Is(err, abort) {
				t.Fatalf("txn %d rollback: %v", i, err)
			}
			w.hooks = w.hooks[:0]
			check(i, "rollback")
			continue
		}
		if err != nil {
			t.Fatalf("txn %d: %v", i, err)
		}
		w.model = staged
		for k := range tombs {
			delete(tombs, k)
		}
		for k := range simTombs {
			tombs[k] = true
		}
		totalOps += int64(ops)
		w.hooks = w.hooks[:0]
		check(i, "commit")
	}
}

// TestConstraintAtomicityEquivalence proves constraint violations fail the
// whole transaction with the documented error and commit nothing: after a
// rejected commit the store still matches the reference model exactly.
func TestConstraintAtomicityEquivalence(t *testing.T) {
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
	modelO := map[string]Owner{}
	modelP := map[string]Pet{}
	verify := func() {
		t.Helper()
		orows, err := owners.Where().Find()
		if err != nil {
			t.Fatal(err)
		}
		if len(orows) != len(modelO) {
			t.Fatalf("owners %d rows want %d", len(orows), len(modelO))
		}
		for _, r := range orows {
			if want, ok := modelO[r.ID]; !ok || want != *r {
				t.Fatalf("owner %+v want %+v", r, want)
			}
		}
		prows, err := pets.Where().Find()
		if err != nil {
			t.Fatal(err)
		}
		if len(prows) != len(modelP) {
			t.Fatalf("pets %d rows want %d", len(prows), len(modelP))
		}
		for _, r := range prows {
			if want, ok := modelP[r.ID]; !ok || want != *r {
				t.Fatalf("pet %+v want %+v", r, want)
			}
		}
	}

	rng := rand.New(rand.NewSource(31337))
	ownerIDs := []string{"o0", "o1", "o2", "o3"}
	petIDs := []string{"p0", "p1", "p2", "p3", "p4", "p5"}
	goodName := func() rime.Optional[string] {
		return rime.Some(fmt.Sprintf("name%d", rng.Intn(100)))
	}
	const txns = 50
	for i := 0; i < txns; i++ {
		// Stage 1-3 valid ops, then optionally one violation. A violation
		// anywhere fails the commit and nothing may apply.
		type staged struct {
			kind      string // "owner" or "pet"
			o         Owner
			p         Pet
			viaInsert bool // duplicate-PK violation uses Insert, not Upsert
		}
		var ops []staged
		simO := cloneMap(modelO)
		simP := cloneMap(modelP)
		for k := 0; k < 1+rng.Intn(3); k++ {
			if rng.Intn(2) == 0 {
				id := ownerIDs[rng.Intn(len(ownerIDs))]
				o := Owner{ID: id, Name: goodName()}
				ops = append(ops, staged{kind: "owner", o: o})
				simO[id] = o
			} else {
				id := petIDs[rng.Intn(len(petIDs))]
				// Valid pets reference committed owners: same-transaction
				// parents are invisible to FK validation (validateFK reads
				// latest-committed only), so they stay out of this suite
				// until that gap is scheduled and fixed.
				if len(modelO) == 0 {
					continue
				}
				oks := make([]string, 0, len(modelO))
				for oid := range modelO {
					oks = append(oks, oid)
				}
				p := Pet{ID: id, Owner: oks[rng.Intn(len(oks))]}
				ops = append(ops, staged{kind: "pet", p: p})
				simP[id] = p
			}
		}
		var wantErr error
		if rng.Intn(2) == 0 {
			switch rng.Intn(4) {
			case 0: // duplicate primary key via Insert
				if len(simO) > 0 {
					for id := range simO {
						ops = append(ops, staged{kind: "owner", o: Owner{ID: id, Name: goodName()}, viaInsert: true})
						break
					}
					wantErr = rime.ErrAlreadyExists
				}
			case 1: // missing NOT NULL value
				ops = append(ops, staged{kind: "owner", o: Owner{ID: fmt.Sprintf("ox%d", i)}})
				wantErr = rime.ErrNotNull
			case 2: // CHECK callback rejection
				ops = append(ops, staged{kind: "owner", o: Owner{ID: fmt.Sprintf("oy%d", i), Name: rime.Some("x")}})
				wantErr = rime.ErrCheck
			case 3: // dangling foreign key
				ops = append(ops, staged{kind: "pet", p: Pet{ID: fmt.Sprintf("pz%d", i), Owner: "missing-owner"}})
				wantErr = rime.ErrForeignKey
			}
		}
		err := db.WriteTx(func(tx *rime.Tx) error {
			ob, pb := owners.In(tx), pets.In(tx)
			for _, op := range ops {
				var err error
				switch op.kind {
				case "owner":
					if op.viaInsert {
						err = ob.Insert(&op.o)
					} else {
						err = ob.Upsert(&op.o)
					}
				case "pet":
					err = pb.Upsert(&op.p)
				}
				if err != nil {
					return err // stage-time rejection also rolls back
				}
			}
			return nil
		})
		if wantErr != nil {
			if !errors.Is(err, wantErr) {
				t.Fatalf("txn %d violation: %v want %v", i, err, wantErr)
			}
			verify() // nothing applied
			continue
		}
		if err != nil {
			t.Fatalf("txn %d valid: %v", i, err)
		}
		modelO, modelP = simO, simP
		verify()
	}
}

func cloneMap[V any](m map[string]V) map[string]V {
	out := make(map[string]V, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// TestEventContentEquivalence proves async table events deliver exactly the
// committed change stream — one event per staged write, in order, with the
// sequential-install old/new values — and that rolled-back transactions emit
// nothing.
func TestEventContentEquivalence(t *testing.T) {
	db := rime.New(rime.WithEventQueueSize(4096))
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	type evRec struct {
		op  rime.Operation
		id  string
		old *Device
		new *Device
	}
	var mu sync.Mutex
	var received []evRec
	record := func(op rime.Operation, o, n *Device) {
		var old, newv *Device
		if o != nil {
			cp := *o
			old = &cp
		}
		if n != nil {
			cp := *n
			newv = &cp
		}
		id := ""
		if newv != nil {
			id = newv.ID
		} else {
			id = old.ID
		}
		mu.Lock()
		received = append(received, evRec{op: op, id: id, old: old, new: newv})
		mu.Unlock()
	}
	dev.OnInserted(func(d *Device) { record(rime.OpInsert, nil, d) })
	dev.OnUpdated(func(o, n *Device) { record(rime.OpUpdate, o, n) })
	dev.OnDeleted(func(d *Device) { record(rime.OpDelete, d, nil) })
	waitFor := func(n int) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			mu.Lock()
			got := len(received)
			mu.Unlock()
			if got >= n {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("only %d events delivered, want %d", got, n)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}

	rng := rand.New(rand.NewSource(2024))
	model := map[string]Device{}
	var expected []evRec
	latCtr := 0
	abort := errors.New("event rollback")
	const txns = 40
	for i := 0; i < txns; i++ {
		rollback := i%6 == 5
		staged := cloneDevices(model)
		sim := cloneDevices(model)
		var want []evRec
		err := db.WriteTx(func(tx *rime.Tx) error {
			bound := dev.In(tx)
			for k := 0; k < 1+rng.Intn(3); k++ {
				key := fmt.Sprintf("k%d", rng.Intn(8))
				switch rng.Intn(10) {
				case 0, 1, 2, 3, 4, 5:
					latCtr++
					row := Device{ID: key, Hostname: "h-" + key,
						Site: fmt.Sprintf("s%d", rng.Intn(3)), Status: rng.Intn(4), Latency: latCtr}
					if err := bound.Upsert(&row); err != nil {
						return err
					}
					op := rime.OpInsert
					if _, ok := sim[key]; ok {
						op = rime.OpUpdate
					}
					want = append(want, evRec{op: op, id: key, old: simPtr(sim, key), new: &row})
					staged[key], sim[key] = row, row
				case 6, 7:
					err := bound.Update(key, func(d *Device) error {
						d.Status = (d.Status + 1) % 4
						latCtr++
						d.Latency = latCtr
						return nil
					})
					if _, ok := staged[key]; !ok {
						if !errors.Is(err, rime.ErrNotFound) {
							t.Fatalf("txn %d missing update %s: %v", i, key, err)
						}
						continue
					}
					if err != nil {
						return err
					}
					got, err := bound.Get(key)
					if err != nil {
						return err
					}
					old := simPtr(sim, key) // install head before this op
					staged[key], sim[key] = *got, *got
					want = append(want, evRec{op: rime.OpUpdate, id: key, old: old, new: got})
				default:
					err := bound.Delete(key)
					if _, ok := staged[key]; ok {
						if err != nil {
							return err
						}
						delete(staged, key)
						want = append(want, evRec{op: rime.OpDelete, id: key, old: simPtr(sim, key)})
						delete(sim, key)
					} else if !errors.Is(err, rime.ErrNotFound) {
						t.Fatalf("txn %d missing delete %s: %v", i, key, err)
					}
				}
			}
			if rollback {
				return abort
			}
			return nil
		})
		if rollback {
			if !errors.Is(err, abort) {
				t.Fatalf("txn %d rollback: %v", i, err)
			}
			time.Sleep(100 * time.Millisecond)
			mu.Lock()
			n := len(received)
			mu.Unlock()
			if n != len(expected) {
				t.Fatalf("txn %d: rollback delivered events (%d vs %d)", i, n, len(expected))
			}
			continue
		}
		if err != nil {
			t.Fatalf("txn %d: %v", i, err)
		}
		model = staged
		expected = append(expected, want...)
	}
	waitFor(len(expected))
	mu.Lock()
	defer mu.Unlock()
	if len(received) != len(expected) {
		t.Fatalf("%d events, want %d", len(received), len(expected))
	}
	for i := range expected {
		a, b := received[i], expected[i]
		if a.op != b.op || a.id != b.id {
			t.Fatalf("event %d: %+v want %+v", i, a, b)
		}
		if (a.old == nil) != (b.old == nil) || (a.new == nil) != (b.new == nil) {
			t.Fatalf("event %d presence: %+v want %+v", i, a, b)
		}
		if a.old != nil && *a.old != *b.old {
			t.Fatalf("event %d old: %+v want %+v", i, a.old, b.old)
		}
		if a.new != nil && *a.new != *b.new {
			t.Fatalf("event %d new: %+v want %+v", i, a.new, b.new)
		}
	}
}

// TestProjectionEquivalence proves Project returns exactly the mapped rows of
// its query — same set, same order — over committed state and staged-write
// overlays, and that projected values are detached from stored records.
func TestProjectionEquivalence(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	rng := rand.New(rand.NewSource(55))
	for i := 0; i < 36; i++ {
		d := Device{
			ID:       fmt.Sprintf("p%02d", i),
			Hostname: fmt.Sprintf("ph-%02d", i),
			Site:     fmt.Sprintf("s%d", rng.Intn(4)),
			Status:   rng.Intn(5),
			Latency:  i,
		}
		if err := dev.Upsert(&d); err != nil {
			t.Fatal(err)
		}
	}
	type status struct {
		Hostname string
		Status   int
	}
	siteF := rime.SF[Device](dev, "Site")
	statusF := rime.OF[Device, int](dev, "Status")
	latF := rime.OF[Device, int](dev, "Latency")
	project := func(d *Device) status {
		return status{Hostname: d.Hostname, Status: d.Status}
	}
	queries := func(tx *rime.Tx) []*rime.Query[Device] {
		return []*rime.Query[Device]{
			dev.Where().In(tx),
			dev.Where(siteF.Eq("s1")).In(tx),
			dev.Where(statusF.Between(1, 3)).In(tx),
			dev.Where(rime.And(siteF.Eq("s2"), statusF.Eq(2))).In(tx),
			dev.Where().In(tx).OrderBy(latF),
			dev.Where(siteF.Eq("s0")).In(tx).OrderByDesc(latF).Limit(5),
			dev.Where().In(tx).OrderBy(latF).Offset(4).Limit(7),
		}
	}
	check := func(tx *rime.Tx, phase string) {
		qs := queries(tx)
		for i, q := range qs {
			got, err := rime.Project(q, project)
			if err != nil {
				t.Fatalf("%s query %d project: %v", phase, i, err)
			}
			// Rebuild the identical query. Unordered queries promise
			// set equality only (index iteration order varies run to
			// run); ordered queries must match exactly, order included.
			ordered := i >= 4
			want, err := queries(tx)[i].Find()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(want) {
				t.Fatalf("%s query %d: %d projected, %d direct", phase, i, len(got), len(want))
			}
			if !ordered {
				byHost := make(map[string]status, len(want))
				for _, r := range want {
					byHost[r.Hostname] = project(r)
				}
				for j := range got {
					if wantJ, ok := byHost[got[j].Hostname]; !ok || got[j] != wantJ {
						t.Fatalf("%s query %d row %d: %+v not in direct set", phase, i, j, got[j])
					}
				}
				continue
			}
			for j := range got {
				if wantJ := project(want[j]); got[j] != wantJ {
					t.Fatalf("%s query %d row %d: %+v vs %+v", phase, i, j, got[j], wantJ)
				}
			}
		}
	}
	check(nil, "committed")
	rollback := errors.New("projection overlay rollback")
	if err := db.WriteTx(func(tx *rime.Tx) error {
		if err := dev.In(tx).Upsert(&Device{ID: "p99", Hostname: "ph-99", Site: "s1", Status: 4, Latency: 1000}); err != nil {
			return err
		}
		if err := dev.In(tx).Update("p00", func(d *Device) error { d.Status = 9; return nil }); err != nil {
			return err
		}
		if err := dev.In(tx).Delete("p01"); err != nil {
			return err
		}
		check(tx, "overlay")
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("overlay txn: %v", err)
	}
	check(nil, "post-rollback")
	// Projected values are detached: mutating them cannot affect the store.
	rows, err := rime.Project(dev.Where(siteF.Eq("s1")), project)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("expected projected rows")
	}
	rows[0].Status = -999
	got, err := dev.Where(siteF.Eq("s1")).Find()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range got {
		if r.Hostname == rows[0].Hostname && r.Status == -999 {
			t.Fatal("projected mutation leaked into stored record")
		}
	}
}

// TestCompiledQueryEquivalence proves compiled (prepared) queries return
// exactly what their direct Where equivalents return, across randomized
// parameter bindings, ordering, pagination, and staged-write overlays.
func TestCompiledQueryEquivalence(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	rng := rand.New(rand.NewSource(97))
	for i := 0; i < 48; i++ {
		d := Device{
			ID:       fmt.Sprintf("c%02d", i),
			Hostname: fmt.Sprintf("ch-%02d", i),
			Site:     fmt.Sprintf("s%d", rng.Intn(4)),
			Status:   rng.Intn(5),
			Latency:  i, // unique: exact order assertions are valid
		}
		if err := dev.Upsert(&d); err != nil {
			t.Fatal(err)
		}
	}
	siteF := rime.SF[Device](dev, "Site")
	statusF := rime.OF[Device, int](dev, "Status")
	latF := rime.OF[Device, int](dev, "Latency")
	hostF := rime.SF[Device](dev, "Hostname")

	ids := func(rows []*Device) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = r.ID
		}
		return out
	}
	setEq := func(a, b []*Device) bool {
		if len(a) != len(b) {
			return false
		}
		m := make(map[string]*Device, len(a))
		for _, r := range a {
			m[r.ID] = r
		}
		for _, r := range b {
			want, ok := m[r.ID]
			if !ok || *want != *r {
				return false
			}
		}
		return true
	}

	eq := dev.Compile(rime.And(siteF.Eq(rime.Param[string]()), statusF.Eq(rime.Param[int]())))
	rngB := rand.New(rand.NewSource(98))
	for i := 0; i < 40; i++ {
		site, status := fmt.Sprintf("s%d", rngB.Intn(5)), rngB.Intn(6)
		got, err := eq.Find(site, status)
		if err != nil {
			t.Fatalf("binding %d: %v", i, err)
		}
		want, err := dev.Where(rime.And(siteF.Eq(site), statusF.Eq(status))).Find()
		if err != nil {
			t.Fatal(err)
		}
		if !setEq(got, want) {
			t.Fatalf("binding %d (%s,%d): compiled %v vs direct %v", i, site, status, ids(got), ids(want))
		}
	}
	rngQ := dev.Compile(rime.And(latF.Ge(rime.Param[int]()), latF.Le(rime.Param[int]())))
	for i := 0; i < 40; i++ {
		lo, hi := rngB.Intn(48), rngB.Intn(48)
		if lo > hi {
			lo, hi = hi, lo
		}
		got, err := rngQ.Find(lo, hi)
		if err != nil {
			t.Fatalf("range %d: %v", i, err)
		}
		want, err := dev.Where(rime.And(latF.Ge(lo), latF.Le(hi))).Find()
		if err != nil {
			t.Fatal(err)
		}
		if !setEq(got, want) {
			t.Fatalf("range %d [%d,%d]: compiled %v vs direct %v", i, lo, hi, ids(got), ids(want))
		}
		if n, err := rngQ.Count(lo, hi); err != nil || n != len(want) {
			t.Fatalf("range %d count %d (%v) want %d", i, n, err, len(want))
		}
	}
	ord := dev.Compile(siteF.Eq(rime.Param[string]())).OrderByAsc(latF).Limit(6).Offset(2)
	for i := 0; i < 10; i++ {
		site := fmt.Sprintf("s%d", rngB.Intn(4))
		got, err := ord.Find(site)
		if err != nil {
			t.Fatalf("ordered %d: %v", i, err)
		}
		want, err := dev.Where(siteF.Eq(site)).OrderBy(latF).Limit(6).Offset(2).Find()
		if err != nil {
			t.Fatal(err)
		}
		if !eqSeq(ids(got), ids(want)) {
			t.Fatalf("ordered %d %s: %v vs %v", i, site, ids(got), ids(want))
		}
	}
	pref := dev.Compile(hostF.StartsWith(rime.Param[string]()))
	for _, p := range []string{"ch-0", "ch-1", "ch-4", "zz"} {
		got, err := pref.Find(p)
		if err != nil {
			t.Fatalf("prefix %q: %v", p, err)
		}
		want, err := dev.Where(hostF.StartsWith(p)).Find()
		if err != nil {
			t.Fatal(err)
		}
		if !setEq(got, want) {
			t.Fatalf("prefix %q: %v vs %v", p, ids(got), ids(want))
		}
	}
	// Compiled queries see the same staged overlay as direct queries.
	rollback := errors.New("rollback overlay")
	if err := db.WriteTx(func(tx *rime.Tx) error {
		if err := dev.In(tx).Upsert(&Device{ID: "c99", Hostname: "ch-99", Site: "s1", Status: 2, Latency: 1000}); err != nil {
			return err
		}
		if err := dev.In(tx).Delete("c00"); err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			site, status := fmt.Sprintf("s%d", rngB.Intn(4)), rngB.Intn(5)
			got, err := eq.In(tx).Find(site, status)
			if err != nil {
				return err
			}
			want, err := dev.Where(rime.And(siteF.Eq(site), statusF.Eq(status))).In(tx).Find()
			if err != nil {
				return err
			}
			if !setEq(got, want) {
				t.Fatalf("overlay binding %d: %v vs %v", i, ids(got), ids(want))
			}
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("overlay txn: %v", err)
	}
}

// TestCompiledQueryConcurrent proves a single shared *Compiled plan is safe
// for concurrent use with different bindings while writers churn: every
// reader pins one snapshot per iteration and the shared plan (via In)
// must return exactly what the equivalent direct query returns on that
// same snapshot, for equality, range, count, and ordered+limit queries.
// Shallow-clone or shared-buffer races surface here as mismatches (or as
// -race failures); params are worker-seeded, so failures reproduce.
func TestCompiledQueryConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	rng := rand.New(rand.NewSource(4242))
	const rows = 200
	for i := 0; i < rows; i++ {
		d := Device{
			ID:       fmt.Sprintf("q%03d", i),
			Hostname: fmt.Sprintf("qh-%03d", i),
			Site:     fmt.Sprintf("s%d", rng.Intn(4)),
			Status:   rng.Intn(5),
			Latency:  i, // unique: exact order assertions are valid
		}
		if err := dev.Upsert(&d); err != nil {
			t.Fatal(err)
		}
	}
	siteF := rime.SF[Device](dev, "Site")
	statusF := rime.OF[Device, int](dev, "Status")
	latF := rime.OF[Device, int](dev, "Latency")

	// Shared roots: every reader goroutine binds these concurrently.
	eq := dev.Compile(rime.And(siteF.Eq(rime.Param[string]()), statusF.Eq(rime.Param[int]())))
	rngQ := dev.Compile(rime.And(latF.Ge(rime.Param[int]()), latF.Le(rime.Param[int]())))
	ord := dev.Compile(siteF.Eq(rime.Param[string]())).OrderByAsc(latF).Limit(6).Offset(2)

	ids := func(rows []*Device) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			out[i] = r.ID
		}
		return out
	}
	setEq := func(a, b []*Device) bool {
		if len(a) != len(b) {
			return false
		}
		m := make(map[string]*Device, len(a))
		for _, r := range a {
			m[r.ID] = r
		}
		for _, r := range b {
			want, ok := m[r.ID]
			if !ok || *want != *r {
				return false
			}
		}
		return true
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Writers churn Status so readers' snapshots actually differ.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(9000 + w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("q%03d", r.Intn(rows))
				st := r.Intn(5)
				_ = db.WriteTx(func(tx *rime.Tx) error {
					return dev.In(tx).Update(id, func(d *Device) error {
						d.Status = st
						return nil
					})
				})
			}
		}(w)
	}

	const readers = 8
	const iters = 150
	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	var rwg sync.WaitGroup
	for w := 0; w < readers; w++ {
		rwg.Add(1)
		go func(w int) {
			defer rwg.Done()
			r := rand.New(rand.NewSource(int64(5000 + w)))
			for i := 0; i < iters; i++ {
				pin := db.ReadTx()
				site, status := fmt.Sprintf("s%d", r.Intn(4)), r.Intn(5)
				got, err := eq.In(pin).Find(site, status)
				if err != nil {
					fail("worker %d iter %d eq: %v", w, i, err)
					pin.Close()
					return
				}
				want, err := dev.Where(rime.And(siteF.Eq(site), statusF.Eq(status))).In(pin).Find()
				if err != nil {
					fail("worker %d iter %d direct eq: %v", w, i, err)
					pin.Close()
					return
				}
				if !setEq(got, want) {
					fail("worker %d iter %d eq (%s,%d): compiled %v vs direct %v",
						w, i, site, status, ids(got), ids(want))
					pin.Close()
					return
				}
				lo, hi := r.Intn(rows), r.Intn(rows)
				if lo > hi {
					lo, hi = hi, lo
				}
				got, err = rngQ.In(pin).Find(lo, hi)
				if err != nil {
					fail("worker %d iter %d range: %v", w, i, err)
					pin.Close()
					return
				}
				want, err = dev.Where(rime.And(latF.Ge(lo), latF.Le(hi))).In(pin).Find()
				if err != nil {
					fail("worker %d iter %d direct range: %v", w, i, err)
					pin.Close()
					return
				}
				if !setEq(got, want) {
					fail("worker %d iter %d range [%d,%d]: compiled %v vs direct %v",
						w, i, lo, hi, ids(got), ids(want))
					pin.Close()
					return
				}
				if n, err := rngQ.In(pin).Count(lo, hi); err != nil || n != len(want) {
					fail("worker %d iter %d count %d (%v) want %d", w, i, n, err, len(want))
					pin.Close()
					return
				}
				osite := fmt.Sprintf("s%d", r.Intn(4))
				got, err = ord.In(pin).Find(osite)
				if err != nil {
					fail("worker %d iter %d ordered: %v", w, i, err)
					pin.Close()
					return
				}
				want, err = dev.Where(siteF.Eq(osite)).OrderBy(latF).Limit(6).Offset(2).In(pin).Find()
				if err != nil {
					fail("worker %d iter %d direct ordered: %v", w, i, err)
					pin.Close()
					return
				}
				if !eqSeq(ids(got), ids(want)) {
					fail("worker %d iter %d ordered %s: %v vs %v",
						w, i, osite, ids(got), ids(want))
					pin.Close()
					return
				}
				pin.Close()
			}
		}(w)
	}
	rwg.Wait()
	close(stop)
	wg.Wait()
	for {
		select {
		case msg := <-errCh:
			t.Fatalf("concurrent compiled query failure: %s", msg)
		default:
			return
		}
	}
}

// TestOrderedConcurrent proves the ordered index stays consistent with the
// records while writers churn the ordered field: every reader pins one
// snapshot per iteration and range, ordered+limit, and min/max queries must
// match a brute-force oracle built from point Gets on that same snapshot.
// Writers assign unique monotonic latencies, so ordered sequences compare
// exactly; params are worker-seeded, so failures reproduce.
func TestOrderedConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	const rows = 200
	for i := 0; i < rows; i++ {
		d := Device{ID: fmt.Sprintf("o%03d", i), Hostname: fmt.Sprintf("oh-%03d", i),
			Site: "s", Status: i % 5, Latency: i}
		if err := dev.Upsert(&d); err != nil {
			t.Fatal(err)
		}
	}
	latF := rime.OF[Device, int](dev, "Latency")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// Writers churn Latency with per-writer unique monotonic values, so the
	// ordered field stays duplicate-free and sequences compare exactly.
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(7000 + w)))
			ctr := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctr++
				lat := 1000000 + w*1000000 + ctr
				id := fmt.Sprintf("o%03d", r.Intn(rows))
				_ = db.WriteTx(func(tx *rime.Tx) error {
					return dev.In(tx).Update(id, func(d *Device) error {
						d.Latency = lat
						return nil
					})
				})
			}
		}(w)
	}

	const readers = 8
	const iters = 100
	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	var rwg sync.WaitGroup
	for w := 0; w < readers; w++ {
		rwg.Add(1)
		go func(w int) {
			defer rwg.Done()
			r := rand.New(rand.NewSource(int64(6000 + w)))
			for i := 0; i < iters; i++ {
				pin := db.ReadTx()
				bound := dev.In(pin)
				// Brute-force oracle: point Gets never touch the ordered index.
				type pair struct {
					id  string
					lat int
				}
				all := make([]pair, 0, rows)
				for k := 0; k < rows; k++ {
					row, err := bound.Get(fmt.Sprintf("o%03d", k))
					if err != nil {
						fail("worker %d iter %d oracle get: %v", w, i, err)
						pin.Close()
						return
					}
					all = append(all, pair{row.ID, row.Latency})
				}
				lo, hi := r.Intn(2100000)-50000, r.Intn(2100000)-50000
				if lo > hi {
					lo, hi = hi, lo
				}
				// 1. Range set equality.
				got, err := dev.Where(rime.And(latF.Ge(lo), latF.Le(hi))).In(pin).Find()
				if err != nil {
					fail("worker %d iter %d range: %v", w, i, err)
					pin.Close()
					return
				}
				wantSet := map[string]int{}
				for _, p := range all {
					if p.lat >= lo && p.lat <= hi {
						wantSet[p.id] = p.lat
					}
				}
				if len(got) != len(wantSet) {
					fail("worker %d iter %d range [%d,%d]: got %d rows want %d",
						w, i, lo, hi, len(got), len(wantSet))
					pin.Close()
					return
				}
				for _, row := range got {
					wantLat, ok := wantSet[row.ID]
					if !ok || wantLat != row.Latency {
						fail("worker %d iter %d range [%d,%d]: row %s lat %d (present=%v)",
							w, i, lo, hi, row.ID, row.Latency, ok)
						pin.Close()
						return
					}
				}
				// 2. Ordered exact sequence with limit/offset.
				klim, koff := 1+r.Intn(10), r.Intn(10)
				gotOrd, err := dev.Where(latF.Ge(lo)).OrderBy(latF).Limit(klim).Offset(koff).In(pin).Find()
				if err != nil {
					fail("worker %d iter %d ordered: %v", w, i, err)
					pin.Close()
					return
				}
				ord := make([]pair, 0, len(all))
				for _, p := range all {
					if p.lat >= lo {
						ord = append(ord, p)
					}
				}
				sort.Slice(ord, func(a, b int) bool { return ord[a].lat < ord[b].lat })
				if koff < len(ord) {
					ord = ord[koff:]
				} else {
					ord = nil
				}
				if len(ord) > klim {
					ord = ord[:klim]
				}
				wantIDs := make([]string, len(ord))
				for j, p := range ord {
					wantIDs[j] = p.id
				}
				gotIDs := make([]string, len(gotOrd))
				for j, row := range gotOrd {
					gotIDs[j] = row.ID
				}
				if !eqSeq(gotIDs, wantIDs) {
					fail("worker %d iter %d ordered lo=%d lim=%d off=%d: %v vs %v",
						w, i, lo, klim, koff, gotIDs, wantIDs)
					pin.Close()
					return
				}
				// 3. Min/max aggregates over the ordered extremes.
				mm, err := dev.Where().In(pin).Aggregate(rime.MinOf(latF), rime.MaxOf(latF))
				if err != nil {
					fail("worker %d iter %d minmax: %v", w, i, err)
					pin.Close()
					return
				}
				mn, mx := all[0].lat, all[0].lat
				for _, p := range all[1:] {
					if p.lat < mn {
						mn = p.lat
					}
					if p.lat > mx {
						mx = p.lat
					}
				}
				if mn != mm[0].(int) || mx != mm[1].(int) {
					fail("worker %d iter %d minmax: got %v want [%d %d]", w, i, mm, mn, mx)
					pin.Close()
					return
				}
				pin.Close()
			}
		}(w)
	}
	rwg.Wait()
	close(stop)
	wg.Wait()
	for {
		select {
		case msg := <-errCh:
			t.Fatalf("concurrent ordered-index failure: %s", msg)
		default:
			return
		}
	}
}

// TestPrefixConcurrent proves the trie index stays consistent with the
// records while writers churn the prefixed field: every reader pins one
// snapshot per iteration and StartsWith queries must match a brute-force
// oracle built from point Gets on that same snapshot. Writers assign
// unique hostnames (the field also carries a unique constraint) and
// retry same-row ErrConflict; params are worker-seeded, so failures
// reproduce.
func TestPrefixConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	const rows = 200
	for i := 0; i < rows; i++ {
		d := Device{ID: fmt.Sprintf("p%03d", i), Hostname: fmt.Sprintf("px-%03d", i),
			Site: "s", Status: i % 5, Latency: i}
		if err := dev.Upsert(&d); err != nil {
			t.Fatal(err)
		}
	}
	hostF := rime.SF[Device](dev, "Hostname")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(8000 + w)))
			ctr := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				ctr++
				host := fmt.Sprintf("w%d-%06d", w, ctr)
				id := fmt.Sprintf("p%03d", r.Intn(rows))
				for {
					err := db.WriteTx(func(tx *rime.Tx) error {
						return dev.In(tx).Update(id, func(d *Device) error {
							d.Hostname = host
							return nil
						})
					})
					if err == nil {
						break
					}
					if !errors.Is(err, rime.ErrConflict) {
						t.Errorf("prefix writer %d: %v", w, err)
						return
					}
					// Same-row contention: retry with the same
					// never-committed hostname.
				}
			}
		}(w)
	}

	prefixes := []string{"w0-", "w1-", "px-", "px-0", "px-1", "w0-000", "w1-999", "zz-no-match"}
	const readers = 8
	const iters = 100
	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	var rwg sync.WaitGroup
	for w := 0; w < readers; w++ {
		rwg.Add(1)
		go func(w int) {
			defer rwg.Done()
			r := rand.New(rand.NewSource(int64(8100 + w)))
			for i := 0; i < iters; i++ {
				pin := db.ReadTx()
				bound := dev.In(pin)
				prefix := prefixes[r.Intn(len(prefixes))]
				got, err := dev.Where(hostF.StartsWith(prefix)).In(pin).Find()
				if err != nil {
					fail("worker %d iter %d prefix %q: %v", w, i, prefix, err)
					pin.Close()
					return
				}
				// Brute-force oracle: point Gets never touch the trie.
				wantSet := map[string]string{}
				for k := 0; k < rows; k++ {
					row, err := bound.Get(fmt.Sprintf("p%03d", k))
					if err != nil {
						fail("worker %d iter %d oracle get: %v", w, i, err)
						pin.Close()
						return
					}
					if strings.HasPrefix(row.Hostname, prefix) {
						wantSet[row.ID] = row.Hostname
					}
				}
				if len(got) != len(wantSet) {
					fail("worker %d iter %d prefix %q: got %d rows want %d",
						w, i, prefix, len(got), len(wantSet))
					pin.Close()
					return
				}
				for _, row := range got {
					wantHost, ok := wantSet[row.ID]
					if !ok || wantHost != row.Hostname {
						fail("worker %d iter %d prefix %q: row %s host %q (present=%v)",
							w, i, prefix, row.ID, row.Hostname, ok)
						pin.Close()
						return
					}
				}
				pin.Close()
			}
		}(w)
	}
	rwg.Wait()
	close(stop)
	wg.Wait()
	for {
		select {
		case msg := <-errCh:
			t.Fatalf("concurrent prefix-index failure: %s", msg)
		default:
			return
		}
	}
}

// TestCompoundConcurrent proves the compound Site+Status index stays
// consistent with the records while writers churn both fields: every reader
// pins one snapshot per iteration and And-equality queries must match a
// brute-force oracle built from point Gets on that same snapshot, with
// full-row equality. A pre-check pins the compound seek plan so the test
// fails fast if the planner ever stops using the index under test.
// Params are worker-seeded, so failures reproduce.
func TestCompoundConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	rng := rand.New(rand.NewSource(31337))
	const rows = 200
	for i := 0; i < rows; i++ {
		d := Device{ID: fmt.Sprintf("m%03d", i), Hostname: fmt.Sprintf("mh-%03d", i),
			Site: fmt.Sprintf("s%d", rng.Intn(4)), Status: rng.Intn(5), Latency: i}
		if err := dev.Upsert(&d); err != nil {
			t.Fatal(err)
		}
	}
	siteF := rime.SF[Device](dev, "Site")
	statusF := rime.OF[Device, int](dev, "Status")

	plan := dev.Where(rime.And(siteF.Eq("s0"), statusF.Eq(0))).Explain()
	if !strings.Contains(plan, "INDEX SEEK site_status") {
		t.Fatalf("compound seek plan lost, got:\n%s", plan)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(8200 + w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("m%03d", r.Intn(rows))
				site, status := fmt.Sprintf("s%d", r.Intn(4)), r.Intn(5)
				for {
					err := db.WriteTx(func(tx *rime.Tx) error {
						return dev.In(tx).Update(id, func(d *Device) error {
							d.Site, d.Status = site, status
							return nil
						})
					})
					if err == nil {
						break
					}
					if !errors.Is(err, rime.ErrConflict) {
						t.Errorf("compound writer %d: %v", w, err)
						return
					}
				}
			}
		}(w)
	}

	const readers = 8
	const iters = 100
	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	var rwg sync.WaitGroup
	for w := 0; w < readers; w++ {
		rwg.Add(1)
		go func(w int) {
			defer rwg.Done()
			r := rand.New(rand.NewSource(int64(8300 + w)))
			for i := 0; i < iters; i++ {
				pin := db.ReadTx()
				bound := dev.In(pin)
				site, status := fmt.Sprintf("s%d", r.Intn(4)), r.Intn(5)
				got, err := dev.Where(rime.And(siteF.Eq(site), statusF.Eq(status))).In(pin).Find()
				if err != nil {
					fail("worker %d iter %d compound (%s,%d): %v", w, i, site, status, err)
					pin.Close()
					return
				}
				// Brute-force oracle: point Gets never touch the compound index.
				wantSet := map[string]Device{}
				for k := 0; k < rows; k++ {
					row, err := bound.Get(fmt.Sprintf("m%03d", k))
					if err != nil {
						fail("worker %d iter %d oracle get: %v", w, i, err)
						pin.Close()
						return
					}
					if row.Site == site && row.Status == status {
						wantSet[row.ID] = *row
					}
				}
				if len(got) != len(wantSet) {
					fail("worker %d iter %d compound (%s,%d): got %d rows want %d",
						w, i, site, status, len(got), len(wantSet))
					pin.Close()
					return
				}
				for _, row := range got {
					want, ok := wantSet[row.ID]
					if !ok || want != *row {
						fail("worker %d iter %d compound (%s,%d): row %s mismatch (present=%v)",
							w, i, site, status, row.ID, ok)
						pin.Close()
						return
					}
				}
				pin.Close()
			}
		}(w)
	}
	rwg.Wait()
	close(stop)
	wg.Wait()
	for {
		select {
		case msg := <-errCh:
			t.Fatalf("concurrent compound-index failure: %s", msg)
		default:
			return
		}
	}
}

// TestAggregateConcurrent proves count, sum, avg, min, max, and grouped
// aggregations stay correct while writers churn the aggregated fields:
// every reader pins one snapshot per iteration and all aggregates must
// match a brute-force oracle built from point Gets on that same snapshot.
// Params are worker-seeded, so failures reproduce.
func TestAggregateConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	rng := rand.New(rand.NewSource(777))
	const rows = 200
	for i := 0; i < rows; i++ {
		d := Device{ID: fmt.Sprintf("g%03d", i), Hostname: fmt.Sprintf("gh-%03d", i),
			Site: fmt.Sprintf("s%d", rng.Intn(4)), Status: rng.Intn(5), Latency: rng.Intn(1000)}
		if err := dev.Upsert(&d); err != nil {
			t.Fatal(err)
		}
	}
	siteF := rime.SF[Device](dev, "Site")
	latF := rime.OF[Device, int](dev, "Latency")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(8400 + w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("g%03d", r.Intn(rows))
				site, status, lat := fmt.Sprintf("s%d", r.Intn(4)), r.Intn(5), r.Intn(1000)
				for {
					err := db.WriteTx(func(tx *rime.Tx) error {
						return dev.In(tx).Update(id, func(d *Device) error {
							d.Site, d.Status, d.Latency = site, status, lat
							return nil
						})
					})
					if err == nil {
						break
					}
					if !errors.Is(err, rime.ErrConflict) {
						t.Errorf("aggregate writer %d: %v", w, err)
						return
					}
				}
			}
		}(w)
	}

	const readers = 8
	const iters = 100
	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	var rwg sync.WaitGroup
	for w := 0; w < readers; w++ {
		rwg.Add(1)
		go func(w int) {
			defer rwg.Done()
			r := rand.New(rand.NewSource(int64(8500 + w)))
			for i := 0; i < iters; i++ {
				pin := db.ReadTx()
				bound := dev.In(pin)
				// Brute-force oracle over the pinned snapshot.
				all := make([]Device, 0, rows)
				for k := 0; k < rows; k++ {
					row, err := bound.Get(fmt.Sprintf("g%03d", k))
					if err != nil {
						fail("worker %d iter %d oracle get: %v", w, i, err)
						pin.Close()
						return
					}
					all = append(all, *row)
				}
				site := fmt.Sprintf("s%d", r.Intn(4))
				var n, sum, mn, mx int
				mn = -1
				groups := map[string][2]int{} // site -> [count, sum]
				for _, d := range all {
					g := groups[d.Site]
					g[0]++
					g[1] += d.Latency
					groups[d.Site] = g
					if d.Site != site {
						continue
					}
					if n == 0 || d.Latency < mn {
						mn = d.Latency
					}
					if n == 0 || d.Latency > mx {
						mx = d.Latency
					}
					n++
					sum += d.Latency
				}
				// 1. Filtered count.
				c, err := dev.Where(siteF.Eq(site)).In(pin).Count()
				if err != nil || c != n {
					fail("worker %d iter %d count %s: got %d (%v) want %d", w, i, site, c, err, n)
					pin.Close()
					return
				}
				// 2. Filtered sum/avg/min/max (empty sets skip min/max).
				if n > 0 {
					vals, err := dev.Where(siteF.Eq(site)).In(pin).Aggregate(
						rime.SumOf(latF), rime.AvgOf(latF), rime.MinOf(latF), rime.MaxOf(latF))
					if err != nil {
						fail("worker %d iter %d aggregate %s: %v", w, i, site, err)
						pin.Close()
						return
					}
					if vals[0].(float64) != float64(sum) ||
						vals[1].(float64) != float64(sum)/float64(n) ||
						vals[2].(int) != mn || vals[3].(int) != mx {
						fail("worker %d iter %d aggregate %s: got %v want [%d %f %d %d]",
							w, i, site, vals, sum, float64(sum)/float64(n), mn, mx)
						pin.Close()
						return
					}
				}
				// 3. Grouped count/sum per site.
				got, err := dev.Where().In(pin).GroupBy(siteF).Aggregate(rime.Count[Device](), rime.SumOf(latF))
				if err != nil {
					fail("worker %d iter %d groupby: %v", w, i, err)
					pin.Close()
					return
				}
				if len(got) != len(groups) {
					fail("worker %d iter %d groupby: %d groups want %d", w, i, len(got), len(groups))
					pin.Close()
					return
				}
				for _, g := range got {
					key, ok := g.Keys[0].(string)
					if !ok {
						fail("worker %d iter %d groupby: key %v not a string", w, i, g.Keys)
						pin.Close()
						return
					}
					want, ok := groups[key]
					if !ok {
						fail("worker %d iter %d groupby: unexpected group %q", w, i, key)
						pin.Close()
						return
					}
					if g.Values[0].(int) != want[0] || g.Values[1].(float64) != float64(want[1]) {
						fail("worker %d iter %d groupby %q: got %v want %v", w, i, key, g.Values, want)
						pin.Close()
						return
					}
					delete(groups, key)
				}
				pin.Close()
			}
		}(w)
	}
	rwg.Wait()
	close(stop)
	wg.Wait()
	for {
		select {
		case msg := <-errCh:
			t.Fatalf("concurrent aggregate failure: %s", msg)
		default:
			return
		}
	}
}

// TestBulkOpEquivalence proves batch and query-scoped writes (UpsertMany,
// query Update/Delete, DeleteMany) leave exactly the model-predicted state,
// checked through the full read-path verifier after every round.
func TestBulkOpEquivalence(t *testing.T) {
	w := newEqWorld(t, 1234)
	abort := errors.New("bulk rollback")
	const rounds = 40
	for i := 0; i < rounds; i++ {
		rollback := i%8 == 7
		staged := cloneDevices(w.model)
		stagedSites := cloneSites(w.siteModel)
		err := w.db.WriteTx(func(tx *rime.Tx) error {
			n := 1 + w.rng.Intn(6)
			recs := make([]*Device, 0, n)
			for k := 0; k < n; k++ {
				key := fmt.Sprintf("k%d", w.rng.Intn(12))
				if k > 0 && w.rng.Intn(5) == 0 {
					key = recs[w.rng.Intn(len(recs))].ID // duplicate in batch
				}
				w.latCtr++
				recs = append(recs, &Device{
					ID:       key,
					Hostname: "h-" + key,
					Site:     fmt.Sprintf("s%d", w.rng.Intn(5)),
					Status:   w.rng.Intn(5),
					Latency:  w.latCtr,
				})
			}
			if err := w.dev.In(tx).UpsertMany(recs); err != nil {
				return err
			}
			for _, r := range recs {
				staged[r.ID] = *r // sequential last-wins mirror
			}
			if w.rng.Intn(2) == 0 {
				site := fmt.Sprintf("s%d", w.rng.Intn(5))
				status := w.rng.Intn(5)
				want := 0
				for _, d := range staged {
					if d.Site == site {
						want++
					}
				}
				n, err := w.dev.Where(rime.SF[Device](w.dev, "Site").Eq(site)).In(tx).Update(func(d *Device) error {
					d.Status = status
					return nil
				})
				if err != nil {
					return err
				}
				if n != want {
					t.Fatalf("round %d query-update count %d want %d", i, n, want)
				}
				for id, d := range staged {
					if d.Site == site {
						d.Status = status
						staged[id] = d
					}
				}
			}
			if w.rng.Intn(2) == 0 {
				cut := w.rng.Intn(w.latCtr + 1)
				want := 0
				for _, d := range staged {
					if d.Latency < cut {
						want++
					}
				}
				n, err := w.dev.Where(rime.OF[Device, int](w.dev, "Latency").Lt(cut)).In(tx).Delete()
				if err != nil {
					return err
				}
				if n != want {
					t.Fatalf("round %d query-delete count %d want %d", i, n, want)
				}
				for id, d := range staged {
					if d.Latency < cut {
						delete(staged, id)
					}
				}
			}
			if w.rng.Intn(3) == 0 {
				var srecs []*Site
				for k := 0; k < w.rng.Intn(3); k++ {
					id := fmt.Sprintf("s%d", w.rng.Intn(4))
					srecs = append(srecs, &Site{ID: id, Region: []string{"East", "West"}[w.rng.Intn(2)]})
				}
				if err := w.sites.In(tx).UpsertMany(srecs); err != nil {
					return err
				}
				for _, r := range srecs {
					stagedSites[r.ID] = *r
				}
			}
			w.verify(tx, staged, stagedSites, i, "bulk-overlay")
			if rollback {
				return abort
			}
			return nil
		})
		if rollback {
			if !errors.Is(err, abort) {
				t.Fatalf("round %d rollback: %v", i, err)
			}
			w.hooks = w.hooks[:0]
			w.verify(nil, w.model, w.siteModel, i, "bulk-rollback")
			continue
		}
		if err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
		w.model, w.siteModel = staged, stagedSites
		w.hooks = w.hooks[:0] // hook streams are asserted by TestModelEquivalence
		w.verify(nil, w.model, w.siteModel, i, "bulk-commit")
	}
	// DeleteMany is atomic: one missing key fails the batch and stages nothing.
	if err := w.dev.DeleteMany([]any{"k0", "k-nope"}); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("DeleteMany with missing key: %v", err)
	}
	w.verify(nil, w.model, w.siteModel, rounds, "bulk-atomic")
	present := make([]any, 0, len(w.model))
	for id := range w.model {
		present = append(present, id)
	}
	if len(present) > 0 {
		if err := w.dev.DeleteMany(present[:min(3, len(present))]); err != nil {
			t.Fatalf("DeleteMany: %v", err)
		}
		for _, id := range present[:min(3, len(present))] {
			delete(w.model, id.(string))
		}
		w.verify(nil, w.model, w.siteModel, rounds, "bulk-deletemany")
	}
}

// TestEventStreamCompleteness proves the async event bus is a complete,
// exactly-once WAL source under concurrent writers: every committed change
// emits exactly one OnCommitted event, aborted transactions emit nothing,
// per-key event order matches each writer's commit order, and each commit
// TxID carries exactly its own transaction's batch. Global TxID delivery
// order is intentionally NOT asserted: fireAfter runs after the commit lock
// is released, so concurrent committers may emit out of TxID order.
func TestEventStreamCompleteness(t *testing.T) {
	db, dev := openDevices(t, rime.WithEventQueueSize(16384))
	t.Cleanup(func() { db.Close() })

	type changeRec struct {
		op        rime.Operation
		key       string
		old, newv Device
		hasOld    bool
		hasNew    bool
		txid      rime.TxID
	}
	var mu sync.Mutex
	var received []changeRec
	dev.OnCommitted(func(ch rime.Change[Device]) {
		var r changeRec
		r.op = ch.Operation
		r.txid = ch.TxID
		if ch.Old != nil {
			r.old, r.hasOld = *ch.Old, true
			r.key = ch.Old.ID
		}
		if ch.New != nil {
			r.newv, r.hasNew = *ch.New, true
			r.key = ch.New.ID
		}
		mu.Lock()
		received = append(received, r)
		mu.Unlock()
	})

	const writers = 8
	const txns = 40
	abortErr := errors.New("stream rollback probe")

	// logs[wid] holds that writer's committed batches in commit order.
	// Keys are writer-disjoint ("w<wid>-k<seq>"), so no conflicts occur.
	logs := make([][][]changeRec, writers)
	var wg sync.WaitGroup
	for wid := 0; wid < writers; wid++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(1000 + wid)))
			alive := map[string]Device{}
			var aliveKeys []string
			seq := 0
			mkDevice := func(key string, status, lat int) Device {
				return Device{ID: key, Hostname: "h-" + key, Site: "s", Status: status, Latency: lat}
			}
			for i := 0; i < txns; i++ {
				abort := rng.Intn(10) == 0
				n := 1 + rng.Intn(3)
				// Scratch model: staged ops apply here, committed to the
				// real model only when the transaction commits.
				scratch := make(map[string]Device, len(alive)+n)
				for k, v := range alive {
					scratch[k] = v
				}
				scratchKeys := append([]string(nil), aliveKeys...)
				touched := map[string]bool{}
				type staged struct {
					op       rime.Operation
					key      string
					old, new Device
					hasOld   bool
				}
				var batch []staged
				err := db.WriteTx(func(tx *rime.Tx) error {
					bound := dev.In(tx)
					for k := 0; k < n; k++ {
						seq++
						status := wid*100000 + seq
						if abort {
							// Sentinel range [70000,80000) is unreachable
							// by committed writes; any leak is detectable.
							status = 70000 + wid
						}
						pickUntouched := func() (string, bool) {
							for tries := 0; tries < 8 && len(scratchKeys) > 0; tries++ {
								c := scratchKeys[rng.Intn(len(scratchKeys))]
								if !touched[c] {
									return c, true
								}
							}
							return "", false
						}
						switch key, ok := pickUntouched(); {
						case !ok || rng.Intn(3) == 0:
							key := fmt.Sprintf("w%d-k%d", wid, seq)
							d := mkDevice(key, status, seq)
							if err := bound.Insert(&d); err != nil {
								return err
							}
							batch = append(batch, staged{op: rime.OpInsert, key: key, new: d})
							scratch[key] = d
							scratchKeys = append(scratchKeys, key)
							touched[key] = true
						case rng.Intn(2) == 0:
							old := scratch[key]
							d := mkDevice(key, status, seq)
							if err := bound.Update(key, func(r *Device) error { *r = d; return nil }); err != nil {
								return err
							}
							batch = append(batch, staged{op: rime.OpUpdate, key: key, old: old, new: d, hasOld: true})
							scratch[key] = d
							touched[key] = true
						default:
							old := scratch[key]
							if err := bound.Delete(key); err != nil {
								return err
							}
							batch = append(batch, staged{op: rime.OpDelete, key: key, old: old, hasOld: true})
							delete(scratch, key)
							for j, c := range scratchKeys {
								if c == key {
									scratchKeys = append(scratchKeys[:j], scratchKeys[j+1:]...)
									break
								}
							}
							touched[key] = true
						}
					}
					if abort {
						return abortErr
					}
					return nil
				})
				if abort {
					if !errors.Is(err, abortErr) {
						t.Errorf("writer %d txn %d: abort returned %v", wid, i, err)
						return
					}
					continue
				}
				if err != nil {
					t.Errorf("writer %d txn %d: %v", wid, i, err)
					return
				}
				alive, aliveKeys = scratch, scratchKeys
				var recs []changeRec
				for _, s := range batch {
					recs = append(recs, changeRec{op: s.op, key: s.key, old: s.old, newv: s.new, hasOld: s.hasOld, hasNew: s.op != rime.OpDelete})
				}
				logs[wid] = append(logs[wid], recs)
			}
		}(wid)
	}
	wg.Wait()

	total := 0
	nTxns := 0
	for _, wl := range logs {
		nTxns += len(wl)
		for _, b := range wl {
			total += len(b)
		}
	}
	if nTxns == 0 || total == 0 {
		t.Fatal("no committed transactions recorded")
	}

	// 1. Every committed change arrives, exactly once.
	deadline := time.Now().Add(30 * time.Second)
	for {
		mu.Lock()
		got := len(received)
		mu.Unlock()
		if got >= total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d events delivered, want %d (lost events)", got, total)
		}
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // quiescence: no duplicates or late events
	mu.Lock()
	final := append([]changeRec(nil), received...)
	mu.Unlock()
	if len(final) != total {
		t.Fatalf("event count = %d, want exactly %d (duplicates or late events)", len(final), total)
	}

	// 2. Nothing staged by an aborted transaction may surface.
	for _, r := range final {
		if r.hasNew && r.newv.Status >= 70000 && r.newv.Status < 80000 {
			t.Fatalf("aborted sentinel surfaced in event stream: %+v", r)
		}
		if r.hasOld && r.old.Status >= 70000 && r.old.Status < 80000 {
			t.Fatalf("aborted sentinel in event Old: %+v", r)
		}
	}

	// 3. Per-key event order matches each writer's commit order.
	wantByKey := map[string][]changeRec{}
	for _, wl := range logs {
		for _, b := range wl {
			for _, r := range b {
				wantByKey[r.key] = append(wantByKey[r.key], r)
			}
		}
	}
	gotByKey := map[string][]changeRec{}
	for _, r := range final {
		gotByKey[r.key] = append(gotByKey[r.key], r)
	}
	if len(gotByKey) != len(wantByKey) {
		t.Fatalf("event keys = %d, want %d", len(gotByKey), len(wantByKey))
	}
	for key, want := range wantByKey {
		got := gotByKey[key]
		if len(got) != len(want) {
			t.Fatalf("key %s: %d events, want %d", key, len(got), len(want))
		}
		for i := range want {
			if got[i].op != want[i].op || got[i].hasOld != want[i].hasOld ||
				got[i].hasNew != want[i].hasNew ||
				(got[i].hasOld && got[i].old != want[i].old) ||
				(got[i].hasNew && got[i].newv != want[i].newv) {
				t.Fatalf("key %s event %d:\n got %+v\nwant %+v", key, i, got[i], want[i])
			}
		}
	}

	// 4. Each commit TxID carries exactly its own transaction's batch.
	byTx := map[rime.TxID][]changeRec{}
	for _, r := range final {
		byTx[r.txid] = append(byTx[r.txid], r)
	}
	if len(byTx) != nTxns {
		t.Fatalf("distinct event TxIDs = %d, want %d committed txns", len(byTx), nTxns)
	}
	used := make([][]bool, writers)
	for i := range used {
		used[i] = make([]bool, len(logs[i]))
	}
	for txid, evs := range byTx {
		matched := false
		for wid, wl := range logs {
		batch:
			for bi, b := range wl {
				if used[wid][bi] || len(b) != len(evs) {
					continue
				}
				byKey := make(map[string]changeRec, len(b))
				for _, r := range b {
					byKey[r.key] = r
				}
				for _, e := range evs {
					w, found := byKey[e.key]
					if !found || e.op != w.op || e.hasOld != w.hasOld || e.hasNew != w.hasNew ||
						(e.hasOld && e.old != w.old) || (e.hasNew && e.newv != w.newv) {
						continue batch
					}
				}
				used[wid][bi] = true
				matched = true
				break
			}
			if matched {
				break
			}
		}
		if !matched {
			t.Fatalf("TxID %d event group matches no committed batch: %+v", txid, evs)
		}
	}
}

// TestHookStreamConcurrent is the synchronous counterpart of
// TestEventStreamCompleteness: under concurrent writers with aborts, Before
// hooks fire exactly once per staged op (including rolled-back ones),
// After hooks fire exactly once per committed change with per-key FIFO
// order, BeforeCommit fires once per committing transaction, AfterCommit
// carries each batch's TxID, and every After hook observes its own change
// through a read (committed state is visible inside After hooks).
func TestHookStreamConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	t.Cleanup(func() { db.Close() })

	type changeRec struct {
		op        rime.Operation
		key       string
		old, newv Device
		hasOld    bool
		hasNew    bool
	}
	var mu sync.Mutex
	var afters []changeRec
	var aftersByKey = map[string][]changeRec{}
	var beforeKeys []changeRec
	var afterSaves int
	var beforeCommits int
	var commitIDs []rime.TxID
	hookErr := make(chan string, 1)
	failHook := func(format string, args ...any) {
		select {
		case hookErr <- fmt.Sprintf(format, args...):
		default:
		}
	}
	recordAfter := func(op rime.Operation, o, n *Device) {
		var r changeRec
		r.op = op
		if o != nil {
			r.old, r.hasOld = *o, true
			r.key = o.ID
		}
		if n != nil {
			r.newv, r.hasNew = *n, true
			r.key = n.ID
		}
		// Committed state is visible inside After hooks; keys are
		// writer-disjoint and each writer is sequential, so the read
		// must observe exactly this change.
		switch op {
		case rime.OpInsert, rime.OpUpdate:
			got, err := dev.Get(r.key)
			if err != nil || *got != r.newv {
				failHook("after %v %s: Get = %+v %v, want %+v", op, r.key, got, err, r.newv)
				return
			}
		case rime.OpDelete:
			if _, err := dev.Get(r.key); !errors.Is(err, rime.ErrNotFound) {
				failHook("after delete %s: Get err = %v, want ErrNotFound", r.key, err)
				return
			}
		}
		mu.Lock()
		afters = append(afters, r)
		aftersByKey[r.key] = append(aftersByKey[r.key], r)
		mu.Unlock()
	}
	dev.BeforeInsert(func(d *Device) error {
		mu.Lock()
		beforeKeys = append(beforeKeys, changeRec{op: rime.OpInsert, key: d.ID})
		mu.Unlock()
		return nil
	})
	dev.BeforeUpdate(func(o, n *Device) error {
		mu.Lock()
		beforeKeys = append(beforeKeys, changeRec{op: rime.OpUpdate, key: n.ID})
		mu.Unlock()
		return nil
	})
	dev.BeforeDelete(func(d *Device) error {
		mu.Lock()
		beforeKeys = append(beforeKeys, changeRec{op: rime.OpDelete, key: d.ID})
		mu.Unlock()
		return nil
	})
	dev.BeforeCommit(func(*rime.Tx) error {
		mu.Lock()
		beforeCommits++
		mu.Unlock()
		return nil
	})
	dev.AfterInsert(func(d *Device) { recordAfter(rime.OpInsert, nil, d) })
	dev.AfterUpdate(func(o, n *Device) { recordAfter(rime.OpUpdate, o, n) })
	dev.AfterDelete(func(d *Device) { recordAfter(rime.OpDelete, d, nil) })
	dev.AfterSave(func(rime.Change[Device]) {
		mu.Lock()
		afterSaves++
		mu.Unlock()
	})
	dev.AfterCommit(func(id rime.TxID) {
		mu.Lock()
		commitIDs = append(commitIDs, id)
		mu.Unlock()
	})

	const writers = 4
	const txns = 30
	abortErr := errors.New("hook rollback probe")
	logs := make([][][]changeRec, writers)
	staged := make([][]changeRec, writers)
	var wg sync.WaitGroup
	for wid := 0; wid < writers; wid++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(2000 + wid)))
			alive := map[string]Device{}
			var aliveKeys []string
			seq := 0
			mkDevice := func(key string, status, lat int) Device {
				return Device{ID: key, Hostname: "hh-" + key, Site: "s", Status: status, Latency: lat}
			}
			for i := 0; i < txns; i++ {
				abort := rng.Intn(10) == 0
				n := 1 + rng.Intn(3)
				scratch := make(map[string]Device, len(alive)+n)
				for k, v := range alive {
					scratch[k] = v
				}
				scratchKeys := append([]string(nil), aliveKeys...)
				touched := map[string]bool{}
				type stagedOp struct {
					op       rime.Operation
					key      string
					old, new Device
					hasOld   bool
				}
				var batch []stagedOp
				err := db.WriteTx(func(tx *rime.Tx) error {
					bound := dev.In(tx)
					for k := 0; k < n; k++ {
						seq++
						status := wid*100000 + seq
						pickUntouched := func() (string, bool) {
							for tries := 0; tries < 8 && len(scratchKeys) > 0; tries++ {
								c := scratchKeys[rng.Intn(len(scratchKeys))]
								if !touched[c] {
									return c, true
								}
							}
							return "", false
						}
						switch key, ok := pickUntouched(); {
						case !ok || rng.Intn(3) == 0:
							key := fmt.Sprintf("hw%d-k%d", wid, seq)
							d := mkDevice(key, status, seq)
							if err := bound.Insert(&d); err != nil {
								return err
							}
							batch = append(batch, stagedOp{op: rime.OpInsert, key: key, new: d})
							scratch[key] = d
							scratchKeys = append(scratchKeys, key)
							touched[key] = true
						case rng.Intn(2) == 0:
							old := scratch[key]
							d := mkDevice(key, status, seq)
							if err := bound.Update(key, func(r *Device) error { *r = d; return nil }); err != nil {
								return err
							}
							batch = append(batch, stagedOp{op: rime.OpUpdate, key: key, old: old, new: d, hasOld: true})
							scratch[key] = d
							touched[key] = true
						default:
							old := scratch[key]
							if err := bound.Delete(key); err != nil {
								return err
							}
							batch = append(batch, stagedOp{op: rime.OpDelete, key: key, old: old, hasOld: true})
							delete(scratch, key)
							for j, c := range scratchKeys {
								if c == key {
									scratchKeys = append(scratchKeys[:j], scratchKeys[j+1:]...)
									break
								}
							}
							touched[key] = true
						}
					}
					if abort {
						return abortErr
					}
					return nil
				})
				var recs []changeRec
				for _, s := range batch {
					rec := changeRec{op: s.op, key: s.key, old: s.old, newv: s.new, hasOld: s.hasOld, hasNew: s.op != rime.OpDelete}
					staged[wid] = append(staged[wid], changeRec{op: s.op, key: s.key})
					recs = append(recs, rec)
				}
				if abort {
					if !errors.Is(err, abortErr) {
						t.Errorf("writer %d txn %d: abort returned %v", wid, i, err)
						return
					}
					continue
				}
				if err != nil {
					t.Errorf("writer %d txn %d: %v", wid, i, err)
					return
				}
				alive, aliveKeys = scratch, scratchKeys
				logs[wid] = append(logs[wid], recs)
			}
		}(wid)
	}
	wg.Wait()
	select {
	case msg := <-hookErr:
		t.Fatalf("after-hook visibility: %s", msg)
	default:
	}

	total, nTxns := 0, 0
	wantByKey := map[string][]changeRec{}
	batchLens := map[int]int{}
	for _, wl := range logs {
		for _, b := range wl {
			nTxns++
			total += len(b)
			batchLens[len(b)]++
			for _, r := range b {
				wantByKey[r.key] = append(wantByKey[r.key], r)
			}
		}
	}
	nStaged := 0
	wantBefore := map[changeRec]int{}
	for _, sl := range staged {
		for _, r := range sl {
			nStaged++
			wantBefore[r]++
		}
	}
	if nTxns == 0 || total == 0 {
		t.Fatal("no committed transactions recorded")
	}

	mu.Lock()
	defer mu.Unlock()
	// 1. Before hooks fire once per staged op, aborted ones included.
	if len(beforeKeys) != nStaged {
		t.Fatalf("before-hook calls = %d, want %d staged ops", len(beforeKeys), nStaged)
	}
	gotBefore := map[changeRec]int{}
	for _, r := range beforeKeys {
		gotBefore[r]++
	}
	for r, want := range wantBefore {
		if gotBefore[r] != want {
			t.Fatalf("before-hook %v: %d calls, want %d", r, gotBefore[r], want)
		}
	}
	// 2. BeforeCommit fires once per committing transaction.
	if beforeCommits != nTxns {
		t.Fatalf("BeforeCommit calls = %d, want %d committed txns", beforeCommits, nTxns)
	}
	// 3. After hooks fire exactly once per committed change, per-key FIFO.
	if len(afters) != total {
		t.Fatalf("after-hook calls = %d, want %d committed changes", len(afters), total)
	}
	if afterSaves != total {
		t.Fatalf("AfterSave calls = %d, want %d", afterSaves, total)
	}
	if len(aftersByKey) != len(wantByKey) {
		t.Fatalf("after-hook keys = %d, want %d", len(aftersByKey), len(wantByKey))
	}
	for key, want := range wantByKey {
		got := aftersByKey[key]
		if len(got) != len(want) {
			t.Fatalf("key %s: %d after-hooks, want %d", key, len(got), len(want))
		}
		for i := range want {
			if got[i].op != want[i].op || got[i].hasOld != want[i].hasOld ||
				got[i].hasNew != want[i].hasNew ||
				(got[i].hasOld && got[i].old != want[i].old) ||
				(got[i].hasNew && got[i].newv != want[i].newv) {
				t.Fatalf("key %s hook %d:\n got %+v\nwant %+v", key, i, got[i], want[i])
			}
		}
	}
	// 4. AfterCommit carries each batch exactly once (histogram of
	// per-TxID call counts matches the committed batch lengths).
	if len(commitIDs) != total {
		t.Fatalf("AfterCommit calls = %d, want %d", len(commitIDs), total)
	}
	perTx := map[rime.TxID]int{}
	for _, id := range commitIDs {
		perTx[id]++
	}
	if len(perTx) != nTxns {
		t.Fatalf("distinct AfterCommit TxIDs = %d, want %d", len(perTx), nTxns)
	}
	gotLens := map[int]int{}
	for _, c := range perTx {
		gotLens[c]++
	}
	for l, want := range batchLens {
		if gotLens[l] != want {
			t.Fatalf("batches of length %d: %d, want %d", l, gotLens[l], want)
		}
	}
}

// TestJoinConcurrent proves inner, left, and filtered joins stay complete
// and correct while writers churn the join keys: every reader pins one
// snapshot per iteration and all three joins must match a brute-force
// pair-set oracle built from point Gets on that same snapshot. Devices
// transition between matched sites and an orphan key, so both the match
// and null arms are exercised every iteration. Writers are seeded, so
// failures reproduce.
func TestJoinConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	sites, err := rime.Register[Site](db, rime.WithTableName[Site]("sites"))
	if err != nil {
		t.Fatal(err)
	}
	siteIDs := []string{"s0", "s1", "s2", "s3", "s4", "s5"}
	for i, id := range siteIDs {
		region := "West"
		if i%2 == 0 {
			region = "East"
		}
		if err := sites.Upsert(&Site{ID: id, Region: region}); err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewSource(60606))
	const rows = 200
	keys := []string{"s0", "s1", "s2", "s3", "s4", "s5", "orphan"}
	for i := 0; i < rows; i++ {
		d := Device{ID: fmt.Sprintf("j%03d", i), Hostname: fmt.Sprintf("jh-%03d", i),
			Site: keys[rng.Intn(len(keys))], Status: rng.Intn(5), Latency: i}
		if err := dev.Upsert(&d); err != nil {
			t.Fatal(err)
		}
	}
	dsite := rime.SF[Device](dev, "Site")
	sid := rime.SF[Site](sites, "ID")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(8600 + w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("j%03d", r.Intn(rows))
				site := keys[r.Intn(len(keys))]
				for {
					err := db.WriteTx(func(tx *rime.Tx) error {
						return dev.In(tx).Update(id, func(d *Device) error {
							d.Site = site
							return nil
						})
					})
					if err == nil {
						break
					}
					if !errors.Is(err, rime.ErrConflict) {
						t.Errorf("join writer %d: %v", w, err)
						return
					}
				}
			}
		}(w)
	}

	const readers = 8
	const iters = 100
	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	var rwg sync.WaitGroup
	for w := 0; w < readers; w++ {
		rwg.Add(1)
		go func(w int) {
			defer rwg.Done()
			for i := 0; i < iters; i++ {
				pin := db.ReadTx()
				devB := dev.In(pin)
				siteB := sites.In(pin)
				// Brute-force oracle over the pinned snapshot.
				devModel := make(map[string]Device, rows)
				for k := 0; k < rows; k++ {
					id := fmt.Sprintf("j%03d", k)
					row, err := devB.Get(id)
					if err != nil {
						fail("worker %d iter %d oracle device get: %v", w, i, err)
						pin.Close()
						return
					}
					devModel[id] = *row
				}
				siteModel := make(map[string]Site, len(siteIDs))
				for _, id := range siteIDs {
					row, err := siteB.Get(id)
					if err != nil {
						fail("worker %d iter %d oracle site get: %v", w, i, err)
						pin.Close()
						return
					}
					siteModel[id] = *row
				}
				// 1. Inner join as an exact pair set.
				inner, err := rime.InnerJoinOn(devB, dsite, siteB, sid)
				if err != nil {
					fail("worker %d iter %d inner: %v", w, i, err)
					pin.Close()
					return
				}
				wantInner := map[string]bool{}
				for id, d := range devModel {
					if _, ok := siteModel[d.Site]; ok {
						wantInner[id+"\x00"+d.Site] = true
					}
				}
				if len(inner) != len(wantInner) {
					fail("worker %d iter %d inner: %d pairs want %d", w, i, len(inner), len(wantInner))
					pin.Close()
					return
				}
				for _, r := range inner {
					if r.Left.Site != r.Right.ID {
						fail("worker %d iter %d inner mismatch %+v %+v", w, i, r.Left, r.Right)
						pin.Close()
						return
					}
					if !wantInner[r.Left.ID+"\x00"+r.Right.ID] {
						fail("worker %d iter %d inner phantom %s/%s", w, i, r.Left.ID, r.Right.ID)
						pin.Close()
						return
					}
					if devModel[r.Left.ID] != *r.Left || siteModel[r.Right.ID] != *r.Right {
						fail("worker %d iter %d inner stale row %s", w, i, r.Left.ID)
						pin.Close()
						return
					}
				}
				// 2. Left join: every device exactly once, orphans null.
				left, err := rime.LeftJoinOn(devB, dsite, siteB, sid)
				if err != nil {
					fail("worker %d iter %d left: %v", w, i, err)
					pin.Close()
					return
				}
				if len(left) != len(devModel) {
					fail("worker %d iter %d left: %d rows want %d", w, i, len(left), len(devModel))
					pin.Close()
					return
				}
				seenLeft := map[string]bool{}
				for _, r := range left {
					if seenLeft[r.Left.ID] {
						fail("worker %d iter %d left: duplicate %s", w, i, r.Left.ID)
						pin.Close()
						return
					}
					seenLeft[r.Left.ID] = true
					want, ok := siteModel[r.Left.Site]
					if !ok {
						if r.Right != nil {
							fail("worker %d iter %d left: orphan %s matched %+v", w, i, r.Left.ID, r.Right)
							pin.Close()
							return
						}
						continue
					}
					if r.Right == nil || *r.Right != want || devModel[r.Left.ID] != *r.Left {
						fail("worker %d iter %d left %s mismatch", w, i, r.Left.ID)
						pin.Close()
						return
					}
				}
				// 3. Filtered inner join (East region only).
				east, err := rime.InnerJoinOn(devB, dsite, siteB, sid,
					func(d *Device, s *Site) bool { return s.Region == "East" })
				if err != nil {
					fail("worker %d iter %d east: %v", w, i, err)
					pin.Close()
					return
				}
				wantEast := map[string]bool{}
				for id, d := range devModel {
					if s, ok := siteModel[d.Site]; ok && s.Region == "East" {
						wantEast[id+"\x00"+d.Site] = true
					}
				}
				if len(east) != len(wantEast) {
					fail("worker %d iter %d east: %d pairs want %d", w, i, len(east), len(wantEast))
					pin.Close()
					return
				}
				for _, r := range east {
					if !wantEast[r.Left.ID+"\x00"+r.Right.ID] {
						fail("worker %d iter %d east phantom %s/%s", w, i, r.Left.ID, r.Right.ID)
						pin.Close()
						return
					}
				}
				pin.Close()
			}
		}(w)
	}
	rwg.Wait()
	close(stop)
	wg.Wait()
	for {
		select {
		case msg := <-errCh:
			t.Fatalf("concurrent join failure: %s", msg)
		default:
			return
		}
	}
}

// TestHashConcurrent proves the hash index stays consistent with the
// records while writers churn the indexed field: every reader pins one
// snapshot per iteration and Eq and In queries must match a brute-force
// oracle built from point Gets on that same snapshot, with full-row
// equality. A pre-check pins the hash seek plan so the test fails fast
// if the planner ever stops using the index under test. Params are
// worker-seeded, so failures reproduce.
func TestHashConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	rng := rand.New(rand.NewSource(90909))
	const rows = 200
	for i := 0; i < rows; i++ {
		d := Device{ID: fmt.Sprintf("h%03d", i), Hostname: fmt.Sprintf("hh-%03d", i),
			Site: fmt.Sprintf("s%d", rng.Intn(4)), Status: rng.Intn(5), Latency: i}
		if err := dev.Upsert(&d); err != nil {
			t.Fatal(err)
		}
	}
	siteF := rime.SF[Device](dev, "Site")

	plan := dev.Where(siteF.Eq("s0")).Explain()
	if !strings.Contains(plan, "INDEX SEEK Site") {
		t.Fatalf("hash seek plan lost, got:\n%s", plan)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(8700 + w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("h%03d", r.Intn(rows))
				site := fmt.Sprintf("s%d", r.Intn(4))
				for {
					err := db.WriteTx(func(tx *rime.Tx) error {
						return dev.In(tx).Update(id, func(d *Device) error {
							d.Site = site
							return nil
						})
					})
					if err == nil {
						break
					}
					if !errors.Is(err, rime.ErrConflict) {
						t.Errorf("hash writer %d: %v", w, err)
						return
					}
				}
			}
		}(w)
	}

	const readers = 8
	const iters = 100
	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	check := func(w, i int, pin *rime.Tx, bound rime.BoundTable[Device], label string, got []*Device, keep func(Device) bool) bool {
		// Brute-force oracle: point Gets never touch the hash index.
		wantSet := map[string]Device{}
		for k := 0; k < rows; k++ {
			row, err := bound.Get(fmt.Sprintf("h%03d", k))
			if err != nil {
				fail("worker %d iter %d %s oracle get: %v", w, i, label, err)
				return false
			}
			if keep(*row) {
				wantSet[row.ID] = *row
			}
		}
		if len(got) != len(wantSet) {
			fail("worker %d iter %d %s: got %d rows want %d", w, i, label, len(got), len(wantSet))
			return false
		}
		for _, row := range got {
			want, ok := wantSet[row.ID]
			if !ok || want != *row {
				fail("worker %d iter %d %s: row %s mismatch (present=%v)", w, i, label, row.ID, ok)
				return false
			}
		}
		return true
	}
	var rwg sync.WaitGroup
	for w := 0; w < readers; w++ {
		rwg.Add(1)
		go func(w int) {
			defer rwg.Done()
			r := rand.New(rand.NewSource(int64(8800 + w)))
			for i := 0; i < iters; i++ {
				pin := db.ReadTx()
				bound := dev.In(pin)
				site := fmt.Sprintf("s%d", r.Intn(4))
				got, err := dev.Where(siteF.Eq(site)).In(pin).Find()
				if err != nil {
					fail("worker %d iter %d eq %s: %v", w, i, site, err)
					pin.Close()
					return
				}
				if !check(w, i, pin, bound, "eq "+site, got, func(d Device) bool { return d.Site == site }) {
					pin.Close()
					return
				}
				a, b := fmt.Sprintf("s%d", r.Intn(4)), fmt.Sprintf("s%d", r.Intn(4))
				got, err = dev.Where(siteF.In(a, b)).In(pin).Find()
				if err != nil {
					fail("worker %d iter %d in (%s,%s): %v", w, i, a, b, err)
					pin.Close()
					return
				}
				if !check(w, i, pin, bound, "in ("+a+","+b+")", got,
					func(d Device) bool { return d.Site == a || d.Site == b }) {
					pin.Close()
					return
				}
				pin.Close()
			}
		}(w)
	}
	rwg.Wait()
	close(stop)
	wg.Wait()
	for {
		select {
		case msg := <-errCh:
			t.Fatalf("concurrent hash-index failure: %s", msg)
		default:
			return
		}
	}
}

// TestBulkConcurrent proves batch writes stay atomic and exact under
// concurrency: writers on disjoint key ranges mix UpsertMany, DeleteMany,
// query-scoped Update, and scope Delete+reseed. Query-scoped counts must
// match the writer's live set exactly, a concurrent reader must never
// observe a value no writer wrote (torn-write detector over per-key
// value sets), and the final state must equal every writer's model
// exactly. Writers are seeded, so failures reproduce.
func TestBulkConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	const writers = 4
	const keysPer = 100
	const iters = 60
	siteOf := func(w int) string { return fmt.Sprintf("bw%d", w) }
	idOf := func(w, k int) string { return fmt.Sprintf("b%d-%03d", w, k) }
	for w := 0; w < writers; w++ {
		for k := 0; k < keysPer; k++ {
			d := Device{ID: idOf(w, k), Hostname: fmt.Sprintf("bh-%d-%03d", w, k),
				Site: siteOf(w)}
			if err := dev.Upsert(&d); err != nil {
				t.Fatal(err)
			}
		}
	}
	siteF := rime.SF[Device](dev, "Site")

	type pair struct{ status, lat int }
	var mu sync.Mutex
	// ever[wid][key] holds every (status,latency) ever committed to key.
	ever := make([]map[string]map[pair]bool, writers)
	models := make([]map[string]pair, writers)
	for w := 0; w < writers; w++ {
		ever[w] = map[string]map[pair]bool{}
		models[w] = map[string]pair{}
		for k := 0; k < keysPer; k++ {
			id := idOf(w, k)
			ever[w][id] = map[pair]bool{{}: true}
			models[w][id] = pair{}
		}
	}
	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(8900 + w)))
			alive := map[string]bool{}
			for k := 0; k < keysPer; k++ {
				alive[idOf(w, k)] = true
			}
			aliveList := func() []string {
				out := make([]string, 0, len(alive))
				for id := range alive {
					out = append(out, id)
				}
				return out
			}
			seq := 0
			for i := 0; i < iters; i++ {
				switch r.Intn(100) {
				case 0, 1, 2, 3, 4:
					// Query-scoped delete + full reseed.
					if len(alive) == 0 {
						continue
					}
					n, err := dev.Where(siteF.Eq(siteOf(w))).Delete()
					if err != nil || n != len(alive) {
						fail("writer %d iter %d scope delete: n=%d err=%v want %d",
							w, i, n, err, len(alive))
						return
					}
					var recs []*Device
					for k := 0; k < keysPer; k++ {
						seq++
						p := pair{status: w*1000000 + seq, lat: seq}
						id := idOf(w, k)
						recs = append(recs, &Device{ID: id, Hostname: fmt.Sprintf("bh-%d-%03d", w, k),
							Site: siteOf(w), Status: p.status, Latency: p.lat})
					}
					// Pre-register: the value set is a superset check, so
					// registering before commit is safe and closes the
					// commit-then-register race window for the reader.
					mu.Lock()
					for _, d := range recs {
						ever[w][d.ID][pair{status: d.Status, lat: d.Latency}] = true
					}
					mu.Unlock()
					if err := dev.UpsertMany(recs); err != nil {
						fail("writer %d iter %d reseed: %v", w, i, err)
						return
					}
					mu.Lock()
					for _, d := range recs {
						models[w][d.ID] = pair{status: d.Status, lat: d.Latency}
						alive[d.ID] = true
					}
					mu.Unlock()
				case 5, 6, 7, 8, 9, 10, 11, 12:
					// DeleteMany over live keys (atomic batch).
					list := aliveList()
					if len(list) == 0 {
						continue
					}
					n := 1 + r.Intn(min(5, len(list)))
					var batch []any
					for j := 0; j < n; j++ {
						batch = append(batch, list[r.Intn(len(list))])
					}
					// Deduplicate so each batch holds each key once.
					seen := map[string]bool{}
					uniq := batch[:0]
					for _, k := range batch {
						if !seen[k.(string)] {
							seen[k.(string)] = true
							uniq = append(uniq, k)
						}
					}
					if err := dev.DeleteMany(uniq); err != nil {
						fail("writer %d iter %d deletemany: %v", w, i, err)
						return
					}
					mu.Lock()
					for _, k := range uniq {
						delete(alive, k.(string))
						delete(models[w], k.(string))
					}
					mu.Unlock()
				case 13, 14, 15, 16, 17, 18, 19, 20, 21, 22:
					// Query-scoped update over the live set.
					if len(alive) == 0 {
						continue
					}
					seq++
					p := pair{status: w*1000000 + seq, lat: seq}
					mu.Lock()
					for id := range alive {
						ever[w][id][p] = true
					}
					mu.Unlock()
					n, err := dev.Where(siteF.Eq(siteOf(w))).Update(func(d *Device) error {
						d.Status, d.Latency = p.status, p.lat
						return nil
					})
					if err != nil || n != len(alive) {
						fail("writer %d iter %d scope update: n=%d err=%v want %d",
							w, i, n, err, len(alive))
						return
					}
					mu.Lock()
					for id := range alive {
						models[w][id] = p
					}
					mu.Unlock()
				default:
					// UpsertMany over a random subset (may resurrect).
					n := 5 + r.Intn(16)
					last := map[string]*Device{}
					for j := 0; j < n; j++ {
						seq++
						p := pair{status: w*1000000 + seq, lat: seq}
						k := r.Intn(keysPer)
						// Collapse duplicates to last-write-wins per key.
						last[idOf(w, k)] = &Device{ID: idOf(w, k),
							Hostname: fmt.Sprintf("bh-%d-%03d", w, k),
							Site:     siteOf(w), Status: p.status, Latency: p.lat}
					}
					var recs []*Device
					for _, d := range last {
						recs = append(recs, d)
					}
					mu.Lock()
					for _, d := range recs {
						ever[w][d.ID][pair{status: d.Status, lat: d.Latency}] = true
					}
					mu.Unlock()
					if err := dev.UpsertMany(recs); err != nil {
						fail("writer %d iter %d upsertmany: %v", w, i, err)
						return
					}
					mu.Lock()
					for _, d := range recs {
						models[w][d.ID] = pair{status: d.Status, lat: d.Latency}
						alive[d.ID] = true
					}
					mu.Unlock()
				}
			}
		}(w)
	}

	// Torn-write reader: every observed value must have been written.
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		r := rand.New(rand.NewSource(8999))
		for {
			select {
			case <-stop:
				return
			default:
			}
			pin := db.ReadTx()
			bound := dev.In(pin)
			for j := 0; j < 20; j++ {
				w, k := r.Intn(writers), r.Intn(keysPer)
				id := idOf(w, k)
				row, err := bound.Get(id)
				if err != nil {
					if !errors.Is(err, rime.ErrNotFound) {
						fail("reader get %s: %v", id, err)
						pin.Close()
						return
					}
					continue // deleted at this snapshot: valid
				}
				p := pair{status: row.Status, lat: row.Latency}
				mu.Lock()
				ok := ever[w][id][p]
				mu.Unlock()
				if !ok {
					fail("reader %s: torn value (%d,%d)", id, row.Status, row.Latency)
					pin.Close()
					return
				}
			}
			pin.Close()
		}
	}()

	wg.Wait()
	close(stop)
	<-readerDone
	for {
		select {
		case msg := <-errCh:
			t.Fatalf("concurrent bulk failure: %s", msg)
		default:
			goto drained
		}
	}
drained:
	// Final exactness against every writer's model.
	mu.Lock()
	defer mu.Unlock()
	total := 0
	for w := 0; w < writers; w++ {
		for k := 0; k < keysPer; k++ {
			id := idOf(w, k)
			want, ok := models[w][id]
			got, err := dev.Get(id)
			if !ok {
				if !errors.Is(err, rime.ErrNotFound) {
					t.Fatalf("final %s: Get err = %v, want ErrNotFound", id, err)
				}
				continue
			}
			total++
			if err != nil || got.Status != want.status || got.Latency != want.lat ||
				got.Site != siteOf(w) {
				t.Fatalf("final %s: got %+v %v, want %+v", id, got, err, want)
			}
		}
	}
	if total == 0 {
		t.Fatal("no live keys at all: writers did nothing")
	}
	t.Logf("bulk writers=%d iters=%d final-live=%d", writers, iters, total)
}

// TestFKConcurrent races child inserts against parent deletes and pins
// the child-side foreign-key check under concurrency: 30 rounds of
// reseed plus barrier-aligned deletes/inserts, where half the child
// inserts reference never-created "ghost" owners. Every ghost insert
// must fail with ErrForeignKey (a commit would prove validation was
// skipped on some path); live-owner inserts and parent deletes may win
// or lose by timing but must report nil or ErrForeignKey only.
//
// KNOWN GAP (not asserted): RIME enforces child-side existence only;
// parent deletes are unrestricted, so insert-then-delete leaves orphans
// (verified single-threaded). That matches murmur's current posture
// (foreign-key enforcement off, no REFERENCES in schema DDL) but diverges
// from RESTRICT-by-default systems. If delete-restrict ever lands, extend
// this test with a quiesced no-orphan scan.
func TestFKConcurrent(t *testing.T) {
	db := rime.New(rime.WithForeignKeys(true))
	defer db.Close()
	type Owner struct {
		ID   string `rime:"primary"`
		Name string
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

	const rounds = 30
	const nOwners = 10
	const perChild = 20
	var totalInserted, totalDeleted, totalFKFail, totalGhostAtt, totalGhostFail int64
	var cntMu sync.Mutex
	bump := func(ins, del, fk, gatt, gfail int) {
		cntMu.Lock()
		totalInserted += int64(ins)
		totalDeleted += int64(del)
		totalFKFail += int64(fk)
		totalGhostAtt += int64(gatt)
		totalGhostFail += int64(gfail)
		cntMu.Unlock()
	}

	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	checkErr := func() {
		select {
		case msg := <-errCh:
			t.Fatalf("concurrent FK failure: %s", msg)
		default:
		}
	}

	for round := 0; round < rounds; round++ {
		// Reseed the transient owners (sequential).
		for o := 0; o < nOwners; o++ {
			id := fmt.Sprintf("t%d", o)
			if err := owners.Upsert(&Owner{ID: id, Name: "n" + id}); err != nil {
				t.Fatalf("round %d reseed: %v", round, err)
			}
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		// 2 parent deleters over a partitioned owner set.
		for p := 0; p < 2; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				<-start
				del, fk := 0, 0
				for o := p; o < nOwners; o += 2 {
					err := db.WriteTx(func(tx *rime.Tx) error {
						return owners.In(tx).Delete(fmt.Sprintf("t%d", o))
					})
					switch {
					case err == nil:
						del++
					case errors.Is(err, rime.ErrForeignKey):
						fk++
					default:
						fail("round %d parent delete t%d: %v", round, o, err)
						return
					}
				}
				bump(0, del, fk, 0, 0)
			}(p)
		}
		// 2 child inserters over unique pet IDs.
		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				r := rand.New(rand.NewSource(int64(9100 + round*10 + w)))
				<-start
				ins, fk, gatt, gfail := 0, 0, 0, 0
				for i := 0; i < perChild; i++ {
					owner := fmt.Sprintf("t%d", r.Intn(nOwners))
					isGhost := r.Intn(2) == 0
					if isGhost {
						owner = fmt.Sprintf("ghost-%d-%d-%d", round, w, i)
						gatt++
					}
					pet := Pet{
						ID:    fmt.Sprintf("r%d-w%d-%d", round, w, i),
						Owner: owner,
					}
					err := db.WriteTx(func(tx *rime.Tx) error {
						return pets.In(tx).Insert(&pet)
					})
					switch {
					case err == nil:
						if isGhost {
							fail("round %d: ghost insert %s committed (validation skipped)", round, pet.ID)
							return
						}
						ins++
					case errors.Is(err, rime.ErrForeignKey):
						if isGhost {
							gfail++
						} else {
							fk++
						}
					default:
						fail("round %d child insert %s: %v", round, pet.ID, err)
						return
					}
				}
				bump(ins, 0, fk, gatt, gfail)
			}(w)
		}
		close(start)
		wg.Wait()
		checkErr()
	}
	checkErr()
	cntMu.Lock()
	defer cntMu.Unlock()
	// Every ghost insert must have failed: a single commit proves the
	// child-side check was skipped on some path.
	if totalGhostAtt == 0 || totalGhostFail != totalGhostAtt {
		t.Fatalf("ghost refs: %d failed of %d attempted, want all rejected",
			totalGhostFail, totalGhostAtt)
	}
	if totalInserted == 0 || totalDeleted == 0 {
		t.Fatalf("one-sided race: inserted=%d deleted=%d (fkfail=%d)",
			totalInserted, totalDeleted, totalFKFail)
	}
	t.Logf("fk rounds=%d inserted=%d deleted=%d fkfail=%d ghostfail=%d",
		rounds, totalInserted, totalDeleted, totalFKFail, totalGhostFail)
}

// TestMultiTableAtomicConcurrent proves multi-table write transactions are
// atomically visible: snapshot readers spanning two tables never observe a
// torn commit (an order row without its line row or vice versa). Writers
// insert correlated order+line pairs in single transactions while readers
// scan both tables under one ReadTx and require exact set correspondence
// (order IDs equal line.Order values) at every snapshot. Any torn snapshot,
// any writer error (pairs use conflict-free distinct keys), or a final
// count/set mismatch fails the test.
func TestMultiTableAtomicConcurrent(t *testing.T) {
	db := rime.New()
	defer db.Close()
	type Order struct {
		ID   string `rime:"primary"`
		Site string `rime:"index"`
	}
	type Line struct {
		ID    string `rime:"primary"`
		Order string `rime:"index"`
	}
	orders, err := rime.Register[Order](db, rime.WithTableName[Order]("orders"))
	if err != nil {
		t.Fatal(err)
	}
	lines, err := rime.Register[Line](db, rime.WithTableName[Line]("lines"))
	if err != nil {
		t.Fatal(err)
	}

	const writers = 4
	const readers = 4
	const batches = 200
	oid := func(w, j int) string { return fmt.Sprintf("o-%d-%06d", w, j) }
	lid := func(w, j int) string { return fmt.Sprintf("l-%d-%06d", w, j) }

	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	var torn, snaps, nonEmpty atomic.Int64
	stopReaders := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
				}
				rtx := db.ReadTx()
				orows, oerr := orders.In(rtx).Where().Find()
				lrows, lerr := lines.In(rtx).Where().Find()
				rtx.Close()
				if oerr != nil || lerr != nil {
					fail("reader scan: %v %v", oerr, lerr)
					return
				}
				snaps.Add(1)
				if len(orows) > 0 {
					nonEmpty.Add(1)
				}
				if len(orows) != len(lrows) {
					torn.Add(1)
					fail("torn snapshot: %d orders vs %d lines", len(orows), len(lrows))
					continue
				}
				want := make(map[string]bool, len(orows))
				for _, o := range orows {
					want[o.ID] = true
				}
				for _, l := range lrows {
					if !want[l.Order] {
						torn.Add(1)
						fail("torn snapshot: line %s references absent order %s", l.ID, l.Order)
						break
					}
				}
			}
		}()
	}
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := 0; j < batches; j++ {
				id := oid(w, j)
				o := Order{ID: id, Site: "OTT"}
				l := Line{ID: lid(w, j), Order: id}
				if err := db.WriteTx(func(tx *rime.Tx) error {
					if err := orders.In(tx).Insert(&o); err != nil {
						return err
					}
					return lines.In(tx).Insert(&l)
				}); err != nil {
					fail("writer %d batch %d: %v", w, j, err)
					return
				}
			}
		}(w)
	}

	// Wait for writers via a separate channel: readers run until told to stop.
	// (Readers and writers share wg; stop readers only after writers drain.)
	done := make(chan struct{})
	go func() {
		// Spin until all pairs are committed, then stop the readers.
		for {
			n, err := orders.Count()
			if err == nil && n == writers*batches {
				close(stopReaders)
				close(done)
				return
			}
			select {
			case msg := <-errCh:
				t.Errorf("concurrent atomic failure: %s", msg)
				close(stopReaders)
				close(done)
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()
	<-done
	wg.Wait()
	select {
	case msg := <-errCh:
		t.Fatalf("concurrent atomic failure: %s", msg)
	default:
	}
	if torn.Load() != 0 {
		t.Fatalf("torn snapshots observed: %d", torn.Load())
	}
	if snaps.Load() == 0 || nonEmpty.Load() == 0 {
		t.Fatalf("one-sided race: snaps=%d nonEmpty=%d", snaps.Load(), nonEmpty.Load())
	}
	// Final exact state: every pair present on both sides.
	if n, err := orders.Count(); err != nil || n != writers*batches {
		t.Fatalf("orders count = %d,%v want %d", n, err, writers*batches)
	}
	if n, err := lines.Count(); err != nil || n != writers*batches {
		t.Fatalf("lines count = %d,%v want %d", n, err, writers*batches)
	}
	orows, err := orders.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	lrows, err := lines.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]bool, len(orows))
	for _, o := range orows {
		want[o.ID] = true
	}
	for _, l := range lrows {
		if !want[l.Order] {
			t.Fatalf("final mismatch: line %s references absent order %s", l.ID, l.Order)
		}
	}
	t.Logf("atomic writers=%d readers=%d batches=%d snaps=%d nonEmpty=%d",
		writers, readers, batches, snaps.Load(), nonEmpty.Load())
}

// TestSnapshotIsolationConcurrent proves a long-lived read snapshot sees a
// frozen view while heavy concurrent churn lands: inserts stay invisible,
// updates keep their original values, and deletes stay visible in the old
// snapshot. Writers own disjoint key ranges (conflict-free by construction,
// every commit must succeed) while a GC loop runs against the pinned
// snapshot and the main goroutine re-scans it repeatedly, requiring exact
// equality with the baseline every time. Afterwards a fresh snapshot must
// match the replayed writer-op model exactly, and the model must differ
// from the baseline (proving churn actually landed while the old view
// stayed frozen).
func TestSnapshotIsolationConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()

	const writers = 4
	const opsPerWriter = 500
	const ownedKeys = 100
	keyOf := func(w, k int) string { return fmt.Sprintf("snap-%d-%03d", w, k) }

	// Seed: every writer's keys present with generation-0 values.
	for w := 0; w < writers; w++ {
		for k := 0; k < ownedKeys; k++ {
			rec := &Device{ID: keyOf(w, k), Hostname: fmt.Sprintf("snap-h-%d-%03d-0", w, k), Site: "OTT", Status: 0, Latency: 0}
			if err := dev.Insert(rec); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
	}

	rtx := db.ReadTx()
	defer rtx.Close()
	snap := func(tx *rime.Tx) map[string]Device {
		t.Helper()
		rows, err := dev.In(tx).Where().Find()
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		m := make(map[string]Device, len(rows))
		for _, r := range rows {
			m[r.ID] = *r
		}
		return m
	}
	baseline := snap(rtx)
	if len(baseline) != writers*ownedKeys {
		t.Fatalf("baseline = %d rows, want %d", len(baseline), writers*ownedKeys)
	}

	type op struct {
		kind      string // "upsert" or "delete"
		key       string
		host      string
		status    int
		latency   int
		committed bool
	}
	committed := make([][]op, writers)
	writersDone := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			present := make([]bool, ownedKeys)
			for k := range present {
				present[k] = true
			}
			ops := make([]op, 0, opsPerWriter)
			for j := 0; j < opsPerWriter; j++ {
				k := (w*31 + j*17) % ownedKeys // deterministic owned-key walk
				id := keyOf(w, k)
				host := fmt.Sprintf("snap-h-%d-%03d-%d", w, k, j+1)
				var o op
				switch {
				case j%4 == 2 && present[k]:
					if err := db.WriteTx(func(tx *rime.Tx) error {
						return dev.In(tx).Delete(id)
					}); err != nil {
						t.Errorf("writer %d op %d delete %s: %v", w, j, id, err)
						return
					}
					present[k] = false
					o = op{kind: "delete", key: id, committed: true}
				default:
					rec := &Device{ID: id, Hostname: host, Site: "MTL", Status: j + 1, Latency: 1000 + j}
					if err := db.WriteTx(func(tx *rime.Tx) error {
						return dev.In(tx).Upsert(rec)
					}); err != nil {
						t.Errorf("writer %d op %d upsert %s: %v", w, j, id, err)
						return
					}
					present[k] = true
					o = op{kind: "upsert", key: id, host: host, status: j + 1, latency: 1000 + j, committed: true}
				}
				ops = append(ops, o)
			}
			committed[w] = ops
		}(w)
	}
	go func() {
		wg.Wait()
		close(writersDone)
	}()
	// GC runs against the pinned snapshot throughout the churn: nothing
	// the old view can see may be reclaimed.
	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)
		for {
			select {
			case <-writersDone:
				return
			default:
				db.GC()
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	// Re-scan the pinned snapshot throughout the churn: exact freeze.
	scans := 0
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
scanLoop:
	for {
		select {
		case <-ticker.C:
			got := snap(rtx)
			scans++
			if len(got) != len(baseline) {
				t.Fatalf("scan %d: %d rows, baseline %d (view moved)", scans, len(got), len(baseline))
			}
			for id, want := range baseline {
				g, ok := got[id]
				if !ok {
					t.Fatalf("scan %d: %s vanished from frozen view", scans, id)
				}
				if g != want {
					t.Fatalf("scan %d: %s = %+v, want %+v (view moved)", scans, id, g, want)
				}
			}
		case <-writersDone:
			break scanLoop
		}
	}
	wg.Wait()
	<-gcDone

	// Post-churn frozen scan, then the fresh view against the replayed model.
	final := snap(rtx)
	if len(final) != len(baseline) {
		t.Fatalf("final frozen scan: %d rows, baseline %d", len(final), len(baseline))
	}
	for id, want := range baseline {
		if g, ok := final[id]; !ok || g != want {
			t.Fatalf("final frozen scan: %s = %+v,%v want %+v", id, g, ok, want)
		}
	}
	want := make(map[string]Device, len(baseline))
	for id, r := range baseline {
		want[id] = r
	}
	var nUpsert, nDelete int
	for w := 0; w < writers; w++ {
		if len(committed[w]) != opsPerWriter {
			t.Fatalf("writer %d committed %d ops, want %d", w, len(committed[w]), opsPerWriter)
		}
		for _, o := range committed[w] {
			if !o.committed {
				t.Fatalf("writer %d has uncommitted op %+v", w, o)
			}
			switch o.kind {
			case "delete":
				delete(want, o.key)
				nDelete++
			case "upsert":
				want[o.key] = Device{ID: o.key, Hostname: o.host, Site: "MTL", Status: o.status, Latency: o.latency}
				nUpsert++
			}
		}
	}
	if nUpsert == 0 || nDelete == 0 {
		t.Fatalf("one-sided churn: upserts=%d deletes=%d", nUpsert, nDelete)
	}
	freshTx := db.ReadTx()
	fresh := snap(freshTx)
	freshTx.Close()
	if len(fresh) != len(want) {
		t.Fatalf("fresh view: %d rows, model %d", len(fresh), len(want))
	}
	for id, w := range want {
		if g, ok := fresh[id]; !ok || g != w {
			t.Fatalf("fresh view: %s = %+v,%v want %+v", id, g, ok, w)
		}
	}
	modelMoved := len(want) != len(baseline)
	if !modelMoved {
		for id, w := range want {
			if b, ok := baseline[id]; !ok || b != w {
				modelMoved = true
				break
			}
		}
	}
	if !modelMoved {
		t.Fatal("model equals baseline: churn never landed (vacuous freeze)")
	}
	if scans == 0 {
		t.Fatal("no mid-churn scans ran")
	}
	t.Logf("snapshot scans=%d upserts=%d deletes=%d final=%d", scans, nUpsert, nDelete, len(want))
}

// TestHookVetoConcurrent proves Before-hook vetoes abort their whole
// transaction atomically under parallel load: no allowed op staged beside
// a vetoed op may leak into committed state. Phase 1 races writers whose
// odd transactions pair an allowed update with a vetoed one; the final
// per-key state must show only even-transaction values, with veto-touched
// keys byte-identical to seed. Phase 2 races vetoed inserts (all must fail
// with the veto sentinel and change nothing) followed by allowed inserts
// (all must succeed with an exact final count).
func TestHookVetoConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	errVeto := errors.New("veto-test")
	dev.BeforeUpdate(func(o, n *Device) error {
		if n.Status > 100 {
			return errVeto
		}
		return nil
	})
	dev.BeforeInsert(func(d *Device) error {
		if strings.HasPrefix(d.Hostname, "veto-") {
			return errVeto
		}
		return nil
	})

	const writers = 4
	const keysPerWriter = 100
	const txns = 100
	keyOf := func(w, k int) string { return fmt.Sprintf("veto-%d-%03d", w, k) }
	for w := 0; w < writers; w++ {
		for k := 0; k < keysPerWriter; k++ {
			rec := &Device{ID: keyOf(w, k), Hostname: fmt.Sprintf("vh-%d-%03d", w, k), Site: "OTT"}
			if err := dev.Insert(rec); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
	}

	var commits, vetoes atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := 0; j < txns; j++ {
				a, b := keyOf(w, j%50), keyOf(w, j%50+50)
				bStatus := j + 1
				if j%2 == 1 {
					bStatus = 999 // vetoed: must abort A alongside B
				}
				err := db.WriteTx(func(tx *rime.Tx) error {
					if err := dev.In(tx).Update(a, func(d *Device) error {
						d.Status = j + 1
						return nil
					}); err != nil {
						return err
					}
					return dev.In(tx).Update(b, func(d *Device) error {
						d.Status = bStatus
						return nil
					})
				})
				if j%2 == 0 {
					if err != nil {
						t.Errorf("writer %d txn %d: %v, want commit", w, j, err)
						return
					}
					commits.Add(1)
				} else {
					if !errors.Is(err, errVeto) {
						t.Errorf("writer %d txn %d: err=%v, want veto", w, j, err)
						return
					}
					vetoes.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	if commits.Load() != writers*txns/2 || vetoes.Load() != writers*txns/2 {
		t.Fatalf("commits=%d vetoes=%d, want %d each", commits.Load(), vetoes.Load(), writers*txns/2)
	}
	// Exact model: key k%50==r holds max even j+1 with j%50==r (r+51 for
	// even r); odd-r keys were touched only by aborted txns and keep seed.
	for w := 0; w < writers; w++ {
		for k := 0; k < keysPerWriter; k++ {
			got, err := dev.Get(keyOf(w, k))
			if err != nil {
				t.Fatalf("Get %s: %v", keyOf(w, k), err)
			}
			wantStatus := 0
			if r := k % 50; r%2 == 0 {
				wantStatus = r + 51
			}
			want := Device{ID: keyOf(w, k), Hostname: fmt.Sprintf("vh-%d-%03d", w, k), Site: "OTT", Status: wantStatus}
			if *got != want {
				t.Fatalf("%s = %+v, want %+v (veto leaked or update lost)", got.ID, *got, want)
			}
		}
	}

	// Phase 2: vetoed inserts fail cleanly, allowed inserts land exactly.
	const racers = 8
	const perRacer = 25
	errs := raceStart(t, racers, func(i int) error {
		for m := 0; m < perRacer; m++ {
			rec := &Device{ID: fmt.Sprintf("veto-ins-%d-%02d", i, m), Hostname: fmt.Sprintf("veto-h-%d-%02d", i, m)}
			if err := dev.Insert(rec); !errors.Is(err, errVeto) {
				return fmt.Errorf("insert %d/%d: err=%v, want veto", i, m, err)
			}
		}
		return nil
	})
	for i, e := range errs {
		if e != nil {
			t.Fatalf("veto racer %d: %v", i, e)
		}
	}
	if n, err := dev.Count(); err != nil || n != writers*keysPerWriter {
		t.Fatalf("count after vetoed inserts = %d,%v want %d", n, err, writers*keysPerWriter)
	}
	for i := 0; i < racers; i++ {
		for m := 0; m < perRacer; m++ {
			rec := &Device{ID: fmt.Sprintf("ok-ins-%d-%02d", i, m), Hostname: fmt.Sprintf("ok-h-%d-%02d", i, m)}
			if err := dev.Insert(rec); err != nil {
				t.Fatalf("allowed insert %d/%d: %v", i, m, err)
			}
		}
	}
	if n, err := dev.Count(); err != nil || n != writers*keysPerWriter+racers*perRacer {
		t.Fatalf("final count = %d,%v want %d", n, err, writers*keysPerWriter+racers*perRacer)
	}
	t.Logf("veto commits=%d vetoes=%d racers=%d", commits.Load(), vetoes.Load(), racers)
}

// TestPointerFreezeConcurrent proves the immutable-pointer contract under
// parallel churn: a *Device obtained from any read must never change
// afterwards, no matter how many updates, upsert-replacements, or deletes
// land on its key. Phase 1 spins readers that copy a row, sleep, and
// re-compare the HELD pointer while writers hammer updates and replacements
// across the key space. Phase 2 has readers hold every row of a doomed set
// while writers delete the set, then requires the held copies byte-exact.
// Any drift fails; the race detector independently covers the same property.
func TestPointerFreezeConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()

	const keys = 200
	const writers = 4
	const readers = 4
	const updatesPerWriter = 500
	keyOf := func(k int) string { return fmt.Sprintf("frz-%04d", k) }
	for k := 0; k < keys; k++ {
		rec := &Device{ID: keyOf(k), Hostname: fmt.Sprintf("frz-h-%04d", k), Site: "OTT", Status: k, Latency: k * 3}
		if err := dev.Insert(rec); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	var checks, updates atomic.Int64
	writersDone := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(7000 + w)))
			for j := 0; j < updatesPerWriter; j++ {
				k := r.Intn(keys)
				id := keyOf(k)
				val := w*1000000 + j + 1
				var err error
				for attempt := 0; ; attempt++ {
					if j%2 == 0 {
						err = dev.Update(id, func(d *Device) error {
							d.Status = val
							d.Latency = val * 2
							return nil
						})
					} else {
						rec := &Device{ID: id, Hostname: fmt.Sprintf("frz-h-%04d", k), Site: "MTL", Status: val, Latency: val * 2}
						err = dev.Upsert(rec)
					}
					if err == nil {
						break
					}
					if !errors.Is(err, rime.ErrConflict) || attempt >= 1000 {
						fail("writer %d op %d: %v", w, j, err)
						return
					}
				}
				updates.Add(1)
			}
		}(w)
	}
	go func() {
		wg.Wait()
		close(writersDone)
	}()
	var rwg sync.WaitGroup
	for r := 0; r < readers; r++ {
		rwg.Add(1)
		go func(r int) {
			defer rwg.Done()
			rnd := rand.New(rand.NewSource(int64(9000 + r)))
			for {
				select {
				case <-writersDone:
					return
				default:
				}
				ptr, err := dev.Get(keyOf(rnd.Intn(keys)))
				if err != nil {
					fail("reader %d Get: %v", r, err)
					return
				}
				snap := *ptr
				time.Sleep(50 * time.Microsecond) // let churn land mid-hold
				if *ptr != snap {
					fail("reader %d: held %s drifted %+v -> %+v", r, snap.ID, snap, *ptr)
					return
				}
				checks.Add(1)
			}
		}(r)
	}
	<-writersDone
	rwg.Wait()
	select {
	case msg := <-errCh:
		t.Fatalf("pointer freeze failure: %s", msg)
	default:
	}
	if updates.Load() != writers*updatesPerWriter {
		t.Fatalf("updates=%d want %d", updates.Load(), writers*updatesPerWriter)
	}
	if checks.Load() == 0 {
		t.Fatal("no freeze checks ran")
	}

	// Phase 2: held pointers survive the delete of their rows.
	const doomed = 50
	held := make([]*Device, doomed)
	want := make([]Device, doomed)
	for k := 0; k < doomed; k++ {
		p, err := dev.Get(keyOf(k))
		if err != nil {
			t.Fatalf("hold %d: %v", k, err)
		}
		held[k], want[k] = p, *p
	}
	var dwg sync.WaitGroup
	for w := 0; w < writers; w++ {
		dwg.Add(1)
		go func(w int) {
			defer dwg.Done()
			for k := w; k < doomed; k += writers {
				if err := dev.Delete(keyOf(k)); err != nil {
					t.Errorf("delete %d: %v", k, err)
					return
				}
			}
		}(w)
	}
	dwg.Wait()
	for k := 0; k < doomed; k++ {
		if *held[k] != want[k] {
			t.Fatalf("held %s drifted across delete: %+v -> %+v", want[k].ID, want[k], *held[k])
		}
		if _, err := dev.Get(keyOf(k)); !errors.Is(err, rime.ErrNotFound) {
			t.Fatalf("deleted %s Get = %v, want ErrNotFound", keyOf(k), err)
		}
	}
	t.Logf("freeze checks=%d updates=%d doomed=%d", checks.Load(), updates.Load(), doomed)
}

// TestNestedFreezeConcurrent proves the default deep-copy cloner holds under
// parallel churn for records with nested mutable state (maps, byte slices,
// pointer items). Phase 1 spins readers that snapshot a row's nested values,
// sleep, and re-compare the HELD nested state while writers hammer in-place
// byte edits, pointee edits, and whole-value replacements across the key
// space; an untouched control entry must also stay exact. Phase 2 races
// inserts whose caller buffers are mutated immediately after admission and
// requires every stored row to show the original bytes. Any drift fails;
// the race detector independently covers the same aliasing property.
func TestNestedFreezeConcurrent(t *testing.T) {
	db := rime.New()
	defer db.Close()
	tab, err := rime.Register[ownedRecord](db)
	if err != nil {
		t.Fatal(err)
	}

	const keys = 100
	const writers = 4
	const readers = 4
	const updatesPerWriter = 250
	keyOf := func(k int) string { return fmt.Sprintf("nest-%03d", k) }
	seedB := func(k int) string { return fmt.Sprintf("control-%03d", k) }
	for k := 0; k < keys; k++ {
		item := fmt.Sprintf("item-%03d", k)
		rec := &ownedRecord{ID: keyOf(k),
			Labels: map[string][]byte{"a": []byte(fmt.Sprintf("seed-a-%03d", k)), "b": []byte(seedB(k))},
			Items:  []*string{&item}}
		if err := tab.Insert(rec); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	var checks, updates atomic.Int64
	writersDone := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(11000 + w)))
			for j := 0; j < updatesPerWriter; j++ {
				k := r.Intn(keys)
				id := keyOf(k)
				val := fmt.Sprintf("w%d-%d", w, j)
				for attempt := 0; ; attempt++ {
					var err error
					if j%2 == 0 {
						// In-place edits of the private copy's nested state.
						err = tab.Update(id, func(rec *ownedRecord) error {
							rec.Labels["a"][0] = byte('A' + w)
							*rec.Items[0] = val
							return nil
						})
					} else {
						// Whole-value replacements.
						err = tab.Update(id, func(rec *ownedRecord) error {
							rec.Labels["a"] = []byte(val)
							s := val
							rec.Items[0] = &s
							return nil
						})
					}
					if err == nil {
						break
					}
					if !errors.Is(err, rime.ErrConflict) || attempt >= 1000 {
						fail("writer %d op %d: %v", w, j, err)
						return
					}
				}
				updates.Add(1)
			}
		}(w)
	}
	go func() {
		wg.Wait()
		close(writersDone)
	}()
	var rwg sync.WaitGroup
	for r := 0; r < readers; r++ {
		rwg.Add(1)
		go func(r int) {
			defer rwg.Done()
			rnd := rand.New(rand.NewSource(int64(13000 + r)))
			for {
				select {
				case <-writersDone:
					return
				default:
				}
				k := rnd.Intn(keys)
				ptr, err := tab.Get(keyOf(k))
				if err != nil {
					fail("reader %d Get: %v", r, err)
					return
				}
				a := append([]byte(nil), ptr.Labels["a"]...)
				b := append([]byte(nil), ptr.Labels["b"]...)
				it := *ptr.Items[0]
				time.Sleep(50 * time.Microsecond) // let churn land mid-hold
				if string(ptr.Labels["a"]) != string(a) || *ptr.Items[0] != it {
					fail("reader %d: held %s nested state drifted", r, ptr.ID)
					return
				}
				if string(ptr.Labels["b"]) != string(b) || string(b) != seedB(k) {
					fail("reader %d: control entry moved on %s", r, ptr.ID)
					return
				}
				checks.Add(1)
			}
		}(r)
	}
	<-writersDone
	rwg.Wait()
	select {
	case msg := <-errCh:
		t.Fatalf("nested freeze failure: %s", msg)
	default:
	}
	if updates.Load() != writers*updatesPerWriter {
		t.Fatalf("updates=%d want %d", updates.Load(), writers*updatesPerWriter)
	}
	if checks.Load() == 0 {
		t.Fatal("no nested checks ran")
	}

	// Phase 2: insert-path caller ownership under racing inserts.
	const perRacer = 50
	var iwg sync.WaitGroup
	for w := 0; w < writers; w++ {
		iwg.Add(1)
		go func(w int) {
			defer iwg.Done()
			for j := 0; j < perRacer; j++ {
				id := fmt.Sprintf("own-%d-%02d", w, j)
				label := []byte(fmt.Sprintf("L-%d-%02d", w, j))
				item := fmt.Sprintf("I-%d-%02d", w, j)
				if err := tab.Insert(&ownedRecord{ID: id,
					Labels: map[string][]byte{"a": label},
					Items:  []*string{&item}}); err != nil {
					t.Errorf("insert %s: %v", id, err)
					return
				}
				label[0] = 'X' // mutate caller buffers post-admission
				item = "MUT"
				got, err := tab.Get(id)
				if err != nil {
					t.Errorf("get %s: %v", id, err)
					return
				}
				if string(got.Labels["a"]) != fmt.Sprintf("L-%d-%02d", w, j) || *got.Items[0] != fmt.Sprintf("I-%d-%02d", w, j) {
					t.Errorf("stored %s shares caller buffers: %+v", id, got)
					return
				}
			}
		}(w)
	}
	iwg.Wait()
	t.Logf("nested checks=%d updates=%d inserts=%d", checks.Load(), updates.Load(), writers*perRacer)
}

// TestMixedTxnConcurrent proves mixed multi-operation transactions stay
// correct under parallel load: every transaction combines an upsert, an
// update-or-upsert, and a delete-or-upsert on three distinct owned keys,
// so writers are conflict-free by construction and every commit must
// succeed. Each writer keeps an exact local model (keys are disjoint, so
// the merged models are the deterministic final state) while readers scan
// for structural validity and a GC loop runs throughout. The final state
// must match the merged models field-for-field.
func TestMixedTxnConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()

	const writers = 4
	const keysPer = 60
	const txns = 150
	keyOf := func(w, k int) string { return fmt.Sprintf("mix-%d-%02d", w, k) }
	sites := []string{"OTT", "MTL", "WPG"}
	for w := 0; w < writers; w++ {
		for k := 0; k < keysPer; k++ {
			rec := &Device{ID: keyOf(w, k), Hostname: fmt.Sprintf("mix-h-%d-%02d-seed", w, k), Site: "SEED"}
			if err := dev.Insert(rec); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
	}

	models := make([]map[string]Device, writers)
	for w := range models {
		models[w] = make(map[string]Device, keysPer)
		for k := 0; k < keysPer; k++ {
			id := keyOf(w, k)
			models[w][id] = Device{ID: id, Hostname: fmt.Sprintf("mix-h-%d-%02d-seed", w, k), Site: "SEED"}
		}
	}
	var nCommit, nUpsert, nUpdate, nDelete, scans atomic.Int64
	writersDone := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			present := make([]bool, keysPer)
			for k := range present {
				present[k] = true
			}
			model := models[w]
			for j := 0; j < txns; j++ {
				ka, kb, kc := (j*7+w)%keysPer, 0, 0
				kb, kc = (ka+20)%keysPer, (ka+40)%keysPer // distinct trio
				seq := j + 1
				hostA := fmt.Sprintf("mix-h-%d-%04d-a", w, seq)
				hostB := fmt.Sprintf("mix-h-%d-%04d-b", w, seq)
				hostC := fmt.Sprintf("mix-h-%d-%04d-c", w, seq)
				site := sites[j%len(sites)]
				type eff struct {
					key       string
					idx       int
					kind      string // "upsert" or "delete"
					host      string
					status    int
					latency   int
				}
				ops := []eff{{keyOf(w, ka), ka, "upsert", hostA, seq, seq * 7}}
				if present[kb] {
					ops = append(ops, eff{keyOf(w, kb), kb, "upsert", hostB, seq, seq * 7})
					// Update path: keep hostname, change the rest.
					ops[len(ops)-1].host = ""
				} else {
					ops = append(ops, eff{keyOf(w, kb), kb, "upsert", hostB, seq, seq * 7})
				}
				if present[kc] {
					ops = append(ops, eff{keyOf(w, kc), kc, "delete", "", 0, 0})
				} else {
					ops = append(ops, eff{keyOf(w, kc), kc, "upsert", hostC, seq, seq * 7})
				}
				err := db.WriteTx(func(tx *rime.Tx) error {
					b := dev.In(tx)
					for _, o := range ops {
						switch {
						case o.kind == "delete":
							if err := b.Delete(o.key); err != nil {
								return err
							}
						case o.host == "":
							st, la := o.status, o.latency
							if err := b.Update(o.key, func(d *Device) error {
								d.Status, d.Latency, d.Site = st, la, site
								return nil
							}); err != nil {
								return err
							}
						default:
							rec := &Device{ID: o.key, Hostname: o.host, Site: site, Status: o.status, Latency: o.latency}
							if err := b.Upsert(rec); err != nil {
								return err
							}
						}
					}
					return nil
				})
				if err != nil {
					t.Errorf("writer %d txn %d: %v, want commit", w, j, err)
					return
				}
				nCommit.Add(1)
				for _, o := range ops {
					switch {
					case o.kind == "delete":
						delete(model, o.key)
						present[o.idx] = false
						nDelete.Add(1)
					case o.host == "":
						m := model[o.key]
						m.Status, m.Latency, m.Site = o.status, o.latency, site
						model[o.key] = m
						nUpdate.Add(1)
					default:
						model[o.key] = Device{ID: o.key, Hostname: o.host, Site: site, Status: o.status, Latency: o.latency}
						present[o.idx] = true
						nUpsert.Add(1)
					}
				}
			}
		}(w)
	}
	go func() {
		wg.Wait()
		close(writersDone)
	}()
	// GC runs throughout; readers assert structural validity mid-churn.
	gcDone := make(chan struct{})
	go func() {
		defer close(gcDone)
		for {
			select {
			case <-writersDone:
				return
			default:
				db.GC()
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()
	errCh := make(chan string, 64)
	var rwg sync.WaitGroup
	for r := 0; r < 2; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for {
				select {
				case <-writersDone:
					return
				default:
				}
				rows, err := dev.Where().Find()
				if err != nil {
					select {
					case errCh <- fmt.Sprintf("scan: %v", err):
					default:
					}
					return
				}
				for _, row := range rows {
					if len(row.ID) < 4 || row.ID[:4] != "mix-" ||
						len(row.Hostname) < 6 || row.Hostname[:6] != "mix-h-" ||
						(row.Site != "SEED" && row.Site != "OTT" && row.Site != "MTL" && row.Site != "WPG") ||
						row.Status < 0 || row.Latency < 0 {
						select {
						case errCh <- fmt.Sprintf("structurally invalid row: %+v", *row):
						default:
						}
						return
					}
				}
				scans.Add(1)
			}
		}()
	}
	<-writersDone
	wg.Wait()
	rwg.Wait()
	<-gcDone
	select {
	case msg := <-errCh:
		t.Fatalf("mixed txn failure: %s", msg)
	default:
	}
	if nCommit.Load() != writers*txns {
		t.Fatalf("commits=%d want %d", nCommit.Load(), writers*txns)
	}
	if nUpsert.Load() == 0 || nUpdate.Load() == 0 || nDelete.Load() == 0 {
		t.Fatalf("one-sided mix: upserts=%d updates=%d deletes=%d", nUpsert.Load(), nUpdate.Load(), nDelete.Load())
	}
	if scans.Load() == 0 {
		t.Fatal("no reader scans ran")
	}
	want := make(map[string]Device)
	for _, m := range models {
		for id, d := range m {
			want[id] = d
		}
	}
	rows, err := dev.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != len(want) {
		t.Fatalf("final rows=%d model=%d", len(rows), len(want))
	}
	for _, row := range rows {
		w, ok := want[row.ID]
		if !ok || *row != w {
			t.Fatalf("final %s = %+v,%v want %+v", row.ID, *row, ok, w)
		}
	}
	t.Logf("mixed commits=%d upserts=%d updates=%d deletes=%d scans=%d final=%d",
		nCommit.Load(), nUpsert.Load(), nUpdate.Load(), nDelete.Load(), scans.Load(), len(want))
}

// TestLongTxnChurnIsolation proves optimistic validation is per-key, not
// global: a write transaction held open across a thousand unrelated commits
// must still commit, while one whose key was touched must fail with
// ErrConflict. Phase 1 stages a holder update, churns disjoint keys from
// four goroutines, then commits the holder and requires success with the
// exact staged value. Phase 2 is the control: a staged-then-bumped key must
// conflict and keep the bumper's value.
func TestLongTxnChurnIsolation(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()

	const churners = 4
	const keysPer = 50
	const txns = 250
	keyOf := func(w, k int) string { return fmt.Sprintf("lng-%d-%02d", w, k) }
	for w := 0; w < churners; w++ {
		for k := 0; k < keysPer; k++ {
			rec := &Device{ID: keyOf(w, k), Hostname: fmt.Sprintf("lng-h-%d-%02d", w, k), Site: "OTT"}
			if err := dev.Insert(rec); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
	}
	holderRec := &Device{ID: "long-holder", Hostname: "long-holder-h", Site: "OTT", Status: 1}
	if err := dev.Insert(holderRec); err != nil {
		t.Fatalf("seed holder: %v", err)
	}

	// Phase 1: holder stages, churn lands elsewhere, holder commits clean.
	htx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := dev.In(htx).Update("long-holder", func(d *Device) error {
		d.Status = 999
		d.Site = "MTL"
		return nil
	}); err != nil {
		t.Fatalf("stage holder: %v", err)
	}
	var churned atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < churners; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := 0; j < txns; j++ {
				id := keyOf(w, (j*13+w)%keysPer)
				rec := &Device{ID: id, Hostname: fmt.Sprintf("lng-h-%d-%04d", w, j), Site: "WPG", Status: j + 1}
				if err := db.WriteTx(func(tx *rime.Tx) error {
					return dev.In(tx).Upsert(rec)
				}); err != nil {
					t.Errorf("churn %d/%d: %v", w, j, err)
					return
				}
				churned.Add(1)
			}
		}(w)
	}
	wg.Wait()
	if churned.Load() != churners*txns {
		t.Fatalf("churned=%d want %d", churned.Load(), churners*txns)
	}
	if err := htx.Commit(); err != nil {
		t.Fatalf("holder commit after unrelated churn: %v, want nil", err)
	}
	got, err := dev.Get("long-holder")
	if err != nil {
		t.Fatalf("Get holder: %v", err)
	}
	if got.Status != 999 || got.Site != "MTL" || got.Hostname != "long-holder-h" {
		t.Fatalf("holder = %+v, want staged values", *got)
	}

	// Phase 2 (control): staged key bumped before commit must conflict.
	if err := dev.Insert(&Device{ID: "long-victim", Hostname: "long-victim-h", Status: 1}); err != nil {
		t.Fatalf("seed victim: %v", err)
	}
	vtx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := dev.In(vtx).Update("long-victim", func(d *Device) error {
		d.Status = 111
		return nil
	}); err != nil {
		t.Fatalf("stage victim: %v", err)
	}
	if err := dev.Update("long-victim", func(d *Device) error {
		d.Status = 222
		return nil
	}); err != nil {
		t.Fatalf("bump victim: %v", err)
	}
	if err := vtx.Commit(); !errors.Is(err, rime.ErrConflict) {
		t.Fatalf("victim commit = %v, want ErrConflict", err)
	}
	vic, err := dev.Get("long-victim")
	if err != nil || vic.Status != 222 {
		t.Fatalf("victim = %+v %v, want bumper's 222", vic, err)
	}
	t.Logf("long-txn churned=%d holder=committed victim=conflicted", churned.Load())
}

// TestNoDirtyReads proves staged (uncommitted) writes are invisible to every
// other transaction while remaining visible to their own. Each round stages
// two inserts plus one update behind a rendezvous: readers first assert the
// new rows are absent and the updated row still shows its old value, then
// the writer commits and readers assert the exact new state. The writer
// also asserts read-your-write on its staged rows before committing. Any
// leaked staged state fails the test.
func TestNoDirtyReads(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()

	const seeds = 20
	const rounds = 50
	const readers = 4
	seedOf := func(k int) string { return fmt.Sprintf("dirty-seed-%02d", k) }
	for k := 0; k < seeds; k++ {
		rec := &Device{ID: seedOf(k), Hostname: fmt.Sprintf("dirty-h-%02d", k), Site: "OTT", Status: k}
		if err := dev.Insert(rec); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	wantStatus := make([]int, seeds)
	for k := range wantStatus {
		wantStatus[k] = k
	}

	var preChecks, postChecks atomic.Int64
	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	for round := 0; round < rounds; round++ {
		upd := round % seeds
		newA := fmt.Sprintf("dirty-new-%02d-a", round)
		newB := fmt.Sprintf("dirty-new-%02d-b", round)
		newStatus := 1000 + round
		tx, err := db.BeginTx(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		b := dev.In(tx)
		if err := b.Insert(&Device{ID: newA, Hostname: newA, Status: newStatus}); err != nil {
			t.Fatal(err)
		}
		if err := b.Insert(&Device{ID: newB, Hostname: newB, Status: newStatus}); err != nil {
			t.Fatal(err)
		}
		if err := b.Update(seedOf(upd), func(d *Device) error {
			d.Status = newStatus
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		// Read-your-write inside the staging transaction.
		for _, id := range []string{newA, newB} {
			got, err := b.Get(id)
			if err != nil || got.Status != newStatus {
				t.Fatalf("round %d: own staged %s = %+v %v", round, id, got, err)
			}
		}
		if got, err := b.Get(seedOf(upd)); err != nil || got.Status != newStatus {
			t.Fatalf("round %d: own staged update = %+v %v", round, got, err)
		}

		// Readers must see none of it until commit.
		staged := make(chan struct{})
		checked := make(chan struct{})
		var wg sync.WaitGroup
		for r := 0; r < readers; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-staged
				for _, id := range []string{newA, newB} {
					if _, err := dev.Get(id); !errors.Is(err, rime.ErrNotFound) {
						fail("round %d: staged %s visible pre-commit: %v", round, id, err)
						return
					}
				}
				got, err := dev.Get(seedOf(upd))
				if err != nil || got.Status != wantStatus[upd] {
					fail("round %d: %s = %+v %v pre-commit, want status %d",
						round, seedOf(upd), got, err, wantStatus[upd])
					return
				}
				preChecks.Add(1)
				<-checked
				for _, id := range []string{newA, newB} {
					got, err := dev.Get(id)
					if err != nil || got.Status != newStatus {
						fail("round %d: %s = %+v %v post-commit", round, id, got, err)
						return
					}
				}
				got, err = dev.Get(seedOf(upd))
				if err != nil || got.Status != newStatus {
					fail("round %d: %s = %+v %v post-commit", round, seedOf(upd), got, err)
					return
				}
				postChecks.Add(1)
			}()
		}
		close(staged)
		// Wait for all pre-commit checks without closing `checked` early:
		// poll the counter (readers block on `checked` afterwards).
		deadline := time.After(30 * time.Second)
		for preChecks.Load() != int64((round+1)*readers) {
			select {
			case msg := <-errCh:
				t.Fatalf("dirty-read failure: %s", msg)
			case <-deadline:
				t.Fatalf("round %d: readers stuck pre-commit", round)
			default:
				time.Sleep(time.Millisecond)
			}
		}
		select {
		case msg := <-errCh:
			t.Fatalf("dirty-read failure: %s", msg)
		default:
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("round %d commit: %v", round, err)
		}
		wantStatus[upd] = newStatus
		close(checked)
		wg.Wait()
		select {
		case msg := <-errCh:
			t.Fatalf("dirty-read failure: %s", msg)
		default:
		}
	}
	if preChecks.Load() != rounds*readers || postChecks.Load() != rounds*readers {
		t.Fatalf("checks pre=%d post=%d want %d each", preChecks.Load(), postChecks.Load(), rounds*readers)
	}
	// Final exact state: seeds at last-written values plus all new rows.
	rows, err := dev.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != seeds+2*rounds {
		t.Fatalf("final rows=%d want %d", len(rows), seeds+2*rounds)
	}
	for k := 0; k < seeds; k++ {
		got, err := dev.Get(seedOf(k))
		if err != nil || got.Status != wantStatus[k] {
			t.Fatalf("final %s = %+v %v want status %d", seedOf(k), got, err, wantStatus[k])
		}
	}
	t.Logf("dirty-read rounds=%d pre=%d post=%d", rounds, preChecks.Load(), postChecks.Load())
}

// TestCancelUnderChurn proves cancellation stays safe under parallel load:
// every raced scan returns either valid results or context.Canceled, never
// another error, never partial garbage with a nil error, and never a hang.
// Four racers fire scans (Find, Count, and GroupBy-aggregate cycling) while
// canceling immediately; two writers land deterministic upserts throughout,
// and the final state must match the writer model exactly. A deterministic
// pre-canceled sweep pins the entrypoints first.
func TestCancelUnderChurn(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()

	const seeds = 2000
	keyOf := func(k int) string { return fmt.Sprintf("can-%04d", k) }
	for k := 0; k < seeds; k++ {
		rec := &Device{ID: keyOf(k), Hostname: fmt.Sprintf("can-h-%04d", k), Site: "OTT", Status: k}
		if err := dev.Insert(rec); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	siteF := rime.SF[Device](dev, "Site")
	latF := rime.OF[Device, int](dev, "Latency")

	// Deterministic sweep: pre-canceled contexts fail closed everywhere.
	preCtx, preCancel := context.WithCancel(context.Background())
	preCancel()
	if _, err := dev.Where().WithContext(preCtx).Find(); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled Find = %v, want Canceled", err)
	}
	if _, err := dev.Where().WithContext(preCtx).Count(); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled Count = %v, want Canceled", err)
	}
	if _, err := dev.WithContext(preCtx).Get(keyOf(0)); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled Get = %v, want Canceled", err)
	}
	if _, err := dev.Where().WithContext(preCtx).GroupBy(siteF).Aggregate(rime.Count[Device]()); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled Aggregate = %v, want Canceled", err)
	}

	const writers = 2
	const txns = 150
	const racers = 4
	const perRacer = 60
	model := make(map[string]Device, seeds)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := 0; j < txns; j++ {
				k := (w*seeds)/writers + (j*7)%(seeds/writers)
				rec := &Device{ID: keyOf(k), Hostname: fmt.Sprintf("can-h-%04d", k), Site: "MTL", Status: j + 1, Latency: j}
				if err := dev.Upsert(rec); err != nil {
					t.Errorf("writer %d/%d: %v", w, j, err)
					return
				}
				mu.Lock()
				model[keyOf(k)] = *rec
				mu.Unlock()
			}
		}(w)
	}
	type outcome struct {
		ok       bool
		canceled bool
		bad      string
	}
	resCh := make(chan outcome, racers*perRacer)
	for r := 0; r < racers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(21000 + r)))
			for i := 0; i < perRacer; i++ {
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan outcome, 1)
				op := (r + i) % 3
				go func() {
					var o outcome
					switch op {
					case 0:
						rows, err := dev.Where().WithContext(ctx).Find()
						switch {
						case err == nil:
							o.ok = true
							for _, row := range rows {
								if row == nil || row.ID == "" {
									o = outcome{bad: "nil/empty row with nil error"}
								}
							}
						case errors.Is(err, context.Canceled):
							o.canceled = true
						default:
							o.bad = fmt.Sprintf("Find err=%v", err)
						}
					case 1:
						n, err := dev.Where().WithContext(ctx).Count()
						switch {
						case err == nil:
							o.ok = true
							if n < 0 {
								o = outcome{bad: "negative count"}
							}
						case errors.Is(err, context.Canceled):
							o.canceled = true
						default:
							o.bad = fmt.Sprintf("Count err=%v", err)
						}
					default:
						_, err := dev.Where().WithContext(ctx).GroupBy(siteF).Aggregate(rime.Count[Device](), rime.SumOf(latF))
						switch {
						case err == nil:
							o.ok = true
						case errors.Is(err, context.Canceled):
							o.canceled = true
						default:
							o.bad = fmt.Sprintf("Aggregate err=%v", err)
						}
					}
					done <- o
				}()
				// Seeded delay straddles the race: some scans finish, some
				// cancel mid-flight. Assertions accept either outcome.
				time.Sleep(time.Duration(rnd.Intn(500)) * time.Microsecond)
				cancel()
				select {
				case o := <-done:
					resCh <- o
				case <-time.After(10 * time.Second):
					resCh <- outcome{bad: "scan hung"}
					return
				}
			}
		}(r)
	}
	wg.Wait()
	close(resCh)
	var ok, canceled int
	for o := range resCh {
		if o.bad != "" {
			t.Fatalf("raced scan: %s", o.bad)
		}
		if o.ok {
			ok++
		}
		if o.canceled {
			canceled++
		}
	}
	if ok+canceled != racers*perRacer {
		t.Fatalf("outcomes ok=%d canceled=%d want %d total", ok, canceled, racers*perRacer)
	}
	if ok == 0 || canceled == 0 {
		t.Fatalf("one-sided race: ok=%d canceled=%d (want both)", ok, canceled)
	}
	// Final exact writer model.
	mu.Lock()
	defer mu.Unlock()
	rows, err := dev.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != seeds {
		t.Fatalf("final rows=%d want %d", len(rows), seeds)
	}
	for id, want := range model {
		got, err := dev.Get(id)
		if err != nil || *got != want {
			t.Fatalf("final %s = %+v %v want %+v", id, got, err, want)
		}
	}
	t.Logf("cancel raced=%d ok=%d canceled=%d", racers*perRacer, ok, canceled)
}

// TestJoinBilateralConcurrent churns BOTH sides of a join while readers verify
// exact pair sets against a brute-force oracle over the same pinned snapshot.
// The single-sided TestJoinConcurrent cannot catch a build-side snapshot escape
// (reading sites at latest-committed instead of the pin); with churning sites,
// any such escape surfaces as a phantom, missing, or stale pair.
func TestJoinBilateralConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	sites, err := rime.Register[Site](db, rime.WithTableName[Site]("bilsites"))
	if err != nil {
		t.Fatal(err)
	}
	const nsites = 20
	siteIDs := make([]string, nsites)
	for i := range siteIDs {
		siteIDs[i] = fmt.Sprintf("bs%02d", i)
		if err := sites.Upsert(&Site{ID: siteIDs[i], Region: "East"}); err != nil {
			t.Fatal(err)
		}
	}
	rng := rand.New(rand.NewSource(70707))
	const rows = 200
	joinKeys := append(append([]string{}, siteIDs...), "orphan")
	for i := 0; i < rows; i++ {
		d := Device{ID: fmt.Sprintf("b%03d", i), Hostname: fmt.Sprintf("bh-%03d", i),
			Site: joinKeys[rng.Intn(len(joinKeys))], Status: rng.Intn(5), Latency: i}
		if err := dev.Upsert(&d); err != nil {
			t.Fatal(err)
		}
	}
	dsite := rime.SF[Device](dev, "Site")
	sid := rime.SF[Site](sites, "ID")

	var devCommits, siteCommits atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(9100 + w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("b%03d", r.Intn(rows))
				site := joinKeys[r.Intn(len(joinKeys))]
				for {
					err := db.WriteTx(func(tx *rime.Tx) error {
						return dev.In(tx).Update(id, func(d *Device) error {
							d.Site = site
							return nil
						})
					})
					if err == nil {
						devCommits.Add(1)
						break
					}
					if !errors.Is(err, rime.ErrConflict) {
						t.Errorf("device writer %d: %v", w, err)
						return
					}
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(9200))
		for {
			select {
			case <-stop:
				return
			default:
			}
			id := siteIDs[r.Intn(len(siteIDs))]
			// Sole site writer: no conflicts possible, no retry needed.
			err := db.WriteTx(func(tx *rime.Tx) error {
				return sites.In(tx).Update(id, func(s *Site) error {
					if s.Region == "East" {
						s.Region = "West"
					} else {
						s.Region = "East"
					}
					return nil
				})
			})
			if err != nil {
				t.Errorf("site churner: %v", err)
				return
			}
			siteCommits.Add(1)
		}
	}()

	var sawWest atomic.Bool
	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	const readers = 6
	const iters = 60
	var rwg sync.WaitGroup
	for w := 0; w < readers; w++ {
		rwg.Add(1)
		go func(w int) {
			defer rwg.Done()
			for i := 0; i < iters; i++ {
				pin := db.ReadTx()
				devB := dev.In(pin)
				siteB := sites.In(pin)
				devModel := make(map[string]Device, rows)
				for k := 0; k < rows; k++ {
					id := fmt.Sprintf("b%03d", k)
					row, err := devB.Get(id)
					if err != nil {
						fail("worker %d iter %d oracle device get: %v", w, i, err)
						pin.Close()
						return
					}
					devModel[id] = *row
				}
				siteModel := make(map[string]Site, len(siteIDs))
				for _, id := range siteIDs {
					row, err := siteB.Get(id)
					if err != nil {
						fail("worker %d iter %d oracle site get: %v", w, i, err)
						pin.Close()
						return
					}
					siteModel[id] = *row
					if row.Region == "West" {
						sawWest.Store(true)
					}
				}
				inner, err := rime.InnerJoinOn(devB, dsite, siteB, sid)
				if err != nil {
					fail("worker %d iter %d inner: %v", w, i, err)
					pin.Close()
					return
				}
				wantInner := map[string]bool{}
				for id, d := range devModel {
					if _, ok := siteModel[d.Site]; ok {
						wantInner[id+"\x00"+d.Site] = true
					}
				}
				if len(inner) != len(wantInner) {
					fail("worker %d iter %d inner: %d pairs want %d", w, i, len(inner), len(wantInner))
					pin.Close()
					return
				}
				for _, r := range inner {
					if r.Left.Site != r.Right.ID {
						fail("worker %d iter %d inner torn %+v %+v", w, i, r.Left, r.Right)
						pin.Close()
						return
					}
					if !wantInner[r.Left.ID+"\x00"+r.Right.ID] {
						fail("worker %d iter %d inner phantom %s/%s", w, i, r.Left.ID, r.Right.ID)
						pin.Close()
						return
					}
					if devModel[r.Left.ID] != *r.Left || siteModel[r.Right.ID] != *r.Right {
						fail("worker %d iter %d inner stale row %s", w, i, r.Left.ID)
						pin.Close()
						return
					}
				}
				left, err := rime.LeftJoinOn(devB, dsite, siteB, sid)
				if err != nil {
					fail("worker %d iter %d left: %v", w, i, err)
					pin.Close()
					return
				}
				if len(left) != len(devModel) {
					fail("worker %d iter %d left: %d rows want %d", w, i, len(left), len(devModel))
					pin.Close()
					return
				}
				seenLeft := map[string]bool{}
				for _, r := range left {
					if seenLeft[r.Left.ID] {
						fail("worker %d iter %d left: duplicate %s", w, i, r.Left.ID)
						pin.Close()
						return
					}
					seenLeft[r.Left.ID] = true
					want, ok := siteModel[r.Left.Site]
					if !ok {
						if r.Right != nil {
							fail("worker %d iter %d left: orphan %s matched %+v", w, i, r.Left.ID, r.Right)
							pin.Close()
							return
						}
						continue
					}
					if r.Right == nil || *r.Right != want || devModel[r.Left.ID] != *r.Left {
						fail("worker %d iter %d left %s mismatch", w, i, r.Left.ID)
						pin.Close()
						return
					}
				}
				pin.Close()
			}
		}(w)
	}
	rwg.Wait()
	close(stop)
	wg.Wait()
	close(errCh)
	for msg := range errCh {
		t.Error(msg)
	}
	if devCommits.Load() == 0 || siteCommits.Load() == 0 {
		t.Fatalf("churn did not run: dev=%d site=%d", devCommits.Load(), siteCommits.Load())
	}
	if !sawWest.Load() {
		t.Fatal("no West region observed: site churn never interleaved with join pins (vacuous)")
	}
	t.Logf("bilateral joins: devCommits=%d siteCommits=%d", devCommits.Load(), siteCommits.Load())
}

// TestSelfJoinConcurrent self-joins devices to devices on the non-unique Site
// field while writers flip Site membership. Build and probe sides share one
// table, so executor aliasing (reused buffers, build/probe snapshot skew) would
// surface as phantom, missing, or torn pairs against the pinned oracle.
func TestSelfJoinConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	rng := rand.New(rand.NewSource(80808))
	const rows = 150
	for i := 0; i < rows; i++ {
		d := Device{ID: fmt.Sprintf("f%03d", i), Hostname: fmt.Sprintf("fh-%03d", i),
			Site: fmt.Sprintf("s%d", rng.Intn(5)), Status: rng.Intn(5), Latency: i}
		if err := dev.Upsert(&d); err != nil {
			t.Fatal(err)
		}
	}
	siteF := rime.SF[Device](dev, "Site")

	var commits atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(9300 + w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("f%03d", r.Intn(rows))
				site := fmt.Sprintf("s%d", r.Intn(5))
				for {
					err := db.WriteTx(func(tx *rime.Tx) error {
						return dev.In(tx).Update(id, func(d *Device) error {
							d.Site = site
							return nil
						})
					})
					if err == nil {
						commits.Add(1)
						break
					}
					if !errors.Is(err, rime.ErrConflict) {
						t.Errorf("writer %d: %v", w, err)
						return
					}
				}
			}
		}(w)
	}

	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	var maxPairs atomic.Int64
	const readers = 6
	const iters = 60
	var rwg sync.WaitGroup
	for w := 0; w < readers; w++ {
		rwg.Add(1)
		go func(w int) {
			defer rwg.Done()
			for i := 0; i < iters; i++ {
				pin := db.ReadTx()
				devB := dev.In(pin)
				model := make(map[string]Device, rows)
				bySite := map[string][]string{}
				for k := 0; k < rows; k++ {
					id := fmt.Sprintf("f%03d", k)
					row, err := devB.Get(id)
					if err != nil {
						fail("worker %d iter %d oracle get: %v", w, i, err)
						pin.Close()
						return
					}
					model[id] = *row
					bySite[row.Site] = append(bySite[row.Site], id)
				}
				// 1. Full self-join: every same-site pair incl. self-pairs.
				got, err := rime.InnerJoinOn(devB, siteF, devB, siteF)
				if err != nil {
					fail("worker %d iter %d self join: %v", w, i, err)
					pin.Close()
					return
				}
				want := map[string]bool{}
				for _, ids := range bySite {
					for _, l := range ids {
						for _, r := range ids {
							want[l+"\x00"+r] = true
						}
					}
				}
				if len(got) != len(want) {
					fail("worker %d iter %d self join: %d pairs want %d", w, i, len(got), len(want))
					pin.Close()
					return
				}
				for n, prev := int64(len(got)), maxPairs.Load(); n > prev; prev = maxPairs.Load() {
					if maxPairs.CompareAndSwap(prev, n) {
						break
					}
				}
				for _, r := range got {
					if r.Left.Site != r.Right.Site {
						fail("worker %d iter %d self join torn %+v %+v", w, i, r.Left, r.Right)
						pin.Close()
						return
					}
					if !want[r.Left.ID+"\x00"+r.Right.ID] {
						fail("worker %d iter %d self join phantom %s/%s", w, i, r.Left.ID, r.Right.ID)
						pin.Close()
						return
					}
					if model[r.Left.ID] != *r.Left || model[r.Right.ID] != *r.Right {
						fail("worker %d iter %d self join stale %s/%s", w, i, r.Left.ID, r.Right.ID)
						pin.Close()
						return
					}
				}
				// 2. Filtered self-join excludes self-pairs via predicate.
				distinct, err := rime.InnerJoinOn(devB, siteF, devB, siteF,
					func(l, r *Device) bool { return l.ID != r.ID })
				if err != nil {
					fail("worker %d iter %d filtered self join: %v", w, i, err)
					pin.Close()
					return
				}
				if len(distinct) != len(want)-len(model) {
					fail("worker %d iter %d filtered: %d pairs want %d", w, i, len(distinct), len(want)-len(model))
					pin.Close()
					return
				}
				for _, r := range distinct {
					if r.Left.ID == r.Right.ID {
						fail("worker %d iter %d filtered kept self-pair %s", w, i, r.Left.ID)
						pin.Close()
						return
					}
					if model[r.Left.ID] != *r.Left || model[r.Right.ID] != *r.Right {
						fail("worker %d iter %d filtered stale %s/%s", w, i, r.Left.ID, r.Right.ID)
						pin.Close()
						return
					}
				}
				pin.Close()
			}
		}(w)
	}
	rwg.Wait()
	close(stop)
	wg.Wait()
	close(errCh)
	for msg := range errCh {
		t.Error(msg)
	}
	if commits.Load() == 0 {
		t.Fatal("writers committed nothing (vacuous)")
	}
	if maxPairs.Load() == 0 {
		t.Fatal("self-join never returned pairs (vacuous)")
	}
	t.Logf("self joins: commits=%d maxPairs=%d", commits.Load(), maxPairs.Load())
}

// TestProjectionConcurrent projects rows while writers flip the filtered and
// projected fields. Each projected row must exactly equal the oracle row at the
// same pin: index/row snapshot skew would surface as a wrong set member or a
// field mismatch.
func TestProjectionConcurrent(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	rng := rand.New(rand.NewSource(90909))
	const rows = 150
	for i := 0; i < rows; i++ {
		d := Device{ID: fmt.Sprintf("g%03d", i), Hostname: fmt.Sprintf("gh-%03d", i),
			Site: fmt.Sprintf("s%d", rng.Intn(5)), Status: rng.Intn(5), Latency: i}
		if err := dev.Upsert(&d); err != nil {
			t.Fatal(err)
		}
	}
	type status struct {
		ID       string
		Hostname string
		Status   int
		Latency  int
	}
	project := func(d *Device) status {
		return status{ID: d.ID, Hostname: d.Hostname, Status: d.Status, Latency: d.Latency}
	}
	siteF := rime.SF[Device](dev, "Site")
	statusF := rime.OF[Device, int](dev, "Status")
	latF := rime.OF[Device, int](dev, "Latency")

	var commits atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(9400 + w)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("g%03d", r.Intn(rows))
				site := fmt.Sprintf("s%d", r.Intn(5))
				st := r.Intn(5)
				for {
					err := db.WriteTx(func(tx *rime.Tx) error {
						return dev.In(tx).Update(id, func(d *Device) error {
							d.Site, d.Status = site, st
							return nil
						})
					})
					if err == nil {
						commits.Add(1)
						break
					}
					if !errors.Is(err, rime.ErrConflict) {
						t.Errorf("writer %d: %v", w, err)
						return
					}
				}
			}
		}(w)
	}

	errCh := make(chan string, 64)
	fail := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	var nonEmpty atomic.Int64
	const readers = 6
	const iters = 40
	var rwg sync.WaitGroup
	for w := 0; w < readers; w++ {
		rwg.Add(1)
		go func(w int) {
			defer rwg.Done()
			for i := 0; i < iters; i++ {
				pin := db.ReadTx()
				model := make(map[string]Device, rows)
				for k := 0; k < rows; k++ {
					id := fmt.Sprintf("g%03d", k)
					row, err := dev.In(pin).Get(id)
					if err != nil {
						fail("worker %d iter %d oracle get: %v", w, i, err)
						pin.Close()
						return
					}
					model[id] = *row
				}
				checkSet := func(name string, q *rime.Query[Device], match func(Device) bool) bool {
					got, err := rime.Project(q, project)
					if err != nil {
						fail("worker %d iter %d %s: %v", w, i, name, err)
						return false
					}
					if len(got) > 0 {
						nonEmpty.Add(1)
					}
					want := map[string]status{}
					for id, d := range model {
						if match(d) {
							want[id] = project(&d)
						}
					}
					if len(got) != len(want) {
						fail("worker %d iter %d %s: %d rows want %d", w, i, name, len(got), len(want))
						return false
					}
					for _, r := range got {
						wantRow, ok := want[r.ID]
						if !ok || wantRow != r {
							fail("worker %d iter %d %s: row %+v mismatch (present=%v)", w, i, name, r, ok)
							return false
						}
					}
					return true
				}
				if !checkSet("site", dev.Where(siteF.Eq("s1")).In(pin),
					func(d Device) bool { return d.Site == "s1" }) {
					pin.Close()
					return
				}
				if !checkSet("status-range", dev.Where(statusF.Between(1, 3)).In(pin),
					func(d Device) bool { return d.Status >= 1 && d.Status <= 3 }) {
					pin.Close()
					return
				}
				// Ordered projection: Latency is never churned, so the
				// limit-20 sequence must match exactly, order included.
				ord, err := rime.Project(dev.Where().In(pin).OrderBy(latF).Limit(20), project)
				if err != nil {
					fail("worker %d iter %d ordered: %v", w, i, err)
					pin.Close()
					return
				}
				ids := make([]string, 0, len(model))
				for id := range model {
					ids = append(ids, id)
				}
				sort.Strings(ids)
				byLat := make([]Device, 0, len(ids))
				for _, id := range ids {
					byLat = append(byLat, model[id])
				}
				sort.Slice(byLat, func(a, b int) bool { return byLat[a].Latency < byLat[b].Latency })
				if len(ord) != 20 {
					fail("worker %d iter %d ordered: %d rows want 20", w, i, len(ord))
					pin.Close()
					return
				}
				for k, r := range ord {
					if want := project(&byLat[k]); want != r {
						fail("worker %d iter %d ordered row %d: %+v want %+v", w, i, k, r, want)
						pin.Close()
						return
					}
				}
				pin.Close()
			}
		}(w)
	}
	rwg.Wait()
	close(stop)
	wg.Wait()
	close(errCh)
	for msg := range errCh {
		t.Error(msg)
	}
	if commits.Load() == 0 {
		t.Fatal("writers committed nothing (vacuous)")
	}
	if nonEmpty.Load() == 0 {
		t.Fatal("all projections empty (vacuous)")
	}
	t.Logf("projections: commits=%d nonEmpty=%d", commits.Load(), nonEmpty.Load())
}
