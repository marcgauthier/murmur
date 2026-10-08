package rime_test

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

func TestPlanCache(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 50)
	site := rime.SF[Device](dev, "Site")

	q := dev.Where(site.Eq("OTT")).Limit(10)
	if _, err := q.Find(); err != nil {
		t.Fatal(err)
	}
	misses0 := dev.TableStats().PlanMisses
	if misses0 == 0 {
		t.Fatal("first execution should miss the plan cache")
	}
	for i := 0; i < 3; i++ {
		if _, err := q.Find(); err != nil {
			t.Fatal(err)
		}
	}
	st := dev.TableStats()
	if st.PlanHits < 3 {
		t.Fatalf("want >= 3 plan hits, got %+v", st)
	}
	// A write invalidates the cached generation; results stay correct.
	mustSave(t, dev, Device{ID: "fresh", Hostname: "fresh-h", Site: "OTT"})
	rows, err := q.Find()
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		if r.Site != "OTT" {
			t.Fatalf("stale plan served wrong rows: %+v", r)
		}
	}
	found := false
	for _, r := range rows {
		if r.ID == "fresh" {
			found = true
		}
	}
	_ = found // Limit(10) may or may not include it; correctness is predicate match
}

func TestSelectivityPlanner(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 60)
	host := rime.SF[Device](dev, "Hostname")
	site := rime.SF[Device](dev, "Site")

	// Hostname is unique (selectivity 1); Site is not. The planner must probe
	// the unique index first.
	exp := dev.Where(rime.And(host.Eq("host-0007"), site.Eq("MTL"))).Explain()
	t.Log("\n" + exp)
	if got := dev.TableStats().Indexes["Hostname"]; got.Selectivity != 1 {
		t.Fatalf("unique selectivity: %+v", got)
	}
	if got := dev.TableStats().Indexes["Site"]; got.Selectivity >= 1 || got.Selectivity <= 0 {
		t.Fatalf("hash selectivity out of range: %+v", got)
	}
	if !contains(exp, "INDEX SEEK Hostname") {
		t.Fatalf("planner did not prefer the selective index:\n%s", exp)
	}
	rows, err := dev.Where(rime.And(host.Eq("host-0007"), site.Eq("MTL"))).Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Hostname != "host-0007" {
		t.Fatalf("selective query wrong: %+v", rows)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestCompoundNonComparable(t *testing.T) {
	db := rime.New()
	defer db.Close()
	type Bad struct {
		ID   string   `rime:"primary"`
		Tags []string `rime:"index"`
	}
	if _, err := rime.Register[Bad](db, rime.WithCompound[Bad]("bad", "Tags")); err == nil {
		t.Fatal("want registration error for non-comparable compound field")
	}
}

func TestTempPoolReuse(t *testing.T) {
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
	seedDevices(t, dev, 40)
	for _, s := range []string{"OTT", "MTL", "WPG"} {
		if err := sites.Upsert(&Site{ID: s, Region: "R"}); err != nil {
			t.Fatal(err)
		}
	}
	dsite := rime.SF[Device](dev, "Site")
	sid := rime.SF[Site](sites, "ID")
	for i := 0; i < 5; i++ {
		if _, err := rime.InnerJoinOn(dev.In(nil), dsite, sites.In(nil), sid); err != nil {
			t.Fatal(err)
		}
	}
	st := db.Stats()
	if st.TempPoolGets == 0 || st.TempPoolHits == 0 {
		t.Fatalf("pool unused: %+v", st)
	}
}

func TestDetectLeaks(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 5)
	if leaks := db.DetectLeaks(0); len(leaks) != 0 {
		t.Fatalf("phantom leaks: %+v", leaks)
	}
	leaked := db.ReadTx()
	time.Sleep(5 * time.Millisecond)
	leaks := db.DetectLeaks(time.Millisecond)
	if len(leaks) != 1 || leaks[0].Write {
		t.Fatalf("leak not reported: %+v", leaks)
	}
	open := db.OpenTransactions()
	if len(open) != 1 || open[0].ID == 0 {
		t.Fatalf("open tx not listed: %+v", open)
	}
	leaked.Close()
	if leaks := db.DetectLeaks(0); len(leaks) != 0 {
		t.Fatalf("closed tx still reported: %+v", leaks)
	}
	if n := db.Stats().ActiveTxns; n != 0 {
		t.Fatalf("active txns: %d", n)
	}
}

func TestMemoryPressure(t *testing.T) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	const keys = 2000
	for i := 0; i < keys; i++ {
		mustSave(t, dev, Device{ID: fmt.Sprintf("m-%04d", i), Hostname: fmt.Sprintf("mh-%04d", i)})
	}
	// Churn: 25 versions per key.
	for r := 0; r < 25; r++ {
		for i := 0; i < keys; i++ {
			if err := dev.Update(fmt.Sprintf("m-%04d", i), func(d *Device) error {
				d.Status++
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	before := dev.TableStats()
	if before.Versions != int64(keys*26) {
		t.Fatalf("want %d versions pre-GC, got %d", keys*26, before.Versions)
	}
	db.GC()
	after := dev.TableStats()
	if after.Versions != keys || after.Records != keys {
		t.Fatalf("GC did not bound versions: %+v", after)
	}
	// Full delete + GC must release everything.
	for i := 0; i < keys; i++ {
		if err := dev.Delete(fmt.Sprintf("m-%04d", i)); err != nil {
			t.Fatal(err)
		}
	}
	db.GC()
	final := dev.TableStats()
	if final.Versions != 0 || final.Records != 0 || final.Tombstones != 0 {
		t.Fatalf("residue after delete+GC: %+v", final)
	}
}

func TestSoak(t *testing.T) {
	secs := 5
	if v := os.Getenv("RIME_SOAK_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			secs = n
		}
	}
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db, rime.WithCompound[Device]("site_status", "Site", "Status"))
	if err != nil {
		t.Fatal(err)
	}
	site := rime.SF[Device](dev, "Site")
	sites := []string{"OTT", "MTL", "WPG"}
	deadline := time.Now().Add(time.Duration(secs) * time.Second)
	var applied, readOK, gcRuns int
	model := map[string]int{} // key -> status
	i := 0
	for time.Now().Before(deadline) {
		key := fmt.Sprintf("s-%03d", i%300)
		switch i % 5 {
		case 0, 1:
			d := Device{ID: key, Hostname: "h-" + key, Site: sites[i%3], Status: i % 4}
			if err := dev.Upsert(&d); err != nil {
				t.Fatal(err)
			}
			model[key] = d.Status
			applied++
		case 2:
			if err := dev.Update(key, func(x *Device) error { x.Status++; return nil }); err == nil {
				model[key]++
				applied++
			}
		case 3:
			if _, err := dev.Where(site.Eq(sites[i%3])).Limit(50).Find(); err != nil {
				t.Fatal(err)
			}
			readOK++
		case 4:
			_ = dev.Delete(key)
			delete(model, key)
			applied++
		}
		if i%200 == 0 {
			db.GC()
			gcRuns++
		}
		i++
	}
	// Final agreement between engine and model.
	for k, want := range model {
		got, err := dev.Get(k)
		if err != nil || got.Status != want {
			t.Fatalf("soak divergence for %s: %+v vs %d (%v)", k, got, want, err)
		}
	}
	total, _ := dev.Where().Count()
	if total != len(model) {
		t.Fatalf("soak total: engine=%d model=%d", total, len(model))
	}
	if open := db.OpenTransactions(); len(open) != 0 {
		t.Fatalf("leaked transactions after soak: %+v", open)
	}
	t.Logf("soak: %d ops (%d applied, %d reads), %d GC runs", i, applied, readOK, gcRuns)
}

func TestSingleWriteFastPath(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 10)

	// Single update changing a unique value: new value visible, old released.
	if err := dev.Update("d-0001", func(d *Device) error { d.Hostname = "moved"; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := dev.Get("d-0001"); err != nil {
		t.Fatal(err)
	}
	if err := dev.Upsert(&Device{ID: "reuser", Hostname: "host-0001"}); err != nil {
		t.Fatalf("released unique value not reusable: %v", err)
	}
	// Single update colliding on a unique value: rejected.
	if err := dev.Update("d-0002", func(d *Device) error { d.Hostname = "moved"; return nil }); !errors.Is(err, rime.ErrUnique) {
		t.Fatalf("want ErrUnique, got %v", err)
	}
	// Single delete releases unique values.
	if err := dev.Delete("d-0003"); err != nil {
		t.Fatal(err)
	}
	if err := dev.Upsert(&Device{ID: "reuser2", Hostname: "host-0003"}); err != nil {
		t.Fatalf("deleted unique value not reusable: %v", err)
	}
	// Single insert colliding on unique: rejected.
	if err := dev.Insert(&Device{ID: "x", Hostname: "moved"}); !errors.Is(err, rime.ErrUnique) {
		t.Fatalf("want ErrUnique, got %v", err)
	}
}

func TestSingleWritePromotion(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "a", Hostname: "h-a"})
	var calls atomic.Int64
	dev.BeforeCommit(func(tx *rime.Tx) error {
		if calls.Add(1) > 1 {
			return nil
		}
		// Grow the transaction from one write to two inside the hook.
		return dev.In(tx).Upsert(&Device{ID: "b", Hostname: "h-b"})
	})
	if err := db.WriteTx(func(tx *rime.Tx) error {
		return dev.In(tx).Upsert(&Device{ID: "c", Hostname: "h-c"})
	}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("BeforeCommit ran %d times, want 1", calls.Load())
	}
	for _, id := range []string{"b", "c"} {
		if _, err := dev.Get(id); err != nil {
			t.Fatalf("promoted write %s missing: %v", id, err)
		}
	}
}

func TestChunkedDeepHistory(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "hot", Hostname: "h-hot", Latency: 0})
	tx0 := db.ReadTx()
	defer tx0.Close()
	for v := 1; v <= 40; v++ {
		v := v
		if err := dev.Update("hot", func(d *Device) error { d.Latency = v; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	tx1 := db.ReadTx()
	defer tx1.Close()
	for v := 41; v <= 80; v++ {
		v := v
		if err := dev.Update("hot", func(d *Device) error { d.Latency = v; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	tx2 := db.ReadTx()
	defer tx2.Close()
	for v := 81; v <= 100; v++ {
		v := v
		if err := dev.Update("hot", func(d *Device) error { d.Latency = v; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	// 101 versions span four 32-version chunks; each pinned snapshot must
	// still resolve to its own version across chunk boundaries.
	for tx, want := range map[*rime.Tx]int{tx0: 0, tx1: 40, tx2: 80} {
		got, err := dev.In(tx).Get("hot")
		if err != nil {
			t.Fatal(err)
		}
		if got.Latency != want {
			t.Fatalf("snapshot got Latency=%d, want %d", got.Latency, want)
		}
	}
	got, err := dev.Get("hot")
	if err != nil {
		t.Fatal(err)
	}
	if got.Latency != 100 {
		t.Fatalf("latest Latency=%d, want 100", got.Latency)
	}
}

func TestChunkedGCPrune(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "g", Hostname: "h-g", Latency: 0})
	for v := 1; v <= 20; v++ {
		v := v
		if err := dev.Update("g", func(d *Device) error { d.Latency = v; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	txA := db.ReadTx()
	defer txA.Close()
	for v := 21; v <= 70; v++ {
		v := v
		if err := dev.Update("g", func(d *Device) error { d.Latency = v; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	txB := db.ReadTx()
	defer txB.Close()
	db.GC()
	// Pinned snapshot keeps its version; newer snapshots see the head.
	got, err := dev.In(txA).Get("g")
	if err != nil {
		t.Fatal(err)
	}
	if got.Latency != 20 {
		t.Fatalf("pinned Latency=%d, want 20", got.Latency)
	}
	for _, tx := range []*rime.Tx{txB, nil} {
		got, err := dev.In(tx).Get("g")
		if err != nil {
			t.Fatal(err)
		}
		if got.Latency != 70 {
			t.Fatalf("head Latency=%d, want 70", got.Latency)
		}
	}
}

func TestChunkedTombstoneDrop(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "z", Hostname: "h-z"})
	for v := 1; v <= 40; v++ {
		v := v
		if err := dev.Update("z", func(d *Device) error { d.Latency = v; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := dev.Delete("z"); err != nil {
		t.Fatal(err)
	}
	tx := db.ReadTx()
	defer tx.Close()
	db.GC()
	if _, err := dev.In(tx).Get("z"); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("want ErrNotFound after GC drop, got %v", err)
	}
	if _, err := dev.Get("z"); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("want ErrNotFound at latest after GC drop, got %v", err)
	}
}

func TestOrderedReplaceOrder(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "a", Hostname: "h-a", Latency: 10})
	mustSave(t, dev, Device{ID: "b", Hostname: "h-b", Latency: 10})
	mustSave(t, dev, Device{ID: "c", Hostname: "h-c", Latency: 30})
	lat := rime.OF[Device, int](dev, "Latency")
	// Move a away and back: reinsertion lands at the bucket end.
	if err := dev.Update("a", func(d *Device) error { d.Latency = 20; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := dev.Update("a", func(d *Device) error { d.Latency = 10; return nil }); err != nil {
		t.Fatal(err)
	}
	rows, err := dev.Where().OrderByAsc(lat).Find()
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range rows {
		got = append(got, r.ID)
	}
	want := []string{"b", "a", "c"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("ordered IDs=%v, want %v", got, want)
	}
	// Repeated in-place updates keep range and delete paths consistent.
	for v := 11; v <= 15; v++ {
		v := v
		if err := dev.Update("c", func(d *Device) error { d.Latency = v; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := dev.Delete("b"); err != nil {
		t.Fatal(err)
	}
	rows, err = dev.Where(lat.Ge(10)).OrderByAsc(lat).Find()
	if err != nil {
		t.Fatal(err)
	}
	got = got[:0]
	for _, r := range rows {
		got = append(got, r.ID)
	}
	if fmt.Sprint(got) != fmt.Sprint([]string{"a", "c"}) {
		t.Fatalf("after updates IDs=%v, want [a c]", got)
	}
}

func TestUniqueStatsKind(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 5)
	st := dev.TableStats()
	if got := st.Indexes["ID"].Kind; got != "unique" {
		t.Fatalf("ID kind=%q, want unique", got)
	}
	if got := st.Indexes["Site"].Kind; got != "hash" {
		t.Fatalf("Site kind=%q, want hash", got)
	}
}

func TestStackingAcrossStageThreshold(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	// 100 writes force staged-map construction mid-transaction; stacked
	// writes to the same key must still resolve newest-wins with
	// read-your-writes visible throughout.
	err := db.WriteTx(func(tx *rime.Tx) error {
		for i := 0; i < 100; i++ {
			if err := dev.In(tx).Upsert(&Device{ID: "k", Hostname: "h-k", Latency: i}); err != nil {
				return err
			}
			got, err := dev.In(tx).Get("k")
			if err != nil {
				return err
			}
			if got.Latency != i {
				t.Fatalf("read-your-writes at %d: got %d", i, got.Latency)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := dev.Get("k")
	if err != nil {
		t.Fatal(err)
	}
	if got.Latency != 99 {
		t.Fatalf("stacked Latency=%d, want 99", got.Latency)
	}
	// Distinct keys colliding on a unique value in one transaction abort.
	err = db.WriteTx(func(tx *rime.Tx) error {
		for i := 0; i < 100; i++ {
			if err := dev.In(tx).Upsert(&Device{ID: "u" + string(rune('a'+i)), Hostname: "dup", Latency: i}); err != nil {
				return err
			}
		}
		return nil
	})
	if !errors.Is(err, rime.ErrUnique) {
		t.Fatalf("want ErrUnique, got %v", err)
	}
}

func TestHashBucketPromotion(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "a", Hostname: "h-a", Site: "OTT"})
	mustSave(t, dev, Device{ID: "b", Hostname: "h-b", Site: "OTT"})
	mustSave(t, dev, Device{ID: "c", Hostname: "h-c", Site: "MTL"})
	site := rime.SF[Device](dev, "Site")
	count := func(value string) int {
		t.Helper()
		rows, err := dev.Where(site.Eq(value)).Find()
		if err != nil {
			t.Fatal(err)
		}
		return len(rows)
	}
	if n := count("OTT"); n != 2 {
		t.Fatalf("OTT=%d, want 2", n)
	}
	// Shrink a promoted bucket to one survivor, then to empty.
	if err := dev.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if n := count("OTT"); n != 1 {
		t.Fatalf("OTT=%d, want 1", n)
	}
	if err := dev.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if n := count("OTT"); n != 0 {
		t.Fatalf("OTT=%d, want 0", n)
	}
	// Re-add under the emptied value, then share it again via update.
	mustSave(t, dev, Device{ID: "d", Hostname: "h-d", Site: "OTT"})
	if n := count("OTT"); n != 1 {
		t.Fatalf("OTT=%d, want 1", n)
	}
	if err := dev.Update("c", func(d *Device) error { d.Site = "OTT"; return nil }); err != nil {
		t.Fatal(err)
	}
	if n := count("OTT"); n != 2 {
		t.Fatalf("OTT=%d, want 2", n)
	}
	if got := dev.TableStats().Indexes["Site"].Entries; got != 2 {
		t.Fatalf("Site entries=%d, want 2", got)
	}
}

type kindStatus int

type KindRow struct {
	ID  string     `rime:"primary"`
	S   string     `rime:"index"`
	I   int        `rime:"index"`
	U   uint       `rime:"index"`
	F   float64    `rime:"index"`
	B   bool       `rime:"index"`
	A   [16]byte   `rime:"index"`
	N   kindStatus `rime:"index"`
	UQ  string     `rime:"unique"`
	UQI int        `rime:"unique"`
	OI  int        `rime:"ordered"`
	OF  float64    `rime:"ordered"`
}

func TestTypedKeyMatrix(t *testing.T) {
	db := rime.New()
	defer db.Close()
	tab, err := rime.Register[KindRow](db)
	if err != nil {
		t.Fatal(err)
	}
	mk := func(i int) *KindRow {
		return &KindRow{ID: fmt.Sprintf("k%d", i), S: fmt.Sprintf("s%d", i%4), I: i % 4, U: uint(i % 4),
			F: float64(i%4) + 0.5, B: i%2 == 0, A: [16]byte{byte(i % 4)}, N: kindStatus(i % 4),
			UQ: fmt.Sprintf("uq%d", i), UQI: 1000 + i, OI: i, OF: float64(i)}
	}
	var recs []*KindRow
	for i := 0; i < 20; i++ {
		recs = append(recs, mk(i))
	}
	if err := tab.UpsertMany(recs); err != nil {
		t.Fatal(err)
	}
	// Equality across every storage kind, including the kAny fallback.
	s := rime.F[KindRow, string](tab, "S")
	if rows, _ := tab.Where(s.Eq("s1")).Find(); len(rows) != 5 {
		t.Fatalf("string index=%d, want 5", len(rows))
	}
	fi := rime.F[KindRow, int](tab, "I")
	if rows, _ := tab.Where(fi.Eq(2)).Find(); len(rows) != 5 {
		t.Fatalf("int index=%d, want 5", len(rows))
	}
	fu := rime.F[KindRow, uint](tab, "U")
	if rows, _ := tab.Where(fu.Eq(3)).Find(); len(rows) != 5 {
		t.Fatalf("uint index=%d, want 5", len(rows))
	}
	ff := rime.F[KindRow, float64](tab, "F")
	if rows, _ := tab.Where(ff.Eq(1.5)).Find(); len(rows) != 5 {
		t.Fatalf("float index=%d, want 5", len(rows))
	}
	fb := rime.F[KindRow, bool](tab, "B")
	if rows, _ := tab.Where(fb.Eq(true)).Find(); len(rows) != 10 {
		t.Fatalf("bool index=%d, want 10", len(rows))
	}
	fa := rime.F[KindRow, [16]byte](tab, "A")
	if rows, _ := tab.Where(fa.Eq([16]byte{2})).Find(); len(rows) != 5 {
		t.Fatalf("array index=%d, want 5", len(rows))
	}
	// Named types normalize through the reflect fallback on both sides.
	fn := rime.F[KindRow, kindStatus](tab, "N")
	if rows, _ := tab.Where(fn.Eq(kindStatus(1))).Find(); len(rows) != 5 {
		t.Fatalf("named index=%d, want 5", len(rows))
	}
	// Unique enforcement per kind, then move + delete visibility.
	dup := mk(100)
	dup.ID, dup.UQI = "zz", 1007
	if err := tab.Upsert(dup); err == nil {
		t.Fatal("want conflict on duplicate unique")
	} else if !errors.Is(err, rime.ErrUnique) {
		t.Fatalf("want ErrUnique, got %v", err)
	}
	uq := rime.F[KindRow, string](tab, "UQ")
	if rows, _ := tab.Where(uq.Eq("uq7")).Find(); len(rows) != 1 {
		t.Fatalf("unique lookup=%d, want 1", len(rows))
	}
	if err := tab.Update("k7", func(r *KindRow) error { r.S = "moved"; r.OI = 100; return nil }); err != nil {
		t.Fatal(err)
	}
	if rows, _ := tab.Where(s.Eq("moved")).Find(); len(rows) != 1 {
		t.Fatalf("moved lookup=%d, want 1", len(rows))
	}
	oi := rime.OF[KindRow, int](tab, "OI")
	rows, err := tab.Where(oi.Ge(100)).Find()
	if err != nil || len(rows) != 1 || rows[0].ID != "k7" {
		t.Fatalf("ordered range=%v,%v, want [k7]", rows, err)
	}
	orows, err := tab.Where().OrderByAsc(oi).Limit(3).Find()
	if err != nil || len(orows) != 3 || orows[0].OI != 0 {
		t.Fatalf("ordered scan=%v,%v", orows, err)
	}
	if err := tab.Delete("k7"); err != nil {
		t.Fatal(err)
	}
	if rows, _ := tab.Where(s.Eq("moved")).Find(); len(rows) != 0 {
		t.Fatalf("deleted lookup=%d, want 0", len(rows))
	}
}

func TestValidateFlagTransitions(t *testing.T) {
	db := rime.New()
	defer db.Close()
	type Item struct {
		ID  string `rime:"primary"`
		Ref string
		N   int
	}
	tab, err := rime.Register[Item](db)
	if err != nil {
		t.Fatal(err)
	}
	// Clean schema: fast path, no validation.
	if err := tab.Upsert(&Item{ID: "a", N: -1}); err != nil {
		t.Fatal(err)
	}
	// Adding a check enables validation for subsequent writes.
	tab.AddCheck(func(it *Item) error {
		if it.N < 0 {
			return errors.New("negative")
		}
		return nil
	})
	if err := tab.Upsert(&Item{ID: "b", N: -1}); !errors.Is(err, rime.ErrCheck) {
		t.Fatalf("want ErrCheck, got %v", err)
	}
	if err := tab.Upsert(&Item{ID: "b", N: 1}); err != nil {
		t.Fatal(err)
	}
	// Foreign keys registered late are enforced once enabled.
	type Parent struct {
		ID string `rime:"primary"`
	}
	if _, err := rime.Register[Parent](db, rime.WithTableName[Parent]("parents")); err != nil {
		t.Fatal(err)
	}
	tab.AddForeignKey("Ref", "parents", "ID").SetForeignKeys(true)
	if err := tab.Upsert(&Item{ID: "c", Ref: "ghost", N: 1}); !errors.Is(err, rime.ErrForeignKey) {
		t.Fatalf("want ErrForeignKey, got %v", err)
	}
	// Tag-declared FKs with database-level enforcement are never skipped.
	db2 := rime.New(rime.WithForeignKeys(true))
	defer db2.Close()
	type Doc struct {
		ID  string `rime:"primary"`
		PID string `rime:"fk:parents.ID"`
	}
	if _, err := rime.Register[Parent](db2, rime.WithTableName[Parent]("parents")); err != nil {
		t.Fatal(err)
	}
	docs, err := rime.Register[Doc](db2)
	if err != nil {
		t.Fatal(err)
	}
	if err := docs.Upsert(&Doc{ID: "d", PID: "ghost"}); !errors.Is(err, rime.ErrForeignKey) {
		t.Fatalf("want ErrForeignKey for tag FK, got %v", err)
	}
}

func TestFusedUniqueValidation(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	mustSave(t, dev, Device{ID: "a", Hostname: "h-a", Latency: 1})
	mustSave(t, dev, Device{ID: "b", Hostname: "h-b", Latency: 2})
	// Same-transaction unique swap exercises the two-phase claims path.
	err := db.WriteTx(func(tx *rime.Tx) error {
		if err := dev.In(tx).Update("a", func(d *Device) error { d.Hostname = "h-b"; return nil }); err != nil {
			return err
		}
		return dev.In(tx).Update("b", func(d *Device) error { d.Hostname = "h-a"; return nil })
	})
	if err != nil {
		t.Fatal(err)
	}
	// A concurrent commit touching our key still aborts via the base check,
	// even though kept uniques skip claim validation.
	err = db.WriteTx(func(tx1 *rime.Tx) error {
		if err := dev.In(tx1).Update("a", func(d *Device) error { d.Latency = 9; return nil }); err != nil {
			return err
		}
		inner := db.WriteTx(func(tx2 *rime.Tx) error {
			if err := dev.In(tx2).Delete("a"); err != nil {
				return err
			}
			return dev.In(tx2).Upsert(&Device{ID: "c", Hostname: "h-c", Latency: 3})
		})
		if inner != nil {
			return inner
		}
		return nil
	})
	if !errors.Is(err, rime.ErrConflict) {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	// Plain keep-uniques multi-update commits through the clean path.
	err = db.WriteTx(func(tx *rime.Tx) error {
		if err := dev.In(tx).Update("b", func(d *Device) error { d.Latency = 7; return nil }); err != nil {
			return err
		}
		return dev.In(tx).Update("c", func(d *Device) error { d.Latency = 8; return nil })
	})
	if err != nil {
		t.Fatal(err)
	}
}
