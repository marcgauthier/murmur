package rime_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/rime"
)

func TestQueryOperators(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 30)
	site := rime.SF[Device](dev, "Site")
	status := rime.OF[Device, int](dev, "Status")
	lat := rime.OF[Device, int](dev, "Latency")
	host := rime.SF[Device](dev, "Hostname")

	// AND of three predicates (compound index + filter).
	rows, err := dev.Where(rime.And(site.Eq("OTT"), status.Eq(1), lat.Lt(100))).Limit(100).Find()
	if err != nil {
		t.Fatal(err)
	}
	// OTT: i%3==0; Status 1: i%4==1 -> i in {1,13,25}∩ OTT {0,3,...,27} -> i=... check manually below.
	for _, r := range rows {
		if r.Site != "OTT" || r.Status != 1 || !(r.Latency < 100) {
			t.Fatalf("predicate leak: %+v", r)
		}
	}
	if len(rows) == 0 {
		t.Fatal("want matches")
	}

	// Ne / Not.
	n, err := dev.Where(status.Ne(1)).Count()
	if err != nil {
		t.Fatal(err)
	}
	n1, _ := dev.Where(status.Eq(1)).Count()
	if n+n1 != 30 {
		t.Fatalf("Ne/Eq partition broken: %d + %d", n, n1)
	}

	// Between, Gt/Ge/Lt/Le.
	bw, _ := dev.Where(lat.Between(10, 19)).Find()
	if len(bw) != 10 {
		t.Fatalf("want 10 between rows, got %d", len(bw))
	}
	ge, _ := dev.Where(lat.Ge(100)).Count()
	lt, _ := dev.Where(lat.Lt(100)).Count()
	if ge+lt != 30 {
		t.Fatalf("range partition broken: %d + %d", ge, lt)
	}

	// In / NotIn.
	in, _ := dev.Where(site.In("OTT", "MTL")).Count()
	nin, _ := dev.Where(site.NotIn("OTT", "MTL")).Count()
	if in != 20 || nin != 10 {
		t.Fatalf("in/notin broken: %d %d", in, nin)
	}

	// Or / Or-union path.
	or, _ := dev.Where(rime.Or(site.Eq("OTT"), site.Eq("WPG"))).Count()
	if or != 20 {
		t.Fatalf("or broken: %d", or)
	}

	// String matching.
	sw, _ := dev.Where(host.StartsWith("host-000")).Find()
	if len(sw) != 10 { // host-0000..host-0009
		t.Fatalf("startswith: %d", len(sw))
	}
	ct, _ := dev.Where(host.Contains("1")).Count()
	if ct == 0 || ct == 30 {
		t.Fatalf("contains suspicious: %d", ct)
	}
	ew, _ := dev.Where(host.EndsWith("5")).Count()
	if ew != 3 { // 5, 15, 25
		t.Fatalf("endswith: %d", ew)
	}
}

func TestPlannerExplain(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 100)
	site := rime.SF[Device](dev, "Site")
	status := rime.OF[Device, int](dev, "Status")
	lat := rime.OF[Device, int](dev, "Latency")

	exp := dev.Where(rime.And(site.Eq("OTT"), status.Eq(1), lat.Lt(100))).Limit(100).Explain()
	t.Log("\n" + exp)
	for _, want := range []string{"TABLE", "ROWS", "INDEX SEEK site_status", "ESTIMATED CANDIDATES", "FILTER", "LIMIT 100"} {
		if !strings.Contains(exp, want) {
			t.Fatalf("explain missing %q:\n%s", want, exp)
		}
	}
	// Range plan.
	exp2 := dev.Where(lat.Between(5, 10)).Explain()
	if !strings.Contains(exp2, "ORDERED RANGE") {
		t.Fatalf("want ordered range:\n%s", exp2)
	}
	// Contradiction.
	if got := dev.Where(rime.And(site.Eq("OTT"), site.Eq("MTL"))).Explain(); !strings.Contains(got, "EMPTY") {
		t.Fatalf("want EMPTY:\n%s", got)
	}
}

func TestSortLimitOffset(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 50)
	lat := rime.OF[Device, int](dev, "Latency")

	rows, err := dev.Where().OrderByAsc(lat).Limit(5).Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 || rows[0].Latency != 0 || rows[4].Latency != 4 {
		t.Fatalf("asc order broken: %+v", rows)
	}
	desc, _ := dev.Where().OrderByDesc(lat).Limit(3).Find()
	if desc[0].Latency != 49 {
		t.Fatalf("desc order broken: %+v", desc[0])
	}
	off, _ := dev.Where().OrderByAsc(lat).Offset(10).Limit(5).Find()
	if len(off) != 5 || off[0].Latency != 10 {
		t.Fatalf("offset broken")
	}
	// Sort without index (unindexed order key still works via sort).
	// ID is a plain comparable Field, which still implements OrderField.
	id := rime.F[Device, string](dev, "ID")
	sorted, err := dev.Where().OrderByAsc(id).Limit(3).Find()
	if err != nil {
		t.Fatal(err)
	}
	if sorted[0].ID != "d-0000" {
		t.Fatalf("string order broken: %s", sorted[0].ID)
	}
}

func TestCompiledQuery(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 30)
	site := rime.SF[Device](dev, "Site")
	status := rime.OF[Device, int](dev, "Status")

	q := dev.Compile(rime.And(site.Eq(rime.Param[string]()), status.Eq(rime.Param[int]())))
	for _, tc := range []struct {
		site   string
		status int
		want   int
	}{
		{"OTT", 1, 2}, // i ≡ 9 mod 12 in 0..29: {9, 21}
		{"MTL", 0, 3},
		{"WPG", 2, 3}, // i ≡ 2 mod 12 in 0..29: {2, 14, 26}
	} {
		rows, err := q.Find(tc.site, tc.status)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != tc.want {
			t.Fatalf("%s/%d: want %d got %d", tc.site, tc.status, tc.want, len(rows))
		}
		for _, r := range rows {
			if r.Site != tc.site || r.Status != tc.status {
				t.Fatalf("param leak: %+v", r)
			}
		}
	}
	if _, err := q.Find("OTT"); err == nil {
		t.Fatal("want arity error")
	}
}

func TestPrefixAndCompound(t *testing.T) {
	db := rime.New()
	defer db.Close()
	type Doc struct {
		ID    string `rime:"primary"`
		Title string `rime:"prefix,index"`
		Kind  string `rime:"index"`
		Rev   int    `rime:"index"`
	}
	docs, err := rime.Register[Doc](db, rime.WithCompound[Doc]("kind_rev", "Kind", "Rev"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		kind := "a"
		if i%2 == 0 {
			kind = "b"
		}
		if err := docs.Upsert(&Doc{ID: string(rune('a' + i)), Title: "report-2024-q1", Kind: kind, Rev: i % 5}); err != nil {
			t.Fatal(err)
		}
	}
	title := rime.SF[Doc](docs, "Title")
	rows, err := docs.Where(title.StartsWith("report-2024")).Find()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 20 {
		t.Fatalf("prefix: %d", len(rows))
	}
	none, _ := docs.Where(title.StartsWith("zzz")).Find()
	if len(none) != 0 {
		t.Fatal("prefix false positive")
	}
	kind := rime.SF[Doc](docs, "Kind")
	rev := rime.OF[Doc, int](docs, "Rev")
	exp := docs.Where(rime.And(kind.Eq("b"), rev.Eq(2))).Explain()
	if !strings.Contains(exp, "INDEX SEEK kind_rev") {
		t.Fatalf("compound not used:\n%s", exp)
	}
	got, _ := docs.Where(rime.And(kind.Eq("b"), rev.Eq(2))).Find()
	if len(got) != 2 { // even i with i%5==2: i in {2,12}
		t.Fatalf("compound result: %d", len(got))
	}
	// Publication must defer trie pruning until every write is installed: the
	// delete removes the last old leaf while the insert reuses that prefix.
	if err := docs.Insert(&Doc{ID: "handoff", Title: "handoff-old", Kind: "x", Rev: 1}); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := docs.In(tx).Delete("handoff"); err != nil {
		t.Fatal(err)
	}
	if err := docs.In(tx).Insert(&Doc{ID: "handoff-new", Title: "handoff-new", Kind: "x", Rev: 2}); err != nil {
		t.Fatal(err)
	}
	if err := docs.In(tx).Insert(&Doc{ID: "fresh-one", Title: "fresh-one", Kind: "fresh", Rev: 1}); err != nil {
		t.Fatal(err)
	}
	if err := docs.In(tx).Insert(&Doc{ID: "fresh-two", Title: "fresh-two", Kind: "fresh", Rev: 2}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	handoff, err := docs.Where(title.StartsWith("handoff-")).Find()
	if err != nil || len(handoff) != 1 || handoff[0].ID != "handoff-new" {
		t.Fatalf("prefix delete/insert handoff = %v, err %v", handoff, err)
	}
	compoundHandoff, err := docs.Where(rime.And(kind.Eq("x"), rev.Eq(2))).Find()
	if err != nil || len(compoundHandoff) != 1 || compoundHandoff[0].ID != "handoff-new" {
		t.Fatalf("compound delete/insert handoff = %v, err %v", compoundHandoff, err)
	}
	freshOne, err := docs.Where(rime.And(kind.Eq("fresh"), rev.Eq(1))).Find()
	if err != nil || len(freshOne) != 1 || freshOne[0].ID != "fresh-one" {
		t.Fatalf("first compound sibling insert = %v, err %v", freshOne, err)
	}
	freshTwo, err := docs.Where(rime.And(kind.Eq("fresh"), rev.Eq(2))).Find()
	if err != nil || len(freshTwo) != 1 || freshTwo[0].ID != "fresh-two" {
		t.Fatalf("second compound sibling insert = %v, err %v", freshTwo, err)
	}
	freshPrefix, err := docs.Where(title.StartsWith("fresh-")).Find()
	if err != nil || len(freshPrefix) != 2 {
		t.Fatalf("prepared prefix sibling inserts = %v, err %v", freshPrefix, err)
	}
}

func TestBulkQueryOps(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 30)
	site := rime.SF[Device](dev, "Site")

	n, err := dev.Where(site.Eq("OTT")).Update(func(d *Device) error { d.Status = 9; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatalf("bulk update count: %d", n)
	}
	status := rime.OF[Device, int](dev, "Status")
	left, _ := dev.Where(status.Eq(9)).Count()
	if left != 10 {
		t.Fatalf("bulk update invisible: %d", left)
	}
	dn, err := dev.Where(site.Eq("WPG")).Delete()
	if err != nil {
		t.Fatal(err)
	}
	if dn != 10 {
		t.Fatalf("bulk delete count: %d", dn)
	}
	total, _ := dev.Where().Count()
	if total != 20 {
		t.Fatalf("total after bulk delete: %d", total)
	}
}

func TestContextCancel(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 100)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := dev.Where().WithContext(ctx).Find(); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled query: %v", err)
	}
}

func TestLike(t *testing.T) {
	db, dev := openDevices(t)
	defer db.Close()
	seedDevices(t, dev, 30)
	host := rime.SF[Device](dev, "Hostname")

	all, _ := dev.Where(host.Like("host-%")).Count()
	if all != 30 {
		t.Fatalf("like prefix%%: %d", all)
	}
	// LIKE "literal%" should use the prefix index.
	if exp := dev.Where(host.Like("host-000%")).Explain(); !strings.Contains(exp, "prefix") {
		t.Fatalf("like prefix plan:\n%s", exp)
	}
	one, _ := dev.Where(host.Like("host-0001")).Find()
	if len(one) != 1 {
		t.Fatalf("like exact: %d", len(one))
	}
	under, _ := dev.Where(host.Like("host-00_1")).Count()
	if under != 3 { // host-0001, host-0011, host-0021
		t.Fatalf("like underscore: %d", under)
	}
	mid, _ := dev.Where(host.Like("%1%")).Count()
	ct, _ := dev.Where(host.Contains("1")).Count()
	if mid != ct {
		t.Fatalf("like %%1%% (%d) vs contains (%d)", mid, ct)
	}
	none, _ := dev.Where(host.Like("zzz%")).Find()
	if len(none) != 0 {
		t.Fatal("like false positive")
	}
}
