package murmur

import (
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/q"
	"github.com/marcgauthier/murmur/rime"
)

func TestItemAggregateLive(t *testing.T) {
	db := openItemTestDB(t)
	seedItemDevices(t, db)

	all := db.Query(itemDevice{})
	values, err := all.Aggregate(q.Count(), q.Sum("Status"), q.Avg("Status"), q.Min("Status"), q.Max("Status"))
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	if len(values) != 5 {
		t.Fatalf("Aggregate returned %d values, want 5", len(values))
	}
	if values[0] != 4 || values[1] != 11.0 || values[2] != 2.75 || values[3] != 1 || values[4] != 5 {
		t.Fatalf("Aggregate = %v, want [4 11 2.75 1 5]", values)
	}
	// Aggregates respect filters.
	filtered, err := db.Query(itemDevice{}, q.Eq("Site", "OTT")).Aggregate(q.Sum("Status"), q.Count())
	if err != nil {
		t.Fatalf("Aggregate filtered: %v", err)
	}
	if len(filtered) != 2 || filtered[0] != 4.0 || filtered[1] != 2 {
		t.Fatalf("Aggregate filtered = %v, want [4 2]", filtered)
	}
	// Min/Max work on strings and return native values.
	strings, err := all.Aggregate(q.Min("Hostname"), q.Max("Hostname"))
	if err != nil {
		t.Fatalf("Aggregate strings: %v", err)
	}
	if len(strings) != 2 || strings[0] != "core-01" || strings[1] != "edge-02" {
		t.Fatalf("Aggregate strings = %v, want [core-01 edge-02]", strings)
	}
	// Empty matches: Sum is zero, Min is nil.
	empty, err := db.Query(itemDevice{}, q.Eq("Site", "NOWHERE")).Aggregate(q.Sum("Status"), q.Min("Status"), q.Count())
	if err != nil {
		t.Fatalf("Aggregate empty: %v", err)
	}
	if len(empty) != 3 || empty[0] != 0.0 || empty[1] != nil || empty[2] != 0 {
		t.Fatalf("Aggregate empty = %v, want [0 <nil> 0]", empty)
	}
}

func TestItemAggregateErrorsLive(t *testing.T) {
	db := openItemTestDB(t)
	seedItemDevices(t, db)
	all := db.Query(itemDevice{})

	if _, err := all.Aggregate(q.Sum("Site")); err == nil || !strings.Contains(err.Error(), "built-in numeric") {
		t.Fatalf("Sum string = %v, want numeric error", err)
	}
	if _, err := all.Aggregate(q.Avg("Online")); err == nil || !strings.Contains(err.Error(), "built-in numeric") {
		t.Fatalf("Avg bool = %v, want numeric error", err)
	}
	if _, err := all.Aggregate(q.Min("Online")); err == nil || !strings.Contains(err.Error(), "string or built-in numeric") {
		t.Fatalf("Min bool = %v, want ordered error", err)
	}
	if _, err := all.Aggregate(q.Sum("Nope")); err == nil || !strings.Contains(err.Error(), `unknown field "Nope"`) {
		t.Fatalf("Sum unknown = %v, want field error", err)
	}
	if _, err := all.Aggregate(q.Aggregate{Op: q.AggOp(99)}); err == nil || !strings.Contains(err.Error(), "unsupported aggregate") {
		t.Fatalf("bad op = %v, want op error", err)
	}
	var nilQuery *ItemQuery
	if _, err := nilQuery.Aggregate(q.Count()); !errors.Is(err, rime.ErrBadView) {
		t.Fatalf("nil Aggregate = %v, want ErrBadView", err)
	}
}

func sortItemGroups(rows []ItemGroupRow) {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].Keys[0].(string) < rows[j].Keys[0].(string)
	})
}

func TestItemGroupByLive(t *testing.T) {
	db := openItemTestDB(t)
	seedItemDevices(t, db)

	rows, err := db.Query(itemDevice{}).GroupBy("Site").Aggregate(q.Count(), q.Sum("Status"))
	if err != nil {
		t.Fatalf("GroupBy: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("GroupBy returned %d rows, want 2", len(rows))
	}
	sortItemGroups(rows)
	if len(rows[0].Names) != 1 || rows[0].Names[0] != "Site" {
		t.Fatalf("group names = %v, want [Site]", rows[0].Names)
	}
	if rows[0].Keys[0] != "OTT" || rows[0].Values[0] != 2 || rows[0].Values[1] != 4.0 {
		t.Fatalf("OTT group = %v/%v, want [OTT]/[2 4]", rows[0].Keys, rows[0].Values)
	}
	if rows[1].Keys[0] != "YUL" || rows[1].Values[0] != 2 || rows[1].Values[1] != 7.0 {
		t.Fatalf("YUL group = %v/%v, want [YUL]/[2 7]", rows[1].Keys, rows[1].Values)
	}
	// Groups respect filters.
	filtered, err := db.Query(itemDevice{}, q.Gte("Status", 2)).GroupBy("Site").Aggregate(q.Count())
	if err != nil {
		t.Fatalf("GroupBy filtered: %v", err)
	}
	if len(filtered) != 2 {
		t.Fatalf("GroupBy filtered returned %d rows, want 2", len(filtered))
	}
	sortItemGroups(filtered)
	if filtered[0].Values[0] != 1 || filtered[1].Values[0] != 2 {
		t.Fatalf("GroupBy filtered counts = %v/%v, want 1/2", filtered[0].Values, filtered[1].Values)
	}
	// Multiple keys group jointly.
	multi, err := db.Query(itemDevice{}).GroupBy("Site", "Online").Aggregate(q.Count())
	if err != nil {
		t.Fatalf("GroupBy multi: %v", err)
	}
	if len(multi) != 4 {
		t.Fatalf("GroupBy multi returned %d rows, want 4", len(multi))
	}
	if len(multi[0].Names) != 2 || len(multi[0].Keys) != 2 {
		t.Fatalf("multi group shape = %v/%v", multi[0].Names, multi[0].Keys)
	}
}

func TestItemGroupByErrorsLive(t *testing.T) {
	db := openItemTestDB(t)
	seedItemDevices(t, db)

	// Unknown group fields fail at build time with a sticky error.
	if _, err := db.Query(itemDevice{}).GroupBy("Nope").Aggregate(q.Count()); err == nil || !strings.Contains(err.Error(), `unknown field "Nope"`) {
		t.Fatalf("GroupBy unknown = %v, want field error", err)
	}
	if _, err := db.Query(itemDevice{}).GroupBy("Site").Aggregate(q.Sum("Nope")); err == nil || !strings.Contains(err.Error(), `unknown field "Nope"`) {
		t.Fatalf("grouped Sum unknown = %v, want field error", err)
	}
	var nilQuery *ItemQuery
	if _, err := nilQuery.GroupBy("Site").Aggregate(q.Count()); !errors.Is(err, rime.ErrBadView) {
		t.Fatalf("nil GroupBy = %v, want ErrBadView", err)
	}
	var nilGrouped *ItemGroupedQuery
	if _, err := nilGrouped.Aggregate(q.Count()); !errors.Is(err, rime.ErrBadView) {
		t.Fatalf("nil grouped Aggregate = %v, want ErrBadView", err)
	}
}
