package murmur

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/q"
	"github.com/marcgauthier/murmur/rime"
)

type shortcutText string
type shortcutRank int8
type shortcutBig int64
type shortcutUnsigned uint64
type shortcutFloat float64
type shortcutBool bool
type shortcutID [16]byte

type shortcutDevice struct {
	Ratio    shortcutFloat    `rime:"ordered"`
	ID       ids.RowID        `rime:"ID"`
	Name     string           `rime:"primary"`
	Host     shortcutText     `rime:"prefix"`
	Site     shortcutText     `rime:"index"`
	Rank     shortcutRank     `rime:"ordered"`
	Big      shortcutBig      `rime:"ordered"`
	Unsigned shortcutUnsigned `rime:"ordered"`
	Enabled  shortcutBool     `rime:"index"`
	Identity shortcutID       `rime:"index"`
	When     time.Time        `rime:"ordered"`
	Stamp    time.Time        `rime:"index"`
	Scanned  time.Time
	Data     map[string][]byte
}

func TestItemShortcutsPredicatesAndReopen(t *testing.T) {
	for _, typed := range []bool{false, true} {
		t.Run(map[bool]string{false: "runtime", true: "typed"}[typed], func(t *testing.T) {
			ctx := context.Background()
			var model any = shortcutDevice{}
			if typed {
				definition, err := Model[shortcutDevice]()
				if err != nil {
					t.Fatal(err)
				}
				model = definition
			}
			cfg := modelTestConfig(t, model)
			db, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { db.Close() }()
			base := time.Now()
			zone := time.FixedZone("other", 3600)
			var identity shortcutID
			identity[0] = 7
			rows := []shortcutDevice{
				{Name: "a", Host: "router-01.net", Site: "OTT", Rank: 2, Big: 1 << 60, Unsigned: 1 << 63, Enabled: true, Identity: identity, When: base, Stamp: base, Scanned: base},
				{Name: "b", Host: "router-02.org", Site: "LAB", Rank: 5, Big: (1 << 60) + 1, Unsigned: (1 << 63) + 1, When: base.Add(time.Second), Stamp: base.Add(time.Second), Scanned: base.Add(time.Second)},
				{Name: "c", Host: "switch.net", Site: "OTT", Rank: 9},
			}
			if err = db.InsertMany(ctx, &rows); err != nil {
				t.Fatal(err)
			}
			check := func(matcher q.Matcher, want []string) {
				t.Helper()
				var got []shortcutDevice
				if err := db.Find(ctx, &got, matcher); err != nil {
					t.Fatal(err)
				}
				names := make([]string, len(got))
				for i, row := range got {
					names[i] = row.Name
				} // unordered results
				for _, name := range want {
					found := false
					for _, n := range names {
						if n == name {
							found = true
						}
					}
					if !found {
						t.Fatalf("%s: names=%v want=%v", q.Describe(matcher), names, want)
					}
				}
				if len(names) != len(want) {
					t.Fatalf("%s: names=%v want=%v", q.Describe(matcher), names, want)
				}
			}
			check(q.StartsWith("Host", "router-"), []string{"a", "b"})
			check(q.EndsWith("Host", ".net"), []string{"a", "c"})
			check(q.Contains("Host", "02"), []string{"b"})
			check(q.Like("Host", "router-0_.%"), []string{"a", "b"})
			check(q.StartsWith("Host", ""), []string{"a", "b", "c"})
			check(q.Between("Rank", 2, 5), []string{"a", "b"})
			check(q.Between("Rank", 5, 2), nil)
			check(q.NotIn("Site", "LAB"), []string{"a", "c"})
			check(q.NotIn("Site"), []string{"a", "b", "c"})
			check(q.Gte("Ratio", math.NaN()), nil)
			check(q.In("Site"), nil)
			check(q.Eq("Enabled", true), []string{"a"})
			check(q.Eq("Identity", [16]byte(identity)), []string{"a"})
			check(q.Gt("Big", shortcutBig(1<<60)), []string{"b"})
			check(q.And(q.Gt("Big", shortcutBig(1<<60)), q.Lte("Big", shortcutBig((1<<60)+1))), []string{"b"})
			check(q.Gt("Unsigned", shortcutUnsigned(1<<63)), []string{"b"})
			same := base.Round(0).In(zone)
			check(q.Eq("Stamp", same), []string{"a"})
			check(q.Eq("Scanned", same), []string{"a"})
			check(q.Ne("Stamp", same), []string{"b", "c"})
			check(q.In("Stamp", same), []string{"a"})
			check(q.NotIn("Stamp", same), []string{"b", "c"})
			check(q.Between("When", same, same.Add(time.Second)), []string{"a", "b"})
			check(q.Eq("When", time.Time{}), []string{"c"})
			check(q.And(q.StartsWith("Host", "router"), q.Or(q.Eq("Site", "OTT"), q.Not(q.Eq("Rank", 5)))), []string{"a"})
			for _, test := range []struct {
				matcher q.Matcher
				plan    string
			}{{q.Eq("Site", "OTT"), "INDEX SEEK"}, {q.Gte("Rank", 2), "ORDERED RANGE"}, {q.StartsWith("Host", "router"), "(prefix)"}, {q.Eq("Stamp", same), "INDEX SEEK"}, {q.Gte("When", same), "ORDERED RANGE"}} {
				plan, err := db.Query(shortcutDevice{}, test.matcher).Explain()
				if err != nil || !strings.Contains(plan, test.plan) {
					t.Fatalf("plan=%q want=%q err=%v", plan, test.plan, err)
				}
			}
			for _, matcher := range []q.Matcher{q.Contains("Rank", "x"), q.StartsWith("Missing", "x"), q.StringMatcher{Field: "Host", Op: 99}, q.Gt("Enabled", true), q.Eq("Rank", 128), q.Gte("When", "2026"), q.Eq("Data", nil)} {
				var got []shortcutDevice
				if err := db.Find(ctx, &got, matcher); err == nil {
					t.Fatalf("accepted %v", matcher)
				}
			}
			for _, field := range []string{"Rank", "Big", "Unsigned", "When", "Host", "Enabled", "Identity"} {
				var got []*shortcutDevice
				if err := db.Query(shortcutDevice{}).OrderBy(field).FindInto(&got); err != nil || len(got) != 3 {
					t.Fatalf("order %s: %v", field, err)
				}
			}
			for _, field := range []string{"Big", "Unsigned", "When", "Scanned"} {
				var sorted []shortcutDevice
				if err := db.Query(shortcutDevice{}).OrderBy(field).FindInto(&sorted); err != nil {
					t.Fatal(err)
				}
				names := []string{sorted[0].Name, sorted[1].Name, sorted[2].Name}
				if !reflect.DeepEqual(names, []string{"c", "a", "b"}) {
					t.Fatalf("sort %s=%v", field, names)
				}
				if err := db.Query(shortcutDevice{}).OrderByDescending(field).FindInto(&sorted); err != nil {
					t.Fatal(err)
				}
				if sorted[0].Name != "b" || sorted[2].Name != "c" {
					t.Fatalf("descending %s=%v", field, sorted)
				}
			}
			var got shortcutDevice
			if err := db.FindOne(ctx, &got, q.Eq("Name", "a")); err != nil || got.ID != rows[0].ID {
				t.Fatalf("one=%+v %v", got, err)
			}
			count, err := db.Count(ctx, shortcutDevice{}, q.StartsWith("Host", "router"))
			if err != nil || count != 2 {
				t.Fatalf("count=%d %v", count, err)
			}
			exists, err := db.Exists(ctx, shortcutDevice{}, q.Eq("Name", "missing"))
			if err != nil || exists {
				t.Fatalf("exists=%v %v", exists, err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			var dest []shortcutDevice
			if err := db.Find(canceled, &dest); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel=%v", err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			check(q.Eq("Stamp", same), []string{"a"})
			check(q.Between("When", same, same.Add(time.Second)), []string{"a", "b"})
		})
	}
}

func TestItemShortcutsAssignmentsAndSnapshots(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, modelTestConfig(t, modelDevice{}))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	row := modelDevice{Name: "a", Site: "OTT", Status: 7, Online: true, Data: map[string][]byte{"x": {1}}}
	if err = db.InsertItem(ctx, &row); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.readTxContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	var got []modelDevice
	for _, invalid := range []any{nil, []modelDevice{}, &row, new(int), new([]any), new([]**modelDevice)} {
		if err := db.Find(ctx, invalid); err == nil {
			t.Fatalf("accepted %T", invalid)
		}
	}
	got = []modelDevice{{Name: "retained"}}
	if err = db.Find(ctx, &got, q.Eq("Missing", 1)); err == nil || got[0].Name != "retained" {
		t.Fatal("error changed destination")
	}
	missing := modelDevice{Name: "retained"}
	if err = db.FindOne(ctx, &missing, q.Eq("Name", "missing")); !errors.Is(err, rime.ErrNotFound) || missing.Name != "retained" {
		t.Fatalf("missing=%+v %v", missing, err)
	}
	for _, assignments := range [][]Assignment{{Set("Status", 2), Set("Status", 3)}, {Set("Status", 2), Set("Missing", 3)}, {Set("Name", "b")}, {Set("ID", NewRowID())}, {Set("Status", "bad")}, {{}}} {
		if err = db.Update(ctx, &row, assignments...); err == nil {
			t.Fatal("accepted invalid assignments")
		}
		var current modelDevice
		if err = db.FindOne(ctx, &current); err != nil || current.Status != 7 {
			t.Fatalf("partial staging=%+v %v", current, err)
		}
	}
	if err = db.Update(ctx, &row, Set("Status", 0), Set("Online", false), Set("Data", nil)); err != nil || row.Status != 0 || row.Online || row.Data != nil {
		t.Fatalf("zero assignments=%+v %v", row, err)
	}
	if err = db.Update(ctx, &row); err != nil {
		t.Fatal(err)
	}
	if err = db.Update(ctx, &modelDevice{Name: "missing"}, Set("Status", 1)); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("update missing=%v", err)
	}
	values := map[string][]byte{"x": {3}}
	if err = db.Update(ctx, &row, Set("Data", values)); err != nil {
		t.Fatal(err)
	}
	values["x"][0] = 9
	if err = db.FindOne(ctx, &row); err != nil || row.Data["x"][0] != 3 {
		t.Fatal("published map aliases caller")
	}
	rollback := errors.New("rollback")
	if err = db.Transaction(ctx, func(tx *Tx) error {
		if err := tx.Update(&row, Set("Status", 8)); err != nil {
			return err
		}
		if err := tx.InsertItem(&modelDevice{Name: "b"}); err != nil {
			return err
		}
		var staged []*modelDevice
		if err := tx.Find(&staged); err != nil || len(staged) != 2 {
			return errors.New("staged find")
		}
		var first modelDevice
		if err := tx.FindOne(&first, q.Eq("Name", "a")); err != nil || first.Status != 8 {
			return errors.New("staged first")
		}
		n, err := tx.Count(modelDevice{})
		if err != nil || n != 2 {
			return errors.New("staged count")
		}
		yes, err := tx.Exists(modelDevice{}, q.Eq("Name", "b"))
		if err != nil || !yes {
			return errors.New("staged exists")
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if row.Status != 8 {
		t.Fatal("rollback should retain caller assignments")
	}
	if err = db.FindOne(ctx, &row); err != nil || row.Status != 0 {
		t.Fatal("rollback persisted")
	}
	var old modelDevice
	if err = snapshot.FindOne(&old); err != nil || old.Status != 7 {
		t.Fatalf("snapshot=%+v %v", old, err)
	}
	if err = snapshot.Find(&got); err != nil || len(got) != 1 {
		t.Fatal(err)
	}
	n, err := snapshot.Count(modelDevice{})
	if err != nil || n != 1 {
		t.Fatal(err)
	}
	yes, err := snapshot.Exists(modelDevice{})
	if err != nil || !yes {
		t.Fatal(err)
	}
	// Detached results are independent.
	old.Data["x"][0] = 42
	var again modelDevice
	if err = snapshot.FindOne(&again); err != nil || reflect.DeepEqual(old, again) {
		t.Fatal("snapshot aliases output")
	}
	snapshot.Close()
	if err = snapshot.Find(&got); !errors.Is(err, rime.ErrTxClosed) {
		t.Fatalf("closed snapshot=%v", err)
	}
}

func TestItemShortcutsValidationAndMergePolicies(t *testing.T) {
	ctx := context.Background()
	cfg := modelTestConfig(t, counterDefinition(t), setDefinition(t), extremaDefinition(t))
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	counter := counterRecord{ID: NewRowID()}
	set := setRecord{ID: NewRowID()}
	extrema := extremaRecord{ID: NewRowID()}
	for _, item := range []any{&counter, &set, &extrema} {
		if err = db.InsertItem(ctx, item); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		item       any
		assignment Assignment
	}{{&counter, Set("Count", 1)}, {&set, Set("Tags", []string{"x"})}, {&extrema, Set("High", 5)}, {&extrema, Set("Low", 0)}} {
		if err = db.Update(ctx, test.item, test.assignment); err == nil {
			t.Fatal("merge assignment accepted")
		}
	}
	type unregistered struct{ Name string }
	var unregisteredRows []unregistered
	if err = db.Find(ctx, &unregisteredRows); err == nil {
		t.Fatal("unregistered model accepted")
	}
	var nilDB *DB
	var rows []counterRecord
	if err = nilDB.Find(ctx, &rows); !errors.Is(err, ErrClosed) {
		t.Fatalf("nil DB=%v", err)
	}
	var nilTx *Tx
	if err = nilTx.Find(&rows); !errors.Is(err, rime.ErrTxClosed) {
		t.Fatalf("nil tx=%v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Count(ctx, counterRecord{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed DB=%v", err)
	}
	// One Go type registered twice cannot be inferred without ambiguity.
	a, err := Model[modelDevice](ModelOptions{Name: "first"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Model[modelDevice](ModelOptions{Name: "second"})
	if err != nil {
		t.Fatal(err)
	}
	ambiguous, err := Open(ctx, modelTestConfig(t, a, b))
	if err != nil {
		t.Fatal(err)
	}
	defer ambiguous.Close()
	var devices []modelDevice
	if err = ambiguous.Find(ctx, &devices); err == nil {
		t.Fatal("ambiguous Find accepted")
	}
	if _, err = ambiguous.Exists(ctx, modelDevice{}); err == nil {
		t.Fatal("ambiguous Exists accepted")
	}
}
