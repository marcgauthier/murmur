package murmur

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/q"
	"github.com/marcgauthier/murmur/rime"
)

func TestItemCompiledQueryLive(t *testing.T) {
	db := openItemTestDB(t)
	seedItemDevices(t, db)
	ctx := context.Background()

	cq := db.Query(itemDevice{},
		q.Eq("Site", q.Param()),
		q.Gte("Status", q.Param()),
	).WithContext(ctx).Compile()

	var results []itemDevice
	if err := cq.FindInto(&results, "OTT", 2); err != nil {
		t.Fatalf("FindInto: %v", err)
	}
	if len(results) != 1 || results[0].Hostname != "edge-01" {
		t.Fatalf("FindInto OTT/2 = %+v, want [edge-01]", results)
	}
	// Rebinding the same handle queries fresh values.
	if err := cq.FindInto(&results, "YUL", 2); err != nil {
		t.Fatalf("FindInto rebind: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("FindInto YUL/2 = %d rows, want 2", len(results))
	}
	if n, err := cq.Count("OTT", 1); err != nil || n != 2 {
		t.Fatalf("Count OTT/1 = %d, %v; want 2", n, err)
	}
	if ok, err := cq.Exists("YUL", 9); err != nil || ok {
		t.Fatalf("Exists YUL/9 = %v, %v; want false", ok, err)
	}
	var first itemDevice
	if err := cq.FirstInto(&first, "OTT", 3); err != nil {
		t.Fatalf("FirstInto: %v", err)
	}
	if first.Hostname != "edge-01" {
		t.Fatalf("FirstInto = %s, want edge-01", first.Hostname)
	}
	plan, err := cq.Explain("OTT", 1)
	if err != nil || plan == "" {
		t.Fatalf("Explain = %q, %v; want a plan", plan, err)
	}
}

func TestItemCompiledQueryShapesLive(t *testing.T) {
	db := openItemTestDB(t)
	seedItemDevices(t, db)

	ordered := db.Query(itemDevice{}, q.Eq("Site", q.Param())).OrderBy("Status").Limit(1).Compile()
	var limited []itemDevice
	if err := ordered.FindInto(&limited, "YUL"); err != nil {
		t.Fatalf("FindInto ordered: %v", err)
	}
	if len(limited) != 1 || limited[0].Hostname != "core-02" {
		t.Fatalf("FindInto ordered = %+v, want [core-02]", limited)
	}

	// One placeholder reused in two positions binds a single argument.
	shared := q.Param()
	or := db.Query(itemDevice{}, q.Or(q.Eq("Hostname", shared), q.Eq("Site", shared))).Compile()
	if n, err := or.Count("OTT"); err != nil || n != 2 {
		t.Fatalf("Count shared OTT = %d, %v; want 2", n, err)
	}
	if n, err := or.Count("edge-01"); err != nil || n != 1 {
		t.Fatalf("Count shared edge-01 = %d, %v; want 1", n, err)
	}

	between := db.Query(itemDevice{}, q.Between("Status", q.Param(), q.Param())).Compile()
	if n, err := between.Count(2, 3); err != nil || n != 2 {
		t.Fatalf("Count between 2..3 = %d, %v; want 2", n, err)
	}

	mixed := db.Query(itemDevice{}, q.In("Hostname", q.Param(), "edge-01")).Compile()
	if n, err := mixed.Count("core-01"); err != nil || n != 2 {
		t.Fatalf("Count mixed In = %d, %v; want 2", n, err)
	}

	not := db.Query(itemDevice{}, q.Not(q.Eq("Site", q.Param()))).Compile()
	if n, err := not.Count("OTT"); err != nil || n != 2 {
		t.Fatalf("Count Not OTT = %d, %v; want 2", n, err)
	}

	// A compiled query without placeholders is a reusable fixed query.
	fixed := db.Query(itemDevice{}, q.Eq("Site", "OTT")).Compile()
	if n, err := fixed.Count(); err != nil || n != 2 {
		t.Fatalf("Count fixed = %d, %v; want 2", n, err)
	}
	if _, err := fixed.Count("OTT"); err == nil || !strings.Contains(err.Error(), "wants 0 args") {
		t.Fatalf("Count fixed with arg = %v, want arity error", err)
	}
}

func TestItemCompiledQueryTxLive(t *testing.T) {
	db := openItemTestDB(t)
	seedItemDevices(t, db)
	ctx := context.Background()

	cq := db.Query(itemDevice{}, q.Eq("Site", q.Param())).Compile()
	err := db.Transaction(ctx, func(tx *Tx) error {
		if err := tx.InsertItem(&itemDevice{ID: ids.NewRowID(), Hostname: "edge-03", Site: "OTT", Status: 4}); err != nil {
			return err
		}
		bound := cq.In(tx)
		if bound == cq {
			t.Fatal("In returned the same handle")
		}
		n, err := bound.Count("OTT")
		if err != nil {
			return err
		}
		if n != 3 {
			t.Fatalf("Count in tx = %d, want 3 (staged write visible)", n)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	if n, err := cq.Count("OTT"); err != nil || n != 3 {
		t.Fatalf("Count after commit = %d, %v; want 3", n, err)
	}
}

func TestItemCompiledQueryErrorsLive(t *testing.T) {
	db := openItemTestDB(t)
	seedItemDevices(t, db)
	ctx := context.Background()

	cq := db.Query(itemDevice{},
		q.Eq("Site", q.Param()),
		q.Gte("Status", q.Param()),
	).Compile()

	var results []itemDevice
	if err := cq.FindInto(&results, "OTT"); err == nil || !strings.Contains(err.Error(), "wants 2 args") {
		t.Fatalf("FindInto short bind = %v, want arity error", err)
	}
	if _, err := cq.Count("OTT", 1, "extra"); err == nil || !strings.Contains(err.Error(), "wants 2 args") {
		t.Fatalf("Count long bind = %v, want arity error", err)
	}
	if _, err := cq.Count("OTT", "high"); err == nil || !strings.Contains(err.Error(), `param 2 for field "Status"`) {
		t.Fatalf("Count bad type = %v, want param error", err)
	}
	// A shared placeholder is checked at each use site.
	shared := q.Param()
	mixed := db.Query(itemDevice{}, q.And(q.Eq("Site", shared), q.Eq("Status", shared))).Compile()
	if _, err := mixed.Count("OTT"); err == nil || !strings.Contains(err.Error(), `param 1 for field "Status"`) {
		t.Fatalf("Count incompatible share = %v, want param error", err)
	}

	// Placeholders without Compile fail at execution, not silently.
	if err := db.Find(ctx, &results, q.Eq("Site", q.Param())); err == nil || !strings.Contains(err.Error(), "unbound parameter") {
		t.Fatalf("Find unbound = %v, want unbound error", err)
	}
	if _, err := db.Count(ctx, itemDevice{}, q.In("Site", q.Param())); err == nil || !strings.Contains(err.Error(), "unbound parameter") {
		t.Fatalf("Count unbound = %v, want unbound error", err)
	}
	// Field and operator mistakes still fail fast at build time.
	bad := db.Query(itemDevice{}, q.Eq("Nope", q.Param())).Compile()
	if _, err := bad.Count("x"); err == nil || !strings.Contains(err.Error(), `unknown field "Nope"`) {
		t.Fatalf("Count unknown field = %v, want field error", err)
	}

	var first itemDevice
	if err := cq.FirstInto(&first, "OTT", 99); !errors.Is(err, rime.ErrNotFound) {
		t.Fatalf("FirstInto missing = %v, want ErrNotFound", err)
	}
	var nilQuery *ItemQuery
	if _, err := nilQuery.Compile().Count(); !errors.Is(err, rime.ErrBadView) {
		t.Fatalf("nil Compile Count = %v, want ErrBadView", err)
	}
	var nilCompiled *ItemCompiledQuery
	if err := nilCompiled.FindInto(&results); !errors.Is(err, rime.ErrBadView) {
		t.Fatalf("nil FindInto = %v, want ErrBadView", err)
	}
}

func BenchmarkItemFindOneShot(b *testing.B) {
	db := openItemTestDB(b)
	seedItemDevices(b, db)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var results []itemDevice
		if err := db.Find(ctx, &results, q.Eq("Site", "OTT"), q.Gte("Status", 2)); err != nil {
			b.Fatal(err)
		}
		if len(results) != 1 {
			b.Fatalf("Find = %d rows, want 1", len(results))
		}
	}
}

func BenchmarkItemFindCompiled(b *testing.B) {
	db := openItemTestDB(b)
	seedItemDevices(b, db)
	ctx := context.Background()
	cq := db.Query(itemDevice{}, q.Eq("Site", q.Param()), q.Gte("Status", q.Param())).WithContext(ctx).Compile()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var results []itemDevice
		if err := cq.FindInto(&results, "OTT", 2); err != nil {
			b.Fatal(err)
		}
		if len(results) != 1 {
			b.Fatalf("FindInto = %d rows, want 1", len(results))
		}
	}
}

// benchUser is a randomizable model for scale benchmarks of the item API.
type benchUser struct {
	ID     ids.RowID `rime:"primary"`
	Name   string    `rime:"index"`
	Site   string    `rime:"index"`
	Age    int       `rime:"ordered"`
	Score  int       `rime:"ordered"`
	Active bool
}

func benchUserDefinition(t testing.TB) TableDefinition {
	t.Helper()
	definition, err := define[benchUser]("bench_users", 81, RecordOptions{
		PrimaryField: "ID",
		FieldIDs: map[string]uint32{
			"ID": 1, "Name": 2, "Site": 3, "Age": 4, "Score": 5, "Active": 6,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

var benchUserSites = []string{"OTT", "YUL", "YYZ", "YVR", "YHZ", "YWG", "YEG", "YOW"}

// seedBenchUsers inserts n deterministic pseudo-random users in one batch.
func seedBenchUsers(t testing.TB, db *DB, n int) {
	t.Helper()
	rng := rand.New(rand.NewSource(42))
	users := make([]benchUser, n)
	for i := range users {
		users[i] = benchUser{
			ID:     ids.NewRowID(),
			Name:   fmt.Sprintf("user-%05d", rng.Intn(n)),
			Site:   benchUserSites[rng.Intn(len(benchUserSites))],
			Age:    18 + rng.Intn(63),
			Score:  rng.Intn(1000),
			Active: rng.Intn(2) == 0,
		}
	}
	if err := db.InsertMany(context.Background(), &users); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkItemUserFindOneShot(b *testing.B) {
	db := openItemTestDB(b, benchUserDefinition(b))
	seedBenchUsers(b, db, 5000)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		site := benchUserSites[i%len(benchUserSites)]
		age := 18 + (i*7)%63
		var users []benchUser
		if err := db.Find(ctx, &users, q.Eq("Site", site), q.Gte("Age", age)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkItemUserFindCompiled(b *testing.B) {
	db := openItemTestDB(b, benchUserDefinition(b))
	seedBenchUsers(b, db, 5000)
	ctx := context.Background()
	cq := db.Query(benchUser{}, q.Eq("Site", q.Param()), q.Gte("Age", q.Param())).WithContext(ctx).Compile()
	// Both forms must agree before timing.
	var want, got []benchUser
	if err := db.Find(ctx, &want, q.Eq("Site", "OTT"), q.Gte("Age", 30)); err != nil {
		b.Fatal(err)
	}
	if err := cq.FindInto(&got, "OTT", 30); err != nil {
		b.Fatal(err)
	}
	if len(want) != len(got) {
		b.Fatalf("one-shot = %d rows, compiled = %d rows", len(want), len(got))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		site := benchUserSites[i%len(benchUserSites)]
		age := 18 + (i*7)%63
		var users []benchUser
		if err := cq.FindInto(&users, site, age); err != nil {
			b.Fatal(err)
		}
	}
}
