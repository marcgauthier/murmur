package rime_test

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

// liveSeconds bounds the live differential tests; override with
// RIME_LIVE_SECONDS for longer hunting runs.
func liveSeconds(t *testing.T) int {
	t.Helper()
	secs := 5
	if v := os.Getenv("RIME_LIVE_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			secs = n
		}
	}
	return secs
}

func seedLiveDevices(t *testing.T, dev *rime.Table[Device], n int) {
	t.Helper()
	recs := make([]*Device, n)
	for i := 0; i < n; i++ {
		recs[i] = &Device{
			ID:       fmt.Sprintf("lv-%06d", i),
			Hostname: fmt.Sprintf("live-host-%06d", i),
			Site:     []string{"OTT", "MTL", "WPG"}[i%3],
			Status:   i % 8,
			Latency:  i % 500,
		}
	}
	if err := dev.UpsertMany(recs); err != nil {
		t.Fatal(err)
	}
}

// idSet returns the sorted IDs of rows for plan-independent comparison.
func idSet(rows []*Device) []string {
	ids := make([]string, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	sort.Strings(ids)
	return ids
}

func sameIDs(a, b []string) bool {
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

// TestLiveIndexScanDifferential hammers one table with concurrent writers and
// GC while readers assert, at one pinned snapshot, that every indexed access
// path returns exactly the rows a manual full-scan filter finds. Any
// index/write skew, premature GC reclamation, or planner candidate error
// shows up as a set mismatch.
func TestLiveIndexScanDifferential(t *testing.T) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db, rime.WithCompound[Device]("site_status", "Site", "Status"))
	if err != nil {
		t.Fatal(err)
	}
	const rows = 500
	seedLiveDevices(t, dev, rows)
	site := rime.SF[Device](dev, "Site")
	host := rime.SF[Device](dev, "Hostname")
	status := rime.OF[Device, int](dev, "Status")
	lat := rime.OF[Device, int](dev, "Latency")

	deadline := time.Now().Add(time.Duration(liveSeconds(t)) * time.Second)
	var stop atomic.Bool
	var wg sync.WaitGroup
	errCh := make(chan string, 16)
	report := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
		stop.Store(true)
	}

	// Writers cycle mutations over the key space.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for !stop.Load() && time.Now().Before(deadline) {
				id := fmt.Sprintf("lv-%06d", (w*127+i)%rows)
				switch i % 4 {
				case 0:
					_ = dev.Upsert(&Device{ID: id, Hostname: fmt.Sprintf("live-host-%s-%d", id, i),
						Site: []string{"OTT", "MTL", "WPG", "YVR"}[i%4], Status: i % 8, Latency: (i * 7) % 500})
				case 1:
					_ = dev.Update(id, func(d *Device) error { d.Status = (d.Status + 1) % 8; d.Latency = (d.Latency + 13) % 500; return nil })
				case 2:
					_ = dev.Delete(id)
				case 3:
					_ = dev.Upsert(&Device{ID: id, Hostname: fmt.Sprintf("live-host-%06d", (w*127+i)%rows),
						Site: []string{"OTT", "MTL", "WPG"}[i%3], Status: i % 8, Latency: i % 500})
				}
				i++
			}
		}(w)
	}

	// GC runs continuously to race version reclamation against readers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() && time.Now().Before(deadline) {
			db.GC()
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Readers pin one snapshot and differential-check every access path.
	var checks atomic.Int64
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() && time.Now().Before(deadline) {
				tx := db.ReadTx()
				func() {
					defer tx.Close()
					base, err := dev.Where().In(tx).Find()
					if err != nil {
						report("base find: %v", err)
						return
					}
					manual := func(match func(*Device) bool) []string {
						var ids []string
						for _, row := range base {
							if match(row) {
								ids = append(ids, row.ID)
							}
						}
						sort.Strings(ids)
						return ids
					}
					check := func(name string, q *rime.Query[Device], want []string) {
						got, err := q.In(tx).Find()
						if err != nil {
							report("%s find: %v", name, err)
							return
						}
						if ids := idSet(got); !sameIDs(ids, want) {
							report("%s mismatch: got %d want %d", name, len(ids), len(want))
							return
						}
						n, err := q.In(tx).Count()
						if err != nil || n != len(want) {
							report("%s count: got %d want %d err %v", name, n, len(want), err)
							return
						}
						checks.Add(1)
					}
					check("hash-eq", dev.Where(site.Eq("OTT")), manual(func(d *Device) bool { return d.Site == "OTT" }))
					check("ordered-range", dev.Where(lat.Between(100, 200)), manual(func(d *Device) bool { return d.Latency >= 100 && d.Latency <= 200 }))
					check("ordered-gt", dev.Where(status.Gt(5)), manual(func(d *Device) bool { return d.Status > 5 }))
					check("compound", dev.Where(rime.And(site.Eq("MTL"), status.Eq(3))), manual(func(d *Device) bool {
						return d.Site == "MTL" && d.Status == 3
					}))
					check("multi-and", dev.Where(rime.And(site.Eq("WPG"), lat.Lt(50))), manual(func(d *Device) bool {
						return d.Site == "WPG" && d.Latency < 50
					}))
					if len(base) > 0 {
						pick := base[len(base)/2]
						check("unique", dev.Where(host.Eq(pick.Hostname)), manual(func(d *Device) bool { return d.Hostname == pick.Hostname }))
						got, err := dev.In(tx).Get(pick.ID)
						if err != nil || got.ID != pick.ID || *got != *pick {
							report("point get mismatch for %s: %+v %v", pick.ID, got, err)
							return
						}
						checks.Add(1)
					}
				}()
			}
		}()
	}
	wg.Wait()
	select {
	case msg := <-errCh:
		t.Fatal(msg)
	default:
	}
	if n := checks.Load(); n == 0 {
		t.Fatal("no differential checks ran")
	} else {
		t.Logf("differential checks: %d", n)
	}
}

// TestLiveUniqueRace hammers one unique value from many goroutines: exactly
// one insert wins per round, every loser fails, and the unique index never
// holds duplicates.
func TestLiveUniqueRace(t *testing.T) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	host := rime.SF[Device](dev, "Hostname")
	const writers = 8
	const rounds = 50
	var totalWins atomic.Int64
	for round := 0; round < rounds; round++ {
		name := fmt.Sprintf("race-%d", round)
		var wg sync.WaitGroup
		var wins atomic.Int64
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				err := dev.Upsert(&Device{ID: fmt.Sprintf("r%d-w%d", round, w), Hostname: name})
				if err == nil {
					wins.Add(1)
					return
				}
				if !errors.Is(err, rime.ErrUnique) && !errors.Is(err, rime.ErrConflict) {
					t.Errorf("round %d: unexpected error %v", round, err)
				}
			}(w)
		}
		wg.Wait()
		if got := wins.Load(); got != 1 {
			t.Fatalf("round %d: winners=%d want 1", round, got)
		}
		totalWins.Add(1)
		// Exactly one row carries the hostname, visible via the unique index.
		rows, err := dev.Where(host.Eq(name)).Find()
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 {
			t.Fatalf("round %d: unique index holds %d rows want 1", round, len(rows))
		}
		if err := dev.Delete(rows[0].ID); err != nil {
			t.Fatal(err)
		}
	}
	if totalWins.Load() != rounds {
		t.Fatalf("total wins=%d want %d", totalWins.Load(), rounds)
	}
}

// TestLiveUniqueSwapRace hunts deadlocks, lost unique enforcement, and state
// corruption when two transactions concurrently swap a unique column: A takes
// B's hostname while B takes A's. Neither side can ever win (each target is
// held in committed state by the other row), so every round must reject both
// sides with ErrUnique or ErrConflict and leave all state untouched. A commit,
// a duplicate/moved hostname, any other error, or a round that never finishes
// (deadlock) fails the test.
func TestLiveUniqueSwapRace(t *testing.T) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	host := rime.SF[Device](dev, "Hostname")
	seed := func() {
		t.Helper()
		for _, d := range []Device{
			{ID: "swap-a", Hostname: "swap-h1", Site: "s"},
			{ID: "swap-b", Hostname: "swap-h2", Site: "s"},
		} {
			d := d
			if err := dev.Upsert(&d); err != nil {
				t.Fatalf("seed: %v", err)
			}
		}
	}
	seed()
	holders := func(name string) []string {
		t.Helper()
		rows, err := dev.Where(host.Eq(name)).Find()
		if err != nil {
			t.Fatalf("query %s: %v", name, err)
		}
		ids := make([]string, len(rows))
		for i, r := range rows {
			ids[i] = r.ID
		}
		return ids
	}

	const rounds = 200
	conflicts := 0
	for round := 0; round < rounds; round++ {
		type outcome struct{ aErr, bErr error }
		done := make(chan outcome, 1)
		go func() {
			start := make(chan struct{})
			var wg sync.WaitGroup
			var aErr, bErr error
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				aErr = db.WriteTx(func(tx *rime.Tx) error {
					if err := dev.In(tx).Update("swap-a", func(d *Device) error {
						d.Hostname = "swap-h2"
						return nil
					}); err != nil {
						return err
					}
					time.Sleep(100 * time.Microsecond) // widen the overlap window
					return nil
				})
			}()
			go func() {
				defer wg.Done()
				<-start
				bErr = db.WriteTx(func(tx *rime.Tx) error {
					if err := dev.In(tx).Update("swap-b", func(d *Device) error {
						d.Hostname = "swap-h1"
						return nil
					}); err != nil {
						return err
					}
					time.Sleep(100 * time.Microsecond)
					return nil
				})
			}()
			close(start)
			wg.Wait()
			done <- outcome{aErr, bErr}
		}()
		var res outcome
		select {
		case res = <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("round %d timed out (possible deadlock)", round)
		}
		// Neither side can ever win: each target hostname is held in
		// committed state by the other row for the whole round. Both
		// must fail cleanly; a nil error means unique enforcement lost.
		for name, e := range map[string]error{"A": res.aErr, "B": res.bErr} {
			if e == nil {
				t.Fatalf("round %d: txn %s unexpectedly committed (unique enforcement lost)", round, name)
			}
			if !errors.Is(e, rime.ErrUnique) && !errors.Is(e, rime.ErrConflict) {
				t.Fatalf("round %d: txn %s unexpected error %v", round, name, e)
			}
			conflicts++
		}
		// State unchanged, at both the unique index and the rows.
		h1, h2 := holders("swap-h1"), holders("swap-h2")
		if len(h1) != 1 || h1[0] != "swap-a" || len(h2) != 1 || h2[0] != "swap-b" {
			t.Fatalf("round %d: holders moved h1=%v h2=%v", round, h1, h2)
		}
		for id, want := range map[string]string{"swap-a": "swap-h1", "swap-b": "swap-h2"} {
			got, err := dev.Get(id)
			if err != nil || got.Hostname != want {
				t.Fatalf("round %d: %s = %+v %v, want hostname %s", round, id, got, err, want)
			}
		}
	}
	if conflicts != 2*rounds {
		t.Fatalf("conflicts=%d want %d (every round must reject both sides)", conflicts, 2*rounds)
	}
	t.Logf("swap rounds=%d rejected=%d", rounds, conflicts)
}

// TestLiveUniqueDeleteReinsertRace races a deleter freeing a unique value
// against an inserter claiming it. The deleter touches nothing else, so it
// must always commit; the inserter wins if and only if the delete landed
// first. Every round asserts the exact end state correlated with the
// inserter's outcome (holders follow the winner, never duplicated, never
// stale), and a per-round watchdog converts deadlock into failure. Sleep
// polarity alternates so both outcomes are exercised deterministically.
func TestLiveUniqueDeleteReinsertRace(t *testing.T) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	host := rime.SF[Device](dev, "Hostname")
	holders := func(name string) []string {
		t.Helper()
		rows, err := dev.Where(host.Eq(name)).Find()
		if err != nil {
			t.Fatalf("query %s: %v", name, err)
		}
		ids := make([]string, len(rows))
		for i, r := range rows {
			ids[i] = r.ID
		}
		return ids
	}

	const rounds = 200
	won, lost := 0, 0
	for round := 0; round < rounds; round++ {
		victim := fmt.Sprintf("dv-%d", round)
		newcomer := fmt.Sprintf("ni-%d", round)
		h := fmt.Sprintf("hv-%d", round)
		d := Device{ID: victim, Hostname: h, Site: "s"}
		if err := dev.Upsert(&d); err != nil {
			t.Fatalf("round %d seed: %v", round, err)
		}
		type outcome struct{ dErr, iErr error }
		done := make(chan outcome, 1)
		go func() {
			start := make(chan struct{})
			var wg sync.WaitGroup
			var dErr, iErr error
			wg.Add(2)
			go func() {
				defer wg.Done()
				<-start
				if round%2 == 1 {
					time.Sleep(100 * time.Microsecond)
				}
				dErr = db.WriteTx(func(tx *rime.Tx) error {
					return dev.In(tx).Delete(victim)
				})
			}()
			go func() {
				defer wg.Done()
				<-start
				if round%2 == 0 {
					time.Sleep(100 * time.Microsecond)
				}
				nd := Device{ID: newcomer, Hostname: h, Site: "s"}
				iErr = db.WriteTx(func(tx *rime.Tx) error {
					return dev.In(tx).Insert(&nd)
				})
			}()
			close(start)
			wg.Wait()
			done <- outcome{dErr, iErr}
		}()
		var res outcome
		select {
		case res = <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("round %d timed out (possible deadlock)", round)
		}
		// The deleter is uncontended: it must always commit.
		if res.dErr != nil {
			t.Fatalf("round %d: deleter failed: %v", round, res.dErr)
		}
		hs := holders(h)
		if len(hs) > 1 {
			t.Fatalf("round %d: duplicate holders %v", round, hs)
		}
		if _, err := dev.Get(victim); !errors.Is(err, rime.ErrNotFound) {
			t.Fatalf("round %d: victim Get = %v, want ErrNotFound", round, err)
		}
		switch {
		case res.iErr == nil:
			won++
			if len(hs) != 1 || hs[0] != newcomer {
				t.Fatalf("round %d: inserter won but holders=%v", round, hs)
			}
			got, err := dev.Get(newcomer)
			if err != nil || got.Hostname != h {
				t.Fatalf("round %d: newcomer = %+v %v", round, got, err)
			}
		case errors.Is(res.iErr, rime.ErrUnique) || errors.Is(res.iErr, rime.ErrConflict):
			lost++
			if len(hs) != 0 {
				t.Fatalf("round %d: inserter lost but holders=%v", round, hs)
			}
		default:
			t.Fatalf("round %d: inserter unexpected error %v", round, res.iErr)
		}
	}
	if won == 0 || lost == 0 {
		t.Fatalf("one-sided race: won=%d lost=%d, want both outcomes", won, lost)
	}
	t.Logf("delete-reinsert rounds=%d won=%d lost=%d", rounds, won, lost)
}

// TestLiveJoinRepeatable pins snapshots while writers mutate both join sides:
// the same join run twice on one snapshot must return identical pairs, and
// every pair must satisfy the join predicate.
func TestLiveJoinRepeatable(t *testing.T) {
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
	for _, s := range []string{"OTT", "MTL", "WPG", "YVR"} {
		if err := sites.Upsert(&Site{ID: s, Region: "R"}); err != nil {
			t.Fatal(err)
		}
	}
	seedLiveDevices(t, dev, 300)
	dsite := rime.SF[Device](dev, "Site")
	sid := rime.SF[Site](sites, "ID")

	deadline := time.Now().Add(time.Duration(liveSeconds(t)) * time.Second)
	var stop atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for !stop.Load() && time.Now().Before(deadline) {
				id := fmt.Sprintf("lv-%06d", (w*131+i)%300)
				if i%2 == 0 {
					_ = dev.Update(id, func(d *Device) error {
						d.Site = []string{"OTT", "MTL", "WPG", "YVR"}[(i+w)%4]
						return nil
					})
				} else {
					_ = dev.Delete(id)
				}
				if i%10 == 0 {
					db.GC()
				}
				i++
			}
		}(w)
	}
	pairs := func(rows []rime.JoinRow[Device, Site]) []string {
		out := make([]string, len(rows))
		for i, r := range rows {
			if r.Right == nil {
				out[i] = r.Left.ID + "->?"
			} else {
				out[i] = r.Left.ID + "->" + r.Right.ID
			}
		}
		sort.Strings(out)
		return out
	}
	rounds := 0
	for time.Now().Before(deadline) {
		tx := db.ReadTx()
		j1, err := rime.InnerJoinOn(dev.In(tx), dsite, sites.In(tx), sid)
		if err != nil {
			tx.Close()
			t.Fatal(err)
		}
		j2, err := rime.InnerJoinOn(dev.In(tx), dsite, sites.In(tx), sid)
		if err != nil {
			tx.Close()
			t.Fatal(err)
		}
		tx.Close()
		p1, p2 := pairs(j1), pairs(j2)
		if !sameIDs(p1, p2) {
			t.Fatalf("join not repeatable at one snapshot: %d vs %d pairs", len(p1), len(p2))
		}
		for _, r := range j1 {
			if r.Right == nil || r.Left.Site != r.Right.ID {
				t.Fatalf("join predicate violated: %+v", r)
			}
		}
		rounds++
	}
	stop.Store(true)
	wg.Wait()
	if rounds == 0 {
		t.Fatal("no join rounds ran")
	}
	t.Logf("repeatable join rounds: %d", rounds)
}

// TestOverlayReadYourWrites pins the in-transaction read semantic
// deterministically: staged inserts, updates, and deletes are visible to
// point reads and queries (filtered, counted, and paginated), vanish on
// rollback, and persist on commit.
func TestOverlayReadYourWrites(t *testing.T) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []Device{
		{ID: "k0", Hostname: "h0", Site: "OTT", Status: 1, Latency: 10},
		{ID: "k1", Hostname: "h1", Site: "OTT", Status: 2, Latency: 20},
		{ID: "k2", Hostname: "h2", Site: "MTL", Status: 3, Latency: 30},
		{ID: "k3", Hostname: "h3", Site: "MTL", Status: 4, Latency: 40},
		{ID: "k4", Hostname: "h4", Site: "WPG", Status: 5, Latency: 50},
	} {
		mustSave(t, dev, d)
	}
	site := rime.SF[Device](dev, "Site")
	stage := func(tx *rime.Tx) error {
		if err := dev.In(tx).Upsert(&Device{ID: "k-new", Hostname: "hn", Site: "OTT", Status: 9, Latency: 90}); err != nil {
			return err
		}
		if err := dev.In(tx).Update("k1", func(d *Device) error { d.Site = "WPG"; return nil }); err != nil {
			return err
		}
		return dev.In(tx).Delete("k2")
	}
	// Staged view: k-new present, k1 moved OTT->WPG, k2 gone.
	wantAll := []string{"k-new", "k0", "k1", "k3", "k4"}
	wantOTT := []string{"k-new", "k0"}
	assertStaged := func(tx *rime.Tx) error {
		rows, err := dev.Where().In(tx).Find()
		if err != nil {
			return err
		}
		if ids := idSet(rows); !sameIDs(ids, wantAll) {
			t.Fatalf("staged all: got %v want %v", ids, wantAll)
		}
		ott, err := dev.Where(site.Eq("OTT")).In(tx).Find()
		if err != nil {
			return err
		}
		if ids := idSet(ott); !sameIDs(ids, wantOTT) {
			t.Fatalf("staged OTT: got %v want %v", ids, wantOTT)
		}
		n, err := dev.Where().In(tx).Count()
		if err != nil || n != len(wantAll) {
			t.Fatalf("staged count: got %d want %d err %v", n, len(wantAll), err)
		}
		lim, err := dev.Where().In(tx).Limit(2).Find()
		if err != nil || len(lim) != 2 {
			t.Fatalf("staged limit: got %d want 2 err %v", len(lim), err)
		}
		if got, err := dev.In(tx).Get("k-new"); err != nil || got.ID != "k-new" {
			t.Fatalf("staged get new: %+v %v", got, err)
		}
		if _, err := dev.In(tx).Get("k2"); !errors.Is(err, rime.ErrNotFound) {
			t.Fatalf("staged get deleted: want ErrNotFound got %v", err)
		}
		return nil
	}
	abort := errors.New("rollback probe")
	if err := db.WriteTx(func(tx *rime.Tx) error {
		if err := stage(tx); err != nil {
			return err
		}
		if err := assertStaged(tx); err != nil {
			return err
		}
		return abort
	}); !errors.Is(err, abort) {
		t.Fatalf("rollback: %v", err)
	}
	// Rolled back: original five rows, original sites.
	rows, err := dev.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	if ids := idSet(rows); !sameIDs(ids, []string{"k0", "k1", "k2", "k3", "k4"}) {
		t.Fatalf("post-rollback: got %v", ids)
	}
	if err := db.WriteTx(func(tx *rime.Tx) error {
		if err := stage(tx); err != nil {
			return err
		}
		return assertStaged(tx)
	}); err != nil {
		t.Fatal(err)
	}
	rows, err = dev.Where().Find()
	if err != nil {
		t.Fatal(err)
	}
	if ids := idSet(rows); !sameIDs(ids, wantAll) {
		t.Fatalf("post-commit: got %v want %v", ids, wantAll)
	}
}

// raceStart runs n racers behind a start gate with a watchdog that converts
// deadlock into failure, returning each racer's error.
func raceStart(t *testing.T, n int, fn func(i int) error) []error {
	t.Helper()
	done := make(chan []error, 1)
	go func() {
		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make([]error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				errs[i] = fn(i)
			}(i)
		}
		close(start)
		wg.Wait()
		done <- errs
	}()
	select {
	case errs := <-done:
		return errs
	case <-time.After(30 * time.Second):
		t.Fatal("racers timed out (possible deadlock)")
		return nil
	}
}

// TestLiveUniqueAbortHygiene proves aborted unique-contention losers leave no
// residue: no phantom rows, no leaked primary-key or unique claims, no
// partial updates. Phase 1 races N inserters on one unique hostname: exactly
// one commits, every loser PK is then reusable with a fresh hostname, and a
// delete-winner/reinsert probe proves no ghost claim on the contested value
// lingers after the aborts. Phase 2 races N updaters retargeting distinct
// rows onto one hostname: exactly one commits and every loser row must be
// identical to its pre-race state. Losers must fail with ErrUnique or
// ErrConflict only; any commit beyond the single winner, any phantom row, or
// any unreusable identity fails the test.
func TestLiveUniqueAbortHygiene(t *testing.T) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	host := rime.SF[Device](dev, "Hostname")

	const racers = 8
	const rounds = 50

	holders := func(name string) []string {
		t.Helper()
		rows, err := dev.Where(host.Eq(name)).Find()
		if err != nil {
			t.Fatalf("query %s: %v", name, err)
		}
		ids := make([]string, len(rows))
		for i, r := range rows {
			ids[i] = r.ID
		}
		return ids
	}
	singleWinner := func(round int, errs []error) int {
		t.Helper()
		winner := -1
		for i, e := range errs {
			if e == nil {
				if winner >= 0 {
					t.Fatalf("round %d: racers %d and %d both committed (unique enforcement lost)", round, winner, i)
				}
				winner = i
				continue
			}
			if !errors.Is(e, rime.ErrUnique) && !errors.Is(e, rime.ErrConflict) {
				t.Fatalf("round %d: racer %d unexpected error %v", round, i, e)
			}
		}
		if winner < 0 {
			t.Fatalf("round %d: no winner (all %d racers failed)", round, len(errs))
		}
		return winner
	}

	// Phase 1: insert race for one unique hostname per round.
	for round := 0; round < rounds; round++ {
		contested := fmt.Sprintf("hyg-c-%d", round)
		ids := make([]string, racers)
		for i := range ids {
			ids[i] = fmt.Sprintf("hyg-i-%d-%d", round, i)
		}
		errs := raceStart(t, racers, func(i int) error {
			rec := &Device{ID: ids[i], Hostname: contested, Site: "OTT", Status: i, Latency: i}
			return db.WriteTx(func(tx *rime.Tx) error {
				if err := dev.In(tx).Insert(rec); err != nil {
					return err
				}
				time.Sleep(100 * time.Microsecond) // widen the overlap window
				return nil
			})
		})
		winner := singleWinner(round, errs)
		winnerID := ids[winner]
		// The contested hostname resolves to the winner only.
		if h := holders(contested); !sameIDs(h, []string{winnerID}) {
			t.Fatalf("round %d: holders(%q) = %v, want [%s]", round, contested, h, winnerID)
		}
		// Losers left no phantom rows behind.
		for i, id := range ids {
			if i == winner {
				continue
			}
			if _, err := dev.Get(id); !errors.Is(err, rime.ErrNotFound) {
				t.Fatalf("round %d: loser %s Get = %v, want ErrNotFound", round, id, err)
			}
		}
		// Loser identities are reusable with fresh hostnames.
		for i, id := range ids {
			if i == winner {
				continue
			}
			rec := &Device{ID: id, Hostname: fmt.Sprintf("hyg-f-%d-%d", round, i), Site: "MTL"}
			if err := dev.Insert(rec); err != nil {
				t.Fatalf("round %d: reuse loser %s: %v (claim leaked)", round, id, err)
			}
		}
		// Ghost-claim probe: with the winner deleted, the contested value
		// must be claimable again. A lingering aborted claim would reject it.
		if err := dev.Delete(winnerID); err != nil {
			t.Fatalf("round %d: delete winner %s: %v", round, winnerID, err)
		}
		ghost := &Device{ID: fmt.Sprintf("hyg-g-%d", round), Hostname: contested, Site: "WPG"}
		if err := dev.Insert(ghost); err != nil {
			t.Fatalf("round %d: reinsert contested %q: %v (ghost claim)", round, contested, err)
		}
		// Net per round: racers rows (winner + reused losers + ghost probe,
		// minus the deleted winner).
		if n, err := dev.Count(); err != nil || n != (round+1)*racers {
			t.Fatalf("round %d: count = %d,%v want %d", round, n, err, (round+1)*racers)
		}
	}

	// Phase 2: update race retargeting distinct rows onto one hostname.
	for round := 0; round < rounds; round++ {
		target := fmt.Sprintf("hyg-t-%d", round)
		ids := make([]string, racers)
		own := make([]string, racers)
		for i := range ids {
			ids[i] = fmt.Sprintf("hyg-u-%d-%d", round, i)
			own[i] = fmt.Sprintf("hyg-own-%d-%d", round, i)
			rec := &Device{ID: ids[i], Hostname: own[i], Site: "WPG", Status: i, Latency: 100 + i}
			if err := dev.Insert(rec); err != nil {
				t.Fatalf("round %d: seed %s: %v", round, ids[i], err)
			}
		}
		errs := raceStart(t, racers, func(i int) error {
			return db.WriteTx(func(tx *rime.Tx) error {
				if err := dev.In(tx).Update(ids[i], func(d *Device) error {
					d.Hostname = target
					return nil
				}); err != nil {
					return err
				}
				time.Sleep(100 * time.Microsecond)
				return nil
			})
		})
		winner := singleWinner(round, errs)
		if h := holders(target); !sameIDs(h, []string{ids[winner]}) {
			t.Fatalf("round %d: holders(%q) = %v, want [%s]", round, target, h, ids[winner])
		}
		for i, id := range ids {
			got, err := dev.Get(id)
			if err != nil {
				t.Fatalf("round %d: Get %s: %v", round, id, err)
			}
			wantHost := own[i]
			if i == winner {
				wantHost = target
			}
			if got.Hostname != wantHost || got.Site != "WPG" || got.Status != i || got.Latency != 100+i {
				t.Fatalf("round %d: %s = %+v, want hostname %s site WPG status %d latency %d",
					round, id, got, wantHost, i, 100+i)
			}
		}
	}
	t.Logf("abort-hygiene rounds=%d racers=%d phases=2", rounds, racers)
}

// TestLiveOCCIncrementStorm is the canonical lost-update proof: N goroutines
// hammer the same row with M increments each, retrying only on ErrConflict.
// Every increment must eventually commit exactly once, so the final counter
// must equal N*M with no other error surfacing. At least one conflict must
// occur (proving the racers actually contended rather than serializing by
// luck), and retry exhaustion fails the test.
func TestLiveOCCIncrementStorm(t *testing.T) {
	db := rime.New()
	defer db.Close()
	dev, err := rime.Register[Device](db)
	if err != nil {
		t.Fatal(err)
	}
	seed := &Device{ID: "ctr", Hostname: "ctr-h", Site: "s"}
	if err := dev.Insert(seed); err != nil {
		t.Fatalf("seed: %v", err)
	}

	const racers = 8
	const perRacer = 100
	var conflicts atomic.Int64
	firstArrivals := atomic.Int64{}
	firstWave := make(chan struct{})
	inc := func(synchronizeFirst bool) error {
		gateFirstAttempt := synchronizeFirst
		for attempt := 0; attempt < 10000; attempt++ {
			err := db.WriteTx(func(tx *rime.Tx) error {
				return dev.In(tx).Update("ctr", func(d *Device) error {
					d.Status++
					if gateFirstAttempt {
						gateFirstAttempt = false
						if firstArrivals.Add(1) == racers {
							close(firstWave)
						}
						<-firstWave
					}
					return nil
				})
			})
			if err == nil {
				return nil
			}
			if errors.Is(err, rime.ErrConflict) {
				conflicts.Add(1)
				continue
			}
			return err
		}
		return fmt.Errorf("retry exhausted")
	}
	errs := raceStart(t, racers, func(i int) error {
		for m := 0; m < perRacer; m++ {
			if err := inc(m == 0); err != nil {
				return fmt.Errorf("racer %d increment %d: %w", i, m, err)
			}
		}
		return nil
	})
	for i, e := range errs {
		if e != nil {
			t.Fatalf("racer %d: %v", i, e)
		}
	}
	got, err := dev.Get("ctr")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != racers*perRacer {
		t.Fatalf("counter = %d, want %d (lost update)", got.Status, racers*perRacer)
	}
	if conflicts.Load() == 0 {
		t.Fatal("no conflicts observed: racers never contended (vacuous storm)")
	}
	t.Logf("occ storm racers=%d per=%d conflicts=%d", racers, perRacer, conflicts.Load())
}
