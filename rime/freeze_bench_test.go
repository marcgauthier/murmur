package rime_test

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

// Frozen acceptance benchmark. Self-contained (stdlib + rime only) so the
// RIME-only baseline never depends on the external comparison harness or CGO.
//
// Canonical acceptance cells (GOMAXPROCS=4, count=5, medians recorded in
// architecture/rime-benchmarks.md):
//
//	update/disjoint/batch=1/writers=1 and writers=4 at 100K rows, matched indexes
//
// Matrix dimensions (sub-benchmarks): op insert/update/delete, writers
// 1/2/4/8, batch 1/16/256, key mix disjoint/hot1/hot64/uniform, index sets
// none/matched/full, rows 1K/100K/1M, plus hooks on/off, pinned readers, and
// a two-table transfer workload. Env overrides: RIME_FREEZE_ROWS,
// RIME_FREEZE_WRITERS, RIME_FREEZE_BATCH, RIME_FREEZE_OP, RIME_FREEZE_MIX,
// RIME_FREEZE_INDEXES. Metrics: rows/s, ns/op, B/op, allocs/op (with
// -benchmem), commit p50/p95/p99 (per-commit time.Now sampling; identical
// methodology before/after), conflicts, retained heap delta.

type freezeUser struct {
	ID    int    `rime:"primary"`
	Name  string `rime:"index"`
	Email string `rime:"unique"`
	Age   int    `rime:"ordered"`
}

type freezeBare struct {
	ID  int `rime:"primary"`
	Age int
}

type freezeFull struct {
	ID    int    `rime:"primary"`
	Name  string `rime:"index,prefix"`
	Email string `rime:"unique"`
	Age   int    `rime:"index,ordered"`
}

type freezeAcct struct {
	ID  int `rime:"primary"`
	Bal int `rime:"ordered"`
}

type freezeCfg struct {
	op      string // insert, update, delete, transfer
	mix     string // disjoint, hot1, hot64, uniform
	indexes string // none, matched, full
	writers int
	batch   int
	rows    int
	hooks   bool
	readers bool // pinned snapshot + background readers during writes
}

func freezeEnv(key string, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func freezeEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func freezeMatrix() []freezeCfg {
	op := freezeEnv("RIME_FREEZE_OP", "")
	mix := freezeEnv("RIME_FREEZE_MIX", "")
	idx := freezeEnv("RIME_FREEZE_INDEXES", "")
	writers := freezeEnvInt("RIME_FREEZE_WRITERS", 0)
	batch := freezeEnvInt("RIME_FREEZE_BATCH", 0)
	rows := freezeEnvInt("RIME_FREEZE_ROWS", 0)
	if op == "" && mix == "" && idx == "" && writers == 0 && batch == 0 && rows == 0 {
		return freezeCanonical()
	}
	ops := []string{"update"}
	if op != "" {
		ops = []string{op}
	}
	mixes := []string{"disjoint"}
	if mix != "" {
		mixes = []string{mix}
	}
	idxs := []string{"matched"}
	if idx != "" {
		idxs = []string{idx}
	}
	writerSet := []int{1, 2, 4, 8}
	if writers > 0 {
		writerSet = []int{writers}
	}
	batchSet := []int{1, 16, 256}
	if batch > 0 {
		batchSet = []int{batch}
	}
	rowSet := []int{100000}
	if rows > 0 {
		rowSet = []int{rows}
	}
	var cfgs []freezeCfg
	for _, o := range ops {
		for _, m := range mixes {
			for _, ix := range idxs {
				for _, w := range writerSet {
					for _, b := range batchSet {
						for _, r := range rowSet {
							cfgs = append(cfgs, freezeCfg{op: o, mix: m, indexes: ix, writers: w, batch: b, rows: r})
						}
					}
				}
			}
		}
	}
	return cfgs
}

func freezeCanonical() []freezeCfg {
	var cfgs []freezeCfg
	for _, w := range []int{1, 2, 4, 8} {
		for _, b := range []int{1, 16, 256} {
			cfgs = append(cfgs, freezeCfg{op: "update", mix: "disjoint", indexes: "matched", writers: w, batch: b, rows: 100000})
		}
	}
	for _, w := range []int{1, 4} {
		cfgs = append(cfgs,
			freezeCfg{op: "update", mix: "hot1", indexes: "matched", writers: w, batch: 1, rows: 100000},
			freezeCfg{op: "update", mix: "hot64", indexes: "matched", writers: w, batch: 1, rows: 100000},
			freezeCfg{op: "update", mix: "uniform", indexes: "matched", writers: w, batch: 1, rows: 100000},
			freezeCfg{op: "insert", mix: "disjoint", indexes: "matched", writers: w, batch: 1, rows: 100000},
			freezeCfg{op: "delete", mix: "disjoint", indexes: "matched", writers: w, batch: 1, rows: 100000},
			freezeCfg{op: "update", mix: "disjoint", indexes: "none", writers: w, batch: 1, rows: 100000},
			freezeCfg{op: "update", mix: "disjoint", indexes: "full", writers: w, batch: 1, rows: 100000},
			freezeCfg{op: "update", mix: "disjoint", indexes: "matched", writers: w, batch: 1, rows: 1000},
			freezeCfg{op: "update", mix: "disjoint", indexes: "matched", writers: w, batch: 1, rows: 1000000},
		)
	}
	cfgs = append(cfgs,
		freezeCfg{op: "update", mix: "disjoint", indexes: "matched", writers: 4, batch: 1, rows: 100000, hooks: true},
		freezeCfg{op: "update", mix: "disjoint", indexes: "matched", writers: 4, batch: 1, rows: 100000, readers: true},
		freezeCfg{op: "transfer", mix: "disjoint", indexes: "matched", writers: 1, batch: 1, rows: 10000},
		freezeCfg{op: "transfer", mix: "disjoint", indexes: "matched", writers: 4, batch: 1, rows: 10000},
	)
	return cfgs
}

func (c freezeCfg) name() string {
	n := fmt.Sprintf("%s/%s/%s/w%d/b%d/n%d", c.op, c.mix, c.indexes, c.writers, c.batch, c.rows)
	if c.hooks {
		n += "/hooks"
	}
	if c.readers {
		n += "/readers"
	}
	return n
}

// freezeOps returns the total timed mutation count. Hot and uniform mixes use
// a fixed op budget so single-key contention phases stay bounded; disjoint
// mixes touch every row once.
func (c freezeCfg) ops() int {
	switch c.mix {
	case "hot1", "hot64", "uniform":
		return 4096
	}
	return c.rows
}

func BenchmarkFreeze(b *testing.B) {
	for _, cfg := range freezeMatrix() {
		b.Run(cfg.name(), func(b *testing.B) {
			freezeRun(b, cfg)
		})
	}
}

func freezeRun(b *testing.B, cfg freezeCfg) {
	if cfg.op == "transfer" {
		freezeTransfer(b, cfg)
		return
	}
	db := rime.New()
	defer db.Close()
	ops := cfg.ops()
	seed := cfg.rows
	if cfg.op == "insert" {
		seed = 0
	}
	switch cfg.indexes {
	case "none":
		tab, err := rime.Register[freezeBare](db, rime.WithTableName[freezeBare]("users"))
		if err != nil {
			b.Fatal(err)
		}
		mk := func(i int) *freezeBare { return &freezeBare{ID: i + 1, Age: i % 100} }
		freezeSeed(b, tab, seed, mk)
		var ins []*freezeBare
		if cfg.op == "insert" {
			ins = make([]*freezeBare, ops)
			for i := range ins {
				ins[i] = mk(i)
			}
		}
		if cfg.hooks {
			freezeArmHooks(tab)
		}
		freezePhase(b, db, cfg, ops,
			func(tx *rime.Tx, id int) error {
				switch cfg.op {
				case "insert":
					return tab.In(tx).Insert(ins[id-1])
				case "delete":
					return tab.In(tx).Delete(id)
				default:
					return tab.In(tx).Update(id, func(r *freezeBare) error { r.Age++; return nil })
				}
			},
			func(tx *rime.Tx, rng *rand.Rand) {
				_, _ = tab.In(tx).Get(rng.Intn(cfg.rows) + 1)
			},
			func() { freezeVerifyCount(b, tab, cfg, ops) })
	case "full":
		tab, err := rime.Register[freezeFull](db,
			rime.WithTableName[freezeFull]("users"),
			rime.WithCompound[freezeFull]("name_age", "Name", "Age"))
		if err != nil {
			b.Fatal(err)
		}
		mk := func(i int) *freezeFull {
			return &freezeFull{ID: i + 1, Name: fmt.Sprintf("n-%d", i%1000), Email: fmt.Sprintf("e-%d", i), Age: i % 100}
		}
		freezeSeed(b, tab, seed, mk)
		var ins []*freezeFull
		if cfg.op == "insert" {
			ins = make([]*freezeFull, ops)
			for i := range ins {
				ins[i] = mk(i)
			}
		}
		if cfg.hooks {
			freezeArmHooks(tab)
		}
		freezePhase(b, db, cfg, ops,
			func(tx *rime.Tx, id int) error {
				switch cfg.op {
				case "insert":
					return tab.In(tx).Insert(ins[id-1])
				case "delete":
					return tab.In(tx).Delete(id)
				default:
					return tab.In(tx).Update(id, func(r *freezeFull) error { r.Age++; return nil })
				}
			},
			func(tx *rime.Tx, rng *rand.Rand) {
				_, _ = tab.In(tx).Get(rng.Intn(cfg.rows) + 1)
			},
			func() { freezeVerifyCount(b, tab, cfg, ops) })
	default:
		tab, err := rime.Register[freezeUser](db, rime.WithTableName[freezeUser]("users"))
		if err != nil {
			b.Fatal(err)
		}
		mk := func(i int) *freezeUser {
			return &freezeUser{ID: i + 1, Name: fmt.Sprintf("n-%d", i%1000), Email: fmt.Sprintf("e-%d", i), Age: i % 100}
		}
		freezeSeed(b, tab, seed, mk)
		var ins []*freezeUser
		if cfg.op == "insert" {
			ins = make([]*freezeUser, ops)
			for i := range ins {
				ins[i] = mk(i)
			}
		}
		if cfg.hooks {
			freezeArmHooks(tab)
		}
		freezePhase(b, db, cfg, ops,
			func(tx *rime.Tx, id int) error {
				switch cfg.op {
				case "insert":
					return tab.In(tx).Insert(ins[id-1])
				case "delete":
					return tab.In(tx).Delete(id)
				default:
					return tab.In(tx).Update(id, func(r *freezeUser) error { r.Age++; return nil })
				}
			},
			func(tx *rime.Tx, rng *rand.Rand) {
				_, _ = tab.In(tx).Get(rng.Intn(cfg.rows) + 1)
			},
			func() { freezeVerifyCount(b, tab, cfg, ops) })
	}
}

func freezeSeed[T any](b *testing.B, tab *rime.Table[T], n int, mk func(i int) *T) {
	b.Helper()
	for start := 0; start < n; start += 2048 {
		end := start + 2048
		if end > n {
			end = n
		}
		recs := make([]*T, 0, end-start)
		for i := start; i < end; i++ {
			recs = append(recs, mk(i))
		}
		if err := tab.UpsertMany(recs); err != nil {
			b.Fatal(err)
		}
	}
}

func freezeVerifyCount[T any](b *testing.B, tab *rime.Table[T], cfg freezeCfg, ops int) {
	b.Helper()
	if cfg.mix != "disjoint" {
		return
	}
	want := cfg.rows
	if cfg.op == "insert" {
		want = ops
	}
	if cfg.op == "delete" {
		want = 0
	}
	n, err := tab.Where().Count()
	if err != nil || n != want {
		b.Fatalf("count=%d want=%d err=%v", n, want, err)
	}
}

func freezeArmHooks[T any](tab *rime.Table[T]) {
	var n atomic.Int64
	tab.AfterSave(func(rime.Change[T]) { n.Add(1) })
	tab.OnInserted(func(*T) { n.Add(1) })
}

// freezePhase runs writers behind a barrier with per-commit latency capture.
// mutate performs one row mutation; read performs one background point read.
func freezePhase(b *testing.B, db *rime.DB, cfg freezeCfg, ops int,
	mutate func(tx *rime.Tx, id int) error,
	read func(tx *rime.Tx, rng *rand.Rand),
	verify func(),
) {
	b.Helper()
	perWorker := (ops + cfg.writers - 1) / cfg.writers
	latCh := make([][]time.Duration, cfg.writers)
	for w := 0; w < cfg.writers; w++ {
		s, e := w*perWorker, (w+1)*perWorker
		if e > ops {
			e = ops
		}
		if e > s {
			latCh[w] = make([]time.Duration, 0, (e-s-1)/cfg.batch+16)
		}
	}
	var conflicts atomic.Int64
	var commits atomic.Int64
	var wg sync.WaitGroup
	barrier := make(chan struct{})
	var readerStop chan struct{}
	var readerWg sync.WaitGroup
	var pinTx *rime.Tx
	if cfg.readers {
		pinTx = db.ReadTx()
		readerStop = make(chan struct{})
		for r := 0; r < 2; r++ {
			readerWg.Add(1)
			go func(r int) {
				defer readerWg.Done()
				rng := rand.New(rand.NewSource(int64(1000 + r)))
				for {
					select {
					case <-readerStop:
						return
					default:
					}
					tx := db.ReadTx()
					for i := 0; i < 8; i++ {
						read(tx, rng)
					}
					tx.Close()
				}
			}(r)
		}
	}
	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	b.ResetTimer()
	start := time.Now()
	for w := 0; w < cfg.writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(7919 + w)))
			s, e := w*perWorker, (w+1)*perWorker
			if e > ops {
				e = ops
			}
			<-barrier
			if s >= e {
				return
			}
			lat := latCh[w]
			for base := s; base < e; base += cfg.batch {
				end := base + cfg.batch
				if end > e {
					end = e
				}
				for {
					t0 := time.Now()
					err := db.WriteTx(func(tx *rime.Tx) error {
						for i := base; i < end; i++ {
							if err := mutate(tx, freezeKey(cfg, rng, i, ops)); err != nil {
								return err
							}
						}
						return nil
					})
					lat = append(lat, time.Since(t0))
					commits.Add(1)
					if err == nil {
						break
					}
					if errors.Is(err, rime.ErrConflict) {
						conflicts.Add(1)
						time.Sleep(50 * time.Microsecond)
						continue
					}
					if cfg.op == "delete" && errors.Is(err, rime.ErrNotFound) {
						break // concurrent mix already removed it
					}
					panic(fmt.Sprintf("cfg=%s worker %d: %v", cfg.name(), w, err))
				}
			}
			latCh[w] = lat
		}(w)
	}
	close(barrier)
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()
	if cfg.readers {
		close(readerStop)
		readerWg.Wait()
		pinTx.Close()
	}
	runtime.ReadMemStats(&memAfter)
	var all []time.Duration
	for _, l := range latCh {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pick := func(q float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		return all[int(q*float64(len(all)-1))]
	}
	b.ReportMetric(float64(ops)/elapsed.Seconds(), "rows/s")
	b.ReportMetric(float64(pick(0.50).Nanoseconds()), "commit-p50-ns")
	b.ReportMetric(float64(pick(0.95).Nanoseconds()), "commit-p95-ns")
	b.ReportMetric(float64(pick(0.99).Nanoseconds()), "commit-p99-ns")
	b.ReportMetric(float64(conflicts.Load()), "conflicts")
	b.ReportMetric(float64(int64(memAfter.HeapAlloc)-int64(memBefore.HeapAlloc)), "heap-delta-B")
	bRow := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / float64(ops)
	aRow := float64(memAfter.Mallocs-memBefore.Mallocs) / float64(ops)
	b.ReportMetric(bRow, "B/row")
	b.ReportMetric(aRow, "allocs/row")
	b.Logf("cfg=%s ops=%d commits=%d conflicts=%d elapsed=%v rows/s=%.0f B/row=%.0f allocs/row=%.1f p50=%v p95=%v p99=%v",
		cfg.name(), ops, commits.Load(), conflicts.Load(), elapsed,
		float64(ops)/elapsed.Seconds(), bRow, aRow, pick(0.50), pick(0.95), pick(0.99))
	verify()
}

// freezeKey maps a worker-local op index to a key. Disjoint mixes partition
// the op range itself, so keys are disjoint by construction.
func freezeKey(cfg freezeCfg, rng *rand.Rand, i, ops int) int {
	switch cfg.mix {
	case "hot1":
		return 1
	case "hot64":
		return (i % 64) + 1
	case "uniform":
		return rng.Intn(cfg.rows) + 1
	default:
		return (i % ops) + 1
	}
}

// freezeTransfer runs a two-table balanced transfer workload with conservation checks.
func freezeTransfer(b *testing.B, cfg freezeCfg) {
	b.Helper()
	db := rime.New()
	defer db.Close()
	a, err := rime.Register[freezeAcct](db, rime.WithTableName[freezeAcct]("accts_a"))
	if err != nil {
		b.Fatal(err)
	}
	c, err := rime.Register[freezeAcct](db, rime.WithTableName[freezeAcct]("accts_b"))
	if err != nil {
		b.Fatal(err)
	}
	freezeSeed(b, a, cfg.rows, func(i int) *freezeAcct { return &freezeAcct{ID: i + 1, Bal: 100} })
	freezeSeed(b, c, cfg.rows, func(i int) *freezeAcct { return &freezeAcct{ID: i + 1, Bal: 100} })
	ops := cfg.rows
	perWorker := (ops + cfg.writers - 1) / cfg.writers
	var conflicts atomic.Int64
	var wg sync.WaitGroup
	barrier := make(chan struct{})
	b.ResetTimer()
	start := time.Now()
	for w := 0; w < cfg.writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			s, e := w*perWorker, (w+1)*perWorker
			if e > ops {
				e = ops
			}
			<-barrier
			for i := s; i < e; {
				id := i + 1
				err := db.WriteTx(func(tx *rime.Tx) error {
					if err := a.In(tx).Update(id, func(r *freezeAcct) error { r.Bal--; return nil }); err != nil {
						return err
					}
					return c.In(tx).Update(id, func(r *freezeAcct) error { r.Bal++; return nil })
				})
				if err == nil {
					i++
					continue
				}
				if errors.Is(err, rime.ErrConflict) {
					conflicts.Add(1)
					time.Sleep(50 * time.Microsecond)
					continue
				}
				panic(fmt.Sprintf("cfg=%s worker %d: %v", cfg.name(), w, err))
			}
		}(w)
	}
	close(barrier)
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()
	b.ReportMetric(float64(ops)/elapsed.Seconds(), "rows/s")
	tx := db.ReadTx()
	defer tx.Close()
	for i := 1; i <= cfg.rows; i++ {
		ra, err := a.In(tx).Get(i)
		if err != nil {
			b.Fatal(err)
		}
		rb, err := c.In(tx).Get(i)
		if err != nil {
			b.Fatal(err)
		}
		if ra.Bal+rb.Bal != 200 {
			b.Fatalf("pair %d unbalanced: %d", i, ra.Bal+rb.Bal)
		}
	}
	b.Logf("cfg=%s ops=%d conflicts=%d elapsed=%v rows/s=%.0f", cfg.name(), ops, conflicts.Load(), elapsed, float64(ops)/elapsed.Seconds())
}
