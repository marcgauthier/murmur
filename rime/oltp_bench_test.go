package rime_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

type oltpBare[K comparable] struct {
	ID    K `rime:"primary"`
	Value int
}
type oltpIndexed[K comparable] struct {
	ID    K      `rime:"primary"`
	Value int    `rime:"index,ordered"`
	Name  string `rime:"unique,prefix"`
}

type oltpCfg struct {
	op, mix, indexes                         string
	rows, workers, batch, operations, sample int
}

// BenchmarkOLTP uses fixed, seeded per-worker schedules and fresh databases for
// every measured phase. One Go benchmark iteration is a complete phase; use
// ns/request, requests/s, B/request and allocs/request for normalized comparisons.
// No key formatting, random generation, seeding or verification is timed.
// Default latency sampling is OFF; enable RIME_OLTP_SAMPLE=64 in separate runs.
// Override RIME_OLTP_{ROWS,KEY,OP,MIX,INDEXES,WORKERS,BATCH,OPS,SAMPLE}.
func BenchmarkOLTP(b *testing.B) {
	rows := oltpInt(b, "ROWS", 1000)
	workers := oltpInt(b, "WORKERS", 4)
	batch := oltpInt(b, "BATCH", 1)
	operations := oltpInt(b, "OPS", 65536)
	sample := oltpInt(b, "SAMPLE", 0)
	if rows < workers || operations < workers*batch {
		b.Fatal("rows and operations must cover workers and batches")
	}
	operations = operations / (workers * batch) * (workers * batch)
	for _, key := range oltpChoices("KEY", "int,string") {
		for _, indexes := range oltpChoices("INDEXES", "none,matched") {
			for _, mix := range oltpChoices("MIX", "uniform") {
				for _, op := range oltpChoices("OP", "get,pinned,read90,read50,update,insert,churn") {
					if !oltpMember(key, "int", "string") || !oltpMember(indexes, "none", "matched") || !oltpMember(mix, "uniform", "hot1", "hot5") || !oltpMember(op, "get", "pinned", "read90", "read50", "update", "insert", "churn") {
						b.Fatal("unsupported OLTP matrix option")
					}
					cfg := oltpCfg{op, mix, indexes, rows, workers, batch, operations, sample}
					name := fmt.Sprintf("%s/%s/%s/%s/n%d/w%d/b%d", key, indexes, mix, op, rows, workers, batch)
					b.Run(name, func(b *testing.B) {
						if key == "int" {
							oltpKey(b, cfg, func(i int) int { return i + 1 })
						} else {
							oltpKey(b, cfg, func(i int) string { return fmt.Sprintf("key-%08d", i) })
						}
					})
				}
			}
		}
	}
}
func oltpChoices(name, fallback string) []string {
	if value := os.Getenv("RIME_OLTP_" + name); value != "" {
		return strings.Split(value, ",")
	}
	return strings.Split(fallback, ",")
}
func oltpInt(b *testing.B, name string, fallback int) int {
	b.Helper()
	if value := os.Getenv("RIME_OLTP_" + name); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 || (n == 0 && name != "SAMPLE") {
			b.Fatalf("invalid RIME_OLTP_%s=%q", name, value)
		}
		return n
	}
	return fallback
}
func oltpMember(s string, values ...string) bool {
	for _, v := range values {
		if s == v {
			return true
		}
	}
	return false
}
func oltpKey[K comparable](b *testing.B, cfg oltpCfg, key func(int) K) {
	if cfg.indexes == "none" {
		oltpRun(b, cfg, key, func(i int) *oltpBare[K] { return &oltpBare[K]{ID: key(i)} }, func(r *oltpBare[K]) K { return r.ID }, func(r *oltpBare[K]) int { return r.Value }, func(r *oltpBare[K]) { r.Value++ })
	} else {
		oltpRun(b, cfg, key, func(i int) *oltpIndexed[K] { return &oltpIndexed[K]{ID: key(i), Name: fmt.Sprintf("name-%08d", i)} }, func(r *oltpIndexed[K]) K { return r.ID }, func(r *oltpIndexed[K]) int { return r.Value }, func(r *oltpIndexed[K]) { r.Value++ })
	}
}

func oltpRun[K comparable, T any](b *testing.B, cfg oltpCfg, key func(int) K, makeRow func(int) *T, rowKey func(*T) K, value func(*T) int, increment func(*T)) {
	b.Helper()
	count := cfg.rows
	if cfg.op == "insert" {
		count += cfg.operations
	}
	keys := make([]K, count)
	records := make([]*T, count)
	for i := range keys {
		keys[i] = key(i)
		records[i] = makeRow(i)
	}
	// Uniform workers own disjoint partitions; hotspot schedules intentionally contend.
	perWorker := cfg.operations / cfg.workers
	trace := make([][]int, cfg.workers)
	expected := make([]int, count)
	writeGroup := func(group int) bool {
		switch cfg.op {
		case "get", "pinned":
			return false
		case "read90":
			return group%10 == 0
		case "read50":
			return group%2 == 0
		default:
			return true
		}
	}
	for w := range trace {
		trace[w] = make([]int, perWorker)
		rng := rand.New(rand.NewPCG(127, uint64(w+1)))
		lo, hi := w*cfg.rows/cfg.workers, (w+1)*cfg.rows/cfg.workers
		for i := range trace[w] {
			id := lo + rng.IntN(hi-lo)
			switch cfg.mix {
			case "hot1":
				id = 0
			case "hot5":
				id = rng.IntN(min(5, cfg.rows))
			}
			if cfg.op == "insert" {
				id = cfg.rows + w*perWorker + i
			}
			trace[w][i] = id
			if writeGroup(i / cfg.batch) {
				expected[id]++
			}
		}
	}
	var retries, commits, allocated, allocations uint64
	var heap uint64
	var latencies []int64
	b.ResetTimer()
	b.StopTimer()
	for round := 0; round < b.N; round++ {
		db := rime.New()
		tab, err := rime.Register[T](db)
		if err != nil {
			b.Fatal(err)
		}
		for start := 0; start < cfg.rows; start += 2048 {
			if err := tab.UpsertMany(records[start:min(start+2048, cfg.rows)]); err != nil {
				b.Fatal(err)
			}
		}
		pinned := db.ReadTx() // also validates history after mutation phases.
		workerPins := make([]*rime.Tx, cfg.workers)
		if cfg.op == "pinned" {
			for w := range workerPins {
				workerPins[w] = db.ReadTx()
			}
		}
		errs := make([]error, cfg.workers)
		retryCounts := make([]uint64, cfg.workers)
		commitCounts := make([]uint64, cfg.workers)
		samples := make([][]int64, cfg.workers)
		if cfg.sample > 0 {
			for w := range samples {
				samples[w] = make([]int64, 0, perWorker/cfg.batch/cfg.sample+1)
			}
		}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		var wg sync.WaitGroup
		wg.Add(cfg.workers)
		stopProfile := oltpProfile[T](b, cfg, round)
		b.StartTimer()
		for w := 0; w < cfg.workers; w++ {
			go func(w int) {
				defer wg.Done()
				for start := 0; start < perWorker; start += cfg.batch {
					group := start / cfg.batch
					sampled := cfg.sample > 0 && group%cfg.sample == 0
					var began time.Time
					if sampled {
						began = time.Now()
					}
					var err error
					if !writeGroup(group) {
						for _, id := range trace[w][start : start+cfg.batch] {
							var row *T
							if cfg.op == "pinned" {
								row, err = tab.In(workerPins[w]).Get(keys[id])
							} else {
								row, err = tab.Get(keys[id])
							}
							if err != nil {
								break
							}
							if rowKey(row) != keys[id] || (cfg.op == "pinned" && value(row) != 0) {
								err = fmt.Errorf("incorrect read at %v", keys[id])
								break
							}
						}
					} else {
						for attempt := 0; attempt < 100000; attempt++ {
							err = db.WriteTx(func(tx *rime.Tx) error {
								for _, id := range trace[w][start : start+cfg.batch] {
									bound := tab.In(tx)
									switch cfg.op {
									case "insert":
										err = bound.Insert(records[id])
									case "churn":
										old, e := bound.Get(keys[id])
										if e != nil {
											return e
										}
										next := *old
										increment(&next)
										if e = bound.Delete(keys[id]); e != nil {
											return e
										}
										err = bound.Insert(&next)
									default:
										err = bound.Update(keys[id], func(row *T) error { increment(row); return nil })
									}
									if err != nil {
										return err
									}
								}
								return nil
							})
							if !errors.Is(err, rime.ErrConflict) {
								break
							}
							retryCounts[w]++
						}
						if err == nil {
							commitCounts[w]++
						}
					}
					if err != nil {
						errs[w] = err
						return
					}
					if sampled {
						samples[w] = append(samples[w], time.Since(began).Nanoseconds())
					}
				}
			}(w)
		}
		wg.Wait()
		b.StopTimer()
		stopProfile()
		runtime.ReadMemStats(&after)
		allocated += after.TotalAlloc - before.TotalAlloc
		allocations += after.Mallocs - before.Mallocs
		for w := range errs {
			if errs[w] != nil {
				b.Fatalf("worker %d: %v", w, errs[w])
			}
			retries += retryCounts[w]
			commits += commitCounts[w]
			latencies = append(latencies, samples[w]...)
			if workerPins[w] != nil {
				workerPins[w].Close()
			}
		}
		// GC must preserve all versions needed by the pinned initial snapshot.
		db.GC()
		for i, k := range keys {
			row, err := tab.Get(k)
			want := expected[i]
			if cfg.op == "insert" {
				want = 0
			}
			if err != nil || rowKey(row) != k || value(row) != want {
				b.Fatalf("final key %v: row=%v err=%v want value=%d", k, row, err, want)
			}
			if i < cfg.rows {
				old, e := tab.In(pinned).Get(k)
				if e != nil || value(old) != 0 {
					b.Fatalf("initial snapshot key %v: %v", k, e)
				}
			} else if _, e := tab.In(pinned).Get(k); !errors.Is(e, rime.ErrNotFound) {
				b.Fatalf("insert visible in initial snapshot: %v", e)
			}
		}
		n, e := tab.Where().Count()
		if e != nil || n != count {
			b.Fatalf("count=%d want=%d err=%v", n, count, e)
		}
		pinned.Close()
		db.GC()
		runtime.GC()
		var retained runtime.MemStats
		runtime.ReadMemStats(&retained)
		heap = retained.HeapAlloc
		db.Close()
	}
	total := float64(b.N * cfg.operations)
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/total, "ns/request")
	b.ReportMetric(total/b.Elapsed().Seconds(), "requests/s")
	b.ReportMetric(float64(allocated)/total, "B/request")
	b.ReportMetric(float64(allocations)/total, "allocs/request")
	b.ReportMetric(float64(retries)/float64(b.N), "conflicts/phase")
	b.ReportMetric(float64(commits)/float64(b.N), "commits/phase")
	b.ReportMetric(float64(heap), "retained-heap-B") // process heap, not a table-size estimate.
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		for _, p := range []int{50, 95, 99} {
			b.ReportMetric(float64(latencies[(len(latencies)-1)*p/100]), fmt.Sprintf("p%d-ns", p))
		}
	}
}
