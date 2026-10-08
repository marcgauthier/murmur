// Package rimequal runs the same real-engine workload from integration tests
// and the standalone qualification command. It has no external dependencies.
package rimequal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcgauthier/murmur/rime"
)

type Config struct {
	Views          bool // Maintain and verify cached filters and two-table joins.
	Rows           int
	Shards         int
	Readers        int
	Writers        int
	Seed           int64
	SampleInterval time.Duration
	Duration       time.Duration
	Output         io.Writer
}

type Sample struct {
	ViewChecks         uint64    `json:"view_checks,omitempty"`
	ElapsedSeconds     float64   `json:"elapsed_seconds"`
	Operations         uint64    `json:"operations"`
	Reads              uint64    `json:"reads"`
	Writes             uint64    `json:"writes"`
	Conflicts          uint64    `json:"conflicts"`
	Events             uint64    `json:"events"`
	OpsPerSecond       float64   `json:"ops_per_second"`
	P50NS              int64     `json:"p50_ns"`
	P95NS              int64     `json:"p95_ns"`
	P99NS              int64     `json:"p99_ns"`
	HeapBytes          uint64    `json:"heap_bytes"`
	RSSBytes           uint64    `json:"rss_bytes,omitempty"`
	Goroutines         int       `json:"goroutines"`
	GCCount            uint32    `json:"go_gc_count"`
	GCPauseNS          uint64    `json:"go_gc_pause_ns"`
	Versions           int64     `json:"versions"`
	IndexEntries       int64     `json:"index_entries"`
	ActiveTransactions int       `json:"active_transactions"`
	Failure            string    `json:"failure,omitempty"`
	RecentAttempts     []Attempt `json:"recent_write_attempts,omitempty"`
	Seed               int64     `json:"seed"`
}

type row struct {
	ID         int `rime:"primary"`
	Balance    int
	Generation int
	Bucket     int `rime:"index"`
}

type Attempt struct {
	Worker     int       `json:"worker"`
	Key        int       `json:"key"`
	Recreate   bool      `json:"recreate"`
	Generation int       `json:"generation"`
	Snapshot   rime.TxID `json:"snapshot"`
	Outcome    string    `json:"outcome"`
}

type counters struct {
	viewChecks  atomic.Uint64
	reads       atomic.Uint64
	writes      atomic.Uint64
	conflicts   atomic.Uint64
	events      atomic.Uint64
	mu          sync.Mutex
	latencies   []int64
	next        uint64
	history     []Attempt
	historyNext uint64
}

func (c *counters) record(attempt Attempt) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.history) < 64 {
		c.history = append(c.history, attempt)
	} else {
		c.history[c.historyNext%64] = attempt
	}
	c.historyNext++
}
func (c *counters) recent() []Attempt {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.history) < 64 {
		return append([]Attempt(nil), c.history...)
	}
	start := int(c.historyNext % 64)
	out := append([]Attempt(nil), c.history[start:]...)
	return append(out, c.history[:start]...)
}

func (c *counters) observe(start time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value := time.Since(start).Nanoseconds()
	if len(c.latencies) < 8192 {
		c.latencies = append(c.latencies, value)
	} else {
		c.latencies[c.next%8192] = value
	}
	c.next++
}
func (c *counters) sample(db *rime.DB, start time.Time, seed int64) Sample {
	c.mu.Lock()
	values := append([]int64(nil), c.latencies...)
	c.mu.Unlock()
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	percentile := func(p int) int64 {
		if len(values) == 0 {
			return 0
		}
		return values[(len(values)-1)*p/100]
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	st := db.Stats()
	reads, writes := c.reads.Load(), c.writes.Load()
	elapsed := time.Since(start).Seconds()
	return Sample{ViewChecks: c.viewChecks.Load(), ElapsedSeconds: elapsed, Operations: reads + writes, Reads: reads, Writes: writes, Conflicts: c.conflicts.Load(), Events: c.events.Load(), OpsPerSecond: float64(reads+writes) / elapsed, P50NS: percentile(50), P95NS: percentile(95), P99NS: percentile(99), HeapBytes: mem.HeapAlloc, RSSBytes: rss(), Goroutines: runtime.NumGoroutine(), GCCount: mem.NumGC, GCPauseNS: mem.PauseTotalNs, Versions: st.Versions, IndexEntries: st.IndexEntries, ActiveTransactions: st.ActiveTxns, Seed: seed}
}
func rss() uint64 {
	// Linux exposes RSS without another dependency. Other platforms omit it.
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}

// Run seeds outside the timed region, then mixes contending two-table commits,
// pinned snapshots, full scans, indexed queries, tombstones, events, and GC.
// Every unexpected error or invariant violation stops the workload.
func Run(ctx context.Context, cfg Config) (Sample, error) {
	if cfg.Rows < 1 || cfg.Readers < 1 || cfg.Writers < 1 || cfg.Shards < 0 {
		return Sample{}, errors.New("rows, readers, and writers must be positive; shards must be nonnegative")
	}
	if cfg.SampleInterval <= 0 {
		cfg.SampleInterval = time.Second
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	db := rime.New(rime.WithShardCount(cfg.Shards), rime.WithEventQueueSize(64))
	defer db.Close()
	left, err := rime.Register[row](db, rime.WithTableName[row]("left"))
	if err != nil {
		return Sample{}, err
	}
	right, err := rime.Register[row](db, rime.WithTableName[row]("right"))
	if err != nil {
		return Sample{}, err
	}
	for base := 0; base < cfg.Rows; base += 512 {
		if err := ctx.Err(); err != nil {
			return Sample{}, err
		}
		end := min(base+512, cfg.Rows)
		err = db.WriteTx(func(tx *rime.Tx) error {
			for i := base; i < end; i++ {
				if err := left.In(tx).Upsert(&row{ID: i, Balance: 50, Bucket: i % 8}); err != nil {
					return err
				}
				if err := right.In(tx).Upsert(&row{ID: i, Balance: 50, Bucket: i % 8}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return Sample{}, err
		}
	}
	// All source commits touch both tables. Holding a read transaction before
	// Snapshot pins history so the reported view commit remains available to ReadAt.
	var filterView *rime.View[*row]
	var pairView *rime.View[rime.JoinRow[row, row]]
	filterQuery := left.Where(rime.F[row, int](left, "Bucket").Eq(0))
	joinQuery := func(tx *rime.Tx) ([]rime.JoinRow[row, row], error) {
		return rime.InnerJoin(left.In(tx), right.In(tx), func(r *row) int { return r.ID }, func(r *row) int { return r.ID })
	}
	if cfg.Views {
		filterView, err = rime.NewView("bucket_zero", filterQuery)
		if err != nil {
			return Sample{}, err
		}
		pairView, err = rime.NewComputedView(db, "pairs", []rime.ViewSource{left, right}, joinQuery)
		if err != nil {
			return Sample{}, err
		}
	}
	if cfg.Duration > 0 {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, cfg.Duration)
		defer stop()
	}
	var counts counters
	left.OnCommitted(func(rime.Change[row]) { counts.events.Add(1) })
	failures := make(chan error, 1)
	fail := func(err error) {
		select {
		case failures <- err:
		default:
		}
		cancel()
	}
	start := time.Now()
	var wg sync.WaitGroup
	// Start GC after seeding, so large datasets do not repeatedly traverse a
	// growing table before the measured workload starts.
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				db.GCContext(ctx)
			}
		}
	}()
	for writer := 0; writer < cfg.Writers; writer++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + int64(worker)))
			for ctx.Err() == nil {
				// Most writes hit a small shared hot set; others cover the whole table.
				domain := cfg.Rows
				if rng.Intn(4) != 0 {
					domain = min(cfg.Rows, 32)
				}
				key := rng.Intn(domain)
				began := time.Now()
				recreate := rng.Intn(8) == 0
				attempt := Attempt{Worker: worker, Key: key, Recreate: recreate, Outcome: "committed"}
				err := db.WriteTxContext(ctx, func(tx *rime.Tx) error {
					l, err := left.In(tx).Get(key)
					if err != nil {
						return err
					}
					r, err := right.In(tx).Get(key)
					if err != nil {
						return err
					}
					if l.Balance+r.Balance != 100 || l.Generation != r.Generation {
						return fmt.Errorf("writer %d key %d inconsistent pair: %+v %+v", worker, key, l, r)
					}
					next := (l.Balance + 1) % 101
					generation := l.Generation + 1
					attempt.Generation = generation
					attempt.Snapshot = tx.Snapshot()
					if recreate {
						if err := left.In(tx).Delete(key); err != nil {
							return err
						}
						if err := right.In(tx).Delete(key); err != nil {
							return err
						}
					}
					if err := left.In(tx).Upsert(&row{ID: key, Balance: next, Generation: generation, Bucket: generation % 8}); err != nil {
						return err
					}
					return right.In(tx).Upsert(&row{ID: key, Balance: 100 - next, Generation: generation, Bucket: generation % 8})
				})
				if err != nil {
					attempt.Outcome = err.Error()
				}
				counts.record(attempt)
				if errors.Is(err, rime.ErrConflict) {
					counts.conflicts.Add(1)
					continue
				}
				if err != nil {
					if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
						return
					}
					fail(fmt.Errorf("seed %d writer %d key %d: %w", cfg.Seed, worker, key, err))
					return
				}
				counts.writes.Add(1)
				counts.observe(began)
			}
		}(writer)
	}
	for reader := 0; reader < cfg.Readers; reader++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(cfg.Seed + 1000 + int64(worker)))
			iteration := 0
			for ctx.Err() == nil {
				key := rng.Intn(cfg.Rows)
				began := time.Now()
				tx := db.ReadTxContext(ctx)
				err := func() error {
					defer tx.Close()
					l, err := left.In(tx).Get(key)
					if err != nil {
						return err
					}
					r, err := right.In(tx).Get(key)
					if err != nil {
						return err
					}
					if l.Balance+r.Balance != 100 || l.Generation != r.Generation {
						return fmt.Errorf("reader %d key %d partial commit: %+v %+v", worker, key, l, r)
					}
					if iteration%16 == 0 {
						records, err := left.In(tx).Where(rime.F[row, int](left, "Bucket").Eq(l.Bucket)).Limit(16).Find()
						if err != nil {
							return err
						}
						for _, record := range records {
							if record.Bucket != l.Bucket {
								return fmt.Errorf("wrong indexed result: %+v", record)
							}
						}
					}
					if iteration%64 == 0 {
						// Explicitly compare both tables under a snapshot while writers and
						// GC continue; a sequential model has exactly Rows pairs and sum 100.
						all, err := left.In(tx).Where().Find()
						if err != nil {
							return err
						}
						if len(all) != cfg.Rows {
							return fmt.Errorf("scan has %d rows, expected %d", len(all), cfg.Rows)
						}
						for _, record := range all {
							peer, err := right.In(tx).Get(record.ID)
							if err != nil {
								return err
							}
							if record.Balance+peer.Balance != 100 || record.Generation != peer.Generation {
								return fmt.Errorf("scan key %d inconsistent pair", record.ID)
							}
						}
						again, err := left.In(tx).Get(key)
						if err != nil {
							return err
						}
						if *again != *l {
							return fmt.Errorf("snapshot changed at key %d", key)
						}
					}
					if cfg.Views && iteration%32 == 0 {
						cached, err := filterView.Snapshot()
						if err != nil {
							return err
						}
						at := db.ReadAt(cached.Commit)
						fresh, err := filterQuery.In(at).Find()
						at.Close()
						if err != nil {
							return err
						}
						sort.Slice(cached.Rows, func(i, j int) bool { return cached.Rows[i].ID < cached.Rows[j].ID })
						sort.Slice(fresh, func(i, j int) bool { return fresh[i].ID < fresh[j].ID })
						if !slices.Equal(cached.Rows, fresh) {
							return fmt.Errorf("filter view differs at commit %d", cached.Commit)
						}
						pairs, err := pairView.Snapshot()
						if err != nil {
							return err
						}
						at = db.ReadAt(pairs.Commit)
						expected, err := joinQuery(at)
						at.Close()
						if err != nil {
							return err
						}
						sort.Slice(pairs.Rows, func(i, j int) bool { return pairs.Rows[i].Left.ID < pairs.Rows[j].Left.ID })
						sort.Slice(expected, func(i, j int) bool { return expected[i].Left.ID < expected[j].Left.ID })
						if !slices.Equal(pairs.Rows, expected) {
							return fmt.Errorf("join view differs at commit %d", pairs.Commit)
						}
						counts.viewChecks.Add(2)
					}
					return nil
				}()
				if err != nil {
					if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
						return
					}
					fail(fmt.Errorf("seed %d reader %d iteration %d: %w", cfg.Seed, worker, iteration, err))
					return
				}
				counts.reads.Add(1)
				counts.observe(began)
				iteration++
			}
		}(reader)
	}
	ticker := time.NewTicker(cfg.SampleInterval)
	defer ticker.Stop()
	var encoder *json.Encoder
	if cfg.Output != nil {
		encoder = json.NewEncoder(cfg.Output)
	}
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			if encoder != nil {
				if err := encoder.Encode(counts.sample(db, start, cfg.Seed)); err != nil {
					fail(err)
				}
			}
		}
	}
	wg.Wait()
	db.Close()
	db.GC()
	runtime.GC()
	final := counts.sample(db, start, cfg.Seed)
	var runErr error
	select {
	case runErr = <-failures:
	default:
	}
	if runErr == nil && (final.ActiveTransactions != 0 || final.Versions != int64(2*cfg.Rows)) {
		runErr = fmt.Errorf("residue after workload: %+v", final)
	}
	if runErr == nil && cfg.Views && final.ViewChecks == 0 {
		runErr = errors.New("no view verification progress")
	}
	if runErr == nil && (final.Reads == 0 || final.Writes == 0) {
		runErr = fmt.Errorf("no workload progress: %+v", final)
	}
	if runErr != nil {
		final.Failure = runErr.Error()
		final.RecentAttempts = counts.recent()
	}
	if encoder != nil {
		if err := encoder.Encode(final); err != nil {
			return final, err
		}
	}
	return final, runErr
}
