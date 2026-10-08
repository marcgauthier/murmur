package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur"
)

// Concurrent-reader query benchmarks against the in-memory typed materializer.
//
// Every case opens the shared dataset template and rebuilds its in-memory
// RIME view. N reader goroutines issue queries concurrently behind a start
// gate; MVCC snapshot reads never block writers. Latency is per query and
// throughput uses wall time.
//
// Reader counts default to 1, 2, 4, 8, 16, 32 and can be overridden with a
// comma-separated list, e.g. MURMUR_BENCH_READERS=1,4,16. Dataset sizes
// follow the standard MURMUR_BENCH_ROWS / -short selection.
//
//	go -C tests-benchmark/benchmark test -bench 'BenchmarkConcurrent' -short -benchtime 2s
//	MURMUR_BENCH_READERS=1,4,8 go -C tests-benchmark/benchmark test -bench 'BenchmarkConcurrentMixed' -short -benchtime 2s
//	MURMUR_READ_BENCH_SECONDS=10 go -C tests-benchmark/benchmark test -run '^TestConcurrentReaderThroughput$' -v -count=1 -timeout=300s

// readerCounts returns the concurrent-reader levels to benchmark.
func readerCounts() []int {
	if raw := os.Getenv("MURMUR_BENCH_READERS"); raw != "" {
		var out []int
		for _, part := range strings.Split(raw, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || n < 1 {
				continue
			}
			out = append(out, n)
		}
		if len(out) > 0 {
			return out
		}
	}
	return []int{1, 2, 4, 8, 16, 32}
}

// benchHandles resolves the typed tables once per benchmark so measured
// queries pay no handle-lookup overhead.
type benchHandles struct {
	contacts *murmur.RecordTable[benchContact]
	orders   *murmur.RecordTable[benchOrder]
}

func openBenchHandles(b testing.TB, db *murmur.DB) *benchHandles {
	b.Helper()
	contacts, err := murmur.TableOf[benchContact](db, "contacts")
	if err != nil {
		b.Fatal(err)
	}
	orders, err := murmur.TableOf[benchOrder](db, "orders")
	if err != nil {
		b.Fatal(err)
	}
	return &benchHandles{contacts: contacts, orders: orders}
}

// openTypedMemoryDB copies the n-row template to a fresh directory,
// opens it, and resolves the typed tables.
func openTypedMemoryDB(b testing.TB, n int) (*murmur.DB, *benchHandles, []murmur.RowID) {
	b.Helper()
	tmpl := templateFor(b, n)
	dest := b.TempDir()
	if err := copyDir(tmpl.dir, dest); err != nil {
		b.Fatal(err)
	}
	cfg := benchConfig(dest, tmpl.node, tmpl.dbid)
	db, err := murmur.Open(context.Background(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db, openBenchHandles(b, db), tmpl.ids
}

// readerQuery runs one read and returns the number of rows scanned.
type readerQuery func(ctx context.Context, tables *benchHandles, ids []murmur.RowID, rng *rand.Rand, i int) (int, error)

func pkLookupQuery(ctx context.Context, tables *benchHandles, ids []murmur.RowID, rng *rand.Rand, _ int) (int, error) {
	got, err := tables.contacts.Get(ids[rng.Intn(len(ids))])
	if err != nil {
		return 0, err
	}
	_ = got
	return 1, nil
}

func rangeLookupQuery(ctx context.Context, tables *benchHandles, _ []murmur.RowID, _ *rand.Rand, i int) (int, error) {
	lo := int64((i * 131) % 900)
	scoreField := murmur.NumericFieldOf[benchContact, int64](tables.contacts, "Score")
	rows, err := tables.contacts.Where(scoreField.Between(lo, lo+100)).Limit(100).Find()
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

func mixedLookupQuery(ctx context.Context, tables *benchHandles, ids []murmur.RowID, rng *rand.Rand, i int) (int, error) {
	if i%2 == 0 {
		return pkLookupQuery(ctx, tables, ids, rng, i)
	}
	return rangeLookupQuery(ctx, tables, ids, rng, i)
}

// indexedEqualityQuery mirrors BenchmarkIndexedEquality: a single-column
// indexed equality probe, the cheapest query shape in the suite.
func indexedEqualityQuery(n int) readerQuery {
	return func(ctx context.Context, tables *benchHandles, _ []murmur.RowID, _ *rand.Rand, i int) (int, error) {
		nameField := murmur.FieldOf[benchContact, string](tables.contacts, "Name")
		rows, err := tables.contacts.Where(nameField.Eq(fmt.Sprintf("ann smith %d", (i*7919)%n))).Find()
		if err != nil {
			return 0, err
		}
		return len(rows), nil
	}
}

// runConcurrentReaders drives b.N queries from readers goroutines against
// the in-memory typed store and reports per-query latency percentiles
// plus wall-clock throughput. When expectedRows >= 0 every query must scan
// exactly that many rows.
func runConcurrentReaders(b *testing.B, n, readers int, q readerQuery, expectedRows int) {
	b.Helper()
	db, tables, ids := openTypedMemoryDB(b, n)
	_ = db
	ctx := context.Background()
	b.ReportMetric(float64(readers), "readers")
	trackPeakAlloc(b)
	b.ReportAllocs()
	b.ResetTimer()
	wallStart := time.Now()
	opsPerReader := b.N / readers
	extra := b.N % readers
	startGate := make(chan struct{})
	errCh := make(chan error, readers)
	latCh := make(chan []time.Duration, readers)
	var wg sync.WaitGroup
	for w := 0; w < readers; w++ {
		count := opsPerReader
		if w < extra {
			count++
		}
		wg.Add(1)
		go func(worker, count int) {
			defer wg.Done()
			<-startGate
			rng := rand.New(rand.NewSource(int64(1000 + worker)))
			samples := make([]time.Duration, 0, count)
			for i := 0; i < count; i++ {
				start := time.Now()
				got, err := q(ctx, tables, ids, rng, i)
				if err != nil {
					errCh <- err
					return
				}
				if expectedRows >= 0 && got != expectedRows {
					errCh <- fmt.Errorf("reader %d op %d: got %d rows, want %d", worker, i, got, expectedRows)
					return
				}
				samples = append(samples, time.Since(start))
			}
			latCh <- samples
		}(w, count)
	}
	close(startGate)
	wg.Wait()
	wall := time.Since(wallStart)
	b.StopTimer()
	close(errCh)
	for err := range errCh {
		b.Fatal(err)
	}
	close(latCh)
	var lat latency
	for s := range latCh {
		lat.samples = append(lat.samples, s...)
	}
	lat.reportWall(b, wall, 1, "queries")
}

// BenchmarkConcurrentPKLookup measures point-lookup speed under concurrent readers.
func BenchmarkConcurrentPKLookup(b *testing.B) {
	for _, n := range datasetSizes(b) {
		for _, readers := range readerCounts() {
			b.Run(fmt.Sprintf("%s/%dreaders", sizeName(n), readers), func(b *testing.B) {
				runConcurrentReaders(b, n, readers, pkLookupQuery, 1)
			})
		}
	}
}

// BenchmarkConcurrentIndexedEquality measures single-column indexed
// equality probes under concurrent readers: the cheapest query shape,
// for comparison with the single-threaded reference.
func BenchmarkConcurrentIndexedEquality(b *testing.B) {
	for _, n := range datasetSizes(b) {
		for _, readers := range readerCounts() {
			b.Run(fmt.Sprintf("%s/%dreaders", sizeName(n), readers), func(b *testing.B) {
				runConcurrentReaders(b, n, readers, indexedEqualityQuery(n), -1)
			})
		}
	}
}

// BenchmarkConcurrentRangeLookup measures indexed-range (100-row cap)
// speed under concurrent readers.
func BenchmarkConcurrentRangeLookup(b *testing.B) {
	for _, n := range datasetSizes(b) {
		for _, readers := range readerCounts() {
			b.Run(fmt.Sprintf("%s/%dreaders", sizeName(n), readers), func(b *testing.B) {
				runConcurrentReaders(b, n, readers, rangeLookupQuery, -1)
			})
		}
	}
}

// BenchmarkConcurrentMixed measures alternating point and range
// lookups under concurrent readers.
func BenchmarkConcurrentMixed(b *testing.B) {
	for _, n := range datasetSizes(b) {
		for _, readers := range readerCounts() {
			b.Run(fmt.Sprintf("%s/%dreaders", sizeName(n), readers), func(b *testing.B) {
				runConcurrentReaders(b, n, readers, mixedLookupQuery, -1)
			})
		}
	}
}

// readerResult is one timed-reader worker's totals.
type readerResult struct {
	queries int
	rows    int
	samples []time.Duration
	err     error
}

// TestConcurrentReaderThroughput measures sustained multi-reader query
// rate against the in-memory typed store over a fixed wall-clock window
// (MURMUR_READ_BENCH_SECONDS, default 10). Readers alternate point and range
// lookups on a 10K-row store; every point lookup must return exactly one row.
func TestConcurrentReaderThroughput(t *testing.T) {
	readFor := 10 * time.Second
	if raw := os.Getenv("MURMUR_READ_BENCH_SECONDS"); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds < 1 {
			t.Fatal("MURMUR_READ_BENCH_SECONDS must be a positive integer")
		}
		readFor = time.Duration(seconds) * time.Second
	}
	const rows = 10_000
	for _, readers := range readerCounts() {
		t.Run(fmt.Sprintf("%d_readers", readers), func(t *testing.T) {
			db, tables, ids := openTypedMemoryDB(t, rows)
			_ = db
			ctx := context.Background()
			startGate := make(chan struct{})
			var deadline time.Time
			results := make(chan readerResult, readers)
			var ready sync.WaitGroup
			ready.Add(readers)
			for worker := 0; worker < readers; worker++ {
				go func(worker int) {
					ready.Done()
					<-startGate
					rng := rand.New(rand.NewSource(int64(5000 + worker)))
					var res readerResult
					for i := 0; time.Now().Before(deadline); i++ {
						start := time.Now()
						var got int
						var err error
						if i%2 == 0 {
							got, err = pkLookupQuery(ctx, tables, ids, rng, i)
							if err == nil && got != 1 {
								err = fmt.Errorf("reader %d op %d: got %d rows, want 1", worker, i, got)
							}
						} else {
							got, err = rangeLookupQuery(ctx, tables, ids, rng, i)
						}
						if err != nil {
							res.err = fmt.Errorf("reader %d: %w", worker+1, err)
							results <- res
							return
						}
						res.queries++
						res.rows += got
						if i%16 == 0 {
							res.samples = append(res.samples, time.Since(start))
						}
					}
					results <- res
				}(worker)
			}
			ready.Wait()
			start := time.Now()
			deadline = start.Add(readFor)
			close(startGate)
			var totalQueries, totalRows int
			var lat latency
			for range readers {
				res := <-results
				if res.err != nil {
					t.Fatal(res.err)
				}
				if res.queries == 0 {
					t.Fatal("a reader completed no queries")
				}
				totalQueries += res.queries
				totalRows += res.rows
				lat.samples = append(lat.samples, res.samples...)
			}
			elapsed := time.Since(start)
			t.Logf("readers=%d queries=%d rows=%d elapsed=%s queries_per_second=%.1f rows_per_second=%.1f p50=%s p95=%s p99=%s (latency sampled 1/16)",
				readers, totalQueries, totalRows, elapsed,
				float64(totalQueries)/elapsed.Seconds(), float64(totalRows)/elapsed.Seconds(),
				lat.percentile(50), lat.percentile(95), lat.percentile(99))
		})
	}
}
