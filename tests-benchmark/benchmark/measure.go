// Package benchmark holds the repeatable performance suite (PLAN section 59-61).
package benchmark

import (
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

// latency records per-operation durations and reports percentile summaries.
// Benchmarks should prefer it over raw b.N loops whenever the matrix asks
// for p50/p95/p99: it preserves Go's own ns/op reporting and adds explicit
// percentile and throughput metrics.
type latency struct {
	samples []time.Duration
}

// record adds one operation sample.
func (l *latency) record(d time.Duration) { l.samples = append(l.samples, d) }

// percentile returns the p-th percentile sample (p in [0,100]).
func (l *latency) percentile(p float64) time.Duration {
	n := len(l.samples)
	if n == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), l.samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(p / 100 * float64(n-1))
	return sorted[idx]
}

// reportWall publishes p50/p95/p99 latency plus wall-clock throughput for
// concurrent benchmarks, where dividing by summed per-op durations would
// overcount elapsed time. unitPerOp scales the throughput metric (1 for
// ops/sec; pass rows or bytes per op for rows/sec or MB/sec companions).
func (l *latency) reportWall(b *testing.B, wall time.Duration, unitPerOp float64, unit string) {
	b.Helper()
	b.ReportMetric(float64(l.percentile(50).Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(l.percentile(95).Nanoseconds()), "p95-ns")
	b.ReportMetric(float64(l.percentile(99).Nanoseconds()), "p99-ns")
	if wall > 0 {
		b.ReportMetric(float64(len(l.samples))*unitPerOp/wall.Seconds(), unit+"/sec")
	}
}

// report publishes p50/p95/p99 latency plus throughput. unitPerOp scales
// the throughput metric (1 for ops/sec; pass rows or bytes per op for
// rows/sec or MB/sec companions).
func (l *latency) report(b *testing.B, unitPerOp float64, unit string) {
	b.Helper()
	var total time.Duration
	for _, d := range l.samples {
		total += d
	}
	b.ReportMetric(float64(l.percentile(50).Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(l.percentile(95).Nanoseconds()), "p95-ns")
	b.ReportMetric(float64(l.percentile(99).Nanoseconds()), "p99-ns")
	if total > 0 {
		b.ReportMetric(float64(len(l.samples))*unitPerOp/total.Seconds(), unit+"/sec")
	}
}

// peakAlloc samples Go heap allocation in the background and reports the
// peak. It approximates the process memory high-water mark portably; pair
// it with OS-level RSS when exact peak RSS is needed.
type peakAlloc struct {
	stop chan struct{}
	wg   sync.WaitGroup
	mu   sync.Mutex
	peak uint64
}

// trackPeakAlloc starts background peak sampling until b completes.
func trackPeakAlloc(b *testing.B) *peakAlloc {
	b.Helper()
	p := &peakAlloc{stop: make(chan struct{})}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		var ms runtime.MemStats
		for {
			select {
			case <-p.stop:
				return
			default:
			}
			runtime.ReadMemStats(&ms)
			p.mu.Lock()
			if ms.HeapAlloc > p.peak {
				p.peak = ms.HeapAlloc
			}
			p.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}()
	b.Cleanup(func() {
		close(p.stop)
		p.wg.Wait()
		p.mu.Lock()
		defer p.mu.Unlock()
		b.ReportMetric(float64(p.peak)/1e6, "peak-alloc-MB")
	})
	return p
}
