package abuse_test

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// reporter streams live progress to the test log (visible under -v) and
// records the same events for the final markdown report. One reporter per
// abuse run; all methods are safe for concurrent use.
type reporter struct {
	t     *testing.T
	start time.Time

	mu     sync.Mutex
	events []string

	attempted atomic.Int64
	succeeded atomic.Int64
	failed    atomic.Int64

	errMu     sync.Mutex
	errSeen   map[string]int
	errSample []string
}

func newReporter(t *testing.T) *reporter {
	t.Helper()
	return &reporter{t: t, start: time.Now(), errSeen: map[string]int{}}
}

// sampleErr records distinct write-error messages (first 8) with counts so
// the report shows why transient writes failed without logging every one.
func (r *reporter) sampleErr(err error) {
	msg := err.Error()
	if len(msg) > 200 {
		msg = msg[:200]
	}
	r.errMu.Lock()
	defer r.errMu.Unlock()
	r.errSeen[msg]++
	if len(r.errSample) < 8 {
		for _, s := range r.errSample {
			if s == msg {
				return
			}
		}
		r.errSample = append(r.errSample, msg)
	}
}

func (r *reporter) elapsed() time.Duration {
	return time.Since(r.start).Round(time.Second)
}

// phase announces a phase transition in the log.
func (r *reporter) phase(name, detail string) {
	r.t.Helper()
	msg := fmt.Sprintf("[T+%s] === PHASE: %s === %s", r.elapsed(), name, detail)
	r.t.Log(msg)
	r.mu.Lock()
	r.events = append(r.events, msg)
	r.mu.Unlock()
}

// logf records a timestamped progress line in the log and the report.
func (r *reporter) logf(format string, args ...any) {
	r.t.Helper()
	msg := fmt.Sprintf("[T+%s] %s", r.elapsed(), fmt.Sprintf(format, args...))
	r.t.Log(msg)
	r.mu.Lock()
	r.events = append(r.events, msg)
	r.mu.Unlock()
}

func (r *reporter) countAttempt(ok bool) {
	r.attempted.Add(1)
	if ok {
		r.succeeded.Add(1)
	} else {
		r.failed.Add(1)
	}
}

// statusLoop logs a one-line heartbeat every interval until ctx is done:
// elapsed time, write totals, and the min/max row counts across reachable
// nodes. Query failures (nodes deliberately down) are counted, not fatal.
func (r *reporter) statusLoop(stop <-chan struct{}, cluster *harness.Cluster, n int, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			min, max, down := -1, -1, 0
			for i := 0; i < n; i++ {
				c, err := cluster.QueryRowCount(i, tableName)
				if err != nil {
					down++
					continue
				}
				if min < 0 || c < min {
					min = c
				}
				if c > max {
					max = c
				}
			}
			r.logf("status: writes attempted=%d ok=%d err=%d | rows min=%d max=%d unreachable=%d/%d",
				r.attempted.Load(), r.succeeded.Load(), r.failed.Load(),
				min, max, down, n)
		}
	}
}

// verdict logs the final PASS/FAIL line, marks the cluster failed on FAIL
// (preserving node logs), and writes the markdown report. It returns the
// report path.
func (r *reporter) verdict(pass bool, cluster *harness.Cluster, summary string) string {
	r.t.Helper()
	status := "PASS"
	if !pass {
		status = "FAIL"
		cluster.MarkFailed(summary)
	}
	msg := fmt.Sprintf("[T+%s] === VERDICT: %s === %s", r.elapsed(), status, summary)
	r.t.Log(msg)
	r.mu.Lock()
	r.events = append(r.events, msg)
	events := append([]string(nil), r.events...)
	r.mu.Unlock()
	r.errMu.Lock()
	errSample := append([]string(nil), r.errSample...)
	errSeen := make(map[string]int, len(r.errSeen))
	for k, v := range r.errSeen {
		errSeen[k] = v
	}
	r.errMu.Unlock()

	path := fmt.Sprintf("abuse-report-%d.md", time.Now().Unix())
	var b []byte
	b = append(b, fmt.Sprintf("# Abuse run report — %s\n\n", status)...)
	b = append(b, fmt.Sprintf("- finished: %s\n- wall time: %s\n- writes attempted: %d\n- writes acked: %d\n- writes failed (transient): %d\n- verdict: %s\n\n",
		time.Now().UTC().Format(time.RFC3339), r.elapsed(),
		r.attempted.Load(), r.succeeded.Load(), r.failed.Load(), summary)...)
	if len(errSample) > 0 {
		b = append(b, "## Distinct write errors\n\n"...)
		for _, s := range errSample {
			b = append(b, fmt.Sprintf("- (%dx) %s\n", errSeen[s], s)...)
		}
		b = append(b, "\n"...)
	}
	b = append(b, "## Timeline\n\n"...)
	for _, e := range events {
		b = append(b, ("- " + e + "\n")...)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		r.t.Logf("report write failed: %v", err)
		return ""
	}
	r.t.Logf("report written to %s", path)
	return path
}
