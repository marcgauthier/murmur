package harness

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testLogf(t *testing.T) (func(string, ...any), *[]string) {
	t.Helper()
	var lines []string
	return func(format string, args ...any) {
		lines = append(lines, format)
	}, &lines
}

// TestWaitUnlockFastSuccess covers the common path: the POST answers at
// once and no watchdog verdict is needed.
func TestWaitUnlockFastSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attempt := func(context.Context) (bool, error) { return false, nil }
	polls := 0
	poll := func(context.Context) (nodeOpenProgress, bool, error) {
		polls++
		return nodeOpenProgress{}, false, errors.New("unreachable")
	}
	logf, _ := testLogf(t)
	if err := waitUnlock(ctx, 5*time.Second, attempt, poll, 100*time.Millisecond, 5*time.Millisecond, time.Hour, logf); err != nil {
		t.Fatalf("waitUnlock: %v", err)
	}
}

// TestWaitUnlockSlowButAdvancing is the regression test for the endurance
// failure mode: an open that takes longer than any fixed allowance must
// succeed while its counters keep moving.
func TestWaitUnlockSlowButAdvancing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attempt := func(ctx context.Context) (bool, error) {
		select {
		case <-time.After(400 * time.Millisecond):
			return false, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	var n atomic.Uint64
	poll := func(context.Context) (nodeOpenProgress, bool, error) {
		v := n.Add(1)
		return nodeOpenProgress{Opening: true, Phase: "rebuilding", ProcessedItems: v}, true, nil
	}
	logf, lines := testLogf(t)
	if err := waitUnlock(ctx, 5*time.Second, attempt, poll, 100*time.Millisecond, 5*time.Millisecond, 50*time.Millisecond, logf); err != nil {
		t.Fatalf("waitUnlock: %v", err)
	}
	if len(*lines) == 0 {
		t.Fatal("slow open logged no waiting status")
	}
}

// TestWaitUnlockFrozenProgress fails fast when snapshots stop moving: a
// wedged open must not burn the whole overall cap.
func TestWaitUnlockFrozenProgress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attempt := func(context.Context) (bool, error) { select {} }
	frozen := nodeOpenProgress{Opening: true, Phase: "rebuilding", ProcessedItems: 42}
	poll := func(context.Context) (nodeOpenProgress, bool, error) { return frozen, true, nil }
	logf, _ := testLogf(t)
	start := time.Now()
	err := waitUnlock(ctx, 5*time.Second, attempt, poll, 100*time.Millisecond, 5*time.Millisecond, time.Hour, logf)
	if err == nil || !strings.Contains(err.Error(), "stalled") {
		t.Fatalf("waitUnlock = %v, want stall error", err)
	}
	if !strings.Contains(err.Error(), "rebuilding") || !strings.Contains(err.Error(), "42") {
		t.Fatalf("stall error lacks last snapshot: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("stall verdict took %s, want fast fail", time.Since(start))
	}
}

// TestWaitUnlockUnreachableDaemon reports a stall carrying the last poll
// error when the daemon never serves progress.
func TestWaitUnlockUnreachableDaemon(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attempt := func(context.Context) (bool, error) { select {} }
	poll := func(context.Context) (nodeOpenProgress, bool, error) {
		return nodeOpenProgress{}, false, errors.New("connection refused")
	}
	logf, _ := testLogf(t)
	err := waitUnlock(ctx, 5*time.Second, attempt, poll, 100*time.Millisecond, 5*time.Millisecond, time.Hour, logf)
	if err == nil || !strings.Contains(err.Error(), "stalled") || !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("waitUnlock = %v, want stall with poll error", err)
	}
}

// TestWaitUnlockOverallCap bounds even a healthy-looking open: advancing
// forever without finishing still fails, with the last snapshot attached.
func TestWaitUnlockOverallCap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	attempt := func(context.Context) (bool, error) { select {} }
	var n atomic.Uint64
	poll := func(context.Context) (nodeOpenProgress, bool, error) {
		return nodeOpenProgress{Opening: true, Phase: "rebuilding", ProcessedItems: n.Add(1)}, true, nil
	}
	logf, _ := testLogf(t)
	err := waitUnlock(ctx, 150*time.Millisecond, attempt, poll, time.Minute, 5*time.Millisecond, time.Hour, logf)
	if err == nil || !strings.Contains(err.Error(), "did not complete") || !strings.Contains(err.Error(), "rebuilding") {
		t.Fatalf("waitUnlock = %v, want overall-cap error with snapshot", err)
	}
}

// TestWaitUnlockDefinitiveRejection passes POST verdicts through: a wrong
// key must fail at once, not spin until a cap.
func TestWaitUnlockDefinitiveRejection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rejected := errors.New("unlock rejected: HTTP 401: unlock failed")
	attempt := func(context.Context) (bool, error) { return false, rejected }
	poll := func(context.Context) (nodeOpenProgress, bool, error) {
		return nodeOpenProgress{}, false, errors.New("unreachable")
	}
	logf, _ := testLogf(t)
	if err := waitUnlock(ctx, 5*time.Second, attempt, poll, time.Minute, 5*time.Millisecond, time.Hour, logf); !errors.Is(err, rejected) {
		t.Fatalf("waitUnlock = %v, want rejection", err)
	}
}

// TestWaitUnlockRetriesTransportFailures proves daemon-start races resolve:
// early connection failures relaunch the attempt instead of failing.
func TestWaitUnlockRetriesTransportFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var calls atomic.Int32
	attempt := func(context.Context) (bool, error) {
		if calls.Add(1) < 3 {
			return true, errors.New("connection refused")
		}
		return false, nil
	}
	poll := func(context.Context) (nodeOpenProgress, bool, error) {
		return nodeOpenProgress{}, false, errors.New("connection refused")
	}
	logf, _ := testLogf(t)
	// The 500ms relaunch pause runs twice; the stall window must cover it.
	if err := waitUnlock(ctx, 5*time.Second, attempt, poll, 3*time.Second, 5*time.Millisecond, time.Hour, logf); err != nil {
		t.Fatalf("waitUnlock: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", calls.Load())
	}
}

func TestNodeOpenProgressSignature(t *testing.T) {
	base := nodeOpenProgress{Opening: true, Phase: "rebuilding", ProcessedItems: 1}
	same := base
	same.ElapsedSeconds = 99
	if same.signature() != base.signature() {
		t.Fatal("elapsed time must not count as progress")
	}
	moved := base
	moved.ProcessedItems = 2
	if moved.signature() == base.signature() {
		t.Fatal("counter advance must change the signature")
	}
	phase := base
	phase.Phase = "finalizing"
	if phase.signature() == base.signature() {
		t.Fatal("phase change must change the signature")
	}
	replay := base
	replay.ReplayRecords = 7
	if replay.signature() == base.signature() {
		t.Fatal("replay advance must change the signature")
	}
	replay.ReplayFilesTotal = 2
	if !strings.Contains(replay.describe(), "replay=0/2files") {
		t.Fatalf("describe = %q, want replay segment", replay.describe())
	}
	if !strings.Contains(base.describe(), "rebuilding") || !strings.Contains(base.describe(), "processed=1") {
		t.Fatalf("describe = %q", base.describe())
	}
	errState := nodeOpenProgress{Phase: "failed", Error: "boom"}
	if !strings.Contains(errState.describe(), "boom") {
		t.Fatalf("describe = %q, want error", errState.describe())
	}
}
