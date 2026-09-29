package harness

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// claimFilePath returns the claim-file path claimPort uses for port.
func claimFilePath(port int) string {
	return filepath.Join(os.TempDir(), "spedsql-portclaims", strconv.Itoa(port))
}

// drawClaimable draws a kernel-free port and claims it, cleaning the
// claim file up so reruns stay idempotent. getFreePort already retries
// draws past live claims left by earlier runs.
func drawClaimable(t *testing.T) int {
	t.Helper()
	port := getFreePort(t)
	t.Cleanup(func() { _ = os.Remove(claimFilePath(port)) })
	return port
}

// TestClaimPortRejectsDuplicates pins the anti-collision contract behind
// getFreePort: each port is handed out at most once while claimed, so
// two nodes (even in different test processes) can never share it.
func TestClaimPortRejectsDuplicates(t *testing.T) {
	port := drawClaimable(t)
	if claimPort(port) {
		t.Fatalf("second claim of %d succeeded, want rejection", port)
	}
	for _, bad := range []int{0, -1, 65536} {
		if claimPort(bad) {
			t.Fatalf("claim of invalid port %d succeeded, want rejection", bad)
		}
	}
}

// TestClaimPortExpiry proves expired claims release their port: the
// first attempt removes the stale file and reports unclaimed, and a
// fresh claim then succeeds.
func TestClaimPortExpiry(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "spedsql-portclaims")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	port := 59998
	t.Cleanup(func() { _ = os.Remove(claimFilePath(port)) })
	stale := fmt.Sprintf("%d %d\n", os.Getpid(), time.Now().Add(-5*time.Hour).UnixNano())
	if err := os.WriteFile(claimFilePath(port), []byte(stale), 0644); err != nil {
		t.Fatal(err)
	}
	if claimPort(port) {
		t.Fatal("claim over expired entry succeeded, want removal+redraw")
	}
	if _, err := os.Stat(claimFilePath(port)); !os.IsNotExist(err) {
		t.Fatal("expired claim file was not removed")
	}
	if !claimPort(port) {
		t.Fatal("fresh claim after expiry failed")
	}
}

// TestClaimPortConcurrent proves exactly one claimant wins a race for
// the same port (O_EXCL atomicity, the cross-process guarantee).
func TestClaimPortConcurrent(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "spedsql-portclaims")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	port := 59997
	t.Cleanup(func() { _ = os.Remove(claimFilePath(port)) })
	_ = os.Remove(claimFilePath(port))
	var wins atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if claimPort(port) {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("%d concurrent claimants won port %d, want exactly 1", got, port)
	}
}
