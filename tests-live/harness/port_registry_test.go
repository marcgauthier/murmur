package harness

import "testing"

// TestClaimPortRejectsDuplicates pins the anti-collision contract behind
// getFreePort: each port is handed out at most once per test process, so
// two nodes can never share a replication or API port.
func TestClaimPortRejectsDuplicates(t *testing.T) {
	const port = 54321
	if !claimPort(port) {
		t.Fatalf("first claim of %d failed", port)
	}
	if claimPort(port) {
		t.Fatalf("second claim of %d succeeded, want rejection", port)
	}
	for _, bad := range []int{0, -1, 65536} {
		if claimPort(bad) {
			t.Fatalf("claim of invalid port %d succeeded, want rejection", bad)
		}
	}
}
