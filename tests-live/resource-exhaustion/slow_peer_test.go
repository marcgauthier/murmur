package resourceexhaustion_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// uniqueRandomValues returns n distinct incompressible ASCII strings of
// size bytes each (seeded for reproducibility).
func uniqueRandomValues(n, size int) []string {
	rng := rand.New(rand.NewSource(0x5eed))
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	out := make([]string, n)
	buf := make([]byte, size)
	for i := range out {
		_, _ = rng.Read(buf)
		for j, b := range buf {
			buf[j] = alphabet[int(b)%len(alphabet)]
		}
		out[i] = string(append([]byte(nil), buf...))
	}
	return out
}

// A very slow peer (replication traffic throttled through a userspace UDP
// proxy — no tc privileges needed) must still converge without wedging:
// the slow node's row count must keep increasing until it catches up,
// and removing the throttle must restore fast convergence.
//
//	MURMUR_RX_SLOW_RATE=262144         throttle bytes/sec per direction
//	MURMUR_RX_SLOW_ROWS=600            churn rows
//	MURMUR_RX_SLOW_VALBYTES=1024       value bytes per churn row
//	MURMUR_RX_SLOW_BOUND_SECONDS=300   convergence bound through throttle
//
// Envelope: replication frames carry up to 128 rows and must clear
// murmur's 30s framed-write deadline (SendTimeout), or the session
// recycles by design. Size rate x rows x values so backlog clears with
// margin (default: 2 Mbit/s, 600 x 1 KiB rows). Observed: 1 KiB rows at
// 1000+ through 1 Mbit/s or less churn sessions faster than they drain
// and wedge on the 90s-no-progress trip.
func TestSlowPeerConvergesWithoutWedge(t *testing.T) {
	rate := harness.EnvInt("MURMUR_RX_SLOW_RATE", 262144)
	rows := harness.EnvInt("MURMUR_RX_SLOW_ROWS", 600)
	valBytes := harness.EnvInt("MURMUR_RX_SLOW_VALBYTES", 1024)
	bound := harness.EnvSeconds("MURMUR_RX_SLOW_BOUND_SECONDS", 300)

	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "resource-slow",
		NumNodes:        2,
		AwaitUnlock:     true,
		ManualPeers:     true,
		TypedRecords:    true,
		TypedContention: true,
	})

	// Both directions cross a throttle: replication data follows the
	// connection the receiver initiated, so throttling only node0->node1
	// would leave the data path (node1's pull from node0) unthrottled.
	// Each proxy paces both directions with drop-tail, like tc-tbf.
	real0, real1 := cluster.Nodes[0].ReplAddr, cluster.Nodes[1].ReplAddr
	toSlow := startThrottleProxy(t, real1, rate)
	t.Cleanup(toSlow.Close)
	toFast := startThrottleProxy(t, real0, rate)
	t.Cleanup(toFast.Close)
	t.Logf("throttle: node0 -> %s -> node1, node1 -> %s -> node0 at %d B/s",
		toSlow.Addr(), toFast.Addr(), rate)
	cluster.Nodes[1].ReplAddr = toSlow.Addr()
	cluster.Nodes[0].ReplAddr = toFast.Addr()
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("add peer 0->1 via proxy: %v", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatalf("add peer 1->0 via proxy: %v", err)
	}

	// Baseline through the throttle proves the proxied path works before
	// the timed leg starts.
	if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: fmt.Sprintf("%032x", 1), Name: "baseline"}); err != nil {
		t.Fatalf("baseline insert: %v", err)
	}
	waitCounts(t, cluster, []int{1}, 1, 2*time.Minute)
	t.Logf("baseline crossed the throttle")

	// Churn on the fast node; the slow node must make steady progress
	// (no wedge) until fully converged within the bound. Values are
	// unique incompressible bytes per row: repeats would zstd away and
	// the throttle would never bind.
	vals := uniqueRandomValues(rows, valBytes)
	for i := 0; i < rows; i++ {
		if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: fmt.Sprintf("%032x", i+2), Name: vals[i]}); err != nil {
			t.Fatalf("churn insert %d: %v", i, err)
		}
	}
	total := rows + 1
	convergeStart := time.Now()
	deadline := time.Now().Add(bound)
	last, lastIncrease := 0, time.Now()
	stalled := false
	for {
		n, err := rxCount(cluster, 1)
		if err != nil {
			t.Fatalf("slow node unreadable: %v", err)
		}
		if n > last {
			last, lastIncrease = n, time.Now()
		}
		if n == total {
			break
		}
		if time.Since(lastIncrease) > 90*time.Second {
			stalled = true
			break
		}
		if time.Now().After(deadline) {
			break
		}
		s1, s2 := toSlow.Stats(), toFast.Stats()
		t.Logf("slow node %d/%d rows; toSlow fwd %dB(%dd) bwd %dB(%dd) | toFast fwd %dB(%dd) bwd %dB(%dd)",
			n, total, s1.FwdBytes, s1.FwdDrops, s1.BwdBytes, s1.BwdDrops,
			s2.FwdBytes, s2.FwdDrops, s2.BwdBytes, s2.BwdDrops)
		time.Sleep(5 * time.Second)
	}
	if stalled {
		t.Fatalf("WEDGE: slow node stuck at %d/%d rows for 90s", last, total)
	}
	if last != total {
		t.Fatalf("slow node reached %d/%d rows within %s", last, total, bound)
	}
	took := time.Since(convergeStart)
	// Byte-level proof the data crossed the throttles: ~1MB of
	// incompressible values must have traversed the slow paths (data may
	// flow through either proxy in either direction). This is the
	// non-vacuous guard: a fast pass with few throttled bytes means
	// traffic bypassed the proxies and the test is broken.
	s1, s2 := toSlow.Stats(), toFast.Stats()
	throttled := s1.FwdBytes + s1.BwdBytes + s2.FwdBytes + s2.BwdBytes
	if throttled < uint64(rows*valBytes/2) {
		t.Fatalf("only %d throttled bytes for %d x %dB rows: data bypassed the proxies?", throttled, rows, valBytes)
	}
	d := waitDigests(t, cluster, []int{0, 1}, time.Minute)
	t.Logf("converged through %d B/s throttles in %s: %d rows, %d throttled bytes (%d drops), digest %s",
		rate, took.Round(time.Second), total, throttled,
		s1.FwdDrops+s1.BwdDrops+s2.FwdDrops+s2.BwdDrops, d)

	// Heal: remove the throttles, re-peer direct, and prove fast
	// convergence is restored.
	toSlow.Close()
	toFast.Close()
	cluster.Nodes[0].ReplAddr = real0
	cluster.Nodes[1].ReplAddr = real1
	if err := cluster.AddPeer(0, 1); err != nil {
		t.Fatalf("re-peer 0->1 direct: %v", err)
	}
	if err := cluster.AddPeer(1, 0); err != nil {
		t.Fatalf("re-peer 1->0 direct: %v", err)
	}
	for i := 0; i < 10; i++ {
		if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: fmt.Sprintf("%032x", total+i+1), Name: "healed"}); err != nil {
			t.Fatalf("post-heal insert %d: %v", i, err)
		}
	}
	waitCounts(t, cluster, []int{0, 1}, total+10, time.Minute)
	d2 := waitDigests(t, cluster, []int{0, 1}, time.Minute)
	t.Logf("PASS: healed to direct peering, %d rows, digest %s", total+10, d2)
}
