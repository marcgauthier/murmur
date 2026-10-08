package resourceexhaustion_test

import (
	"crypto/tls"
	"fmt"
	"net"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// A node that runs out of file descriptors must fail closed (clean
// errors, live process, intact data) and recover fully once descriptors
// are available again. Descriptors are squeezed two ways: the victim's
// RLIMIT_NOFILE is lowered with prlimit, then idle TLS connections hold
// server sockets until a probe write observably fails.
//
//	MURMUR_RX_FD_NOFILE=128   victim soft fd limit during the squeeze
//	MURMUR_RX_FD_MAXHOLD=300  cap on held idle connections
func TestFDExhaustionFailClosed(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("fd squeeze needs prlimit and /proc (linux-only)")
	}
	if _, err := exec.LookPath("prlimit"); err != nil {
		t.Skip("prlimit not installed")
	}
	nofile := harness.EnvInt("MURMUR_RX_FD_NOFILE", 128)
	maxHold := harness.EnvInt("MURMUR_RX_FD_MAXHOLD", 300)

	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "resource-fd",
		NumNodes:        2,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
	})

	// Baseline: 50 rows converged everywhere before the squeeze.
	for i := 0; i < 50; i++ {
		if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: fmt.Sprintf("%032x", i+1), Name: fmt.Sprintf("base-%d", i)}); err != nil {
			t.Fatalf("baseline insert %d: %v", i, err)
		}
	}
	waitCounts(t, cluster, []int{0, 1}, 50, time.Minute)
	t.Logf("baseline: 50 rows converged")

	victimPID := nodePID(t, cluster, 1)
	if out, err := exec.Command("prlimit", fmt.Sprintf("--nofile=%d", nofile),
		"--pid", fmt.Sprintf("%d", victimPID)).CombinedOutput(); err != nil {
		t.Fatalf("prlimit nofile=%d on pid %d: %v (%s)", nofile, victimPID, err, out)
	}
	t.Logf("victim node2 pid=%d squeezed to nofile=%d", victimPID, nofile)

	// Hold idle completed-TLS connections and probe until a victim write
	// observably fails: that failure is the exhaustion proof (the loop
	// cannot pass vacuously).
	api := cluster.Nodes[1].APIAddr
	var held []net.Conn
	defer func() {
		for _, c := range held {
			_ = c.Close()
		}
	}()
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	acked := 0
	exhausted := false
	seq := 1000
hold:
	for len(held) < maxHold && !exhausted {
		conn, err := tls.DialWithDialer(dialer, "tcp", api, &tls.Config{InsecureSkipVerify: true})
		if err != nil {
			t.Logf("dial %d failed (%v): server out of fds", len(held), err)
			exhausted = true
			break
		}
		held = append(held, conn)
		if len(held)%10 == 0 {
			seq++
			if err := cluster.TypedContentionInsert(1, harness.TypedContentionRow{ID: fmt.Sprintf("%032x", seq), Name: "probe"}); err != nil {
				t.Logf("probe write failed after %d held conns (%v): exhausted", len(held), err)
				exhausted = true
				break hold
			}
			acked++
		}
	}
	if !exhausted {
		t.Fatalf("could not exhaust victim fds with %d held conns (nofile=%d)", len(held), nofile)
	}

	// Fail-closed: the process is alive and its pre-squeeze data is
	// intact. (Replication out of the victim may stall while it cannot
	// open sockets, so cross-node equality is asserted after recovery.)
	if !procAlive(victimPID) {
		t.Fatal("victim process died under fd exhaustion")
	}
	t.Logf("exhausted with %d held conns, %d probes acked; victim alive", len(held), acked)

	// Release everything and restart the victim (fresh process, default
	// limits): full recovery to exact convergence, including every row
	// acked before and during the squeeze.
	for _, c := range held {
		_ = c.Close()
	}
	held = nil
	cluster.StopNode(1)
	cluster.StartNode(1)
	cluster.UnlockNode(1, cluster.Nodes[1].KeyHex)
	cluster.WaitNodeReady(1)

	total := 50 + acked
	waitCounts(t, cluster, []int{0, 1}, total, 3*time.Minute)
	d := waitDigests(t, cluster, []int{0, 1}, 3*time.Minute)
	t.Logf("PASS: recovered to %d rows on both nodes, digest %s", total, d)
}
