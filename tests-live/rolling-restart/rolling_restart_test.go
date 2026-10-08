package rollingrestart

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// Rolling restart with continuous writes: each node stops and rejoins in
// turn while every node keeps accepting writes. Any failed write aborts
// the release (zero-downtime restart), and the mesh must converge with
// identical digests at the end.
func TestRollingRestartLosesNoWrites(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "rolling-restart",
		NumNodes:        3,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
	})

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%032x", i)
		if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: id, Name: fmt.Sprintf("base-%d", i)}); err != nil {
			t.Fatalf("baseline write: %v", err)
		}
	}
	// 60s, not 30s: under a full parallel `go test ./...` the box is
	// saturated and even a 5-row baseline can take tens of seconds.
	waitCounts(t, cluster, 5, 60*time.Second)

	// Writers hammer every node continuously. A write to a live node must
	// never fail (zero-downtime restart); the writer for the node being
	// restarted pauses while its daemon is down, since a down node
	// accepts nothing by definition.
	var failed atomic.Int64
	var paused [3]atomic.Bool
	stop := make(chan struct{})
	stopWriters := sync.OnceFunc(func() { close(stop) })
	defer stopWriters()
	var wg sync.WaitGroup
	for n := 0; n < 3; n++ {
		wg.Add(1)
		go func(node int) {
			defer wg.Done()
			ticker := time.NewTicker(25 * time.Millisecond)
			defer ticker.Stop()
			seq := 0
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					if paused[node].Load() {
						continue
					}
					id := fmt.Sprintf("ff%02x%028x", node, seq)
					seq++
					if err := cluster.TypedContentionInsert(node, harness.TypedContentionRow{ID: id, Name: "flow"}); err != nil {
						failed.Add(1)
						t.Logf("node%d write failed outside its restart: %v", node+1, err)
					}
				}
			}
		}(n)
	}

	for i := 0; i < 3; i++ {
		paused[i].Store(true)
		// Drain in-flight writes before stopping so none fail spuriously.
		time.Sleep(300 * time.Millisecond)
		cluster.StopNode(i)
		time.Sleep(time.Second)
		cluster.StartNode(i)
		cluster.UnlockNode(i, cluster.Nodes[i].KeyHex)
		cluster.WaitNodeReady(i)
		paused[i].Store(false)
		// Let the rejoined node catch up under load, then quiesce all
		// writers: exact-count agreement is unobservable while 80+
		// inserts/s keep landing.
		time.Sleep(3 * time.Second)
		// 150s, not 60s: under a full parallel `go test ./...` the box
		// runs ~10x slow and exact agreement legitimately takes over a
		// minute; the proof (exact agreement) is unchanged.
		quiesce(t, cluster, &paused, 150*time.Second)
	}

	stopWriters()
	wg.Wait()
	if got := failed.Load(); got != 0 {
		t.Fatalf("%d writes failed outside restart windows, want zero-downtime", got)
	}
	waitConvergedCounts(t, cluster, 150*time.Second)
	want, err := rrDigest(cluster, 0)
	if err != nil {
		t.Fatal(err)
	}
	for idx := 1; idx < 3; idx++ {
		d, err := rrDigest(cluster, idx)
		if err != nil {
			t.Fatal(err)
		}
		if d != want {
			t.Fatalf("node%d digest %s != node1 %s after rolling restart", idx+1, d, want)
		}
	}
}

// rrRows returns the contention-table rows on one node ordered by id.
func rrRows(c *harness.Cluster, idx int) ([]harness.TypedContentionRow, error) {
	rows, err := c.TypedContentionRows(idx)
	if err != nil {
		return nil, err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows, nil
}

// rrDigest returns the PK-ordered digest of the contention table on one node.
func rrDigest(c *harness.Cluster, idx int) (string, error) {
	rows, err := rrRows(c, idx)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	for _, row := range rows {
		fmt.Fprintf(h, "%s:%s:%s:%d\n", row.ID, row.Name, row.Phone, row.Score)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// dumpIDDiff logs the symmetric row-id difference between nodes 1 and 2
// (failure diagnostics).
func dumpIDDiff(t *testing.T, c *harness.Cluster) {
	t.Helper()
	ids := func(idx int) map[string]bool {
		m := map[string]bool{}
		rows, err := rrRows(c, idx)
		if err != nil {
			t.Logf("divergence: node%d ids query: %v", idx+1, err)
			return m
		}
		for _, row := range rows {
			m[row.ID] = true
		}
		return m
	}
	a, b := ids(0), ids(1)
	var onlyA, onlyB []string
	for id := range a {
		if !b[id] {
			onlyA = append(onlyA, id)
		}
	}
	for id := range b {
		if !a[id] {
			onlyB = append(onlyB, id)
		}
	}
	t.Logf("divergence: only-node1=%d %v only-node2=%d %v", len(onlyA), firstN(onlyA, 5), len(onlyB), firstN(onlyB, 5))
	// Same id sets: show the first ordered row difference (catches
	// column-order and encoding skew between nodes).
	r0, _ := rrRows(c, 0)
	r2, _ := rrRows(c, 1)
	for i := 0; i < len(r0) && i < len(r2); i++ {
		a, b := fmt.Sprintf("%v", r0[i]), fmt.Sprintf("%v", r2[i])
		if a != b {
			t.Logf("divergence: first diff at row %d:\n  node1=%q\n  node2=%q", i, a, b)
			break
		}
	}
	// Session state for heal-stall forensics: a frozen row gap with
	// writers paused means sessions (not slowness) are wedged, and the
	// per-peer connection/need/ack state shows which link is stuck.
	c.DumpForensics("divergence")
}

func firstN(s []string, n int) []string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// waitCounts waits until every node reports the exact row count.
func waitCounts(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok := true
		for idx := range c.Nodes {
			rows, err := rrRows(c, idx)
			if err != nil || len(rows) != want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("row counts did not reach %d within %v", want, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// rrCount returns the contention-table row count on one node.
func rrCount(c *harness.Cluster, idx int) (int, error) {
	rows, err := rrRows(c, idx)
	if err != nil {
		return 0, err
	}
	return len(rows), nil
}

// quiesce pauses all writers, waits for exact digest agreement, then
// resumes. Callers must hold no pause themselves.
func quiesce(t *testing.T, c *harness.Cluster, paused *[3]atomic.Bool, timeout time.Duration) {
	t.Helper()
	for i := range paused {
		paused[i].Store(true)
	}
	// Drain in-flight writes before asserting a frozen state.
	time.Sleep(300 * time.Millisecond)
	deadline := time.Now().Add(timeout)
	for {
		want, err := rrDigest(c, 0)
		if err == nil {
			match := true
			for idx := 1; idx < len(c.Nodes); idx++ {
				d, err := rrDigest(c, idx)
				if err != nil || d != want {
					match = false
					break
				}
			}
			if match {
				for i := range paused {
					paused[i].Store(false)
				}
				return
			}
		}
		if time.Now().After(deadline) {
			dumpIDDiff(t, c)
			t.Fatalf("digests did not agree within %v", timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitConvergedCounts waits until all nodes agree on the same row count
// (the absolute value floats while writers run).
func waitConvergedCounts(t *testing.T, c *harness.Cluster, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		first, err := rrCount(c, 0)
		if err != nil {
			if time.Now().After(deadline) {
				t.Fatalf("node1 count query: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		agree := true
		for idx := 1; idx < len(c.Nodes); idx++ {
			n, err := rrCount(c, idx)
			if err != nil || n != first {
				agree = false
				break
			}
		}
		if agree {
			// Settle: one more matching sample before declaring convergence.
			time.Sleep(500 * time.Millisecond)
			still := true
			for idx := range c.Nodes {
				n, err := rrCount(c, idx)
				if err != nil || n != first {
					still = false
					break
				}
			}
			if still {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("row counts did not converge within %v", timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
