package rollingrestart

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

// Rolling restart with continuous writes: each node stops and rejoins in
// turn while every node keeps accepting writes. Any failed write aborts
// the release (zero-downtime restart), and the mesh must converge with
// identical digests at the end.
func TestRollingRestartLosesNoWrites(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "rolling-restart",
		NumNodes:    3,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "rr_rows",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
	})

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%032x", i)
		if err := cluster.ExecSQL(0, "INSERT INTO rr_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("base-%d", i)); err != nil {
			t.Fatalf("baseline write: %v", err)
		}
	}
	// 60s, not 30s: under a full parallel `go test ./...` the box is
	// saturated and even a 5-row baseline can take tens of seconds.
	waitCounts(t, cluster, "rr_rows", 5, 60*time.Second)

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
					if err := cluster.ExecSQL(node, "INSERT INTO rr_rows (id, name) VALUES (?, ?)", id, "flow"); err != nil {
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
		quiesce(t, cluster, "rr_rows", &paused, 150*time.Second)
	}

	stopWriters()
	wg.Wait()
	if got := failed.Load(); got != 0 {
		t.Fatalf("%d writes failed outside restart windows, want zero-downtime", got)
	}
	waitConvergedCounts(t, cluster, "rr_rows", 150*time.Second)
	want, err := cluster.ComputeTableDigest(0, "rr_rows", "id")
	if err != nil {
		t.Fatal(err)
	}
	for idx := 1; idx < 3; idx++ {
		d, err := cluster.ComputeTableDigest(idx, "rr_rows", "id")
		if err != nil {
			t.Fatal(err)
		}
		if d != want {
			t.Fatalf("node%d digest %s != node1 %s after rolling restart", idx+1, d, want)
		}
	}
}

// dumpIDDiff logs the symmetric row-id difference between nodes 1 and 2
// (failure diagnostics).
func dumpIDDiff(t *testing.T, c *harness.Cluster, table string) {
	t.Helper()
	ids := func(idx int) map[string]bool {
		m := map[string]bool{}
		res, err := c.QuerySQL(idx, "SELECT id FROM "+table)
		if err != nil {
			t.Logf("divergence: node%d ids query: %v", idx+1, err)
			return m
		}
		for _, row := range res.Rows {
			if len(row) > 0 {
				m[string(fmt.Sprintf("%v", row[0]))] = true
			}
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
	r0, _ := c.QuerySQL(0, "SELECT * FROM "+table+" ORDER BY id")
	r2, _ := c.QuerySQL(1, "SELECT * FROM "+table+" ORDER BY id")
	for i := 0; i < len(r0.Rows) && i < len(r2.Rows); i++ {
		a, b := fmt.Sprintf("%v", r0.Rows[i]), fmt.Sprintf("%v", r2.Rows[i])
		if a != b {
			t.Logf("divergence: first diff at row %d:\n  node1=%q\n  node2=%q", i, a, b)
			break
		}
	}
	// Session state for heal-stall forensics: a frozen row gap with
	// writers paused means sessions (not slowness) are wedged, and the
	// per-peer connection/need/ack state shows which link is stuck.
	for idx, node := range c.Nodes {
		t.Logf("divergence: node%d status: %s", idx+1, fetchStatus(t, node.APIAddr))
	}
}

// fetchStatus returns the node's /v1/status body, truncated for logs.
// Plain http.Get works: the harness routes it through the mTLS client.
func fetchStatus(t *testing.T, apiAddr string) string {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/v1/status")
	if err != nil {
		return "unreachable: " + err.Error()
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 2048))
	if err != nil {
		return "read error: " + err.Error()
	}
	return string(raw)
}

func firstN(s []string, n int) []string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// waitCounts waits until every node reports the exact row count.
func waitCounts(t *testing.T, c *harness.Cluster, table string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok := true
		for idx := range c.Nodes {
			n, err := c.QueryRowCount(idx, table)
			if err != nil || n != want {
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

// quiesce pauses all writers, waits for exact digest agreement, then
// resumes. Callers must hold no pause themselves.
func quiesce(t *testing.T, c *harness.Cluster, table string, paused *[3]atomic.Bool, timeout time.Duration) {
	t.Helper()
	for i := range paused {
		paused[i].Store(true)
	}
	// Drain in-flight writes before asserting a frozen state.
	time.Sleep(300 * time.Millisecond)
	deadline := time.Now().Add(timeout)
	for {
		want, err := c.ComputeTableDigest(0, table, "id")
		if err == nil {
			match := true
			for idx := 1; idx < len(c.Nodes); idx++ {
				d, err := c.ComputeTableDigest(idx, table, "id")
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
			dumpIDDiff(t, c, table)
			t.Fatalf("digests did not agree within %v", timeout)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// waitConvergedCounts waits until all nodes agree on the same row count
// (the absolute value floats while writers run).
func waitConvergedCounts(t *testing.T, c *harness.Cluster, table string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		first, err := c.QueryRowCount(0, table)
		if err != nil {
			if time.Now().After(deadline) {
				t.Fatalf("node1 count query: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		agree := true
		for idx := 1; idx < len(c.Nodes); idx++ {
			n, err := c.QueryRowCount(idx, table)
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
				n, err := c.QueryRowCount(idx, table)
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
