// Flapping-partition acceptance.
//
// Three fully meshed daemons take continuous writes from all nodes while
// node3's links flap (split/heal cycles). A partition must never fail a
// write to a live node (all commits are local), and after the final heal
// every node must converge on the identical row set with equal digests.
package flappartition_test

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

const tableName = "flap_rows"

func TestFlappingPartitionLosesNoWrites(t *testing.T) {
	cycles := envInt("SPEDSQL_FLAP_CYCLES", 5)
	split := time.Duration(envInt("SPEDSQL_FLAP_SPLIT_SECONDS", 3)) * time.Second
	healGap := time.Duration(envInt("SPEDSQL_FLAP_HEAL_SECONDS", 2)) * time.Second

	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "flap-partition",
		NumNodes:    3,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: tableName,
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
	})

	// Baseline converges before flapping (honest-path control).
	for i := 0; i < 6; i++ {
		id := fmt.Sprintf("%032x", 1000+i)
		if err := cluster.ExecSQL(0, "INSERT INTO "+tableName+" (id, name) VALUES (?, ?)", id, fmt.Sprintf("base-%d", i)); err != nil {
			t.Fatalf("baseline write: %v", err)
		}
	}
	waitConverged(t, cluster, 6, 150*time.Second)

	// Continuous writers on every node; node3 stays live (running, only
	// unreachable), so its local commits must succeed too.
	var written, failed atomic.Int64
	stop := make(chan struct{})
	var writers sync.WaitGroup
	for i := range cluster.Nodes {
		writers.Add(1)
		go func(node int) {
			defer writers.Done()
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for seq := int64(1); ; seq++ {
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
				id := fmt.Sprintf("%032x", int64(node+1)*1_000_000+seq)
				if err := cluster.ExecSQL(node, "INSERT INTO "+tableName+" (id, name) VALUES (?, ?)", id,
					fmt.Sprintf("n%d-s%d", node, seq)); err != nil {
					failed.Add(1)
					continue
				}
				written.Add(1)
			}
		}(i)
	}

	// Flap node3's partition: split from both peers, hold, heal, settle.
	for c := 0; c < cycles; c++ {
		setPartition(t, cluster, true)
		t.Logf("cycle %d/%d: node3 isolated for %v", c+1, cycles, split)
		time.Sleep(split)
		setPartition(t, cluster, false)
		t.Logf("cycle %d/%d: healed, settling %v", c+1, cycles, healGap)
		time.Sleep(healGap)
	}
	close(stop)
	writers.Wait()

	// Final heal (idempotent) then full convergence on everything written.
	setPartition(t, cluster, false)
	want := int(6 + written.Load())
	t.Logf("flapping done: %d cycles, %d rows written, %d failed", cycles, want, failed.Load())
	if got := failed.Load(); got != 0 {
		t.Fatalf("partition failed %d writes to live nodes, want 0", got)
	}
	waitConverged(t, cluster, want, 150*time.Second)
	t.Logf("post-flap convergence: %d rows, identical digests", want)
}

// setPartition isolates (true) or rejoins (false) node index 2.
func setPartition(t *testing.T, c *harness.Cluster, isolate bool) {
	t.Helper()
	for _, peer := range []int{0, 1} {
		var err1, err2 error
		if isolate {
			err1 = c.RemovePeer(2, peer)
			err2 = c.RemovePeer(peer, 2)
		} else {
			err1 = c.AddPeer(2, peer)
			err2 = c.AddPeer(peer, 2)
		}
		if err1 != nil || err2 != nil {
			t.Fatalf("partition link 2<->%d (isolate=%v): %v %v", peer, isolate, err1, err2)
		}
	}
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	lastLog := time.Now()
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			n, err := c.QueryRowCount(i, tableName)
			if err != nil || n != want {
				ok = false
				break
			}
			d, err := c.ComputeTableDigest(i, tableName, "name")
			if err != nil {
				ok = false
				break
			}
			if i == 0 {
				first = d
			} else if d != first {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		if time.Since(lastLog) > 10*time.Second {
			lastLog = time.Now()
			counts := make([]int, len(c.Nodes))
			for i := range c.Nodes {
				n, _ := c.QueryRowCount(i, tableName)
				counts[i] = n
			}
			t.Logf("converge progress: counts=%v want=%d", counts, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
	for i := range c.Nodes {
		n, _ := c.QueryRowCount(i, tableName)
		d, _ := c.ComputeTableDigest(i, tableName, "name")
		t.Logf("node %d at timeout: count=%d digest=%s", i, n, d)
	}
	t.Fatalf("nodes did not converge on %d rows with equal digests within %v", want, timeout)
}

func envInt(name string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return fallback
}
