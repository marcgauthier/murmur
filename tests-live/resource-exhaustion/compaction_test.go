package resourceexhaustion_test

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// A node whose compactions stall (automatic compactions disabled, small
// memtables to pile L0 debt fast) must stay responsive and exactly
// correct; restarting with compactions re-enabled must drain the debt
// and converge. The SST-count gap between victim and control is the
// non-vacuous proof that the stall actually happened.
//
//	MURMUR_RX_COMPACT_ROWS=3000   churn rows (1 KiB values)
func TestStalledCompactionStaysCorrect(t *testing.T) {
	rows := harness.EnvInt("MURMUR_RX_COMPACT_ROWS", 3000)

	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "resource-compact",
		NumNodes:        2,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
		SpoolByNode: map[int]*harness.SpoolOptions{
			1: {MemTableBytes: 1 << 20, DisableAutomaticCompactions: true},
		},
	})
	victimSST := func() int { return countSSTs(t, cluster.Nodes[1].Dir) }
	controlSST := func() int { return countSSTs(t, cluster.Nodes[0].Dir) }

	// Churn through the victim so its L0 debt is its own writes (plus
	// what replicates back); probe responsiveness along the way.
	val := strings.Repeat("c", 1<<10)
	for i := 0; i < rows; i++ {
		if err := cluster.TypedContentionInsert(1, harness.TypedContentionRow{ID: fmt.Sprintf("%032x", i+1), Name: val}); err != nil {
			t.Fatalf("churn insert %d on victim: %v", i, err)
		}
		if (i+1)%500 == 0 {
			if _, err := rxCount(cluster, 1); err != nil {
				t.Fatalf("victim unresponsive at churn row %d: %v", i+1, err)
			}
			t.Logf("churn %d/%d rows; sst victim=%d control=%d",
				i+1, rows, victimSST(), controlSST())
		}
	}

	// Stall proof: the victim must pile up observably more tables than
	// the compacting control (flushes still run; nothing folds L0 away).
	deadline := time.Now().Add(2 * time.Minute)
	for {
		v, c := victimSST(), controlSST()
		if v >= 3 && v > c {
			t.Logf("stall proven: victim sst=%d control sst=%d", v, c)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no compaction debt: victim sst=%d control sst=%d", v, c)
		}
		time.Sleep(2 * time.Second)
	}

	// Correct under stall: exact convergence with the control node.
	waitCounts(t, cluster, []int{0, 1}, rows, 3*time.Minute)
	d := waitDigests(t, cluster, []int{0, 1}, 3*time.Minute)
	t.Logf("converged under stall: %d rows, digest %s", rows, d)

	// Re-enable compactions (drop the storage override from the victim
	// config) and restart on the same data dir: debt must drain. The
	// pre-restart count is the baseline: sampling after the restart would
	// race the drain itself.
	before := victimSST()
	enableCompactions(t, cluster.Nodes[1].ConfigFile)
	cluster.StopNode(1)
	cluster.StartNode(1)
	cluster.UnlockNode(1, cluster.Nodes[1].KeyHex)
	cluster.WaitNodeReady(1)

	deadline = time.Now().Add(3 * time.Minute)
	for {
		after := victimSST()
		if after < before {
			t.Logf("debt draining: victim sst %d -> %d", before, after)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("debt did not drain after re-enable: sst stuck at %d (was %d)", after, before)
		}
		time.Sleep(2 * time.Second)
	}

	waitCounts(t, cluster, []int{0, 1}, rows, 3*time.Minute)
	d2 := waitDigests(t, cluster, []int{0, 1}, 3*time.Minute)
	if d2 != d {
		t.Fatalf("digest moved across compaction drain: %s -> %s", d, d2)
	}
	t.Logf("PASS: stall survived, debt drained, digest stable %s", d2)
}

// enableCompactions removes the storage override section from a node config
// file so the next start uses production compaction behavior.
func enableCompactions(t *testing.T, configFile string) {
	t.Helper()
	raw, err := os.ReadFile(configFile)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("parse config: %v", err)
	}
	delete(cfg, "spool")
	raw, err = json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("encode config: %v", err)
	}
	if err := os.WriteFile(configFile, raw, 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
}
