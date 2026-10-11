package resourceexhaustion_test

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

// randVal returns n bytes of incompressible hex: Spool compresses blocks,
// so repeated-character values would collapse and never rotate segments.
func randVal(rng *rand.Rand, n int) string {
	raw := make([]byte, n/2)
	_, _ = rng.Read(raw)
	return hex.EncodeToString(raw)
}

// A node whose reclamation stalls (tiny segments to pile sealed files
// fast, hourly reclaim ticks so nothing is collected) must stay
// responsive and exactly correct; restarting with reclamation
// re-enabled must delete fully-dead segments and converge. The sealed
// segment-count gap between victim and control is the non-vacuous
// proof that the stall actually happened.
//
//	MURMUR_RX_COMPACT_ROWS=2000   update churn over a fixed 200-key set
func TestStalledCompactionStaysCorrect(t *testing.T) {
	updates := harness.EnvInt("MURMUR_RX_COMPACT_ROWS", 2000)

	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "resource-compact",
		NumNodes:        2,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
		Replication: &harness.ReplicationOptions{
			// Aggressive log retention so acked batches expire
			// and sealed segments go fully dead: without it the
			// pinned replication log keeps every file alive and
			// nothing is ever collectible.
			MinLogRetentionMs:        1000,
			MaxOfflineLogRetentionMs: 20000,
			MinRetainedBatches:       10,
		},
		SpoolByNode: map[int]*harness.SpoolOptions{
			// Spool validates MaxBlockBytes against the 17 MiB
			// key+value ceiling and segments above blocks, so
			// 32 MiB segments are the practical floor; 64 KiB
			// values over 2000 updates write ~128 MiB, piling
			// about four sealed segments while the 256 MiB
			// control holds one.
			1: {
				MaxBlockBytes:          20 << 20,
				MaxSegmentSizeBytes:    32 << 20,
				ReclaimIntervalSeconds: 3600,
			},
		},
	})
	victimSegments := func() int { return countSegments(t, cluster.Nodes[1].Dir) }
	controlSegments := func() int { return countSegments(t, cluster.Nodes[0].Dir) }

	// Fixed 1-based key set: every update supersedes the previous
	// version, so sealed segments go fully dead and are collectible.
	// (The all-zero ID is the API's unset sentinel, not a storable key.)
	const keys = 200
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < keys; i++ {
		id := fmt.Sprintf("%032x", i+1)
		if err := cluster.TypedContentionInsert(1, harness.TypedContentionRow{ID: id, Name: randVal(rng, 64<<10)}); err != nil {
			t.Fatalf("seed %d on victim: %v", i, err)
		}
	}
	waitCounts(t, cluster, []int{0, 1}, keys, time.Minute)

	// Churn through the victim so its sealed-segment debt is its own
	// writes (plus what replicates back); probe responsiveness along
	// the way.
	for i := 0; i < updates; i++ {
		id := fmt.Sprintf("%032x", i%keys+1)
		if err := cluster.TypedContentionUpdate(1, id, "name", randVal(rng, 64<<10)); err != nil {
			t.Fatalf("churn update %d on victim: %v", i, err)
		}
		if (i+1)%500 == 0 {
			if _, err := rxCount(cluster, 1); err != nil {
				t.Fatalf("victim unresponsive at churn update %d: %v", i+1, err)
			}
			t.Logf("churn %d/%d updates; segments victim=%d control=%d",
				i+1, updates, victimSegments(), controlSegments())
		}
	}

	// Stall proof: the victim must pile up observably more sealed
	// segments than the control (rotation still runs; nothing is
	// collected away).
	deadline := time.Now().Add(2 * time.Minute)
	for {
		v, c := victimSegments(), controlSegments()
		if v >= 3 && v > c {
			t.Logf("stall proven: victim segments=%d control segments=%d", v, c)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no compaction debt: victim segments=%d control segments=%d", v, c)
		}
		time.Sleep(2 * time.Second)
	}

	// Correct under stall: exact convergence with the control node.
	waitCounts(t, cluster, []int{0, 1}, keys, 3*time.Minute)
	d := waitDigests(t, cluster, []int{0, 1}, 3*time.Minute)
	t.Logf("converged under stall: %d rows, digest %s", keys, d)

	// Re-enable reclamation (drop the storage override from the victim
	// config) and restart on the same data dir: fully-dead segments
	// must drain. The pre-restart count is the baseline: sampling
	// after the restart would race the drain itself.
	before := victimSegments()
	enableCompactions(t, cluster.Nodes[1].ConfigFile)
	cluster.StopNode(1)
	cluster.StartNode(1)
	cluster.UnlockNode(1, cluster.Nodes[1].KeyHex)
	cluster.WaitNodeReady(1)

	// Trigger log+receipt collection explicitly: per-commit receipts
	// pin old segments, and background receipt collection runs every
	// tenth GC round. Each pass expires more history; the 30 s spool
	// reclaim then deletes fully-dead files.
	deadline = time.Now().Add(4 * time.Minute)
	for {
		after := victimSegments()
		if after < before {
			t.Logf("debt draining: victim segments %d -> %d", before, after)
			break
		}
		if time.Now().After(deadline) {
			comp, runs, collected := spoolGCStats(t, cluster.Nodes[1].APIAddr)
			t.Fatalf("debt did not drain after re-enable: segments stuck at %d (was %d); spool_compactions=%v gc_runs=%v gc_collected=%v",
				after, before, comp, runs, collected)
		}
		if err := cluster.TriggerGC(1); err != nil {
			t.Logf("trigger GC: %v", err)
		}
		time.Sleep(10 * time.Second)
	}

	waitCounts(t, cluster, []int{0, 1}, keys, 3*time.Minute)
	d2 := waitDigests(t, cluster, []int{0, 1}, 3*time.Minute)
	if d2 != d {
		t.Fatalf("digest moved across compaction drain: %s -> %s", d, d2)
	}
	t.Logf("PASS: stall survived, debt drained, digest stable %s", d2)
}

// spoolGCStats scrapes spool compaction and log-GC counters (debug probe).
func spoolGCStats(t *testing.T, apiAddr string) (comp, runs, collected float64) {
	t.Helper()
	resp, err := http.Get("https://" + apiAddr + "/metrics")
	if err != nil {
		return -1, -1, -1
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return -1, -1, -1
	}
	samples, ok := harness.MetricSamples(string(body))
	if !ok {
		return -1, -1, -1
	}
	comp, runs, collected = -1, -1, -1
	for _, sample := range samples {
		switch sample.Name {
		case "spedsql_spool_compactions_total", "spool_compactions_total":
			comp = sample.Value
		case "spedsql_gc_runs_total":
			runs = sample.Value
		case "spedsql_gc_log_collected_total":
			collected = sample.Value
		}
	}
	return comp, runs, collected
}

// enableCompactions removes the storage override section from a node config
// file so the next start uses production storage behavior.
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
