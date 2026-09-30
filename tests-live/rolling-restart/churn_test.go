package rollingrestart

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

// TestRollingRestartChurn stresses session re-establishment: nodes restart
// back-to-back with no quiesce between, maximizing handshake/attach churn
// while writes flow. Regression test for the heal stall (restart N+1 while
// restart N still healing wedged one link forever).
func TestRollingRestartChurn(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:        "rolling-churn",
		NumNodes:    3,
		AwaitUnlock: true,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{
			Name: "ch_rows",
			Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "name", Type: schema.ColText, Nullable: true},
			},
		}}},
	})

	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("%032x", i)
		if err := cluster.ExecSQL(0, "INSERT INTO ch_rows (id, name) VALUES (?, ?)", id, fmt.Sprintf("base-%d", i)); err != nil {
			t.Fatalf("baseline write: %v", err)
		}
	}
	waitCounts(t, cluster, "ch_rows", 5, 60*time.Second)

	stop := make(chan struct{})
	done := make(chan struct{})
	// Writers run on every node; a node's writer pauses while its daemon
	// is down, and quiesce pauses all three for the agreement proof.
	var paused [3]atomic.Bool
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		seq := 0
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				for n := 0; n < 3; n++ {
					if paused[n].Load() {
						continue
					}
					id := fmt.Sprintf("cc%02x%028x", n, seq)
					_ = cluster.ExecSQL(n, "INSERT INTO ch_rows (id, name) VALUES (?, ?)", id, "flow")
				}
				seq++
			}
		}
	}()
	defer func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
	}()

	// Six back-to-back restarts: node order 1,2,3,1,2,3 with only the
	// readiness wait between them (no catch-up sleep, no quiesce).
	for i := 0; i < 6; i++ {
		node := i % 3
		paused[node].Store(true)
		time.Sleep(200 * time.Millisecond)
		cluster.StopNode(node)
		cluster.StartNode(node)
		cluster.UnlockNode(node, cluster.Nodes[node].KeyHex)
		cluster.WaitNodeReady(node)
		paused[node].Store(false)
		t.Logf("churn: restart %d (node%d) complete", i+1, node+1)
	}

	quiesce(t, cluster, "ch_rows", &paused, 150*time.Second)
	// quiesce resumes writers on success; stop them before the final
	// digest proof (digests under active writers never agree).
	close(stop)
	<-done
	waitConvergedCounts(t, cluster, "ch_rows", 150*time.Second)
	want, err := cluster.ComputeTableDigest(0, "ch_rows", "id")
	if err != nil {
		t.Fatal(err)
	}
	for idx := 1; idx < 3; idx++ {
		d, err := cluster.ComputeTableDigest(idx, "ch_rows", "id")
		if err != nil {
			t.Fatal(err)
		}
		if d != want {
			dumpIDDiff(t, cluster, "ch_rows")
			t.Fatalf("node%d digest %s != node1 %s after churn", idx+1, d, want)
		}
	}
}
