// Commit-path budget enforcement and pressure convergence.
//
// Two daemons run with small commit budgets (4 KiB values, 64 KiB
// transactions). Oversize writes must be rejected cleanly with no partial
// effects and no writer poisoning; an HTTP request past the 1 MiB service
// body cap must fail without harming the node; and a concurrent blast
// must fully converge with bounded tail latency while the nodes stay
// responsive.
package overloadbudgets_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestBudgetRejectionAndPressureConvergence(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:            "overload-budgets",
		NumNodes:        2,
		AwaitUnlock:     true,
		TypedRecords:    true,
		TypedContention: true,
		Limits: &harness.LimitsOptions{
			MaxValueBytes:       4 << 10,
			MaxTransactionBytes: 64 << 10,
		},
	})

	// Past the 1 MiB HTTP body cap: clean rejection, node unharmed.
	huge := strings.Repeat("H", 2<<20)
	if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: fmt.Sprintf("%032x", 1), Name: huge}); err == nil {
		t.Fatal("2 MiB POST accepted, want HTTP body rejection")
	}
	cluster.WaitNodeReady(0)

	// Past the 4 KiB commit value budget: "too large", no partial row.
	big := strings.Repeat("V", (4<<10)+1)
	if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: fmt.Sprintf("%032x", 2), Name: big}); err == nil {
		t.Fatal("5 KiB value accepted, want budget rejection")
	} else if !strings.Contains(strings.ToLower(err.Error()), "too large") {
		t.Fatalf("rejection error %q does not name the budget", err)
	}
	for i := range cluster.Nodes {
		rows, _ := cluster.TypedContentionRows(i)
		if len(rows) != 0 {
			t.Fatalf("node %d has %d rows after rejections, want 0 (partial apply?)", i, len(rows))
		}
	}

	// Writer not poisoned: a small write works and converges.
	if err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: fmt.Sprintf("%032x", 3), Name: "ok"}); err != nil {
		t.Fatalf("post-rejection write: %v", err)
	}
	waitConverged(t, cluster, 1, 30*time.Second)

	// Pressure blast: concurrent writers converge with bounded tail.
	const writers = 8
	const perWriter = 50
	var mu sync.Mutex
	var lat []time.Duration
	var failed atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				id := fmt.Sprintf("%032x", 10000+w*1000+i)
				start := time.Now()
				err := cluster.TypedContentionInsert(0, harness.TypedContentionRow{ID: id, Name: fmt.Sprintf("w%d-%d", w, i)})
				el := time.Since(start)
				mu.Lock()
				lat = append(lat, el)
				mu.Unlock()
				if err != nil {
					failed.Add(1)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if failed.Load() != 0 {
		t.Fatalf("%d blast writes failed", failed.Load())
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p99 := lat[int(float64(len(lat))*0.99)]
	t.Logf("blast: %d inserts p50=%v p99=%v max=%v", len(lat), lat[len(lat)/2], p99, lat[len(lat)-1])
	if p99 > 10*time.Second {
		t.Fatalf("blast p99 %v exceeds 10s bound", p99)
	}
	waitConverged(t, cluster, 1+writers*perWriter, 60*time.Second)
	cluster.WaitNodeReady(0)
	cluster.WaitNodeReady(1)
}

func waitConverged(t *testing.T, c *harness.Cluster, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		var first string
		for i := range c.Nodes {
			rows, err := c.TypedContentionRows(i)
			if err != nil || len(rows) != want {
				ok = false
				break
			}
			sort.Slice(rows, func(a, b int) bool {
				if rows[a].Name != rows[b].Name {
					return rows[a].Name < rows[b].Name
				}
				return rows[a].ID < rows[b].ID
			})
			h := sha256.New()
			for _, row := range rows {
				fmt.Fprintf(h, "%s:%s:%s:%d\n", row.ID, row.Name, row.Phone, row.Score)
			}
			d := hex.EncodeToString(h.Sum(nil))
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
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("nodes did not converge on %d rows within %v", want, timeout)
}
