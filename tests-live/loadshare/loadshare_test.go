// Multi-process writer-share acceptance: two spedsql daemon processes, one
// saturated with local writers while its peer replicates, must converge
// fully (no remote starvation) with both scheduler classes acquiring and
// bounded service debt, scraped from Prometheus metrics.
package loadshare_test

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tests-live/harness"
)

func TestWriterSharesUnderLoad(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:         "loadshare",
		NumNodes:     2,
		TypedRecords: true,
	})

	const writers = 4
	const perWriter = 60
	const remote = 20
	lat := make([]time.Duration, 0, writers*perWriter)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				start := time.Now()
				err := cluster.TypedInsert(0, fmt.Sprintf("local-%d-%d", w, i))
				mu.Lock()
				lat = append(lat, time.Since(start))
				mu.Unlock()
				if err != nil {
					t.Errorf("local write: %v", err)
					return
				}
			}
		}(w)
	}
	for i := 0; i < remote; i++ {
		if err := cluster.TypedInsert(1, fmt.Sprintf("remote-%d", i)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	wg.Wait()

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if rowCount(t, cluster, 0) == writers*perWriter+remote {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := rowCount(t, cluster, 0); n != writers*perWriter+remote {
		t.Fatalf("converged to %d rows, remote apply starved", n)
	}

	local, remoteAcq := schedAcquisitions(t, cluster, 0)
	if local < writers*perWriter {
		t.Fatalf("local acquisitions = %d", local)
	}
	if remoteAcq == 0 {
		t.Fatal("remote class never acquired under local saturation")
	}
	if debt := schedDebt(t, cluster, 0); debt > float64(time.Second)/float64(time.Second) {
		t.Fatalf("service debt = %f s, exceeds 1s bound", debt)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	t.Logf("local-commit latency under load: p50=%v p95=%v max=%v (n=%d)",
		lat[len(lat)/2], lat[len(lat)*95/100], lat[len(lat)-1], len(lat))
	if lat[len(lat)-1] > 30*time.Second {
		t.Fatalf("max local-commit latency %v is absurd", lat[len(lat)-1])
	}
}

func rowCount(t *testing.T, cluster *harness.Cluster, idx int) int {
	t.Helper()
	names, err := cluster.TypedNames(idx)
	if err != nil {
		t.Fatalf("typed row count: %v", err)
	}
	return len(names)
}

func scrapeMetrics(t *testing.T, cluster *harness.Cluster, idx int) string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("https://%s/metrics", cluster.Nodes[idx].APIAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func schedAcquisitions(t *testing.T, cluster *harness.Cluster, idx int) (local, remote uint64) {
	t.Helper()
	samples, ok := harness.MetricSamples(scrapeMetrics(t, cluster, idx))
	if !ok {
		t.Fatal("decode metrics JSON")
	}
	for _, sample := range samples {
		if sample.Name != "spedsql_sched_acquisitions_total" {
			continue
		}
		v := uint64(sample.Value)
		switch sample.Labels["class"] {
		case "local":
			local = v
		case "remote":
			remote = v
		}
	}
	return local, remote
}

func schedDebt(t *testing.T, cluster *harness.Cluster, idx int) float64 {
	t.Helper()
	if v, ok := harness.MetricValueFrom(scrapeMetrics(t, cluster, idx), "spedsql_sched_debt_seconds"); ok {
		return v
	}
	t.Fatal("sched debt metric missing")
	return 0
}
