// Multi-process writer-share acceptance: two spedsql daemon processes, one
// saturated with local writers while its peer replicates, must converge
// fully (no remote starvation) with both scheduler classes acquiring and
// bounded service debt, scraped from Prometheus metrics.
package loadshare_test

import (
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	db "github.com/nomadsql/replicateddb"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/schema"
	"github.com/nomadsql/replicateddb/tests-live/harness"
)

func TestWriterSharesUnderLoad(t *testing.T) {
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "loadshare",
		NumNodes: 2,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{Name: "load", Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "w", Type: schema.ColText, Nullable: true},
		}}}},
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
				id := ids.NewRowID()
				start := time.Now()
				err := cluster.ExecSQL(0, `INSERT INTO load (id, w) VALUES (?, ?)`, hex.EncodeToString(id[:]), "local")
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
		id := ids.NewRowID()
		if err := cluster.ExecSQL(1, `INSERT INTO load (id, w) VALUES (?, ?)`, hex.EncodeToString(id[:]), "remote"); err != nil {
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
	res, err := cluster.QuerySQL(idx, `SELECT count(*) FROM load`)
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("row count: %+v %v", res, err)
	}
	count, _ := res.Rows[0][0].(float64)
	return int(count)
}

func scrapeMetrics(t *testing.T, cluster *harness.Cluster, idx int) string {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", cluster.Nodes[idx].APIAddr))
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
	for _, line := range strings.Split(scrapeMetrics(t, cluster, idx), "\n") {
		if !strings.HasPrefix(line, "spedsql_sched_acquisitions_total") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		v, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch {
		case strings.Contains(line, `class="local"`):
			local = v
		case strings.Contains(line, `class="remote"`):
			remote = v
		}
	}
	return local, remote
}

func schedDebt(t *testing.T, cluster *harness.Cluster, idx int) float64 {
	t.Helper()
	for _, line := range strings.Split(scrapeMetrics(t, cluster, idx), "\n") {
		if !strings.HasPrefix(line, "spedsql_sched_debt_seconds") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		v, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			continue
		}
		return v
	}
	t.Fatal("sched debt metric missing")
	return 0
}
