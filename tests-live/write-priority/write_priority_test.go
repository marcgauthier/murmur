// Local-write priority under replication load: two spedsql daemons take
// continuous writes on both nodes while serving reads, then the test
// gates on local progress, the measured remote (replication) share of
// contended writer service time, and SHA-256-identical convergence.
package writepriority_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/schema"
	"github.com/marcgauthier/spedsql/tests-live/harness"
)

func envSeconds(t *testing.T, key string, fallback int) time.Duration {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		return time.Duration(fallback) * time.Second
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 1 {
		t.Fatalf("%s must be a positive number of seconds", key)
	}
	return time.Duration(n) * time.Second
}

func TestLocalWritePriority(t *testing.T) {
	writeFor := envSeconds(t, "SPEDSQL_LIVE_WRITE_PRIORITY_SECONDS", 12)
	syncTimeout := envSeconds(t, "SPEDSQL_LIVE_WRITE_PRIORITY_SYNC_TIMEOUT_SECONDS", 120)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "write-priority",
		NumNodes: 2,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{Name: "priority_records", Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "val", Type: schema.ColText, Nullable: true},
		}}}},
	})

	dualBefore := dualService(t, cluster)
	deadline := time.Now().Add(writeFor)

	// Sixteen writers per node with disjoint key ranges; the daemon's
	// single-statement service surface issues one transaction each.
	// Deep queues maximize dual contention (both classes backlogged,
	// where the 90/10 policy binds) so the policy is actually
	// exercised, including under `-race` on a loaded box, where each
	// synchronous driver loop runs several times slower.
	var wg sync.WaitGroup
	errCh := make(chan error, 32)
	var written atomic.Int64
	for node := 0; node < 2; node++ {
		for worker := 0; worker < 16; worker++ {
			wg.Add(1)
			go func(node, worker int) {
				defer wg.Done()
				seq := 0
				for time.Now().Before(deadline) {
					// 16-byte row ID with node/worker/seq embedded;
					// disjoint across writers by construction.
					var raw [16]byte
					raw[0], raw[1] = byte(node), byte(worker)
					binary.BigEndian.PutUint64(raw[8:], uint64(seq))
					hexID := hex.EncodeToString(raw[:])
					if err := cluster.ExecSQL(node,
						`INSERT INTO priority_records (id, val) VALUES (?, ?)`,
						hexID, fmt.Sprintf("v-n%d-w%d-%d", node, worker, seq)); err != nil {
						errCh <- err
						return
					}
					seq++
					written.Add(1)
				}
			}(node, worker)
		}
	}

	// Reads stay served on both nodes while both replicate.
	var reads int
	for time.Now().Before(deadline) {
		for node := 0; node < 2; node++ {
			if _, err := cluster.QuerySQL(node, `SELECT count(*) FROM priority_records`); err != nil {
				t.Fatalf("read: %v", err)
			}
			reads++
		}
		time.Sleep(50 * time.Millisecond)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("writer: %v", err)
	}
	expected := int(written.Load())
	if expected < 80 || reads < 4 {
		t.Fatalf("insufficient local progress: %d rows, %d reads", expected, reads)
	}

	dualAfter := dualService(t, cluster)
	local := dualAfter.local - dualBefore.local
	remote := dualAfter.remote - dualBefore.remote
	// Vacuity gate: the policy assertion needs proven dual contention
	// (service granted while both classes waited). Near-zero dual mass
	// means the queues never overlapped and there is no policy decision
	// to measure — fail loudly instead of asserting on borrowing.
	t.Logf("dual-contention writer service: local=%.3fs replication=%.3fs share=%.3f",
		local, remote, remote/(local+remote))
	if local+remote < 1.0 {
		t.Fatalf("only %.3fs of dual-contention service, want >= 1s to measure the 90/10 policy", local+remote)
	}
	if share := remote / (local + remote); share > 0.25 {
		t.Fatalf("replication occupied %.1f%% of dual-contention writer service time", share*100)
	}

	// Convergence: identical row counts and identical hashes.
	syncDeadline := time.Now().Add(syncTimeout)
	for {
		aCount, aHash := stateOf(t, cluster, 0)
		bCount, bHash := stateOf(t, cluster, 1)
		if aCount == expected && bCount == expected && aHash == bHash {
			t.Logf("converged: %d rows, %d reads, identical SHA-256 %x", expected, reads, aHash)
			return
		}
		if time.Now().After(syncDeadline) {
			t.Fatalf("peers did not converge to %d rows (counts %d vs %d)", expected, aCount, bCount)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func stateOf(t *testing.T, cluster *harness.Cluster, idx int) (int, [32]byte) {
	t.Helper()
	res, err := cluster.QuerySQL(idx, `SELECT id, val FROM priority_records ORDER BY id`)
	if err != nil {
		t.Fatalf("state query: %v", err)
	}
	return len(res.Rows), sha256.Sum256([]byte(fmt.Sprintf("%v", res.Rows)))
}

// dualCounters sums dual-contention service seconds (granted while the
// other interactive class waited) by scheduler class across both
// daemons. Values render as float64 text (scientific notation at the
// extremes), so ParseFloat, not ParseUint.
type dualCounters struct {
	local  float64
	remote float64
}

func dualService(t *testing.T, cluster *harness.Cluster) (out dualCounters) {
	t.Helper()
	for i := range cluster.Nodes {
		// Default client: the harness hijacks it with the cluster CA
		// and a per-target client certificate.
		resp, err := http.Get(fmt.Sprintf("https://%s/metrics", cluster.Nodes[i].APIAddr))
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(body), "\n") {
			if !strings.HasPrefix(line, "spedsql_sched_dual_service_seconds_total{") {
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
			switch {
			case strings.Contains(line, `class="local"`):
				out.local += v
			case strings.Contains(line, `class="remote"`):
				out.remote += v
			}
		}
	}
	return out
}
