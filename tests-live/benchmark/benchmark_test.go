// Multi-process two-node replication benchmark: one spedsql daemon writes
// continuously while its peer converges; the report covers statement and
// mutation rates, convergence time, and logical protocol byte rates, and
// the test gates on identical row counts plus matching SHA-256
// application-data hashes.
package benchmark_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	db "github.com/nomadsql/replicateddb"
	"github.com/nomadsql/replicateddb/ids"
	"github.com/nomadsql/replicateddb/schema"
	"github.com/nomadsql/replicateddb/tests-live/harness"
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

func TestTwoNodeReplicationBenchmark(t *testing.T) {
	writeFor := envSeconds(t, "SPEDSQL_LIVE_BENCHMARK_WRITE_SECONDS", 10)
	syncTimeout := envSeconds(t, "SPEDSQL_LIVE_BENCHMARK_SYNC_TIMEOUT_SECONDS", 120)
	cluster := harness.NewCluster(t, harness.ClusterOptions{
		Name:     "benchmark",
		NumNodes: 2,
		Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{Name: "bench", Columns: []schema.ColumnSchema{
			{Name: "id", Type: schema.ColBlob},
			{Name: "val", Type: schema.ColText, Nullable: true},
		}}}},
	})

	// Seed hot rows that every update round touches.
	const hotRows = 100
	hot := make([]string, 0, hotRows)
	for i := 0; i < hotRows; i++ {
		id := ids.NewRowID()
		hexID := hex.EncodeToString(id[:])
		if err := cluster.ExecSQL(0, `INSERT INTO bench (id, val) VALUES (?, ?)`, hexID, fmt.Sprintf("hot-%d-v0", i)); err != nil {
			t.Fatal(err)
		}
		hot = append(hot, hexID)
	}

	sentBefore, recvBefore := replBatchBytes(t, cluster)
	var inserts, updates int
	version := 0
	deadline := time.Now().Add(writeFor)
	start := time.Now()
	for time.Now().Before(deadline) {
		version++
		// One insert plus one hot-row update per round; the daemon's
		// single-statement service surface issues one transaction each.
		id := ids.NewRowID()
		if err := cluster.ExecSQL(0, `INSERT INTO bench (id, val) VALUES (?, ?)`,
			hex.EncodeToString(id[:]), fmt.Sprintf("row-v%d", version)); err != nil {
			t.Fatalf("insert: %v", err)
		}
		inserts++
		if err := cluster.ExecSQL(0, `UPDATE bench SET val=? WHERE id=?`,
			fmt.Sprintf("hot-%d-v%d", version%hotRows, version), hot[version%hotRows]); err != nil {
			t.Fatalf("update: %v", err)
		}
		updates++
	}
	elapsed := time.Since(start)
	sentAfter, recvAfter := replBatchBytes(t, cluster)

	// Convergence: identical row counts and identical application hashes.
	syncStart := time.Now()
	syncDeadline := time.Now().Add(syncTimeout)
	for {
		aCount, aHash := stateOf(t, cluster, 0)
		bCount, bHash := stateOf(t, cluster, 1)
		if aCount == bCount && aHash == bHash {
			t.Logf("benchmark writes=%ds statements=%d (inserts=%d updates=%d) rate=%.1f/s convergence=%s rows=%d sha256=%x batch_bytes_sent=%d batch_bytes_recv=%d",
				int(writeFor.Seconds()), inserts+updates, inserts, updates,
				float64(inserts+updates)/elapsed.Seconds(), time.Since(syncStart), aCount, aHash,
				sentAfter-sentBefore, recvAfter-recvBefore)
			return
		}
		if time.Now().After(syncDeadline) {
			t.Fatalf("peers did not converge within %s (counts %d vs %d)", syncTimeout, aCount, bCount)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func stateOf(t *testing.T, cluster *harness.Cluster, idx int) (int, [32]byte) {
	t.Helper()
	res, err := cluster.QuerySQL(idx, `SELECT id, val FROM bench ORDER BY id`)
	if err != nil {
		t.Fatalf("state query: %v", err)
	}
	return len(res.Rows), sha256.Sum256([]byte(fmt.Sprintf("%v", res.Rows)))
}

func replBatchBytes(t *testing.T, cluster *harness.Cluster) (sent, received uint64) {
	t.Helper()
	for i := range cluster.Nodes {
		resp, err := http.Get(fmt.Sprintf("http://%s/metrics", cluster.Nodes[i].APIAddr))
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(body), "\n") {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				continue
			}
			// Counters render as float64 text; large values use
			// scientific notation, which ParseUint rejects.
			fv, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				continue
			}
			v := uint64(fv)
			switch {
			case strings.HasPrefix(line, "spedsql_repl_batch_bytes_sent_total"):
				sent += v
			case strings.HasPrefix(line, "spedsql_repl_batch_bytes_received_total"):
				received += v
			}
		}
	}
	return sent, received
}
