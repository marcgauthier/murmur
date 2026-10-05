package benchmark_test

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

// TestWriterThroughput measures application-visible, acknowledged SQL inserts
// against one encrypted daemon. Each insert is its own transaction.
func TestWriterThroughput(t *testing.T) {
	writeFor := envSeconds(t, "MURMUR_LIVE_WRITER_BENCH_SECONDS", 10)
	for _, writers := range []int{1, 4} {
		t.Run(fmt.Sprintf("%d_writers", writers), func(t *testing.T) {
			cluster := harness.NewCluster(t, harness.ClusterOptions{
				Name:     fmt.Sprintf("writer-throughput-%d", writers),
				NumNodes: 1,
				Schema: &db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{{Name: "writer_bench", Columns: []schema.ColumnSchema{
					{Name: "id", Type: schema.ColBlob},
					{Name: "val", Type: schema.ColText},
				}}}},
			})

			type result struct {
				writes int
				err    error
			}
			results := make(chan result, writers)
			startGate := make(chan struct{})
			var deadline time.Time
			var ready sync.WaitGroup
			ready.Add(writers)
			for worker := 0; worker < writers; worker++ {
				go func(worker int) {
					ready.Done()
					<-startGate
					var writes int
					for time.Now().Before(deadline) {
						var id [16]byte
						binary.BigEndian.PutUint64(id[:8], uint64(worker+1))
						binary.BigEndian.PutUint64(id[8:], uint64(writes+1))
						if err := cluster.ExecSQL(0, `INSERT INTO writer_bench (id, val) VALUES (?, ?)`,
							hex.EncodeToString(id[:]), "writer-throughput"); err != nil {
							results <- result{writes: writes, err: fmt.Errorf("writer %d: %w", worker+1, err)}
							return
						}
						writes++
					}
					results <- result{writes: writes}
				}(worker)
			}
			ready.Wait()
			start := time.Now()
			deadline = start.Add(writeFor)
			close(startGate)

			total := 0
			var writeErr error
			for range writers {
				res := <-results
				if res.err != nil && writeErr == nil {
					writeErr = res.err
				}
				if res.writes == 0 && writeErr == nil {
					writeErr = fmt.Errorf("a writer completed no inserts")
				}
				total += res.writes
			}
			elapsed := time.Since(start)
			if writeErr != nil {
				t.Fatal(writeErr)
			}

			rows, err := cluster.QuerySQL(0, `SELECT COUNT(*) FROM writer_bench`)
			if err != nil {
				t.Fatal(err)
			}
			if len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 {
				t.Fatalf("unexpected count result: %+v", rows.Rows)
			}
			count, ok := rows.Rows[0][0].(float64)
			if !ok || count != float64(total) {
				t.Fatalf("queryable rows = %v; acknowledged inserts = %d", rows.Rows[0][0], total)
			}
			t.Logf("writers=%d acknowledged_inserts=%d elapsed=%s writes_per_second=%.1f",
				writers, total, elapsed, float64(total)/elapsed.Seconds())
		})
	}
}
