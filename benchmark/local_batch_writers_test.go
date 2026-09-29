package benchmark

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	replicateddb "github.com/marcgauthier/spedsql"
)

// TestLocalTransactionBatchThroughput compares rows per SQL transaction on
// one encrypted database. Each SQL transaction becomes one Pebble batch.
func TestLocalTransactionBatchThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("local transaction-size benchmark runs outside -short")
	}
	writeFor := 2 * time.Second
	if raw := os.Getenv("SPEDSQL_LOCAL_BATCH_BENCH_SECONDS"); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds < 1 {
			t.Fatal("SPEDSQL_LOCAL_BATCH_BENCH_SECONDS must be a positive integer")
		}
		writeFor = time.Duration(seconds) * time.Second
	}

	modes := []struct {
		name       string
		durability replicateddb.DurabilityConfig
	}{
		{name: "sync_each"},
		{name: "sync_1s", durability: replicateddb.DurabilityConfig{
			Mode: replicateddb.DurabilityAsync, SyncInterval: time.Second,
		}},
	}
	for _, mode := range modes {
		for _, rowsPerTx := range []int{1, 10, 100, 1000} {
			for _, writers := range []int{1, 4} {
				name := fmt.Sprintf("%s/%d_rows_per_tx/%d_writers", mode.name, rowsPerTx, writers)
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					cfg := localWriterConfig(t.TempDir(), mode.durability)
					db, err := replicateddb.Open(ctx, cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if db != nil {
							_ = db.Close()
						}
					}()

					type result struct {
						worker int
						txns   int
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
							var txns, sequence int
							for time.Now().Before(deadline) {
								tx, err := db.BeginTx(ctx, nil)
								if err != nil {
									results <- result{worker: worker, txns: txns, err: err}
									return
								}
								for row := 0; row < rowsPerTx; row++ {
									sequence++
									var id [16]byte
									binary.BigEndian.PutUint64(id[:8], uint64(worker+1))
									binary.BigEndian.PutUint64(id[8:], uint64(sequence))
									if _, err := tx.ExecContext(ctx,
										`INSERT INTO writer_bench (id, val) VALUES (?, ?)`, id[:], "writer-throughput"); err != nil {
										_ = tx.Rollback()
										results <- result{worker: worker, txns: txns, err: err}
										return
									}
								}
								if err := tx.Commit(); err != nil {
									results <- result{worker: worker, txns: txns, err: err}
									return
								}
								txns++
							}
							results <- result{worker: worker, txns: txns}
						}(worker)
					}
					ready.Wait()
					start := time.Now()
					deadline = start.Add(writeFor)
					close(startGate)

					perWriter := make([]int, writers)
					var writeErr error
					for range writers {
						res := <-results
						perWriter[res.worker] = res.txns
						if res.err != nil && writeErr == nil {
							writeErr = fmt.Errorf("writer %d: %w", res.worker+1, res.err)
						}
					}
					elapsed := time.Since(start)
					if writeErr != nil {
						t.Fatal(writeErr)
					}
					totalTxns := 0
					for _, n := range perWriter {
						totalTxns += n
					}
					if totalTxns == 0 {
						t.Fatal("no transaction completed")
					}
					totalRows := totalTxns * rowsPerTx
					periodicSyncs := db.Metrics().PeriodicSyncs
					if mode.durability.SyncInterval > 0 && elapsed >= 2*time.Second && periodicSyncs == 0 {
						t.Fatal("no periodic disk sync occurred during writes")
					}

					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					db = nil
					db, err = replicateddb.Open(ctx, cfg)
					if err != nil {
						t.Fatalf("reopen after writes: %v", err)
					}
					var count int
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM writer_bench`).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != totalRows {
						t.Fatalf("durable rows = %d; acknowledged inserts = %d", count, totalRows)
					}
					t.Logf("mode=%s writers=%d rows_per_tx=%d acknowledged_transactions=%d acknowledged_rows=%d elapsed=%s transactions_per_second=%.1f rows_per_second=%.1f periodic_syncs=%d per_writer_transactions=%v",
						mode.name, writers, rowsPerTx, totalTxns, totalRows, elapsed,
						float64(totalTxns)/elapsed.Seconds(), float64(totalRows)/elapsed.Seconds(), periodicSyncs, perWriter)
				})
			}
		}
	}
}
