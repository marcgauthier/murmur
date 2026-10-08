package benchmark

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/marcgauthier/murmur/internal/testdb"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/ids"
)

type writerBenchRow struct {
	ID  ids.RowID `rime:"primary"`
	Val string
}

func mustWriterBenchTables() []murmur.TableDefinition {
	definition, err := murmur.Define[writerBenchRow]("writer_bench", 93, murmur.RecordOptions{
		PrimaryField: "ID",
		FieldIDs:     map[string]uint32{"ID": 1, "Val": 2},
	})
	if err != nil {
		panic(err)
	}
	return []murmur.TableDefinition{definition}
}

// TestLocalWriterThroughput measures direct Go API writes to one encrypted
// database. No daemon, HTTP service, peers, or QUIC connections are started.
func TestLocalWriterThroughput(t *testing.T) {
	runLocalWriterThroughput(t, murmur.DurabilityConfig{})
}

// TestLocalPeriodicSyncThroughput measures the opt-in one-second sync mode
// against the same single-row workload as TestLocalWriterThroughput.
func TestLocalPeriodicSyncThroughput(t *testing.T) {
	runLocalWriterThroughput(t, murmur.DurabilityConfig{
		Mode: murmur.DurabilityAsync, SyncInterval: time.Second,
	})
}

func runLocalWriterThroughput(t *testing.T, durability murmur.DurabilityConfig) {
	writeFor := 10 * time.Second
	if raw := os.Getenv("MURMUR_LOCAL_WRITE_BENCH_SECONDS"); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds < 1 {
			t.Fatal("MURMUR_LOCAL_WRITE_BENCH_SECONDS must be a positive integer")
		}
		writeFor = time.Duration(seconds) * time.Second
	}

	for _, writers := range []int{1, 4} {
		t.Run(fmt.Sprintf("%d_writers", writers), func(t *testing.T) {
			ctx := context.Background()
			cfg := localWriterConfig(t.TempDir(), durability)
			db, err := murmur.Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if db != nil {
					_ = db.Close()
				}
			}()
			table, err := murmur.TableOf[writerBenchRow](db, "writer_bench")
			if err != nil {
				t.Fatal(err)
			}

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
						var id murmur.RowID
						binary.BigEndian.PutUint64(id[:8], uint64(worker+1))
						binary.BigEndian.PutUint64(id[8:], uint64(writes+1))
						tx, err := db.BeginTx(ctx)
						if err != nil {
							results <- result{writes: writes, err: fmt.Errorf("writer %d: %w", worker+1, err)}
							return
						}
						if err := table.Insert(tx, &writerBenchRow{ID: id, Val: "writer-throughput"}); err != nil {
							results <- result{writes: writes, err: fmt.Errorf("writer %d: %w", worker+1, err)}
							return
						}
						if err := tx.Commit(); err != nil {
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
			periodicSyncs := db.Metrics().PeriodicSyncs
			if durability.SyncInterval > 0 && periodicSyncs == 0 {
				t.Fatal("no periodic disk sync occurred during writes")
			}

			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = nil
			db, err = murmur.Open(ctx, cfg)
			if err != nil {
				t.Fatalf("reopen after writes: %v", err)
			}
			table, err = murmur.TableOf[writerBenchRow](db, "writer_bench")
			if err != nil {
				t.Fatal(err)
			}
			count, err := table.Where().Count()
			if err != nil {
				t.Fatal(err)
			}
			if count != total {
				t.Fatalf("durable rows = %d; acknowledged inserts = %d", count, total)
			}
			t.Logf("writers=%d acknowledged_inserts=%d elapsed=%s writes_per_second=%.1f periodic_syncs=%d",
				writers, total, elapsed, float64(total)/elapsed.Seconds(), periodicSyncs)
		})
	}
}

func localWriterConfig(path string, durability murmur.DurabilityConfig) murmur.Config {
	return testdb.Configure(murmur.Config{
		Path:       path,
		NodeID:     murmur.NewNodeID(),
		DBID:       murmur.NewDBID(),
		Tables:     mustWriterBenchTables(),
		Spool:      murmur.DefaultSpoolConfig(),
		Durability: durability,
		Encryption: murmur.EncryptionConfig{
			Key: bytes.Clone(benchKey), KeyID: "bench",
		},
	})
}
