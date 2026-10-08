package benchmark

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	replicateddb "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/internal/testidentity"
)

// TestGroupCommitDurabilityMatrix compares acknowledged single-row write
// throughput across durability configurations on one encrypted database:
// synchronous (default) against asynchronous mode (ten-second interval
// plus ten-megabyte size trigger) with group settings present and absent.
// Group metrics are vestigial on the typed path (no grouper); the arms
// compare sync-vs-async durability.
//
// Every case reopens the store and checks the durable row count: async arms
// rely on the scheduled syncs plus the graceful-close final sync.
func TestGroupCommitDurabilityMatrix(t *testing.T) {
	writeFor := 10 * time.Second
	if raw := os.Getenv("MURMUR_GROUP_BENCH_SECONDS"); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds < 1 {
			t.Fatal("MURMUR_GROUP_BENCH_SECONDS must be a positive integer")
		}
		writeFor = time.Duration(seconds) * time.Second
	}

	arms := []struct {
		name       string
		durability replicateddb.DurabilityConfig
	}{
		{"sync-group", replicateddb.DurabilityConfig{}},
		{"async-10s-10mb-group", replicateddb.DurabilityConfig{
			Mode: replicateddb.DurabilityAsync, SyncInterval: 10 * time.Second, MaxUnsyncedBytes: 10 << 20,
			GroupCommit: replicateddb.GroupCommitConfig{MaxDelay: time.Millisecond},
		}},
		{"async-10s-10mb-nogroup", replicateddb.DurabilityConfig{
			Mode: replicateddb.DurabilityAsync, SyncInterval: 10 * time.Second, MaxUnsyncedBytes: 10 << 20,
			GroupCommit: replicateddb.GroupCommitConfig{MaxDelay: -1},
		}},
	}

	for _, arm := range arms {
		for _, writers := range []int{1, 4, 8} {
			t.Run(fmt.Sprintf("%s/%d_writers", arm.name, writers), func(t *testing.T) {
				runDurabilityMatrixCase(t, arm.durability, writers, writeFor)
			})
		}
	}
}

func runDurabilityMatrixCase(t *testing.T, durability replicateddb.DurabilityConfig, writers int, writeFor time.Duration) {
	ctx := context.Background()
	cfg := matrixBenchConfig(t.TempDir(), durability)
	db, err := replicateddb.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	table, err := replicateddb.TableOf[writerBenchRow](db, "writer_bench")
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
				var id replicateddb.RowID
				binary.BigEndian.PutUint64(id[:8], uint64(worker+1))
				binary.BigEndian.PutUint64(id[8:], uint64(writes+1))
				tx, err := db.BeginTx(ctx)
				if err != nil {
					results <- result{writes: writes, err: fmt.Errorf("writer %d: %w", worker+1, err)}
					return
				}
				if err := table.Insert(tx, &writerBenchRow{ID: id, Val: "durability-matrix"}); err != nil {
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
	m := db.Metrics()
	meanGroup := 0.0
	if m.GroupCommits > 0 {
		meanGroup = float64(m.GroupCommitMembers) / float64(m.GroupCommits)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db, err = replicateddb.Open(ctx, cfg)
	if err != nil {
		t.Fatalf("reopen after writes: %v", err)
	}
	table, err = replicateddb.TableOf[writerBenchRow](db, "writer_bench")
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
	t.Logf("writers=%d acknowledged_inserts=%d elapsed=%s writes_per_second=%.1f groups=%d mean_group=%.2f periodic_syncs=%d",
		writers, total, elapsed, float64(total)/elapsed.Seconds(), m.GroupCommits, meanGroup, m.PeriodicSyncs)
}

func matrixBenchConfig(path string, durability replicateddb.DurabilityConfig) replicateddb.Config {
	node := replicateddb.NewNodeID()
	return replicateddb.Config{
		Path:          path,
		NodeID:        node,
		OriginSigning: testidentity.Config(node),
		Tables:        mustWriterBenchTables(),
		Spool:         replicateddb.DefaultSpoolConfig(),
		Durability:    durability,
		Encryption: replicateddb.EncryptionConfig{
			Key: bytes.Clone(benchKey), KeyID: "bench",
		},
	}
}
