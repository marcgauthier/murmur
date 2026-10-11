package benchmark

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/marcgauthier/murmur/tool/cmd"
)

// TestBenchmarkSpeed runs the comprehensive speed and storage benchmark.
// Under -short it runs a quick pass (10 tables x 1,000 rows).
// When MURMUR_BENCH_SPEED_FULL=1 or outside short, it scales according to configuration.
func TestBenchmarkSpeed(t *testing.T) {
	ctx := context.Background()
	rowsPerTable := 1000
	sampleSingle := 100

	if !testing.Short() && os.Getenv("MURMUR_BENCH_SPEED_FULL") == "1" {
		rowsPerTable = 100000
		sampleSingle = 1000
	}

	opts := cmd.BenchmarkSpeedOptions{
		Tables:       10,
		RowsPerTable: rowsPerTable,
		SampleSingle: sampleSingle,
		BatchSizes:   []int{10, 100, 1000},
		Durability:   "sync_each",
		SyncInterval: time.Second,
		Markdown:     false,
	}

	var buf bytes.Buffer
	res, err := cmd.RunBenchmarkSpeed(ctx, opts, &buf, os.Stderr)
	if err != nil {
		t.Fatalf("RunBenchmarkSpeed failed: %v", err)
	}

	if res.Population.TotalRows != opts.Tables*opts.RowsPerTable {
		t.Fatalf("expected %d total populated rows, got %d",
			opts.Tables*opts.RowsPerTable, res.Population.TotalRows)
	}

	if res.DiskPopulated.TotalBytes <= 0 {
		t.Fatalf("expected non-zero disk footprint, got %d", res.DiskPopulated.TotalBytes)
	}

	t.Logf("Completed speed benchmark: %d tables, %d total rows, %s total disk (%.2f bytes/row)",
		opts.Tables, res.Population.TotalRows, res.DiskPopulated.TotalBytesHuman, res.DiskPopulated.BytesPerRow)
	t.Logf("Insert single: %.1f rows/s, Batch 1000: %.1f rows/s (speedup: %.1fx)",
		res.InsertSingle.RowsPerSec, res.InsertBatches[len(res.InsertBatches)-1].RowsPerSec,
		res.InsertBatches[len(res.InsertBatches)-1].RowsPerSec/res.InsertSingle.RowsPerSec)
	t.Logf("Point lookup QPS: %.1f ops/s (avg latency: %s)",
		res.Queries.PointLookup.OpsPerSec, res.Queries.PointLookup.AvgLatency)
}
