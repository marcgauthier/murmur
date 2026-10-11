package cmd

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/origin"
	"github.com/marcgauthier/murmur/tool/format"
)

type BenchmarkSpeedCommand struct{}

func (c *BenchmarkSpeedCommand) Name() string { return "benchmark-speed" }
func (c *BenchmarkSpeedCommand) Description() string {
	return "Comprehensive performance and storage benchmark testing 10 tables, batching, and disk space"
}
func (c *BenchmarkSpeedCommand) Usage() string {
	return "murmur benchmark-speed [--tables=10] [--rows-per-table=100000] [--durability=sync_each|async] [--sample-single=1000] [--dir=<path>] [--keep-db] [--quick] [--json] [--markdown]"
}

func init() {
	Register(&BenchmarkSpeedCommand{})
}

type BenchmarkSpeedOptions struct {
	Tables         int           `json:"tables"`
	RowsPerTable   int           `json:"rows_per_table"`
	SampleSingle   int           `json:"sample_single"`
	BatchSizes     []int         `json:"batch_sizes"`
	Durability       string        `json:"durability"`
	SyncInterval     time.Duration `json:"sync_interval"`
	MaxUnsyncedBytes int64         `json:"max_unsynced_bytes"`
	DataDir          string        `json:"data_dir"`
	KeepDB           bool          `json:"keep_db"`
	Quick            bool          `json:"quick"`
	JSON             bool          `json:"json"`
	Markdown         bool          `json:"markdown"`
}

type OpMetric struct {
	Operation     string        `json:"operation"`
	BatchSize     int           `json:"batch_size"`
	TotalOps      int           `json:"total_ops"`
	TotalRows     int           `json:"total_rows"`
	Elapsed       time.Duration `json:"elapsed"`
	ElapsedHuman  string        `json:"elapsed_human"`
	OpsPerSec     float64       `json:"ops_per_sec"`
	RowsPerSec    float64       `json:"rows_per_sec"`
	P50Latency    time.Duration `json:"p50_latency"`
	P95Latency    time.Duration `json:"p95_latency"`
	P99Latency    time.Duration `json:"p99_latency"`
	AvgLatency    time.Duration `json:"avg_latency"`
}

type PopulationMetric struct {
	TotalTables    int           `json:"total_tables"`
	RowsPerTable   int           `json:"rows_per_table"`
	TotalRows      int           `json:"total_rows"`
	BatchSize      int           `json:"batch_size"`
	Elapsed        time.Duration `json:"elapsed"`
	ElapsedHuman   string        `json:"elapsed_human"`
	RowsPerSec     float64       `json:"rows_per_sec"`
}

type QueryMetrics struct {
	PointLookup     OpMetric `json:"point_lookup"`
	FilteredLookup  OpMetric `json:"filtered_lookup"`
	ScanLimit10     OpMetric `json:"scan_limit_10"`
	ScanLimit100    OpMetric `json:"scan_limit_100"`
	ScanLimit1000   OpMetric `json:"scan_limit_1000"`
	CountAllTables  OpMetric `json:"count_all_tables"`
}

type DiskFootprint struct {
	TotalBytes      int64   `json:"total_bytes"`
	TotalBytesHuman string  `json:"total_bytes_human"`
	FileCount       int     `json:"file_count"`
	DataBytes       int64   `json:"data_bytes"`
	SpoolBytes      int64   `json:"spool_bytes"`
	KeysBytes       int64   `json:"keys_bytes"`
	BytesPerRow     float64 `json:"bytes_per_row"`
}

type BenchmarkSpeedResult struct {
	Options       BenchmarkSpeedOptions `json:"options"`
	DiskInitial   DiskFootprint         `json:"disk_initial"`
	DiskPopulated DiskFootprint         `json:"disk_populated"`
	DiskFinal     DiskFootprint         `json:"disk_final"`
	InsertSingle  OpMetric              `json:"insert_single"`
	InsertBatches []OpMetric            `json:"insert_batches"`
	Population    PopulationMetric      `json:"population"`
	Queries       QueryMetrics          `json:"queries"`
	UpdateSingle  OpMetric              `json:"update_single"`
	UpdateBatches []OpMetric            `json:"update_batches"`
	DeleteSingle  OpMetric              `json:"delete_single"`
	DeleteBatches []OpMetric            `json:"delete_batches"`
}

type durationRecorder struct {
	samples []time.Duration
}

func (r *durationRecorder) record(d time.Duration) {
	r.samples = append(r.samples, d)
}

func (r *durationRecorder) summarize(op string, batchSize int, totalRows int, elapsed time.Duration) OpMetric {
	m := OpMetric{
		Operation:    op,
		BatchSize:    batchSize,
		TotalOps:     len(r.samples),
		TotalRows:    totalRows,
		Elapsed:      elapsed,
		ElapsedHuman: elapsed.Round(time.Millisecond).String(),
	}
	if elapsed > 0 {
		m.OpsPerSec = float64(len(r.samples)) / elapsed.Seconds()
		m.RowsPerSec = float64(totalRows) / elapsed.Seconds()
	}
	if len(r.samples) == 0 {
		return m
	}
	sorted := append([]time.Duration(nil), r.samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	m.AvgLatency = total / time.Duration(len(sorted))
	m.P50Latency = sorted[int(0.50*float64(len(sorted)-1))]
	m.P95Latency = sorted[int(0.95*float64(len(sorted)-1))]
	m.P99Latency = sorted[int(0.99*float64(len(sorted)-1))]
	return m
}

func (c *BenchmarkSpeedCommand) Run(ctx context.Context, globalOpts GlobalOptions, args []string, stdout, stderr io.Writer) error {
	opts := BenchmarkSpeedOptions{
		Tables:       10,
		RowsPerTable: 100000,
		SampleSingle: 1000,
		BatchSizes:   []int{10, 100, 1000},
		Durability:   "sync_each",
		SyncInterval: time.Second,
		JSON:         globalOpts.JSON,
		Markdown:     globalOpts.Markdown,
	}

	for _, arg := range args {
		switch {
		case strings.HasPrefix(arg, "--tables="):
			if n, err := strconv.Atoi(strings.TrimPrefix(arg, "--tables=")); err == nil && n > 0 {
				opts.Tables = n
			}
		case strings.HasPrefix(arg, "--rows-per-table="):
			if n, err := strconv.Atoi(strings.TrimPrefix(arg, "--rows-per-table=")); err == nil && n > 0 {
				opts.RowsPerTable = n
			}
		case strings.HasPrefix(arg, "--sample-single="):
			if n, err := strconv.Atoi(strings.TrimPrefix(arg, "--sample-single=")); err == nil && n > 0 {
				opts.SampleSingle = n
			}
		case strings.HasPrefix(arg, "--durability="):
			opts.Durability = strings.TrimPrefix(arg, "--durability=")
		case strings.HasPrefix(arg, "--sync-interval="):
			if d, err := time.ParseDuration(strings.TrimPrefix(arg, "--sync-interval=")); err == nil {
				opts.SyncInterval = d
			}
		case strings.HasPrefix(arg, "--max-unsynced-bytes="):
			raw := strings.TrimPrefix(arg, "--max-unsynced-bytes=")
			opts.MaxUnsyncedBytes = parseByteSize(raw)
		case strings.HasPrefix(arg, "--dir="):
			opts.DataDir = strings.TrimPrefix(arg, "--dir=")
		case arg == "--keep-db":
			opts.KeepDB = true
		case arg == "--quick":
			opts.Quick = true
			opts.RowsPerTable = 1000
			opts.SampleSingle = 100
		case arg == "--json":
			opts.JSON = true
		case arg == "--markdown":
			opts.Markdown = true
		}
	}

	res, err := RunBenchmarkSpeed(ctx, opts, stdout, stderr)
	if err != nil {
		return err
	}

	if opts.JSON {
		return format.RenderJSON(stdout, res, true)
	}

	renderBenchmarkSpeedReport(stdout, res, opts.Markdown)
	return nil
}

func RunBenchmarkSpeed(ctx context.Context, opts BenchmarkSpeedOptions, stdout, stderr io.Writer) (*BenchmarkSpeedResult, error) {
	if opts.Tables < 1 {
		opts.Tables = 1
	}
	allRunners := getSpeedTableRunners()
	if opts.Tables > len(allRunners) {
		opts.Tables = len(allRunners)
	}
	runners := allRunners[:opts.Tables]

	if opts.SampleSingle > opts.RowsPerTable {
		opts.SampleSingle = opts.RowsPerTable
	}
	if len(opts.BatchSizes) == 0 {
		opts.BatchSizes = []int{10, 100, 1000}
	}

	tempDir := opts.DataDir
	createdTemp := false
	if tempDir == "" {
		d, err := os.MkdirTemp("", "murmur-speed-bench-*")
		if err != nil {
			return nil, fmt.Errorf("create temp benchmark dir: %w", err)
		}
		tempDir = d
		createdTemp = true
	}
	if !opts.KeepDB && createdTemp {
		defer os.RemoveAll(tempDir)
	}

	dbPath := filepath.Join(tempDir, "data")

	tableDefs := make([]murmur.TableDefinition, len(runners))
	for i, r := range runners {
		def, err := r.TableDefinition()
		if err != nil {
			return nil, fmt.Errorf("define table %s: %w", r.Name(), err)
		}
		tableDefs[i] = def
	}

	nodeID := murmur.NewNodeID()
	_, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate signing key: %w", err)
	}
	trustedKeys, err := origin.NewKeyRegistry(nil)
	if err != nil {
		return nil, fmt.Errorf("create origin key registry: %w", err)
	}
	if err := trustedKeys.Add(nodeID, signingKey.Public().(ed25519.PublicKey)); err != nil {
		return nil, fmt.Errorf("register origin key: %w", err)
	}

	durabilityCfg := murmur.DurabilityConfig{Mode: murmur.DurabilitySynchronous}
	if strings.ToLower(opts.Durability) == "async" {
		interval := opts.SyncInterval
		if interval <= 0 {
			interval = time.Second
		}
		durabilityCfg = murmur.DurabilityConfig{
			Mode:             murmur.DurabilityAsync,
			SyncInterval:     interval,
			MaxUnsyncedBytes: opts.MaxUnsyncedBytes,
		}
	}

	dbCfg := murmur.Config{
		Path:          dbPath,
		NodeID:        nodeID,
		Schema:        murmur.SchemaConfig{Version: 1},
		Tables:        tableDefs,
		Spool:         murmur.DefaultSpoolConfig(),
		Durability:    durabilityCfg,
		Encryption: murmur.EncryptionConfig{
			Key:   []byte("0123456789abcdef0123456789abcdef"),
			KeyID: "speed-bench-key",
		},
		OriginSigning: murmur.OriginSigningConfig{
			PrivateKey:  signingKey,
			TrustedKeys: trustedKeys,
		},
	}

	if !opts.JSON {
		fmt.Fprintf(stdout, "\n=== MURMUR BENCHMARK SPEED ===\n")
		fmt.Fprintf(stdout, "Target: %d tables x %s rows = %s total records\n",
			opts.Tables, formatNumber(opts.RowsPerTable), formatNumber(opts.Tables*opts.RowsPerTable))
		fmt.Fprintf(stdout, "Durability: %s (SyncInterval: %s)\n", opts.Durability, durabilityCfg.SyncInterval)
		fmt.Fprintf(stdout, "Path: %s\n\n", dbPath)
	}

	db, err := murmur.Open(ctx, dbCfg)
	if err != nil {
		return nil, fmt.Errorf("open benchmark database: %w", err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()

	res := &BenchmarkSpeedResult{
		Options: opts,
	}

	initDisk, _ := measureDiskFootprint(dbPath, 0)
	res.DiskInitial = initDisk

	// -------------------------------------------------------------
	// PHASE 1: Insert Benchmarks (Single vs Batches of 10, 100, 1000)
	// -------------------------------------------------------------
	if !opts.JSON {
		fmt.Fprintf(stdout, "[1/7] Running Insert Benchmarks (Single vs Batches)...\n")
	}

	// 1a. Single Inserts (unbatched)
	primaryRunner := runners[0]
	var singleRec durationRecorder
	singleStart := time.Now()
	for i := 0; i < opts.SampleSingle; i++ {
		row, _ := primaryRunner.SingleItem(1_000_000_000 + i)
		t0 := time.Now()
		if err := db.InsertItem(ctx, row); err != nil {
			return nil, fmt.Errorf("single insert error: %w", err)
		}
		singleRec.record(time.Since(t0))
	}
	res.InsertSingle = singleRec.summarize("Insert Single", 1, opts.SampleSingle, time.Since(singleStart))

	// 1b. Batch Inserts
	for _, batchSize := range opts.BatchSizes {
		targetBatchRows := opts.SampleSingle * 5
		if targetBatchRows < batchSize*5 {
			targetBatchRows = batchSize * 5
		}
		batchesCount := targetBatchRows / batchSize
		if batchesCount < 2 {
			batchesCount = 2
		}
		var bRec durationRecorder
		bStart := time.Now()
		seqBase := 2_000_000_000 + (batchSize * 100000)
		for b := 0; b < batchesCount; b++ {
			batch := primaryRunner.GenerateBatch(seqBase+(b*batchSize), batchSize)
			t0 := time.Now()
			if err := db.InsertMany(ctx, batch); err != nil {
				return nil, fmt.Errorf("batch insert (size %d): %w", batchSize, err)
			}
			bRec.record(time.Since(t0))
		}
		bMetric := bRec.summarize(fmt.Sprintf("Insert Batch %d", batchSize), batchSize, batchesCount*batchSize, time.Since(bStart))
		res.InsertBatches = append(res.InsertBatches, bMetric)
	}

	// -------------------------------------------------------------
	// PHASE 2: Database Population (10 tables x rowsPerTable)
	// -------------------------------------------------------------
	if !opts.JSON {
		fmt.Fprintf(stdout, "\n[2/7] Populating Database (%d tables x %s rows = %s total records)...\n",
			opts.Tables, formatNumber(opts.RowsPerTable), formatNumber(opts.Tables*opts.RowsPerTable))
	}

	popBatchSize := 1000
	if opts.RowsPerTable < popBatchSize {
		popBatchSize = opts.RowsPerTable
	}

	popStart := time.Now()
	totalPopulatedRows := 0

	sampledIDs := make(map[string][]ids.RowID)

	for tableIdx, runner := range runners {
		tableStart := time.Now()
		rowsRemaining := opts.RowsPerTable
		currentSeq := 1

		for rowsRemaining > 0 {
			batchSize := popBatchSize
			if rowsRemaining < batchSize {
				batchSize = rowsRemaining
			}
			batch := runner.GenerateBatch(currentSeq, batchSize)
			if err := db.InsertMany(ctx, batch); err != nil {
				return nil, fmt.Errorf("populating table %s at seq %d: %w", runner.Name(), currentSeq, err)
			}

			if len(sampledIDs[runner.Name()]) < 2000 {
				extracted := runner.ExtractIDs(batch)
				sampledIDs[runner.Name()] = append(sampledIDs[runner.Name()], extracted...)
			}

			currentSeq += batchSize
			rowsRemaining -= batchSize
			totalPopulatedRows += batchSize
		}

		tableElapsed := time.Since(tableStart)
		if !opts.JSON {
			fmt.Fprintf(stdout, "  -> [%d/%d] %-18s : %s rows inserted in %-8s (%.0f rows/sec)\n",
				tableIdx+1, opts.Tables, runner.Name(),
				formatNumber(opts.RowsPerTable),
				tableElapsed.Round(time.Millisecond),
				float64(opts.RowsPerTable)/tableElapsed.Seconds())
		}
	}

	popElapsed := time.Since(popStart)
	res.Population = PopulationMetric{
		TotalTables:  opts.Tables,
		RowsPerTable: opts.RowsPerTable,
		TotalRows:    totalPopulatedRows,
		BatchSize:    popBatchSize,
		Elapsed:      popElapsed,
		ElapsedHuman: popElapsed.Round(time.Millisecond).String(),
		RowsPerSec:   float64(totalPopulatedRows) / popElapsed.Seconds(),
	}

	// -------------------------------------------------------------
	// PHASE 3: Disk Space Profiling (Baseline)
	// -------------------------------------------------------------
	if !opts.JSON {
		fmt.Fprintf(stdout, "\n[3/7] Measuring Disk Space Footprint on Disk...\n")
	}

	// Flushes and disk footprint
	popDisk, err := measureDiskFootprint(dbPath, int64(totalPopulatedRows))
	if err != nil {
		return nil, fmt.Errorf("measure populated disk footprint: %w", err)
	}
	res.DiskPopulated = popDisk

	if !opts.JSON {
		fmt.Fprintf(stdout, "  Total Disk Size : %s (%s bytes across %d files)\n",
			popDisk.TotalBytesHuman, formatNumber(int(popDisk.TotalBytes)), popDisk.FileCount)
		fmt.Fprintf(stdout, "  Avg Per Record  : %.2f bytes / record\n", popDisk.BytesPerRow)
	}

	// -------------------------------------------------------------
	// PHASE 4: Query Benchmarks
	// -------------------------------------------------------------
	if !opts.JSON {
		fmt.Fprintf(stdout, "\n[4/7] Running Query Benchmarks (Point Lookups, Predicates, Scans, Counts)...\n")
	}

	// 4a. Point Lookups (GetItem by ID)
	var pointRec durationRecorder
	pointStart := time.Now()
	querySamples := opts.SampleSingle * 2
	if querySamples < 500 {
		querySamples = 500
	}
	for i := 0; i < querySamples; i++ {
		runner := runners[i%len(runners)]
		idsList := sampledIDs[runner.Name()]
		if len(idsList) == 0 {
			continue
		}
		targetID := idsList[i%len(idsList)]
		t0 := time.Now()
		if err := runner.PointQuery(ctx, db, targetID); err != nil {
			return nil, fmt.Errorf("point query error: %w", err)
		}
		pointRec.record(time.Since(t0))
	}
	res.Queries.PointLookup = pointRec.summarize("Point Lookup (GetItem)", 1, querySamples, time.Since(pointStart))

	// 4b. Filtered Queries (equality search)
	var filterRec durationRecorder
	filterStart := time.Now()
	filterSamples := querySamples / 2
	if filterSamples < 100 {
		filterSamples = 100
	}
	for i := 0; i < filterSamples; i++ {
		runner := runners[i%len(runners)]
		t0 := time.Now()
		_, err := runner.FilterQuery(ctx, db, i+1)
		if err != nil {
			return nil, fmt.Errorf("filtered query error: %w", err)
		}
		filterRec.record(time.Since(t0))
	}
	res.Queries.FilteredLookup = filterRec.summarize("Indexed Filter (Find)", 1, filterSamples, time.Since(filterStart))

	// 4c. Scans (Limit 10, Limit 100, Limit 1000)
	scanLimits := []int{10, 100, 1000}
	for _, lim := range scanLimits {
		var scanRec durationRecorder
		scanStart := time.Now()
		runs := 50
		if lim == 1000 {
			runs = 20
		}
		totalRowsRead := 0
		for i := 0; i < runs; i++ {
			runner := runners[i%len(runners)]
			t0 := time.Now()
			n, err := runner.ScanQuery(ctx, db, lim)
			if err != nil {
				return nil, fmt.Errorf("scan limit %d error: %w", lim, err)
			}
			totalRowsRead += n
			scanRec.record(time.Since(t0))
		}
		m := scanRec.summarize(fmt.Sprintf("Scan (Limit %d)", lim), lim, totalRowsRead, time.Since(scanStart))
		switch lim {
		case 10:
			res.Queries.ScanLimit10 = m
		case 100:
			res.Queries.ScanLimit100 = m
		case 1000:
			res.Queries.ScanLimit1000 = m
		}
	}

	// 4d. Table Counts (Count across all tables)
	var countRec durationRecorder
	countStart := time.Now()
	for _, runner := range runners {
		t0 := time.Now()
		cnt, err := runner.Count(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("count table %s: %w", runner.Name(), err)
		}
		if cnt < opts.RowsPerTable {
			return nil, fmt.Errorf("table %s has count %d < expected %d", runner.Name(), cnt, opts.RowsPerTable)
		}
		countRec.record(time.Since(t0))
	}
	res.Queries.CountAllTables = countRec.summarize("Table Count", 1, opts.Tables, time.Since(countStart))

	// -------------------------------------------------------------
	// PHASE 5: Update Benchmarks
	// -------------------------------------------------------------
	if !opts.JSON {
		fmt.Fprintf(stdout, "\n[5/7] Running Update Benchmarks (Single vs Batches)...\n")
	}

	// 5a. Single Updates
	var updateSingleRec durationRecorder
	updateSingleStart := time.Now()
	for i := 0; i < opts.SampleSingle; i++ {
		row, _ := primaryRunner.SingleItem(1_000_000_000 + i)
		primaryRunner.MutateSingle(row)
		t0 := time.Now()
		if err := db.UpdateItem(ctx, row); err != nil {
			return nil, fmt.Errorf("single update error: %w", err)
		}
		updateSingleRec.record(time.Since(t0))
	}
	res.UpdateSingle = updateSingleRec.summarize("Update Single", 1, opts.SampleSingle, time.Since(updateSingleStart))

	// 5b. Batch Updates
	for _, batchSize := range opts.BatchSizes {
		targetBatchRows := opts.SampleSingle * 5
		if targetBatchRows < batchSize*5 {
			targetBatchRows = batchSize * 5
		}
		batchesCount := targetBatchRows / batchSize
		if batchesCount < 2 {
			batchesCount = 2
		}
		var bRec durationRecorder
		bStart := time.Now()
		seqBase := 2_000_000_000 + (batchSize * 100000)
		for b := 0; b < batchesCount; b++ {
			batch := primaryRunner.GenerateBatch(seqBase+(b*batchSize), batchSize)
			primaryRunner.MutateBatch(batch)
			t0 := time.Now()
			if err := db.UpdateMany(ctx, batch); err != nil {
				return nil, fmt.Errorf("batch update (size %d): %w", batchSize, err)
			}
			bRec.record(time.Since(t0))
		}
		bMetric := bRec.summarize(fmt.Sprintf("Update Batch %d", batchSize), batchSize, batchesCount*batchSize, time.Since(bStart))
		res.UpdateBatches = append(res.UpdateBatches, bMetric)
	}

	// -------------------------------------------------------------
	// PHASE 6: Delete Benchmarks
	// -------------------------------------------------------------
	if !opts.JSON {
		fmt.Fprintf(stdout, "\n[6/7] Running Delete Benchmarks (Single vs Batches)...\n")
	}

	// 6a. Single Deletes
	var deleteSingleRec durationRecorder
	deleteSingleStart := time.Now()
	for i := 0; i < opts.SampleSingle; i++ {
		row, _ := primaryRunner.SingleItem(1_000_000_000 + i)
		t0 := time.Now()
		if err := db.DeleteItem(ctx, row); err != nil {
			return nil, fmt.Errorf("single delete error: %w", err)
		}
		deleteSingleRec.record(time.Since(t0))
	}
	res.DeleteSingle = deleteSingleRec.summarize("Delete Single", 1, opts.SampleSingle, time.Since(deleteSingleStart))

	// 6b. Batch Deletes
	for _, batchSize := range opts.BatchSizes {
		targetBatchRows := opts.SampleSingle * 5
		if targetBatchRows < batchSize*5 {
			targetBatchRows = batchSize * 5
		}
		batchesCount := targetBatchRows / batchSize
		if batchesCount < 2 {
			batchesCount = 2
		}
		var bRec durationRecorder
		bStart := time.Now()
		seqBase := 2_000_000_000 + (batchSize * 100000)
		for b := 0; b < batchesCount; b++ {
			batch := primaryRunner.GenerateBatch(seqBase+(b*batchSize), batchSize)
			t0 := time.Now()
			if err := db.DeleteMany(ctx, batch); err != nil {
				return nil, fmt.Errorf("batch delete (size %d): %w", batchSize, err)
			}
			bRec.record(time.Since(t0))
		}
		bMetric := bRec.summarize(fmt.Sprintf("Delete Batch %d", batchSize), batchSize, batchesCount*batchSize, time.Since(bStart))
		res.DeleteBatches = append(res.DeleteBatches, bMetric)
	}

	// -------------------------------------------------------------
	// PHASE 7: Final Disk Footprint Measurement
	// -------------------------------------------------------------
	if !opts.JSON {
		fmt.Fprintf(stdout, "\n[7/7] Measuring Final Storage Footprint...\n")
	}

	finalDisk, err := measureDiskFootprint(dbPath, int64(totalPopulatedRows))
	if err != nil {
		return nil, fmt.Errorf("measure final disk footprint: %w", err)
	}
	res.DiskFinal = finalDisk

	return res, nil
}

func measureDiskFootprint(dir string, rowCount int64) (DiskFootprint, error) {
	var fp DiskFootprint
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		size := info.Size()
		fp.TotalBytes += size
		fp.FileCount++

		rel, _ := filepath.Rel(dir, path)
		switch {
		case strings.HasPrefix(rel, "data"):
			fp.DataBytes += size
		case strings.HasPrefix(rel, "spool"):
			fp.SpoolBytes += size
		case strings.HasPrefix(rel, "keys"):
			fp.KeysBytes += size
		}
		return nil
	})
	fp.TotalBytesHuman = formatBytes(fp.TotalBytes)
	if rowCount > 0 {
		fp.BytesPerRow = float64(fp.TotalBytes) / float64(rowCount)
	}
	return fp, err
}

func renderBenchmarkSpeedReport(w io.Writer, res *BenchmarkSpeedResult, markdown bool) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "================================================================================")
	fmt.Fprintln(w, "                          MURMUR SPEED BENCHMARK REPORT                          ")
	fmt.Fprintln(w, "================================================================================")

	// Summary Configuration
	fmt.Fprintln(w, "\n--- DATABASE CONFIGURATION ---")
	format.RenderKV(w, [][2]string{
		{"Tables", fmt.Sprintf("%d", res.Options.Tables)},
		{"Records per Table", formatNumber(res.Options.RowsPerTable)},
		{"Total Records Created", formatNumber(res.Population.TotalRows)},
		{"Durability Mode", res.Options.Durability},
		{"Replication", "Disabled (local encrypted KV store)"},
		{"Total Population Time", res.Population.ElapsedHuman},
		{"Bulk Ingestion Speed", fmt.Sprintf("%.1f rows/sec", res.Population.RowsPerSec)},
	})

	// Storage & Disk Space
	fmt.Fprintln(w, "\n--- DISK FOOTPRINT & STORAGE PROFILE ---")
	format.RenderKV(w, [][2]string{
		{"Total Populated Disk", res.DiskPopulated.TotalBytesHuman},
		{"Total Files", fmt.Sprintf("%d files", res.DiskPopulated.FileCount)},
		{"Data Store (RIME/KV)", formatBytes(res.DiskPopulated.DataBytes)},
		{"Spool Log / WAL", formatBytes(res.DiskPopulated.SpoolBytes)},
		{"Key Registry", formatBytes(res.DiskPopulated.KeysBytes)},
		{"Average Storage per Record", fmt.Sprintf("%.2f bytes / record", res.DiskPopulated.BytesPerRow)},
		{"Final Disk (post-mutations)", res.DiskFinal.TotalBytesHuman},
	})

	// Operations Throughput and Speed Comparison
	fmt.Fprintln(w, "\n--- OPERATION SPEED MATRIX (SINGLE VS BATCH) ---")
	headers := []string{"Operation", "Batch Size", "Throughput", "Rows/sec", "Avg Latency", "p95 Latency", "Speedup"}
	var rows [][]string

	singleRowSpeed := res.InsertSingle.RowsPerSec
	addMetricRow := func(m OpMetric, baselineSpeed float64) {
		speedup := "-"
		if baselineSpeed > 0 && m.RowsPerSec > 0 {
			speedup = fmt.Sprintf("%.1fx", m.RowsPerSec/baselineSpeed)
		}
		rows = append(rows, []string{
			m.Operation,
			fmt.Sprintf("%d", m.BatchSize),
			fmt.Sprintf("%.1f ops/s", m.OpsPerSec),
			fmt.Sprintf("%.1f rows/s", m.RowsPerSec),
			formatLatency(m.AvgLatency),
			formatLatency(m.P95Latency),
			speedup,
		})
	}

	addMetricRow(res.InsertSingle, singleRowSpeed)
	for _, bm := range res.InsertBatches {
		addMetricRow(bm, singleRowSpeed)
	}

	updateBaseSpeed := res.UpdateSingle.RowsPerSec
	addMetricRow(res.UpdateSingle, updateBaseSpeed)
	for _, bm := range res.UpdateBatches {
		addMetricRow(bm, updateBaseSpeed)
	}

	deleteBaseSpeed := res.DeleteSingle.RowsPerSec
	addMetricRow(res.DeleteSingle, deleteBaseSpeed)
	for _, bm := range res.DeleteBatches {
		addMetricRow(bm, deleteBaseSpeed)
	}

	format.RenderTable(w, headers, rows, markdown)

	// Query Speeds
	fmt.Fprintln(w, "\n--- QUERY & RETRIEVAL SPEEDS ---")
	qHeaders := []string{"Query Pattern", "Target / Limit", "Throughput", "Rows/sec", "Avg Latency", "p95 Latency"}
	var qRows [][]string
	addQRow := func(m OpMetric, target string) {
		qRows = append(qRows, []string{
			m.Operation,
			target,
			fmt.Sprintf("%.1f ops/s", m.OpsPerSec),
			fmt.Sprintf("%.1f rows/s", m.RowsPerSec),
			formatLatency(m.AvgLatency),
			formatLatency(m.P95Latency),
		})
	}
	addQRow(res.Queries.PointLookup, "PK ID lookup")
	addQRow(res.Queries.FilteredLookup, "Indexed equality")
	addQRow(res.Queries.ScanLimit10, "Limit 10")
	addQRow(res.Queries.ScanLimit100, "Limit 100")
	addQRow(res.Queries.ScanLimit1000, "Limit 1,000")
	addQRow(res.Queries.CountAllTables, fmt.Sprintf("%d tables", res.Options.Tables))
	format.RenderTable(w, qHeaders, qRows, markdown)

	fmt.Fprintln(w)
}

func formatBytes(n int64) string {
	const (
		kb = 1024
		mb = kb * 1024
		gb = mb * 1024
	)
	switch {
	case n >= gb:
		return fmt.Sprintf("%.2f GB", float64(n)/float64(gb))
	case n >= mb:
		return fmt.Sprintf("%.2f MB", float64(n)/float64(mb))
	case n >= kb:
		return fmt.Sprintf("%.2f KB", float64(n)/float64(kb))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func formatLatency(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.2fµs", float64(d.Nanoseconds())/1000.0)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000.0)
	}
	return fmt.Sprintf("%.3fs", d.Seconds())
}

func formatNumber(n int) string {
	in := strconv.Itoa(n)
	var out []byte
	l := len(in)
	for i, c := range in {
		if i > 0 && (l-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	return string(out)
}

func parseByteSize(s string) int64 {
	s = strings.TrimSpace(strings.ToUpper(s))
	multiplier := int64(1)
	switch {
	case strings.HasSuffix(s, "GB") || strings.HasSuffix(s, "G"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimRight(s, "GB")
	case strings.HasSuffix(s, "MB") || strings.HasSuffix(s, "M"):
		multiplier = 1024 * 1024
		s = strings.TrimRight(s, "MB")
	case strings.HasSuffix(s, "KB") || strings.HasSuffix(s, "K"):
		multiplier = 1024
		s = strings.TrimRight(s, "KB")
	case strings.HasSuffix(s, "B"):
		s = strings.TrimRight(s, "B")
	}
	val, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0
	}
	return val * multiplier
}

