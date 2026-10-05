// This opt-in live benchmark uses real encrypted disk storage and fresh
// processes. It never creates a persistent SQLite materialization.
package reloadbenchmark_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"github.com/cockroachdb/pebble/v2/vfs"
	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/crypto"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/schema"
	"github.com/marcgauthier/murmur/sqlengine"
	"github.com/marcgauthier/murmur/state"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

const defaultTarget = int64(10_000_000_000)

type tableCheck struct {
	Rows         int64  `json:"rows"`
	SHA256       string `json:"sha256"`
	PayloadBytes int64  `json:"payload_bytes"`
}

type manifest struct {
	Version           int                   `json:"version"`
	Complete          bool                  `json:"complete"`
	CreatedAt         string                `json:"created_at"`
	TargetBytes       int64                 `json:"target_bytes"`
	Seed              uint64                `json:"seed"`
	ConfigHash        string                `json:"config_hash"`
	NodeID            db.NodeID             `json:"node_id"`
	DBID              db.DBID               `json:"db_id"`
	Generation        uint64                `json:"generation"`
	SQLiteBytes       int64                 `json:"sqlite_bytes"`
	PebbleBytes       int64                 `json:"pebble_file_bytes"`
	GenerationSeconds float64               `json:"generation_seconds"`
	SQLiteVersion     string                `json:"sqlite_version"`
	Tables            map[string]tableCheck `json:"tables"`
}

type request struct {
	RunDir   string   `json:"run_dir"`
	Manifest manifest `json:"manifest"`
}

type measurement struct {
	OpenProgress        []db.OpenProgress     `json:"open_progress,omitempty"`
	Path                string                `json:"path"`
	RegistryOpenSeconds float64               `json:"registry_open_seconds,omitempty"`
	StoreOpenSeconds    float64               `json:"store_open_seconds,omitempty"`
	EngineOpenSeconds   float64               `json:"engine_open_seconds,omitempty"`
	RebuildSeconds      float64               `json:"rebuild_seconds,omitempty"`
	FullOpenSeconds     float64               `json:"full_open_seconds,omitempty"`
	FirstQuerySeconds   float64               `json:"first_query_seconds"`
	ValidationSeconds   float64               `json:"validation_seconds"`
	RowsPerSecond       float64               `json:"rows_per_second"`
	PeakRSSBytes        int64                 `json:"peak_rss_bytes_before_validation,omitempty"`
	SQLiteBytes         int64                 `json:"sqlite_bytes"`
	PebbleBytes         int64                 `json:"pebble_file_bytes_after_close"`
	SQLiteVersion       string                `json:"sqlite_version"`
	Tables              map[string]tableCheck `json:"tables"`
}

type report struct {
	Manifest     manifest      `json:"dataset"`
	Measurements []measurement `json:"measurements"`
	GoVersion    string        `json:"go_version"`
	Platform     string        `json:"platform"`
	CachePolicy  string        `json:"cache_policy"`
}

func checked(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func readJSON(t *testing.T, name string, dst any) {
	t.Helper()
	b, err := os.ReadFile(name)
	checked(t, err)
	checked(t, json.Unmarshal(b, dst))
}

// Publish complete metadata only after its bytes and rename are durable.
func writeJSON(t *testing.T, name string, value any) {
	t.Helper()
	b, err := json.MarshalIndent(value, "", "  ")
	checked(t, err)
	f, err := os.CreateTemp(filepath.Dir(name), ".metadata-*")
	checked(t, err)
	defer f.Close()
	_, err = f.Write(append(b, '\n'))
	checked(t, err)
	checked(t, f.Sync())
	checked(t, f.Close())
	checked(t, os.Rename(f.Name(), name))
	d, err := os.Open(filepath.Dir(name))
	checked(t, err)
	defer d.Close()
	checked(t, d.Sync())
}

func configHash(t *testing.T) string {
	t.Helper()
	b, err := json.Marshal(struct {
		Tables         []schema.TableSchema
		DDL            []string
		Pebble         db.PebbleConfig
		DatasetVersion int
	}{tables(), localDDL(), db.DefaultPebbleConfig(), datasetVersion})
	checked(t, err)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func envInt(t *testing.T, name string, fallback int64) int64 {
	t.Helper()
	if value := harness.GetEnv(name); value != "" {
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil || n <= 0 {
			t.Fatalf("%s must be a positive integer, got %q", name, value)
		}
		return n
	}
	return fallback
}

func TestReloadBenchmark(t *testing.T) {
	if harness.GetEnv("MURMUR_RELOAD_BENCH") != "1" {
		t.Skip("explicit large benchmark: bash tests-live/run.sh reload-benchmark")
	}
	target := envInt(t, "MURMUR_RELOAD_TARGET_BYTES", defaultTarget)
	seed := uint64(envInt(t, "MURMUR_RELOAD_SEED", 42))
	repetitions := envInt(t, "MURMUR_RELOAD_REPETITIONS", 1)
	root := harness.GetEnv("MURMUR_RELOAD_ROOT")
	if root == "" {
		root = "/media/marc/2TB/TEST"
	}
	info, err := os.Stat(root)
	checked(t, err)
	if !info.IsDir() {
		t.Fatalf("destination is not a directory: %s", root)
	}
	root, err = filepath.Abs(root)
	checked(t, err)
	m := manifest{Version: datasetVersion, CreatedAt: time.Now().UTC().Format(time.RFC3339),
		TargetBytes: target, Seed: seed, ConfigHash: configHash(t), NodeID: db.NewNodeID(), DBID: db.NewDBID()}
	runDir := harness.GetEnv("MURMUR_RELOAD_REUSE")
	if runDir == "" {
		checked(t, os.MkdirAll(filepath.Join(root, "reload-benchmark"), 0700))
		runDir, err = os.MkdirTemp(filepath.Join(root, "reload-benchmark"), time.Now().UTC().Format("20060102T150405Z")+"-*")
		checked(t, err)
	} else {
		runDir, err = filepath.Abs(runDir)
		checked(t, err)
		readJSON(t, filepath.Join(runDir, "manifest.json"), &m)
		if !m.Complete || m.Version != datasetVersion || m.ConfigHash != configHash(t) || m.TargetBytes != target || m.Seed != seed || m.SQLiteBytes < target || len(m.Tables) != len(logTables) {
			t.Fatal("reuse manifest is incomplete or does not match target, seed, or configuration")
		}
	}
	checked(t, os.MkdirAll(filepath.Join(runDir, "measurements"), 0700))
	resultDir, err := os.MkdirTemp(filepath.Join(runDir, "measurements"), time.Now().UTC().Format("20060102T150405Z")+"-*")
	checked(t, err)
	requestFile := filepath.Join(resultDir, "request.json")
	writeJSON(t, requestFile, request{RunDir: runDir, Manifest: m})
	t.Logf("dataset: %s; target=%d bytes; seed=%d; results: %s", runDir, target, seed, resultDir)
	// Leave time for child cleanup before the outer go-test deadline.
	deadline := time.Now().Add(5*time.Hour + 55*time.Minute)
	if d, ok := t.Deadline(); ok {
		deadline = d.Add(-10 * time.Second)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if harness.GetEnv("MURMUR_RELOAD_REUSE") == "" {
		runWorker(t, ctx, requestFile, filepath.Join(resultDir, "populate"), "populate")
		readJSON(t, filepath.Join(runDir, "manifest.json"), &m)
	}
	summary := report{Manifest: m, GoVersion: runtime.Version(), Platform: runtime.GOOS + "/" + runtime.GOARCH,
		CachePolicy: "fresh process for each measurement; OS filesystem cache is uncontrolled; full-open precedes direct rebuild"}
	for i := int64(0); i < repetitions; i++ {
		for _, role := range []string{"full-open", "direct-rebuild"} {
			prefix := filepath.Join(resultDir, fmt.Sprintf("%s-%02d", role, i+1))
			runWorker(t, ctx, requestFile, prefix, role)
			var result measurement
			readJSON(t, prefix+".json", &result)
			summary.Measurements = append(summary.Measurements, result)
			writeJSON(t, filepath.Join(resultDir, "results.json"), summary)
			t.Logf("%s: full-open=%.3fs rebuild=%.3fs store-open=%.3fs first-query=%.6fs rows/sec=%.0f peak-RSS=%d bytes", role,
				result.FullOpenSeconds, result.RebuildSeconds, result.StoreOpenSeconds, result.FirstQuerySeconds, result.RowsPerSecond, result.PeakRSSBytes)
		}
	}
	t.Logf("PASS: validated all table counts and full content digests; results: %s", filepath.Join(resultDir, "results.json"))
}

func runWorker(t *testing.T, ctx context.Context, requestFile, prefix, role string) {
	t.Helper()
	executable, err := os.Executable()
	checked(t, err)
	log, err := os.OpenFile(prefix+".log", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	checked(t, err)
	defer log.Close()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestReloadWorker$", "-test.v", "-test.timeout=6h")
	cmd.Env = append(os.Environ(), "MURMUR_RELOAD_WORKER="+role, "MURMUR_RELOAD_REQUEST="+requestFile, "MURMUR_RELOAD_RESULT="+prefix+".json")
	cmd.Stdout = io.MultiWriter(os.Stdout, log)
	cmd.Stderr = cmd.Stdout
	configureChild(cmd)
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s failed: %v; diagnostics: %s", role, err, prefix+".log")
	}
}

func TestReloadWorker(t *testing.T) {
	role := harness.GetEnv("MURMUR_RELOAD_WORKER")
	if role == "" {
		t.Skip("subprocess entry point")
	}
	var req request
	readJSON(t, harness.GetEnv("MURMUR_RELOAD_REQUEST"), &req)
	if role == "populate" {
		populate(t, req)
		return
	}
	var m manifest
	readJSON(t, filepath.Join(req.RunDir, "manifest.json"), &m)
	if !m.Complete || m.ConfigHash != configHash(t) {
		t.Fatal("invalid completed dataset")
	}
	result := measurement{Path: role}
	var query queryFn
	var closeDB func() error
	if role == "full-open" {
		start := time.Now()
		cfg := config(m, filepath.Join(req.RunDir, "db"))
		if harness.GetEnv("MURMUR_RELOAD_PROGRESS") == "1" {
			cfg.OnOpenProgress = func(p db.OpenProgress) {
				if len(result.OpenProgress) == 0 || result.OpenProgress[len(result.OpenProgress)-1].Phase != p.Phase {
					t.Logf("open progress: phase=%s cells=%d elapsed=%s", p.Phase, p.ProcessedItems, p.Elapsed)
				}
				result.OpenProgress = append(result.OpenProgress, p)
			}
		}
		live, err := db.Open(context.Background(), cfg)
		checked(t, err)
		result.FullOpenSeconds = time.Since(start).Seconds()
		if cfg.OnOpenProgress != nil {
			last := result.OpenProgress[len(result.OpenProgress)-1]
			var previous uint64
			for _, p := range result.OpenProgress {
				if p.Phase == db.OpenCounting || p.TotalItemsKnown || p.EstimateKnown || p.TotalItems != 0 || p.CountedItems != 0 || p.PercentComplete != 0 || p.EstimatedRemaining != 0 || p.ProcessedItems < previous {
					_ = live.Close()
					t.Fatalf("unexpected work-done progress: %+v", p)
				}
				previous = p.ProcessedItems
			}
			var expectedCells, expectedRows uint64
			for _, table := range tables() {
				n := uint64(m.Tables[table.Name].Rows)
				expectedCells += uint64(len(table.Columns)) * n
				expectedRows += n
			}
			if last.Phase != db.OpenReady || last.TotalItemsKnown || last.EstimateKnown || last.TotalItems != 0 || last.PercentComplete != 0 || last.ProcessedItems != expectedCells || last.RowsInserted != expectedRows {
				_ = live.Close()
				t.Fatalf("unexpected open progress: %+v; want cells=%d", last, expectedCells)
			}
		}
		closeDB = live.Close
		defer closeDB()
		query = dbQuery(live)
		status := live.Status()
		if status.State != db.StateReady || status.StateGeneration != m.Generation || status.MaterializedGeneration != m.Generation {
			t.Fatal("reopened database generation/state mismatch")
		}
	} else if role == "direct-rebuild" {
		query, closeDB = directRebuild(t, req.RunDir, m, &result)
		defer closeDB()
	} else {
		t.Fatalf("unknown worker role: %s", role)
	}
	start := time.Now()
	if got := scalarInt(t, query, "SELECT count(*) FROM "+logTables[0].name); got != m.Tables[logTables[0].name].Rows {
		t.Fatal("first query count mismatch")
	}
	result.FirstQuerySeconds = time.Since(start).Seconds()
	result.PeakRSSBytes = peakRSS()
	t.Logf("%s query-ready: full-open=%.3fs rebuild=%.3fs peak-RSS=%d bytes", role, result.FullOpenSeconds, result.RebuildSeconds, result.PeakRSSBytes)
	start = time.Now()
	result.SQLiteBytes = sqliteBytes(t, query)
	result.SQLiteVersion = scalarString(t, query, "SELECT sqlite_version()")
	result.Tables = validate(t, query)
	for name, want := range m.Tables {
		if got := result.Tables[name]; got != want {
			t.Fatalf("%s content mismatch: got %+v, want %+v", name, got, want)
		}
	}
	result.ValidationSeconds = time.Since(start).Seconds()
	var rows int64
	for _, table := range m.Tables {
		rows += table.Rows
	}
	elapsed := result.FullOpenSeconds
	if role == "direct-rebuild" {
		elapsed = result.RebuildSeconds
	}
	result.RowsPerSecond = float64(rows) / elapsed
	checked(t, closeDB())
	result.PebbleBytes = directoryBytes(t, filepath.Join(req.RunDir, "db", "data"))
	writeJSON(t, harness.GetEnv("MURMUR_RELOAD_RESULT"), result)
}

type cursor interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close() error
}
type queryFn func(string, ...any) (cursor, error)

func dbQuery(live *db.DB) queryFn {
	return func(sql string, args ...any) (cursor, error) {
		return live.QueryContext(context.Background(), sql, args...)
	}
}

func scalarInt(t *testing.T, query queryFn, sql string, args ...any) int64 {
	t.Helper()
	r, err := query(sql, args...)
	checked(t, err)
	defer r.Close()
	if !r.Next() {
		checked(t, r.Err())
		t.Fatalf("no result: %s", sql)
	}
	var n int64
	checked(t, r.Scan(&n))
	return n
}

func scalarString(t *testing.T, query queryFn, sql string) string {
	t.Helper()
	r, err := query(sql)
	checked(t, err)
	defer r.Close()
	if !r.Next() {
		checked(t, r.Err())
		t.Fatalf("no result: %s", sql)
	}
	var s string
	checked(t, r.Scan(&s))
	return s
}

func sqliteBytes(t *testing.T, query queryFn) int64 {
	return scalarInt(t, query, "PRAGMA page_count") * scalarInt(t, query, "PRAGMA page_size")
}

func populate(t *testing.T, req request) {
	m := req.Manifest
	start := time.Now()
	live, err := db.Open(context.Background(), config(m, filepath.Join(req.RunDir, "db")))
	checked(t, err)
	defer live.Close()
	query := dbQuery(live)
	faker := gofakeit.New(m.Seed)
	tableSchemas := tables()
	sql := make([]string, len(tableSchemas))
	for i, table := range tableSchemas {
		sql[i] = insertSQL(table)
	}
	counts := make([]int64, len(tableSchemas))
	var totalRows, logicalBytes int64
	lastProgress := time.Now()
	for m.SQLiteBytes < m.TargetBytes || totalRows < int64(len(logTables)) {
		tx, err := live.BeginTx(context.Background(), nil)
		checked(t, err)
		var batchBytes int64
		for n := 0; n < 5000 && batchBytes < 8<<20; n++ {
			i := int(totalRows % int64(len(tableSchemas)))
			values := randomValues(faker, i, counts[i])
			_, err := tx.ExecContext(context.Background(), sql[i], values...)
			if err != nil {
				_ = tx.Rollback()
				t.Fatal(err)
			}
			// Generous cell/version/key overhead keeps the batch under the
			// default 64 MiB transaction budget, not just its payload size.
			payload := payloadBytes(values)
			batchBytes += payload + int64(len(values))*128
			logicalBytes += payload
			counts[i]++
			totalRows++
		}
		checked(t, tx.Commit())
		m.SQLiteBytes = sqliteBytes(t, query)
		if time.Since(lastProgress) >= 15*time.Second {
			t.Logf("populate: rows=%d SQLite=%d/%d bytes (%.1f%%) payload=%d elapsed=%s peak-RSS=%d", totalRows, m.SQLiteBytes, m.TargetBytes,
				100*float64(m.SQLiteBytes)/float64(m.TargetBytes), logicalBytes, time.Since(start).Round(time.Second), peakRSS())
			lastProgress = time.Now()
		}
	}
	m.GenerationSeconds = time.Since(start).Seconds()
	m.Generation = live.Status().StateGeneration
	m.SQLiteVersion = scalarString(t, query, "SELECT sqlite_version()")
	m.Tables = validate(t, query)
	var validatedBytes int64
	for i, table := range logTables {
		if m.Tables[table.name].Rows != counts[i] {
			t.Fatalf("population count mismatch: %s", table.name)
		}
		validatedBytes += m.Tables[table.name].PayloadBytes
	}
	if validatedBytes != logicalBytes {
		t.Fatalf("population payload mismatch: got %d, want %d", validatedBytes, logicalBytes)
	}
	checked(t, live.Close())
	m.PebbleBytes = directoryBytes(t, filepath.Join(req.RunDir, "db", "data"))
	m.Complete = true
	writeJSON(t, filepath.Join(req.RunDir, "manifest.json"), m)
	t.Logf("population complete: SQLite=%d bytes Pebble=%d bytes rows=%d generation=%.3fs", m.SQLiteBytes, m.PebbleBytes, totalRows, m.GenerationSeconds)
}

func validate(t *testing.T, query queryFn) map[string]tableCheck {
	r, err := query("PRAGMA database_list")
	checked(t, err)
	defer r.Close()
	mainFound := false
	for r.Next() {
		var seq int
		var name, file string
		checked(t, r.Scan(&seq, &name, &file))
		if file != "" {
			t.Fatalf("unexpected SQLite backing file: %s", file)
		}
		if name == "main" {
			mainFound = true
		}
	}
	checked(t, r.Err())
	checked(t, r.Close())
	if !mainFound {
		t.Fatal("SQLite main database missing")
	}
	out := make(map[string]tableCheck)
	for _, table := range tables() {
		count := scalarInt(t, query, "SELECT count(*) FROM "+table.Name)
		if count == 0 {
			t.Fatalf("empty table: %s", table.Name)
		}
		for _, suffix := range []string{"timestamp", "service_severity"} {
			if scalarInt(t, query, "SELECT count(*) FROM sqlite_master WHERE type='index' AND name=? AND tbl_name=?", "idx_"+table.Name+"_"+suffix, table.Name) != 1 {
				t.Fatalf("missing index: %s %s", table.Name, suffix)
			}
		}
		names := make([]string, len(table.Columns))
		for i, col := range table.Columns {
			names[i] = col.Name
		}
		r, err := query("SELECT " + strings.Join(names, ",") + " FROM " + table.Name + " ORDER BY id")
		checked(t, err)
		// Also release the engine read lock if validation calls Fatal.
		defer r.Close()
		values := make([]any, len(names))
		ptrs := make([]any, len(names))
		for i := range values {
			ptrs[i] = &values[i]
		}
		hash := sha256.New()
		var buf []byte
		check := tableCheck{}
		for r.Next() {
			checked(t, r.Scan(ptrs...))
			for _, value := range values {
				v, err := codec.FromAny(value)
				checked(t, err)
				buf = codec.EncodeCellState(buf[:0], codec.CellState{Value: v})
				_, err = hash.Write(buf)
				checked(t, err)
			}
			check.PayloadBytes += payloadBytes(values)
			check.Rows++
		}
		checked(t, r.Err())
		checked(t, r.Close())
		if count != check.Rows {
			t.Fatalf("streamed count mismatch: %s", table.Name)
		}
		check.SHA256 = hex.EncodeToString(hash.Sum(nil))
		out[table.Name] = check
		t.Logf("validated %s: rows=%d payload=%d SHA256=%s", table.Name, check.Rows, check.PayloadBytes, check.SHA256)
	}
	return out
}

type progressReader struct {
	*state.Store
	t     *testing.T
	names map[uint32]string
}

func (r progressReader) IterateTable(id uint32, fn func(*state.Row) error) error {
	start := time.Now()
	var rows int64
	r.t.Logf("rebuild table %s started", r.names[id])
	err := r.Store.IterateTable(id, func(row *state.Row) error { rows++; return fn(row) })
	r.t.Logf("rebuild table %s: rows=%d elapsed=%s", r.names[id], rows, time.Since(start).Round(time.Millisecond))
	return err
}

func directRebuild(t *testing.T, runDir string, m manifest, result *measurement) (queryFn, func() error) {
	start := time.Now()
	provider := &crypto.MapProvider{Keys: map[string][]byte{"reload-benchmark": fixtureKey}, CurrentID: "reload-benchmark", Algorithm: crypto.DefaultAlgorithm}
	reg, err := crypto.OpenRegistry(filepath.Join(runDir, "db", "keys"), provider, m.DBID)
	checked(t, err)
	t.Cleanup(reg.Close)
	efs, err := crypto.NewEncryptedFS(crypto.FSOptions{Base: vfs.Default, Registry: reg, DBID: m.DBID})
	checked(t, err)
	result.RegistryOpenSeconds = time.Since(start).Seconds()
	p := db.DefaultPebbleConfig()
	start = time.Now()
	store, err := state.Open(filepath.Join(runDir, "db", "data"), m.NodeID, m.DBID, state.Options{
		OriginSigning: testidentity.Config(m.NodeID),
		FS:            efs, CacheBytes: p.CacheBytes, MemTableSize: p.MemTableBytes, MemTableStopWritesThreshold: p.MemTableCount,
		MaxOpenFiles: p.MaxOpenFiles, CompactionConcurrency: p.MaxConcurrentCompactions,
		Compression: block.CompressionProfileByName("zstd"), Limits: codec.DefaultLimits(),
	})
	checked(t, err)
	var closeOnce sync.Once
	var closeErr error
	closeStore := func() error {
		closeOnce.Do(func() { closeErr = store.Close() })
		return closeErr
	}
	t.Cleanup(func() { _ = closeStore() })
	result.StoreOpenSeconds = time.Since(start).Seconds()
	storedManifest, err := store.LoadSchemaManifest()
	checked(t, err)
	if storedManifest == nil {
		t.Fatal("missing stored schema")
	}
	registry, err := storedManifest.Registry()
	checked(t, err)
	expected, err := schema.BuildRegistry(1, tables())
	checked(t, err)
	if registry.Hash != expected.Hash {
		t.Fatal("stored schema differs from benchmark schema")
	}
	gen, err := store.StateGeneration()
	checked(t, err)
	if gen != m.Generation {
		t.Fatal("stored generation differs from dataset manifest")
	}
	start = time.Now()
	engine, err := sqlengine.Open(registry, nil, localDDL(), 256)
	checked(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	result.EngineOpenSeconds = time.Since(start).Seconds()
	names := make(map[uint32]string)
	for _, table := range registry.Tables {
		names[table.ID] = table.Name
	}
	start = time.Now()
	checked(t, engine.Rebuild(progressReader{store, t, names}))
	result.RebuildSeconds = time.Since(start).Seconds()
	return func(sql string, args ...any) (cursor, error) { return engine.Query(context.Background(), sql, args...) }, func() error {
		if err := engine.Close(); err != nil {
			return err
		}
		return closeStore()
	}
}

func directoryBytes(t *testing.T, root string) int64 {
	t.Helper()
	var total int64
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err == nil {
			total += info.Size()
		}
		return err
	})
	checked(t, err)
	return total
}
