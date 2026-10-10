// Performance characterization matrix: tiered, repeatable measurements
// across dataset sizes, transaction batches, and the supported storage cipher,
// with machine-stamped JSON reports for publication.
//
// Tiers (MURMUR_PERF_TIER=smoke|standard|full, default standard):
//
//	smoke:    10K rows, one cipher; minutes.
//	standard: 10K+100K rows, AES-256-GCM; tens of minutes.
//	full:     adds 1M rows (template build takes several minutes); ~1h.
//
// Live mesh / impairment / reconnect cells live in perf_live_test.go and
// run under the same tier selection. Every cell records wall time,
// throughput, p50/p95 where sampled, on-disk bytes, and peak RSS.
package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/q"
)

// perfTier selects the matrix size. Short test mode (-short) forces smoke
// so the standard `go test ./...` fast path never builds 100K templates.
func perfTier() string {
	if testing.Short() {
		return "smoke"
	}
	switch v := strings.ToLower(os.Getenv("MURMUR_PERF_TIER")); v {
	case "", "standard":
		return "standard"
	case "smoke", "full":
		return v
	default:
		return "standard"
	}
}

func perfRowSizes(tier string) []int {
	switch tier {
	case "smoke":
		return []int{10_000}
	case "full":
		return []int{10_000, 100_000, 1_000_000}
	default:
		return []int{10_000, 100_000}
	}
}

func perfCiphers(tier string) []struct {
	name string
	alg  murmur.EncryptionAlgorithm
} {
	return []struct {
		name string
		alg  murmur.EncryptionAlgorithm
	}{
		{"aes256gcm", murmur.AES256GCM},
	}
}

// perfCell is one published measurement. Unused dimensions stay at their
// zero values with omitempty so the JSON doubles as the results table.
type perfCell struct {
	Name      string  `json:"name"`
	Rows      int     `json:"rows,omitempty"`
	Nodes     int     `json:"nodes,omitempty"`
	Cipher    string  `json:"cipher,omitempty"`
	Variant   string  `json:"variant,omitempty"`
	Backlog   int     `json:"backlog,omitempty"`
	Batch     int     `json:"batch,omitempty"`
	Ops       int64   `json:"ops"`
	WallMs    int64   `json:"wall_ms"`
	PerSec    float64 `json:"per_sec,omitempty"`
	P50Ms     float64 `json:"p50_ms,omitempty"`
	P95Ms     float64 `json:"p95_ms,omitempty"`
	RowsSec   float64 `json:"rows_sec,omitempty"`
	DiskBytes int64   `json:"disk_bytes,omitempty"`
	PeakRSSMB int64   `json:"peak_rss_mb,omitempty"`
	Extra     string  `json:"extra,omitempty"`
	Skipped   string  `json:"skipped,omitempty"`
}

type perfMachine struct {
	OS      string `json:"os"`
	Arch    string `json:"arch"`
	CPU     string `json:"cpu"`
	MemGB   int    `json:"mem_gb"`
	Cores   int    `json:"cores"`
	Go      string `json:"go"`
	Tags    string `json:"tags"`
	Storage string `json:"storage"`
}

type perfReport struct {
	Suite   string      `json:"suite"`
	Tier    string      `json:"tier"`
	Started string      `json:"started_utc"`
	Machine perfMachine `json:"machine"`
	Cells   []perfCell  `json:"cells"`
}

func (r *perfReport) add(c perfCell) { r.Cells = append(r.Cells, c) }

func (r *perfReport) logMarkdown(t *testing.T) {
	t.Helper()
	t.Logf("perf-matrix tier=%s cells=%d", r.Tier, len(r.Cells))
	t.Logf("| cell | rows | nodes/batch | per_sec | rows_sec | p50_ms | p95_ms | disk_MB | rss_MB | extra |")
	t.Logf("|---|---|---|---|---|---|---|---|---|---|")
	for _, c := range r.Cells {
		dim := ""
		switch {
		case c.Nodes > 0:
			dim = fmt.Sprintf("%dn", c.Nodes)
		case c.Batch > 0:
			dim = fmt.Sprintf("b%d", c.Batch)
		case c.Backlog > 0:
			dim = fmt.Sprintf("bl%d", c.Backlog)
		}
		if c.Cipher != "" {
			dim += "/" + c.Cipher
		}
		if c.Variant != "" {
			dim += "/" + c.Variant
		}
		if c.Skipped != "" {
			t.Logf("| %s | %d | %s | - | - | - | - | - | - | SKIP: %s |",
				c.Name, c.Rows, dim, c.Skipped)
			continue
		}
		t.Logf("| %s | %d | %s | %.1f | %.1f | %.3f | %.3f | %.1f | %d | %s |",
			c.Name, c.Rows, dim, c.PerSec, c.RowsSec, c.P50Ms, c.P95Ms,
			float64(c.DiskBytes)/(1<<20), c.PeakRSSMB, c.Extra)
	}
}

func (r *perfReport) writeJSON(t *testing.T) string {
	t.Helper()
	path := fmt.Sprintf("perf-report-%d.json", time.Now().Unix())
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		t.Fatalf("marshal perf report: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write perf report: %v", err)
	}
	t.Logf("perf report written to %s", path)
	return path
}

func perfMachineStamp() perfMachine {
	return perfMachine{
		OS:      runtime.GOOS,
		Arch:    runtime.GOARCH,
		CPU:     hostCPUModel(),
		MemGB:   hostMemTotalGB(),
		Cores:   runtime.NumCPU(),
		Go:      runtime.Version(),
		Tags:    buildTags(),
		Storage: "spool+aes-256-gcm",
	}
}

func hostCPUModel() string {
	raw, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return "unknown"
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "model name") {
			if i := strings.Index(line, ":"); i >= 0 {
				return strings.TrimSpace(line[i+1:])
			}
		}
	}
	return "unknown"
}

func hostMemTotalGB() int {
	raw, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				if kb, err := strconv.Atoi(f[1]); err == nil {
					return kb / (1024 * 1024)
				}
			}
		}
	}
	return 0
}

// procPeakRSSMB returns the calling process's peak RSS in MiB (VmHWM on
// Linux; HeapAlloc fallback elsewhere). It is a footprint measure, not a
// limit: cells never constrain memory, they record what the workload took.
func procPeakRSSMB() int64 {
	if raw, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "VmHWM:") {
				f := strings.Fields(line)
				if len(f) >= 2 {
					if kb, err := strconv.ParseInt(f[1], 10, 64); err == nil {
						return kb / 1024
					}
				}
			}
		}
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc / (1 << 20))
}

// pidPeakRSSMB reads another process's peak RSS (live daemon footprint).
func pidPeakRSSMB(pid int) int64 {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return -1
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				if kb, err := strconv.ParseInt(f[1], 10, 64); err == nil {
					return kb / 1024
				}
			}
		}
	}
	return -1
}

func dirSizeBytes(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	return total
}

// buildTags reports the backend tags the suite ran with: the runner's
// MURMUR_TAGS override when set, else "none" (the typed backend needs no
// build tags).
func buildTags() string {
	if v := os.Getenv("MURMUR_TAGS"); v != "" {
		return v
	}
	return "none"
}

// perfLat collects samples and reports percentiles in milliseconds.
type perfLat struct {
	samples []float64
}

func (l *perfLat) record(d time.Duration) {
	l.samples = append(l.samples, float64(d.Microseconds())/1000.0)
}

func (l *perfLat) percentile(p float64) float64 {
	if len(l.samples) == 0 {
		return 0
	}
	s := append([]float64(nil), l.samples...)
	sort.Float64s(s)
	k := int(float64(len(s))*p/100.0 + 0.5)
	if k < 1 {
		k = 1
	}
	if k > len(s) {
		k = len(s)
	}
	return s[k-1]
}

func TestPerfMatrix(t *testing.T) {
	tier := perfTier()
	only := strings.ToLower(os.Getenv("MURMUR_PERF_ONLY"))
	rep := &perfReport{
		Suite:   "perf-matrix",
		Tier:    tier,
		Started: time.Now().UTC().Format(time.RFC3339),
		Machine: perfMachineStamp(),
	}
	t.Logf("perf-matrix tier=%s only=%q machine=%s/%s cores=%d mem=%dGB go=%s",
		tier, only, rep.Machine.OS, rep.Machine.Arch, rep.Machine.Cores,
		rep.Machine.MemGB, rep.Machine.Go)

	want := func(name string) bool {
		return only == "" || strings.Contains(only, strings.ToLower(name))
	}
	if want("store") || want("lookup") || want("range") || want("update") {
		for _, n := range perfRowSizes(tier) {
			perfStoreCells(t, rep, n)
		}
	}
	if want("cipher") {
		perfCipherCells(t, rep, tier)
	}
	if want("tx") {
		perfTxCells(t, rep, tier)
	}
	if want("mesh") || want("impair") || want("reconnect") {
		perfLiveCells(t, rep, tier, only)
	}

	rep.logMarkdown(t)
	rep.writeJSON(t)
}

// perfStoreCells measures open, reads, and writes against the shared
// n-row template store (built once per size, copied per cell).
func perfStoreCells(t *testing.T, rep *perfReport, n int) {
	t.Helper()
	ctx := context.Background()

	// Open: template copy + full production open (rebuild included).
	func() {
		tmpl := templateFor(t, n)
		dest := t.TempDir()
		start := time.Now()
		if err := copyDir(tmpl.dir, dest); err != nil {
			t.Fatal(err)
		}
		copyMs := time.Since(start)
		db, err := murmur.Open(ctx, benchConfig(dest, tmpl.node, tmpl.dbid))
		if err != nil {
			t.Fatal(err)
		}
		openWall := time.Since(start)
		// First query proves the materialization is usable.
		if _, err := db.Count(ctx, benchContact{}); err != nil {
			t.Fatal(err)
		}
		disk := dirSizeBytes(dest)
		rss := procPeakRSSMB()
		_ = db.Close()
		rep.add(perfCell{
			Name: "store_open", Rows: n, Ops: 1, WallMs: openWall.Milliseconds(),
			PerSec: 1 / openWall.Seconds(), DiskBytes: disk, PeakRSSMB: rss,
			Extra: fmt.Sprintf("copy_ms=%d rows_sec=%.0f", copyMs.Milliseconds(), float64(n)/openWall.Seconds()),
		})
		t.Logf("store_open rows=%d wall=%s disk=%dMB rss=%dMB", n, openWall.Round(time.Millisecond), disk/(1<<20), rss)
	}()

	// Point lookups.
	func() {
		db, ids := openTemplateDB(t, n)
		rng := rand.New(rand.NewSource(7))
		const ops = 10000
		var lat perfLat
		start := time.Now()
		for i := 0; i < ops; i++ {
			op := time.Now()
			var got benchContact
			got.ID = ids[rng.Intn(len(ids))]
			if err := db.GetItem(ctx, &got); err != nil {
				t.Fatal(err)
			}
			lat.record(time.Since(op))
		}
		wall := time.Since(start)
		rep.add(perfCell{
			Name: "pk_lookup", Rows: n, Ops: ops, WallMs: wall.Milliseconds(),
			PerSec: float64(ops) / wall.Seconds(),
			P50Ms:  lat.percentile(50), P95Ms: lat.percentile(95),
			PeakRSSMB: procPeakRSSMB(),
		})
		_ = db.Close()
	}()

	// Capped indexed ranges.
	func() {
		db, _ := openTemplateDB(t, n)
		const ops = 1000
		var lat perfLat
		start := time.Now()
		for i := 0; i < ops; i++ {
			lo := int64((i * 131) % 900)
			op := time.Now()
			var rows []benchContact
			if err := db.Query(benchContact{}, q.Between("Score", lo, lo+100)).Limit(100).FindInto(&rows); err != nil {
				t.Fatal(err)
			}
			for range rows {
			}
			lat.record(time.Since(op))
		}
		wall := time.Since(start)
		rep.add(perfCell{
			Name: "range_100", Rows: n, Ops: ops, WallMs: wall.Milliseconds(),
			PerSec: float64(ops) / wall.Seconds(),
			P50Ms:  lat.percentile(50), P95Ms: lat.percentile(95),
			PeakRSSMB: procPeakRSSMB(),
		})
		_ = db.Close()
	}()

	// Single-cell synchronous updates.
	func() {
		db, ids := openTemplateDB(t, n)
		rng := rand.New(rand.NewSource(11))
		const ops = 300
		var lat perfLat
		start := time.Now()
		for i := 0; i < ops; i++ {
			op := time.Now()
			score := int64(rng.Intn(1000))
			tx, err := db.BeginTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Update(&benchContact{ID: ids[rng.Intn(len(ids))]}, murmur.Set("Score", score)); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			lat.record(time.Since(op))
		}
		wall := time.Since(start)
		rep.add(perfCell{
			Name: "single_update", Rows: n, Ops: ops, WallMs: wall.Milliseconds(),
			PerSec: float64(ops) / wall.Seconds(), RowsSec: float64(ops) / wall.Seconds(),
			P50Ms: lat.percentile(50), P95Ms: lat.percentile(95),
			PeakRSSMB: procPeakRSSMB(),
		})
		_ = db.Close()
	}()
}

// perfTxCells measures transaction-size throughput (1/10/100/1000 rows)
// on a 100K store (10K in smoke): one atomic Spool commit per typed
// transaction, synchronous durability.
func perfTxCells(t *testing.T, rep *perfReport, tier string) {
	t.Helper()
	ctx := context.Background()
	n := 100_000
	if tier == "smoke" {
		n = 10_000
	}
	batches := []int{1, 10, 100, 1000}
	if tier == "smoke" {
		batches = []int{1, 100}
	}
	for _, bsize := range batches {
		db, _ := openTemplateDB(t, n)
		ops := 200
		switch {
		case bsize >= 1000:
			ops = 20
		case bsize >= 100:
			ops = 50
		}
		var lat perfLat
		start := time.Now()
		for i := 0; i < ops; i++ {
			op := time.Now()
			tx, err := db.BeginTx(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for r := 0; r < bsize; r++ {
				if err := tx.InsertItem(&benchContact{
					ID: murmur.NewRowID(), Name: fmt.Sprintf("tx-%d-%d", i, r),
					Phone: "555-0000", Score: int64(r),
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			lat.record(time.Since(op))
		}
		wall := time.Since(start)
		rep.add(perfCell{
			Name: "tx_batch", Rows: n, Batch: bsize, Ops: int64(ops), WallMs: wall.Milliseconds(),
			PerSec: float64(ops) / wall.Seconds(), RowsSec: float64(ops*bsize) / wall.Seconds(),
			P50Ms: lat.percentile(50), P95Ms: lat.percentile(95),
			PeakRSSMB: procPeakRSSMB(),
		})
		t.Logf("tx_batch rows=%d batch=%d tx/s=%.1f rows/s=%.0f", n, bsize,
			float64(ops)/wall.Seconds(), float64(ops*bsize)/wall.Seconds())
		_ = db.Close()
	}
}

// perfCipherCells measures the supported cipher at bulk-I/O scale: fresh-store
// populate throughput plus full-reopen time and on-disk footprint.
// Single-row updates are deliberately NOT the cipher probe (fsync cost
// hides cipher cost at that granularity; see BenchmarkCipherMatrix).
func perfCipherCells(t *testing.T, rep *perfReport, tier string) {
	t.Helper()
	ctx := context.Background()
	n := 100_000
	if tier == "smoke" {
		n = 10_000
	}
	for _, c := range perfCiphers(tier) {
		dir := t.TempDir()
		cfg := benchConfig(dir, murmur.NewNodeID(), murmur.NewDBID())
		cfg.Encryption.Algorithm = c.alg
		db, err := murmur.Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		populate(t, db, n)
		popWall := time.Since(start)
		diskPop := dirSizeBytes(dir)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		// Full reopen of the populated store (reads + rebuild).
		start = time.Now()
		db2, err := murmur.Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		openWall := time.Since(start)
		if _, err := db2.Count(ctx, benchContact{}); err != nil {
			t.Fatal(err)
		}
		_ = db2.Close()
		rep.add(perfCell{
			Name: "cipher_bulk", Rows: n, Cipher: c.name, Ops: int64(n), WallMs: popWall.Milliseconds(),
			PerSec: float64(n) / popWall.Seconds(), RowsSec: float64(n) / popWall.Seconds(),
			DiskBytes: diskPop, PeakRSSMB: procPeakRSSMB(),
			Extra: fmt.Sprintf("reopen_ms=%d", openWall.Milliseconds()),
		})
		t.Logf("cipher_bulk rows=%d cipher=%s populate=%.0frows/s disk=%dMB reopen=%s",
			n, c.name, float64(n)/popWall.Seconds(), diskPop/(1<<20), openWall.Round(time.Millisecond))
	}
}
