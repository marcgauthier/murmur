// Live Pebble compression benchmark: for several realistic traffic shapes,
// compare the on-disk Pebble folder size across compression modes (disabled,
// zstd-3, zstd-9, zstd-12). Each run opens a real database on real disk,
// writes a seeded deterministic dataset through the full SQL stack, shuts
// down cleanly, and measures the data directory.
//
// In-process by design: a disk-size comparison needs real engine bytes, not
// replication processes, and one database per (mode, traffic) cell keeps the
// matrix fast. Run with: bash tests-live/run.sh compression
package compression_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	db "github.com/marcgauthier/spedsql"
	"github.com/marcgauthier/spedsql/ids"
	"github.com/marcgauthier/spedsql/schema"
)

type compMode struct {
	name string
	cfg  db.CompressionConfig
}

var compModes = []compMode{
	{"none", db.CompressionConfig{Algorithm: db.CompressionNone}},
	{"zstd-3", db.CompressionConfig{Algorithm: db.CompressionZstd, ZstdLevel: 3}},
	{"zstd-9", db.CompressionConfig{Algorithm: db.CompressionZstd, ZstdLevel: 9}},
	{"zstd-12", db.CompressionConfig{Algorithm: db.CompressionZstd, ZstdLevel: 12}},
}

type trafficProfile struct {
	name  string
	table schema.TableSchema
	rows  int
	// values returns one row's column values (excluding the id blob).
	values func(rng *rand.Rand, i int) []any
	// update returns a SET clause rhs for the churn pass (nil skips updates).
	update func(rng *rand.Rand, i int) (string, []any)
}

var loremWords = strings.Fields(`lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod
tempor incididunt ut labore et dolore magna aliqua enim ad minim veniam quis nostrud exercitation
ullamco laboris nisi aliquip ex ea commodo consequat duis aute irure in reprehenderit voluptate
velit esse cillum fugiat nulla pariatur excepteur sint occaecat cupidatat non proident sunt culpa
qui officia deserunt mollit anim id est laborum perspiciatis unde omnis natus error voluptatem`)

func lorem(rng *rand.Rand, words int) string {
	var sb strings.Builder
	for i := 0; i < words; i++ {
		if i > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(loremWords[rng.Intn(len(loremWords))])
	}
	return sb.String()
}

func trafficProfiles() []trafficProfile {
	return []trafficProfile{
		{
			name: "oltp-small",
			table: schema.TableSchema{Name: "orders", Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "code", Type: schema.ColText},
				{Name: "qty", Type: schema.ColInteger},
				{Name: "price", Type: schema.ColReal},
			}},
			rows: 20000,
			values: func(rng *rand.Rand, i int) []any {
				return []any{
					fmt.Sprintf("SKU-%05d", rng.Intn(2000)),
					int64(rng.Intn(100)),
					rng.Float64() * 999.99,
				}
			},
			update: func(rng *rand.Rand, i int) (string, []any) {
				return "qty = ?", []any{int64(rng.Intn(100))}
			},
		},
		{
			name: "text-heavy",
			table: schema.TableSchema{Name: "articles", Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "title", Type: schema.ColText},
				{Name: "body", Type: schema.ColText},
				{Name: "tags", Type: schema.ColText},
			}},
			rows: 8000,
			values: func(rng *rand.Rand, i int) []any {
				return []any{
					lorem(rng, 4+rng.Intn(6)),
					lorem(rng, 60+rng.Intn(120)),
					"news,tech,local",
				}
			},
			update: func(rng *rand.Rand, i int) (string, []any) {
				return "tags = ?", []any{"news,tech,local,updated"}
			},
		},
		{
			name: "json-docs",
			table: schema.TableSchema{Name: "events", Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "kind", Type: schema.ColText},
				{Name: "payload", Type: schema.ColText},
			}},
			rows: 8000,
			values: func(rng *rand.Rand, i int) []any {
				payload := fmt.Sprintf(`{"event_id":%d,"user":{"id":%d,"name":"user-%d","roles":["reader","editor"]},`+
					`"context":{"app":"storefront","region":"eu-west","build":%d},"metrics":{"latency_ms":%.3f,`+
					`"retries":%d,"flags":[true,false,true]},"trace":["recv","auth","load","render","send"]}`,
					i, rng.Intn(50000), rng.Intn(50000), 1000+rng.Intn(50),
					rng.Float64()*250, rng.Intn(4))
				return []any{"pageview", payload}
			},
			update: func(rng *rand.Rand, i int) (string, []any) {
				return "kind = ?", []any{"pageview-v2"}
			},
		},
		{
			name: "blobs-random",
			table: schema.TableSchema{Name: "chunks", Columns: []schema.ColumnSchema{
				{Name: "id", Type: schema.ColBlob},
				{Name: "data", Type: schema.ColBlob},
			}},
			rows: 2000,
			values: func(rng *rand.Rand, i int) []any {
				buf := make([]byte, 4096)
				if _, err := rng.Read(buf); err != nil {
					panic(err)
				}
				return []any{buf}
			},
			update: nil, // immutable content-addressed chunks
		},
	}
}

type cellResult struct {
	rows     int
	total    int64
	sstBytes int64
	sstFiles int
	walBytes int64
	writeDur time.Duration
	checksum string
}

func envScale(t *testing.T) int {
	t.Helper()
	raw := os.Getenv("SPEDSQL_LIVE_COMPRESSION_SCALE")
	if raw == "" {
		return 1
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		t.Fatalf("SPEDSQL_LIVE_COMPRESSION_SCALE must be a positive integer")
	}
	return n
}

func runCell(t *testing.T, ctx context.Context, mode compMode, prof trafficProfile, scale int, seed int64) cellResult {
	t.Helper()
	dir := t.TempDir()
	cfg := db.Config{
		Path:   filepath.Join(dir, "node"),
		NodeID: ids.NewNodeID(),
		Schema: db.SchemaConfig{Version: 1, Tables: []schema.TableSchema{prof.table}},
		Pebble: db.DefaultPebbleConfig(),
		Encryption: db.EncryptionConfig{
			Key:   bytes.Repeat([]byte{0x3a}, 32),
			KeyID: "compression-bench",
		},
	}
	// Small memtables force real flushes/compactions so the comparison
	// measures SST bytes, not memtable residency.
	cfg.Pebble.MemTableBytes = 256 << 10
	cfg.Pebble.CacheBytes = 8 << 20
	cfg.Pebble.Compression = mode.cfg

	database, err := db.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	table := prof.table.Name
	cols := make([]string, 0, len(prof.table.Columns))
	placeholders := make([]string, 0, len(prof.table.Columns))
	for _, c := range prof.table.Columns {
		cols = append(cols, c.Name)
		placeholders = append(placeholders, "?")
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		table, strings.Join(cols, ", "), strings.Join(placeholders, ", "))

	rows := prof.rows * scale
	rng := rand.New(rand.NewSource(seed))
	rowIDs := make([]ids.RowID, 0, rows)
	hash := sha256.New()

	start := time.Now()
	const batch = 500
	for base := 0; base < rows; base += batch {
		tx, err := database.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		end := base + batch
		if end > rows {
			end = rows
		}
		for i := base; i < end; i++ {
			// IDs come from the seeded stream too, so every mode gets
			// byte-identical keys and datasets.
			var id ids.RowID
			if _, err := rng.Read(id[:]); err != nil {
				t.Fatal(err)
			}
			rowIDs = append(rowIDs, id)
			args := append([]any{id[:]}, prof.values(rng, i)...)
			if _, err := tx.ExecContext(ctx, insertSQL, args...); err != nil {
				t.Fatal(err)
			}
			fmt.Fprintf(hash, "%x", id[:])
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	// Churn pass: update every 10th row like live traffic would.
	if prof.update != nil {
		for base := 0; base < rows; base += batch * 10 {
			tx, err := database.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := base; i < rows && i < base+batch*10; i += 10 {
				setSQL, args := prof.update(rng, i)
				args = append(args, rowIDs[i][:])
				if _, err := tx.ExecContext(ctx,
					fmt.Sprintf("UPDATE %s SET %s WHERE id = ?", table, setSQL), args...); err != nil {
					t.Fatal(err)
				}
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}
	}
	writeDur := time.Since(start)

	var count int
	if err := database.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != rows {
		t.Fatalf("mode %s profile %s: count = %d, want %d", mode.name, prof.name, count, rows)
	}
	checksum := hex.EncodeToString(hash.Sum(nil))
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	var res cellResult
	res.rows = count
	res.writeDur = writeDur
	res.checksum = checksum
	dataDir := filepath.Join(cfg.Path, "data")
	err = filepath.WalkDir(dataDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		res.total += info.Size()
		switch filepath.Ext(path) {
		case ".sst":
			res.sstBytes += info.Size()
			res.sstFiles++
		case ".log":
			res.walBytes += info.Size()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func formatBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for n/div >= unit && exp < 3 {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGT"[exp])
}

func TestPebbleCompressionSizes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping compression benchmark suite in short mode (-short or -race); run without -short for full matrix")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	scale := envScale(t)
	const seed = 42

	results := make(map[string]map[string]cellResult) // profile -> mode -> result
	for _, prof := range trafficProfiles() {
		results[prof.name] = make(map[string]cellResult)
		for _, mode := range compModes {
			res := runCell(t, ctx, mode, prof, scale, seed)
			results[prof.name][mode.name] = res
			t.Logf("%-12s %-8s rows=%d total=%s sst=%s (%d files) wal=%s write=%s",
				prof.name, mode.name, res.rows, formatBytes(res.total),
				formatBytes(res.sstBytes), res.sstFiles, formatBytes(res.walBytes),
				res.writeDur.Round(100*time.Millisecond))
		}
	}

	t.Log("mode \\ profile | " + strings.Join([]string{"oltp-small", "text-heavy", "json-docs", "blobs-random"}, " | "))
	for _, mode := range compModes {
		cells := make([]string, 0, 4)
		for _, prof := range trafficProfiles() {
			res := results[prof.name][mode.name]
			base := results[prof.name]["none"].sstBytes
			ratio := 0.0
			if base > 0 {
				ratio = float64(res.sstBytes) / float64(base)
			}
			cells = append(cells, fmt.Sprintf("%s (%.0f%%)", formatBytes(res.sstBytes), ratio*100))
		}
		t.Logf("%-8s | %s", mode.name, strings.Join(cells, " | "))
	}

	// Sanity gates: identical datasets must hold identical rows with
	// identical checksums; compressible traffic must shrink under zstd;
	// incompressible traffic must not explode; and SSTs must exist.
	for _, prof := range trafficProfiles() {
		var wantSum string
		for _, mode := range compModes {
			res := results[prof.name][mode.name]
			if wantSum == "" {
				wantSum = res.checksum
			} else if res.checksum != wantSum {
				t.Fatalf("profile %s: mode %s dataset diverged", prof.name, mode.name)
			}
			if res.rows != prof.rows*scale {
				t.Fatalf("profile %s mode %s: rows = %d", prof.name, mode.name, res.rows)
			}
			if res.sstFiles == 0 || res.sstBytes == 0 {
				t.Fatalf("profile %s mode %s: no SST bytes measured", prof.name, mode.name)
			}
		}
	}
	textNone := results["text-heavy"]["none"].sstBytes
	for _, mode := range []string{"zstd-3", "zstd-9", "zstd-12"} {
		if got := results["text-heavy"][mode].sstBytes; got >= textNone {
			t.Fatalf("text-heavy %s (%d) not smaller than none (%d): compression disengaged?", mode, got, textNone)
		}
	}
	blobNone := results["blobs-random"]["none"].sstBytes
	for _, mode := range []string{"zstd-3", "zstd-9", "zstd-12"} {
		if got := results["blobs-random"][mode].sstBytes; got > blobNone*11/10 {
			t.Fatalf("blobs-random %s grew >10%% over none (%d vs %d)", mode, got, blobNone)
		}
	}
}
