// Live Spool compression benchmark: for several realistic traffic shapes,
// compare the on-disk Spool directory size across compression modes (disabled,
// deflate). Each run opens a real database on real disk,
// writes a seeded deterministic dataset through managed RIME records, shuts
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

	db "github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/internal/testdb"
	"github.com/marcgauthier/murmur/tests-live/harness"
)

type compressionRecord struct {
	ID      ids.RowID `rime:"primary"`
	Code    string
	Qty     int64
	Price   float64
	Title   string
	Body    string
	Tags    string
	Kind    string
	Payload string
	Data    []byte
}

type compMode struct {
	name string
	algo db.CompressionAlgorithm
}

var compModes = []compMode{
	{"none", db.CompressionNone},
	{"deflate", db.CompressionDeflate},
}

type trafficProfile struct {
	name   string
	rows   int
	create func(rng *rand.Rand, i int) compressionRecord
	update func(rng *rand.Rand, record *compressionRecord)
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
			rows: 20000,
			create: func(rng *rand.Rand, i int) compressionRecord {
				return compressionRecord{
					Code:  fmt.Sprintf("SKU-%05d", rng.Intn(2000)),
					Qty:   int64(rng.Intn(100)),
					Price: rng.Float64() * 999.99,
				}
			},
			update: func(rng *rand.Rand, row *compressionRecord) {
				row.Qty = int64(rng.Intn(100))
			},
		},
		{
			name: "text-heavy",
			rows: 8000,
			create: func(rng *rand.Rand, i int) compressionRecord {
				return compressionRecord{
					Title: lorem(rng, 4+rng.Intn(6)),
					Body:  lorem(rng, 100+rng.Intn(200)),
					Tags:  "news,tech,local",
				}
			},
			update: func(rng *rand.Rand, row *compressionRecord) {
				row.Tags = "news,tech,local,updated"
			},
		},
		{
			name: "json-docs",
			rows: 8000,
			create: func(rng *rand.Rand, i int) compressionRecord {
				payload := fmt.Sprintf(`{"event_id":%d,"user":{"id":%d,"name":"user-%d","roles":["reader","editor"]},`+
					`"context":{"app":"storefront","region":"eu-west","build":%d},"metrics":{"latency_ms":%.3f,`+
					`"retries":%d,"flags":[true,false,true]},"trace":["recv","auth","load","render","send"]}`,
					i, rng.Intn(50000), rng.Intn(50000), 1000+rng.Intn(50),
					rng.Float64()*250, rng.Intn(4))
				return compressionRecord{Kind: "pageview", Payload: payload}
			},
			update: func(rng *rand.Rand, row *compressionRecord) {
				row.Kind = "pageview-v2"
			},
		},
		{
			name: "blobs-random",
			rows: 2000,
			create: func(rng *rand.Rand, i int) compressionRecord {
				buf := make([]byte, 4096)
				if _, err := rng.Read(buf); err != nil {
					panic(err)
				}
				return compressionRecord{Data: buf}
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
	raw := harness.GetEnv("MURMUR_LIVE_COMPRESSION_SCALE")
	if raw == "" {
		return 1
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		t.Fatalf("MURMUR_LIVE_COMPRESSION_SCALE must be a positive integer")
	}
	return n
}

func runCell(t *testing.T, ctx context.Context, mode compMode, prof trafficProfile, scale int, seed int64) cellResult {
	t.Helper()
	dir := t.TempDir()
	definition, err := db.Model[compressionRecord](db.ModelOptions{
		Name: prof.name, TableID: 401,
		RecordOptions: db.RecordOptions{
			FieldIDs: map[string]uint32{
				"ID": 1, "Code": 2, "Qty": 3, "Price": 4, "Title": 5,
				"Body": 6, "Tags": 7, "Kind": 8, "Payload": 9, "Data": 10,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := testdb.Configure(db.Config{
		Path:   filepath.Join(dir, "node"),
		NodeID: ids.NewNodeID(),
		Schema: db.SchemaConfig{Version: 1},
		Tables: []db.TableDefinition{definition},
		Spool:  db.DefaultSpoolConfig(),
		Encryption: db.EncryptionConfig{
			Key:   bytes.Repeat([]byte{0x3a}, 32),
			KeyID: "compression-bench",
		},
	})
	cfg.Spool.TargetBlockBytes = 64 << 10
	cfg.Spool.Compression = mode.algo

	database, err := db.Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	rows := prof.rows * scale
	rng := rand.New(rand.NewSource(seed))
	rowIDs := make([]ids.RowID, 0, rows)
	hash := sha256.New()

	start := time.Now()
	const batch = 500
	for base := 0; base < rows; base += batch {
		end := base + batch
		if end > rows {
			end = rows
		}
		values := make([]*compressionRecord, 0, end-base)
		for i := base; i < end; i++ {
			// IDs come from the seeded stream too, so every mode gets
			// byte-identical keys and datasets.
			var id ids.RowID
			if _, err := rng.Read(id[:]); err != nil {
				t.Fatal(err)
			}
			rowIDs = append(rowIDs, id)
			value := prof.create(rng, i)
			value.ID = id
			if value.Data == nil {
				value.Data = []byte{}
			}
			values = append(values, &value)
			fmt.Fprintf(hash, "%x", id[:])
		}
		if err := database.WriteTxContext(ctx, func(tx *db.Tx) error {
			return tx.InsertMany(values)
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Churn pass: update every 10th row like live traffic would.
	if prof.update != nil {
		for base := 0; base < rows; base += batch * 10 {
			end := base + batch*10
			if end > rows {
				end = rows
			}
			if err := database.WriteTxContext(ctx, func(tx *db.Tx) error {
				for i := base; i < end; i += 10 {
					var record compressionRecord
					record.ID = rowIDs[i]
					if err := tx.GetItem(&record); err != nil {
						return err
					}
					prof.update(rng, &record)
					if err := tx.UpdateItem(&record); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	writeDur := time.Since(start)

	count, err := database.Count(ctx, compressionRecord{})
	if err != nil {
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
		res.sstBytes += info.Size()
		res.sstFiles++
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

func TestSpoolCompressionSizes(t *testing.T) {
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
	// identical checksums; compressible traffic must shrink under deflate;
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
	if got := results["text-heavy"]["deflate"].sstBytes; got >= textNone {
		t.Fatalf("text-heavy deflate (%d) not smaller than none (%d): compression disengaged?", got, textNone)
	}
	blobNone := results["blobs-random"]["none"].sstBytes
	for _, mode := range []string{"deflate"} {
		if got := results["blobs-random"][mode].sstBytes; got > blobNone*11/10 {
			t.Fatalf("blobs-random %s grew >10%% over none (%d vs %d)", mode, got, blobNone)
		}
	}
}
