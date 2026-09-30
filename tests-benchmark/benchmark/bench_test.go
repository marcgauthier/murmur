package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// --- point and indexed reads ---

func BenchmarkPKLookup(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db, ids := openTemplateDB(b, n)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(7))
			var lat latency
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				rows, err := db.QueryContext(ctx,
					`SELECT name, phone, score FROM contacts WHERE id = ?`, ids[rng.Intn(len(ids))][:])
				if err != nil {
					b.Fatal(err)
				}
				if got := drainRows(b, rows); got != 1 {
					b.Fatalf("got %d rows", got)
				}
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}

func BenchmarkIndexedEquality(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db, _ := openTemplateDB(b, n)
			ctx := context.Background()
			var lat latency
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				rows, err := db.QueryContext(ctx,
					`SELECT id FROM contacts WHERE name = ?`, fmt.Sprintf("ann smith %d", (i*7919)%n))
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}

func BenchmarkIndexedRange(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db, _ := openTemplateDB(b, n)
			ctx := context.Background()
			var lat latency
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lo := (i * 131) % 900
				start := time.Now()
				rows, err := db.QueryContext(ctx,
					`SELECT id FROM contacts WHERE score BETWEEN ? AND ? LIMIT 100`, lo, lo+100)
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}

func BenchmarkOrderLimit(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db, _ := openTemplateDB(b, n)
			ctx := context.Background()
			var lat latency
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				rows, err := db.QueryContext(ctx,
					`SELECT name, score FROM contacts ORDER BY score DESC LIMIT 20`)
				if err != nil {
					b.Fatal(err)
				}
				if got := drainRows(b, rows); got != 20 {
					b.Fatalf("got %d rows", got)
				}
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}

func BenchmarkJoin(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db, _ := openTemplateDB(b, n)
			ctx := context.Background()
			var lat latency
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				rows, err := db.QueryContext(ctx,
					`SELECT c.name, SUM(o.amount) FROM contacts c
					 JOIN orders o ON o.contact_id = c.id
					 WHERE c.score > ? GROUP BY c.id LIMIT 100`, (i*17)%500)
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}

func BenchmarkGroupBy(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db, _ := openTemplateDB(b, n)
			ctx := context.Background()
			var lat latency
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				rows, err := db.QueryContext(ctx,
					`SELECT score / 100 AS bucket, COUNT(*) FROM contacts GROUP BY bucket`)
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}

// --- FTS ---

func BenchmarkFTSTerm(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db, _ := openTemplateDB(b, n)
			ctx := context.Background()
			var lat latency
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				rows, err := db.QueryContext(ctx,
					`SELECT rowid FROM contacts_fts WHERE contacts_fts MATCH 'smith' LIMIT 100`)
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}

func BenchmarkFTSPrefix(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db, _ := openTemplateDB(b, n)
			ctx := context.Background()
			var lat latency
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				rows, err := db.QueryContext(ctx,
					`SELECT rowid FROM contacts_fts WHERE contacts_fts MATCH 'smi*' LIMIT 100`)
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}

// --- writes ---

func BenchmarkSingleCellUpdate(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db, ids := openTemplateDB(b, n)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(9))
			var lat latency
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				if _, err := db.ExecContext(ctx,
					`UPDATE contacts SET phone = ? WHERE id = ?`,
					fmt.Sprintf("555-%04d", i%10000), ids[rng.Intn(len(ids))][:]); err != nil {
					b.Fatal(err)
				}
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}

func BenchmarkMultiColumnUpdate(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db, ids := openTemplateDB(b, n)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(11))
			var lat latency
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				if _, err := db.ExecContext(ctx,
					`UPDATE contacts SET name = ?, phone = ?, score = ? WHERE id = ?`,
					fmt.Sprintf("upd %d", i), fmt.Sprintf("555-%04d", i%10000), i%1000,
					ids[rng.Intn(len(ids))][:]); err != nil {
					b.Fatal(err)
				}
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}

func benchmarkTxnRows(b *testing.B, n, rowsPerTx int) {
	db, ids := openTemplateDB(b, n)
	ctx := context.Background()
	var lat latency
	trackPeakAlloc(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}
		for r := 0; r < rowsPerTx; r++ {
			id := ids[(i*rowsPerTx+r)%len(ids)]
			if _, err := tx.ExecContext(ctx,
				`UPDATE contacts SET score = ? WHERE id = ?`, (i+r)%1000, id[:]); err != nil {
				b.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
		lat.record(time.Since(start))
	}
	b.ReportMetric(float64(rowsPerTx), "rows/txn")
	lat.report(b, float64(rowsPerTx), "rows")
}

func BenchmarkTxn10Rows(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) { benchmarkTxnRows(b, n, 10) })
	}
}

func BenchmarkTxn1000Rows(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) { benchmarkTxnRows(b, n, 1000) })
	}
}

func BenchmarkTxn10000Rows(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) { benchmarkTxnRows(b, n, 10000) })
	}
}

func BenchmarkMixedReadWrite(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db, ids := openTemplateDB(b, n)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(13))
			var lat latency
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				if i%10 == 9 {
					if _, err := db.ExecContext(ctx,
						`UPDATE contacts SET score = ? WHERE id = ?`,
						i%1000, ids[rng.Intn(len(ids))][:]); err != nil {
						b.Fatal(err)
					}
					lat.record(time.Since(start))
					continue
				}
				rows, err := db.QueryContext(ctx,
					`SELECT name, score FROM contacts WHERE id = ?`, ids[rng.Intn(len(ids))][:])
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}
