package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/ids"
	"github.com/marcgauthier/murmur/q"
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
				var got benchContact
				got.ID = ids[rng.Intn(len(ids))]
				if err := db.GetItem(ctx, &got); err != nil {
					b.Fatal(err)
				}
				if got.Name == "" && got.Phone == "" && got.Score == 0 {
					b.Fatal("empty row")
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
				var rows []benchContact
				if err := db.Find(ctx, &rows, q.Eq("Name", fmt.Sprintf("ann smith %d", (i*7919)%n))); err != nil {
					b.Fatal(err)
				}
				for range rows {
				}
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
			var lat latency
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lo := int64((i * 131) % 900)
				start := time.Now()
				var rows []benchContact
				if err := db.Query(benchContact{}, q.Between("Score", lo, lo+100)).Limit(100).FindInto(&rows); err != nil {
					b.Fatal(err)
				}
				for range rows {
				}
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
			var lat latency
			trackPeakAlloc(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				var rows []benchContact
				if err := db.Query(benchContact{}).OrderByDescending("Score").Limit(20).FindInto(&rows); err != nil {
					b.Fatal(err)
				}
				if len(rows) != 20 {
					b.Fatalf("got %d rows", len(rows))
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
				rtx, err := db.ReadTxContext(ctx)
				if err != nil {
					b.Fatal(err)
				}
				pairs, err := rtx.InnerJoin(benchContact{}, "ID", benchOrder{}, "ContactID")
				if err != nil {
					_ = rtx.Close()
					b.Fatal(err)
				}
				// Filter + group + limit client-side (score > ? GROUP BY id LIMIT 100).
				threshold := int64((i * 17) % 500)
				sums := make(map[ids.RowID]int64, 128)
				for _, pair := range pairs {
					left := pair.Left.(*benchContact)
					right := pair.Right.(*benchOrder)
					if left.Score > threshold {
						sums[left.ID] += right.Amount
					}
					if len(sums) >= 100 {
						break
					}
				}
				if err := rtx.Close(); err != nil {
					b.Fatal(err)
				}
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
				var rows []benchContact
				if err := db.Find(ctx, &rows); err != nil {
					b.Fatal(err)
				}
				// Bucket by score/100, mirroring GROUP BY bucket.
				var buckets [10]int
				for _, row := range rows {
					buckets[row.Score/100]++
				}
				_ = buckets
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
				phone := fmt.Sprintf("555-%04d", i%10000)
				tx, err := db.BeginTx(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if err := tx.Update(&benchContact{ID: ids[rng.Intn(len(ids))]}, murmur.Set("Phone", phone)); err != nil {
					b.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
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
				name := fmt.Sprintf("upd %d", i)
				phone := fmt.Sprintf("555-%04d", i%10000)
				score := int64(i % 1000)
				tx, err := db.BeginTx(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if err := tx.Update(&benchContact{ID: ids[rng.Intn(len(ids))]},
					murmur.Set("Name", name), murmur.Set("Phone", phone), murmur.Set("Score", score)); err != nil {
					b.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
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
		tx, err := db.BeginTx(ctx)
		if err != nil {
			b.Fatal(err)
		}
		for r := 0; r < rowsPerTx; r++ {
			id := ids[(i*rowsPerTx+r)%len(ids)]
			score := int64((i + r) % 1000)
			if err := tx.Update(&benchContact{ID: id}, murmur.Set("Score", score)); err != nil {
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
					score := int64(i % 1000)
					tx, err := db.BeginTx(ctx)
					if err != nil {
						b.Fatal(err)
					}
					if err := tx.Update(&benchContact{ID: ids[rng.Intn(len(ids))]}, murmur.Set("Score", score)); err != nil {
						b.Fatal(err)
					}
					if err := tx.Commit(); err != nil {
						b.Fatal(err)
					}
					lat.record(time.Since(start))
					continue
				}
				var got benchContact
				got.ID = ids[rng.Intn(len(ids))]
				if err := db.GetItem(ctx, &got); err != nil {
					b.Fatal(err)
				}
				_ = got
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}
