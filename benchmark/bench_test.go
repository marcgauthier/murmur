package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
)

// --- point and indexed reads ---

func BenchmarkPKLookup(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db := openBenchDB(b, b.TempDir())
			ids := populate(b, db, n)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(7))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.QueryContext(ctx,
					`SELECT name, phone, score FROM contacts WHERE id = ?`, ids[rng.Intn(len(ids))][:])
				if err != nil {
					b.Fatal(err)
				}
				if got := drainRows(b, rows); got != 1 {
					b.Fatalf("got %d rows", got)
				}
			}
		})
	}
}

func BenchmarkIndexedEquality(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db := openBenchDB(b, b.TempDir())
			populate(b, db, n)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.QueryContext(ctx,
					`SELECT id FROM contacts WHERE name = ?`, fmt.Sprintf("ann smith %d", (i*7919)%n))
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
			}
		})
	}
}

func BenchmarkIndexedRange(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db := openBenchDB(b, b.TempDir())
			populate(b, db, n)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lo := (i * 131) % 900
				rows, err := db.QueryContext(ctx,
					`SELECT id FROM contacts WHERE score BETWEEN ? AND ? LIMIT 100`, lo, lo+100)
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
			}
		})
	}
}

func BenchmarkOrderLimit(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db := openBenchDB(b, b.TempDir())
			populate(b, db, n)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.QueryContext(ctx,
					`SELECT name, score FROM contacts ORDER BY score DESC LIMIT 20`)
				if err != nil {
					b.Fatal(err)
				}
				if got := drainRows(b, rows); got != 20 {
					b.Fatalf("got %d rows", got)
				}
			}
		})
	}
}

func BenchmarkJoin(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db := openBenchDB(b, b.TempDir())
			populate(b, db, n)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.QueryContext(ctx,
					`SELECT c.name, SUM(o.amount) FROM contacts c
					 JOIN orders o ON o.contact_id = c.id
					 WHERE c.score > ? GROUP BY c.id LIMIT 100`, (i*17)%500)
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
			}
		})
	}
}

func BenchmarkGroupBy(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db := openBenchDB(b, b.TempDir())
			populate(b, db, n)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.QueryContext(ctx,
					`SELECT score / 100 AS bucket, COUNT(*) FROM contacts GROUP BY bucket`)
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
			}
		})
	}
}

// --- FTS ---

func BenchmarkFTSTerm(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db := openBenchDB(b, b.TempDir())
			populate(b, db, n)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.QueryContext(ctx,
					`SELECT rowid FROM contacts_fts WHERE contacts_fts MATCH 'smith' LIMIT 100`)
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
			}
		})
	}
}

func BenchmarkFTSPrefix(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db := openBenchDB(b, b.TempDir())
			populate(b, db, n)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.QueryContext(ctx,
					`SELECT rowid FROM contacts_fts WHERE contacts_fts MATCH 'smi*' LIMIT 100`)
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
			}
		})
	}
}

// --- writes ---

func BenchmarkSingleCellUpdate(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db := openBenchDB(b, b.TempDir())
			ids := populate(b, db, n)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(9))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.ExecContext(ctx,
					`UPDATE contacts SET phone = ? WHERE id = ?`,
					fmt.Sprintf("555-%04d", i%10000), ids[rng.Intn(len(ids))][:]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMultiColumnUpdate(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db := openBenchDB(b, b.TempDir())
			ids := populate(b, db, n)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(11))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.ExecContext(ctx,
					`UPDATE contacts SET name = ?, phone = ?, score = ? WHERE id = ?`,
					fmt.Sprintf("upd %d", i), fmt.Sprintf("555-%04d", i%10000), i%1000,
					ids[rng.Intn(len(ids))][:]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkTxnRows(b *testing.B, n, rowsPerTx int) {
	db := openBenchDB(b, b.TempDir())
	ids := populate(b, db, n)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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
	}
	b.ReportMetric(float64(rowsPerTx), "rows/txn")
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

func BenchmarkMixedReadWrite(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db := openBenchDB(b, b.TempDir())
			ids := populate(b, db, n)
			ctx := context.Background()
			rng := rand.New(rand.NewSource(13))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if i%10 == 9 {
					if _, err := db.ExecContext(ctx,
						`UPDATE contacts SET score = ? WHERE id = ?`,
						i%1000, ids[rng.Intn(len(ids))][:]); err != nil {
						b.Fatal(err)
					}
					continue
				}
				rows, err := db.QueryContext(ctx,
					`SELECT name, score FROM contacts WHERE id = ?`, ids[rng.Intn(len(ids))][:])
				if err != nil {
					b.Fatal(err)
				}
				drainRows(b, rows)
			}
		})
	}
}
