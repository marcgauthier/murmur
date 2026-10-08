package rime_test

import (
	"fmt"
	"testing"

	"github.com/marcgauthier/murmur/rime"
)

// BenchmarkQualification isolates engine reads from formatting and seeding.
// Run the million-row case explicitly on a machine with adequate memory.
func BenchmarkQualification(b *testing.B) {
	type record struct {
		ID     int `rime:"primary"`
		Bucket int `rime:"index"`
	}
	for _, size := range []int{1000, 100000, 1000000} {
		b.Run(fmt.Sprintf("rows=%d", size), func(b *testing.B) {
			db := rime.New()
			defer db.Close()
			tab, err := rime.Register[record](db)
			if err != nil {
				b.Fatal(err)
			}
			keys := make([]any, size)
			for start := 0; start < size; start += 512 {
				records := make([]*record, 0, min(512, size-start))
				for i := start; i < min(start+512, size); i++ {
					keys[i] = i
					records = append(records, &record{ID: i, Bucket: i % 1000})
				}
				if err := tab.UpsertMany(records); err != nil {
					b.Fatal(err)
				}
			}
			b.Run("get", func(b *testing.B) {
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					if _, err := tab.Get(keys[i%size]); err != nil {
						b.Fatal(err)
					}
					i++
				}
			})
			bucket := rime.F[record, int](tab, "Bucket")
			var expectedSum int64
			for i := 0; i < size; i++ {
				expectedSum += int64(i) + int64(i%1000)
			}
			b.Run("full-scan", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					rows, err := tab.Where().Find()
					if err != nil {
						b.Fatal(err)
					}
					var sum int64
					for _, row := range rows {
						sum += int64(row.ID) + int64(row.Bucket)
					}
					if len(rows) != size || sum != expectedSum {
						b.Fatalf("full scan rows=%d checksum=%d want rows=%d checksum=%d", len(rows), sum, size, expectedSum)
					}
				}
				b.ReportMetric(float64(size), "rows/op")
				b.ReportMetric(float64(size)*float64(b.N)/b.Elapsed().Seconds(), "rows/s")
			})
			b.Run("indexed", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					rows, err := tab.Where(bucket.Eq(17)).Limit(16).Find()
					if err != nil || len(rows) != min(size/1000, 16) {
						b.Fatalf("indexed result count %d: %v", len(rows), err)
					}
				}
			})
		})
	}
}
