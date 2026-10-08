package benchmark

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/marcgauthier/murmur"
)

// BenchmarkQueryMaterialization measures point and range lookups against
// the typed RIME materializer backing the replicated database.
func BenchmarkQueryMaterialization(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			tmpl := templateFor(b, n)
			dest := b.TempDir()
			if err := copyDir(tmpl.dir, dest); err != nil {
				b.Fatal(err)
			}
			cfg := benchConfig(dest, tmpl.node, tmpl.dbid)
			db, err := murmur.Open(context.Background(), cfg)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = db.Close() })
			contacts, err := murmur.TableOf[benchContact](db, "contacts")
			if err != nil {
				b.Fatal(err)
			}
			scoreField := murmur.NumericFieldOf[benchContact, int64](contacts, "Score")
			rng := rand.New(rand.NewSource(21))
			var lat latency
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				if i%2 == 0 {
					got, err := contacts.Get(tmpl.ids[rng.Intn(len(tmpl.ids))])
					if err != nil {
						b.Fatal(err)
					}
					_ = got
				} else {
					lo := int64((i * 131) % 900)
					rows, err := contacts.Where(scoreField.Between(lo, lo+100)).Limit(100).Find()
					if err != nil {
						b.Fatal(err)
					}
					for range rows {
					}
				}
				lat.record(time.Since(start))
			}
			lat.report(b, 1, "ops")
		})
	}
}
