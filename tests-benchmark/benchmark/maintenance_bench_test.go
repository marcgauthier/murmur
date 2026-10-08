package benchmark

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur"
)

// BenchmarkCheckpoint times online checkpoint creation on a template
// copy and reports seconds plus checkpoint bytes.
func BenchmarkCheckpoint(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			db, _ := openTemplateDB(b, n)
			base := b.TempDir()
			b.ResetTimer()
			var elapsed time.Duration
			var staging string
			ctx := context.Background()
			for i := 0; i < b.N; i++ {
				staging = filepath.Join(base, fmt.Sprintf("ckpt-%d", i))
				start := time.Now()
				release, err := db.Checkpoint(ctx, staging)
				if err != nil {
					b.Fatal(err)
				}
				if release != nil {
					_ = release()
				}
				elapsed += time.Since(start)
			}
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N), "checkpoint-ns")
			b.ReportMetric(float64(dirBytes(b, staging))/1e6, "checkpoint-MB")
		})
	}
}

// BenchmarkMaintenanceRewrite times a full encrypted-file rewrite
// (rotation rewrite path) over a template copy.
func BenchmarkMaintenanceRewrite(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			tmpl := templateFor(b, n)
			dest := b.TempDir()
			if err := copyDir(tmpl.dir, dest); err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			db, err := murmur.Open(ctx, benchConfig(dest, tmpl.node, tmpl.dbid))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			b.ResetTimer()
			start := time.Now()
			err = db.RewriteEncryptedFiles(ctx)
			elapsed := time.Since(start)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(elapsed.Nanoseconds()), "rewrite-ns")
			b.ReportMetric(float64(dirBytes(b, dest))/1e6/elapsed.Seconds(), "MB/sec")
		})
	}
}
