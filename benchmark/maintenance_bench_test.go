package benchmark

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/nomadsql/replicateddb/crypto"
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
			for i := 0; i < b.N; i++ {
				staging = filepath.Join(base, fmt.Sprintf("ckpt-%d", i))
				start := time.Now()
				if err := db.Checkpoint(staging); err != nil {
					b.Fatal(err)
				}
				elapsed += time.Since(start)
			}
			b.ReportMetric(float64(elapsed.Nanoseconds())/float64(b.N), "checkpoint-ns")
			b.ReportMetric(float64(dirBytes(b, staging))/1e6, "checkpoint-MB")
		})
	}
}

// BenchmarkMaintenanceRewrite times a full encrypted-file rewrite
// (rotation rewrite path) over a template copy with Pebble closed.
func BenchmarkMaintenanceRewrite(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			tmpl := templateFor(b, n)
			dest := b.TempDir()
			if err := copyDir(tmpl.dir, dest); err != nil {
				b.Fatal(err)
			}
			var dbid [16]byte
			copy(dbid[:], tmpl.dbid[:])
			prov := benchProvider()
			reg, err := crypto.OpenRegistry(filepath.Join(dest, "keys"), prov, dbid)
			if err != nil {
				b.Fatal(err)
			}
			defer reg.Close()
			efs, err := crypto.NewEncryptedFS(crypto.FSOptions{Base: vfs.Default, Registry: reg, DBID: dbid})
			if err != nil {
				b.Fatal(err)
			}
			mgr, err := crypto.NewManager(crypto.ManagerOptions{
				FS: efs, Roots: []string{filepath.Join(dest, "data")},
				MaxKeyLifetime: time.Hour,
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			start := time.Now()
			rewrote, skipped, err := mgr.RewriteEncryptedFiles(context.Background())
			elapsed := time.Since(start)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(rewrote), "files")
			b.ReportMetric(float64(skipped), "skipped")
			b.ReportMetric(float64(elapsed.Nanoseconds()), "rewrite-ns")
			b.ReportMetric(float64(dirBytes(b, dest))/1e6/elapsed.Seconds(), "MB/sec")
		})
	}
}
