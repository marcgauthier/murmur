package benchmark

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/marcgauthier/murmur"
	"github.com/marcgauthier/murmur/codec"
	"github.com/marcgauthier/murmur/internal/testidentity"
	"github.com/marcgauthier/murmur/spool"
	"github.com/marcgauthier/murmur/state"
)

// BenchmarkStartupComponents breaks Open into externally measurable
// phases on template copies: store (Spool) open, full open (store plus
// engine rebuild), and time to first query. Compare full-open against
// store-open across runs to size the rebuild. Single-shot per size
// with explicit metrics.
func BenchmarkStartupComponents(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			ctx := context.Background()
			tmpl := templateFor(b, n)

			// Store open on one copy.
			dir1 := b.TempDir()
			if err := copyDir(tmpl.dir, dir1); err != nil {
				b.Fatal(err)
			}
			dataPath := filepath.Join(dir1, "data")
			start := time.Now()
			st, err := state.Open(dataPath, tmpl.node, tmpl.dbid,
				state.Options{
					Spool: spool.Options{
						Path:          dataPath,
						MasterKey:     append([]byte(nil), benchKey...),
						WrappingKeyID: "bench",
						Encryption:    spool.EncryptionAES256GCM,
					},
					Limits:        codec.DefaultLimits(),
					OriginSigning: testidentity.Config(tmpl.node),
				})
			storeOpen := time.Since(start)
			if err != nil {
				b.Fatal(err)
			}
			_ = st.Close()

			// Full open on another copy.
			dir2 := b.TempDir()
			if err := copyDir(tmpl.dir, dir2); err != nil {
				b.Fatal(err)
			}
			start = time.Now()
			db, err := murmur.Open(ctx, benchConfig(dir2, tmpl.node, tmpl.dbid))
			fullOpen := time.Since(start)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			qstart := time.Now()
			count, err := db.Count(ctx, benchContact{})
			if err != nil {
				b.Fatal(err)
			}
			if count != n {
				b.Fatalf("rebuilt %d rows, want %d", count, n)
			}
			firstQuery := time.Since(qstart)
			b.ReportMetric(float64(storeOpen.Nanoseconds()), "store-open-ns")
			b.ReportMetric(float64(fullOpen.Nanoseconds()), "full-open-ns")
			b.ReportMetric(float64(firstQuery.Nanoseconds()), "first-query-ns")
			b.ReportMetric(float64(n)/fullOpen.Seconds(), "rows/sec")
		})
	}
}

// BenchmarkReplicationReady measures time from Open until a fresh node
// establishes its first replication session (open + dial + handshake).
func BenchmarkReplicationReady(b *testing.B) {
	ctx := context.Background()
	mesh := newBenchMesh(b)
	nodeA := murmur.NewNodeID()
	dbA, err := murmur.Open(ctx, mesh.config(b, b.TempDir(), nodeA, benchPortA, nil))
	if err != nil {
		b.Fatal(err)
	}
	defer dbA.Close()
	b.ResetTimer()
	var total time.Duration
	for i := 0; i < b.N; i++ {
		nodeB := murmur.NewNodeID()
		cfgB := mesh.config(b, b.TempDir(), nodeB, "127.0.0.1:0",
			[]murmur.Peer{{NodeID: nodeA, Addrs: []string{benchPortA}}})
		start := time.Now()
		dbB, err := murmur.Open(ctx, cfgB)
		if err != nil {
			b.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for dbB.Status().ConnectedPeers == 0 {
			if time.Now().After(deadline) {
				dbB.Close()
				b.Fatal("never connected")
			}
			time.Sleep(10 * time.Millisecond)
		}
		total += time.Since(start)
		dbB.Close()
	}
	b.ReportMetric(float64(total.Nanoseconds())/float64(b.N), "ready-ns")
}
