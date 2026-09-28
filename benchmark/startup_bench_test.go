package benchmark

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	replicateddb "github.com/nomadsql/replicateddb"
	"github.com/nomadsql/replicateddb/codec"
	"github.com/nomadsql/replicateddb/crypto"
	"github.com/nomadsql/replicateddb/state"
)

// BenchmarkStartupComponents breaks Open into externally measurable
// phases on template copies: registry open, store (Pebble) open, full
// open (store plus engine rebuild), and time to first query. Compare
// full-open against store-open across runs to size the rebuild; same-run
// subtraction is polluted by page-cache warmth. Single-shot per size
// with explicit metrics.
func BenchmarkStartupComponents(b *testing.B) {
	for _, n := range datasetSizes(b) {
		b.Run(sizeName(n), func(b *testing.B) {
			ctx := context.Background()
			tmpl := templateFor(b, n)
			var dbid [16]byte
			copy(dbid[:], tmpl.dbid[:])
			prov := benchProvider()

			// Registry + store opens on one copy.
			dir1 := b.TempDir()
			if err := copyDir(tmpl.dir, dir1); err != nil {
				b.Fatal(err)
			}
			start := time.Now()
			reg, err := crypto.OpenRegistry(filepath.Join(dir1, "keys"), prov, dbid)
			regOpen := time.Since(start)
			if err != nil {
				b.Fatal(err)
			}
			efs, err := crypto.NewEncryptedFS(crypto.FSOptions{Base: vfs.Default, Registry: reg, DBID: dbid})
			if err != nil {
				reg.Close()
				b.Fatal(err)
			}
			start = time.Now()
			st, err := state.Open(filepath.Join(dir1, "data"), tmpl.node, tmpl.dbid,
				state.Options{FS: efs, Limits: codec.DefaultLimits()})
			storeOpen := time.Since(start)
			if err != nil {
				reg.Close()
				b.Fatal(err)
			}
			_ = st.Close()
			reg.Close()

			// Full open on another copy.
			dir2 := b.TempDir()
			if err := copyDir(tmpl.dir, dir2); err != nil {
				b.Fatal(err)
			}
			start = time.Now()
			db, err := replicateddb.Open(ctx, benchConfig(dir2, tmpl.node, tmpl.dbid))
			fullOpen := time.Since(start)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			qstart := time.Now()
			rows, err := db.QueryContext(ctx, `SELECT COUNT(*) FROM contacts`)
			if err != nil {
				b.Fatal(err)
			}
			var count int
			for rows.Next() {
				_ = rows.Scan(&count)
			}
			rows.Close()
			if count != n {
				b.Fatalf("rebuilt %d rows, want %d", count, n)
			}
			firstQuery := time.Since(qstart)
			b.ReportMetric(float64(regOpen.Nanoseconds()), "registry-open-ns")
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
	nodeA := replicateddb.NewNodeID()
	dbA, err := replicateddb.Open(ctx, mesh.config(b, b.TempDir(), nodeA, benchPortA, nil))
	if err != nil {
		b.Fatal(err)
	}
	defer dbA.Close()
	b.ResetTimer()
	var total time.Duration
	for i := 0; i < b.N; i++ {
		nodeB := replicateddb.NewNodeID()
		cfgB := mesh.config(b, b.TempDir(), nodeB, "127.0.0.1:0",
			[]replicateddb.Peer{{NodeID: nodeA, Addrs: []string{benchPortA}}})
		start := time.Now()
		dbB, err := replicateddb.Open(ctx, cfgB)
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
